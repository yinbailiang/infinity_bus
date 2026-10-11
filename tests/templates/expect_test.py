import asyncio
from typing import Any, AsyncGenerator, Optional
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from infinity_bus import (
    Event,
    EventBus,
    EventDeclaration,
    EventHandlerRegistry,
    EventRegistry,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
    Regex,
)
from infinity_bus.templates import OneShotEventHandler, expect


# ---------------------------------------------------------------------------
# 测试用事件与负载
# ---------------------------------------------------------------------------
class SimpleTestPayload(BaseModel):
    value: int
    msg: str


class SimpleTestEventDecl(EventDeclaration):
    name = 'test.event'
    payload_type = SimpleTestPayload


class OtherEventDecl(EventDeclaration):
    name = 'test.other'
    payload_type = SimpleTestPayload


class NoPayloadEventDecl(EventDeclaration):
    name = 'test.no_payload'
    payload_type = None


# ---------------------------------------------------------------------------
# Fixtures：复用 event_bus 基础环境
# ---------------------------------------------------------------------------
@pytest.fixture
def event_registry() -> EventRegistry:
    reg = EventRegistry()
    reg.register(SimpleTestEventDecl)
    reg.register(OtherEventDecl)
    reg.register(NoPayloadEventDecl)
    return reg


@pytest.fixture
def handler_registry() -> EventHandlerRegistry:
    return EventHandlerRegistry()


@pytest.fixture
async def running_bus(
    event_registry: EventRegistry, handler_registry: EventHandlerRegistry
) -> AsyncGenerator[EventBus, None]:
    bus = EventBus(event_registry, handler_registry, queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)))
    await bus.start()
    yield bus
    await bus.stop()


# ---------------------------------------------------------------------------
# 测试用例
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_expect_matches_event(running_bus: EventBus) -> None:
    """正常流程：在 expect 上下文中发布匹配事件，future 应收到负载"""
    proxy = running_bus.proxy('test')

    async with expect(proxy, 'test.event') as future:
        await proxy.publish('test.event', {'value': 42, 'msg': 'hello'})
        result = await asyncio.wait_for(future, timeout=1.0)

    assert isinstance(result.data, SimpleTestPayload)
    assert result.data.value == 42
    assert result.data.msg == 'hello'


@pytest.mark.asyncio
async def test_expect_only_matches_once(running_bus: EventBus) -> None:
    """expect 应只捕获第一个匹配的事件，后续事件被忽略"""
    proxy = running_bus.proxy('test')

    async with expect(proxy, 'test.event') as future:
        await proxy.publish('test.event', {'value': 1, 'msg': 'first'})
        await proxy.publish('test.event', {'value': 2, 'msg': 'second'})
        result = await asyncio.wait_for(future, timeout=1.0)

    assert isinstance(result.data, SimpleTestPayload)
    assert result.data.value == 1  # 只有第一个被捕获
    assert result.data.msg == 'first'


@pytest.mark.asyncio
async def test_expect_ignores_non_matching_events(running_bus: EventBus) -> None:
    """不匹配的事件不应触发 future"""
    proxy = running_bus.proxy('test')

    async with expect(proxy, 'test.event') as future:
        await proxy.publish('test.other', {'value': 99, 'msg': 'other'})

        # 发布匹配事件后才应完成
        await proxy.publish('test.event', {'value': 1, 'msg': 'match'})
        result = await asyncio.wait_for(future, timeout=1.0)

    assert isinstance(result.data, SimpleTestPayload)
    assert result.data.value == 1


@pytest.mark.asyncio
async def test_expect_filter_func_sync(running_bus: EventBus) -> None:
    """同步过滤器应正确工作：返回 True 才触发"""
    proxy = running_bus.proxy('test')

    def filt(event: Event) -> bool:
        data = event.data
        return isinstance(data, SimpleTestPayload) and data.value > 10

    async with expect(proxy, 'test.event', filter_func=filt) as future:
        # 发布 value <= 10 的应被忽略
        await proxy.publish('test.event', {'value': 5, 'msg': 'ignored'})
        await proxy.publish('test.event', {'value': 15, 'msg': 'accepted'})
        result = await asyncio.wait_for(future, timeout=1.0)

    assert isinstance(result.data, SimpleTestPayload)
    assert result.data.value == 15


@pytest.mark.asyncio
async def test_expect_filter_func_async(running_bus: EventBus) -> None:
    """异步过滤器应正确工作"""
    proxy = running_bus.proxy('test')

    async def async_filt(event: Event) -> bool:
        await asyncio.sleep(0.01)  # 模拟异步判断
        assert isinstance(event.data, SimpleTestPayload)
        return event.data.value > 10

    async with expect(proxy, 'test.event', filter_func=async_filt) as future:
        await proxy.publish('test.event', {'value': 5, 'msg': 'no'})
        await proxy.publish('test.event', {'value': 20, 'msg': 'yes'})
        result = await asyncio.wait_for(future, timeout=1.0)

    assert isinstance(result.data, SimpleTestPayload)
    assert result.data.value == 20


@pytest.mark.asyncio
async def test_expect_filter_exception_propagates_to_future(running_bus: EventBus) -> None:
    """过滤器抛出的异常应通过 future 传递给等待方"""
    proxy = running_bus.proxy('test')

    def bad_filter(event: Event) -> bool:
        raise ValueError('filter exploded')

    async with expect(proxy, 'test.event', filter_func=bad_filter) as future:
        await proxy.publish('test.event', {'value': 1, 'msg': 'test'})

        with pytest.raises(ValueError, match='filter exploded'):
            await asyncio.wait_for(future, timeout=1.0)


@pytest.mark.asyncio
async def test_expect_timeout_while_waiting(running_bus: EventBus) -> None:
    """如果在上下文中等待超时，应抛出 TimeoutError 且 future 被取消"""
    proxy = running_bus.proxy('test')

    async with expect(proxy, 'test.event') as future:
        # 不发布事件，故意等待超时
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(future, timeout=0.1)

    # 上下文退出后，future 已被取消
    assert future.cancelled() or future.done()


@pytest.mark.asyncio
async def test_expect_context_exit_cancels_future(running_bus: EventBus) -> None:
    """如果上下文退出时 future 尚未完成，应自动取消 future"""
    proxy = running_bus.proxy('test')
    future_ref: Optional[asyncio.Future[Any]] = None

    async with expect(proxy, 'test.event') as future:
        future_ref = future
        # 不发布事件，直接退出

    assert future_ref is not None
    assert future_ref.cancelled()


@pytest.mark.asyncio
async def test_expect_cleans_up_handler_after_context(
    running_bus: EventBus, handler_registry: EventHandlerRegistry
) -> None:
    """上下文退出后，临时处理器应从注册表中注销"""
    proxy = running_bus.proxy('test')

    initial_count = handler_registry.handlers_count

    async with expect(proxy, 'test.event') as future:
        # 处理器应被注册
        assert handler_registry.handlers_count == initial_count + 1
        await proxy.publish('test.event', {'value': 1, 'msg': 'done'})
        await future

    # 退出后应注销
    assert handler_registry.handlers_count == initial_count


@pytest.mark.asyncio
async def test_expect_handles_publish_before_await(running_bus: EventBus) -> None:
    """事件在 await future 之前发布也应正确捕获"""
    proxy = running_bus.proxy('test')

    async with expect(proxy, 'test.event') as future:
        await proxy.publish('test.event', {'value': 99, 'msg': 'early'})
        # 此时 future 可能已经完成
        result = await future  # 直接返回结果

    assert isinstance(result.data, SimpleTestPayload)
    assert result.data.value == 99


@pytest.mark.asyncio
async def test_expect_multiple_patterns(running_bus: EventBus) -> None:
    """支持事件名列表，匹配其中任意一个"""
    proxy = running_bus.proxy('test')

    async with expect(proxy, ['test.event', 'test.other']) as future:
        await proxy.publish('test.other', {'value': 77, 'msg': 'other'})
        result = await asyncio.wait_for(future, timeout=1.0)

    assert isinstance(result.data, SimpleTestPayload)
    assert result.data.value == 77


@pytest.mark.asyncio
async def test_expect_regex_pattern(running_bus: EventBus) -> None:
    """支持正则表达式模式"""
    proxy = running_bus.proxy('test')

    async with expect(proxy, Regex(r'test\..*')) as future:
        await proxy.publish('test.other', {'value': 88, 'msg': 'regex'})
        result = await asyncio.wait_for(future, timeout=1.0)

    assert isinstance(result.data, SimpleTestPayload)
    assert result.data.value == 88


@pytest.mark.asyncio
async def test_expect_without_payload(running_bus: EventBus) -> None:
    """无负载事件应返回 None"""
    proxy = running_bus.proxy('test')

    async with expect(proxy, 'test.no_payload') as future:
        await proxy.publish('test.no_payload', None)
        result = await asyncio.wait_for(future, timeout=1.0)

    assert result.data is None


@pytest.mark.asyncio
async def test_one_shot_handler_filter_exception_without_on_error() -> None:
    """OneShotEventHandler 未设置 on_error 回调时，过滤器异常应直接 raise"""

    def bad_filter(event: Event) -> bool:
        raise ValueError('filter exploded without handler')

    handler = OneShotEventHandler(
        event_patterns=['test.event'],
        on_match=lambda e: None,
        filter_func=bad_filter,
        # 不传 on_error —— 异常应向上传播
    )

    event = Event(name='test.event', data=SimpleTestPayload(value=1, msg='test'))
    with pytest.raises(ValueError, match='filter exploded without handler'):
        await handler.handle(None, MagicMock(), event)


@pytest.mark.asyncio
async def test_one_shot_handler_direct_usage() -> None:
    """直接测试 OneShotEventHandler 的基本行为（不通过 expect）"""
    matched_event: Optional[Event] = None

    def on_match(event: Event) -> None:
        nonlocal matched_event
        matched_event = event

    handler = OneShotEventHandler(
        event_patterns=['test.event'],
        on_match=on_match,
    )

    # 模拟事件触发
    event = Event(name='test.event', data=SimpleTestPayload(value=1, msg='direct'))
    await handler.handle(None, MagicMock(), event)

    assert matched_event is not None
    assert matched_event.name == 'test.event'

    # 第二次触发应被忽略（_active 已清除）
    matched_event = None
    event2 = Event(name='test.event', data=SimpleTestPayload(value=2, msg='second'))
    await handler.handle(None, MagicMock(), event2)
    assert matched_event is None
