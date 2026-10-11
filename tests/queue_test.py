"""EventQueue 抽象与 InMemoryEventQueue 实现的单元/集成测试。"""

import asyncio
from typing import Any, Callable, cast

import pytest
from conftest import CountingHandler, wait_for_condition

from infinity_bus import (
    Event,
    EventBus,
    EventHandlerRegistry,
    EventQueue,
    EventRegistry,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
)

# ============================================================================
# EventQueue 抽象
# ============================================================================


def test_event_queue_is_abstract() -> None:
    """EventQueue 为抽象基类，不能直接实例化。"""
    with pytest.raises(TypeError):
        cast(Any, EventQueue)()


def test_event_queue_requires_all_methods() -> None:
    """缺少任一抽象方法的子类也不能实例化。"""

    class PartialQueue(EventQueue):
        async def put(self, event: Event) -> None:
            pass

    with pytest.raises(TypeError):
        cast(Any, PartialQueue)()


# ============================================================================
# InMemoryEventQueueConfig
# ============================================================================


def test_config_defaults() -> None:
    """默认配置为有界 1024（沿用既有总线默认容量）。"""
    cfg = InMemoryEventQueueConfig()
    assert cfg.maxsize == 1024


def test_config_custom_maxsize() -> None:
    """maxsize=0 表示无界。"""
    assert InMemoryEventQueueConfig(maxsize=0).maxsize == 0
    assert InMemoryEventQueueConfig(maxsize=5).maxsize == 5


# ============================================================================
# InMemoryEventQueue 基础行为
# ============================================================================


@pytest.mark.asyncio
async def test_is_instance_of_event_queue() -> None:
    """InMemoryEventQueue 是 EventQueue 的实现。"""
    q = InMemoryEventQueue()
    assert isinstance(q, EventQueue)


@pytest.mark.asyncio
async def test_fifo_order_and_qsize() -> None:
    """FIFO 顺序出队，qsize 反映积压。"""
    q = InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=8))
    events = [Event(name=f'test.{i}') for i in range(3)]
    for e in events:
        await q.put(e)

    assert q.qsize() == 3
    for expected in events:
        got = await q.get()
        assert got is expected
        q.task_done()

    assert q.qsize() == 0
    await q.join()  # 全部 task_done 后 join 立即返回


@pytest.mark.asyncio
async def test_get_blocks_when_empty() -> None:
    """队列为空时 get 阻塞直至有事件入队。"""
    q = InMemoryEventQueue()
    event = Event(name='test.blocking_get')

    get_task = asyncio.create_task(q.get())
    await asyncio.sleep(0.05)
    assert not get_task.done()

    await q.put(event)
    got = await asyncio.wait_for(get_task, timeout=1.0)
    assert got is event
    q.task_done()


@pytest.mark.asyncio
async def test_put_blocks_when_full() -> None:
    """有界队列满时 put 阻塞（背压），消费后恢复。"""
    q = InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=1))
    first = Event(name='test.first')
    second = Event(name='test.second')
    await q.put(first)

    put_task = asyncio.create_task(q.put(second))
    await asyncio.sleep(0.05)
    assert not put_task.done(), '队列满时 put 应阻塞'

    got = await q.get()
    assert got is first
    q.task_done()

    await asyncio.wait_for(put_task, timeout=1.0)
    assert q.qsize() == 1


@pytest.mark.asyncio
async def test_unbounded_when_maxsize_zero() -> None:
    """maxsize=0 时队列无界，put 不会阻塞。"""
    q = InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=0))
    for i in range(1000):
        await q.put(Event(name=f'test.{i}'))
    assert q.qsize() == 1000


@pytest.mark.asyncio
async def test_join_waits_until_all_task_done() -> None:
    """join 阻塞直至所有已取出事件均调用 task_done。"""
    q = InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=2))
    await q.put(Event(name='test.a'))
    await q.put(Event(name='test.b'))

    join_task = asyncio.create_task(q.join())
    await asyncio.sleep(0.05)
    assert not join_task.done()

    first = await q.get()
    q.task_done()
    await asyncio.sleep(0.05)
    assert not join_task.done(), '仅消费一条时 join 不应返回'

    second = await q.get()
    assert second is not first
    q.task_done()
    await asyncio.wait_for(join_task, timeout=1.0)
    assert join_task.done()


@pytest.mark.asyncio
async def test_config_is_optional() -> None:
    """不传 config 时使用默认配置（有界 1024）。"""
    q = InMemoryEventQueue()
    assert q.qsize() == 0
    for i in range(10):
        await q.put(Event(name=f'test.{i}'))
    assert q.qsize() == 10


# ============================================================================
# EventBus 注入集成
# ============================================================================


class RecordingEventQueue(EventQueue):
    """记录 put/get/join 调用的自定义队列实现，验证总线仅依赖抽象。"""

    def __init__(self) -> None:
        self._inner = InMemoryEventQueue()
        self.put_count = 0
        self.get_count = 0

    async def put(self, event: Event) -> None:
        self.put_count += 1
        await self._inner.put(event)

    async def get(self) -> Event:
        event = await self._inner.get()
        self.get_count += 1
        return event

    def task_done(self) -> None:
        self._inner.task_done()

    def qsize(self) -> int:
        return self._inner.qsize()

    async def join(self) -> None:
        await self._inner.join()


@pytest.mark.asyncio
async def test_bus_uses_injected_queue_for_dispatch(
    base_event_registry: EventRegistry,
    handler_registry: EventHandlerRegistry,
) -> None:
    """注入的自定义 EventQueue 被总线用于完整发布/分发链路。"""
    recording = RecordingEventQueue()
    bus = EventBus(base_event_registry, handler_registry, queue=recording)
    handler = CountingHandler()
    handler_registry.register(handler)

    async with bus:
        await bus.proxy('queue_test').publish('test.event', {'value': 1})
        await wait_for_condition(lambda: handler.count == 1)

    assert recording.put_count >= 1
    assert recording.get_count >= 1
    assert bus.queue_size == 0


@pytest.mark.asyncio
async def test_bus_queue_size_reflects_injected_queue(
    base_event_registry: EventRegistry,
    handler_registry: EventHandlerRegistry,
) -> None:
    """bus.queue_size 委托给注入队列的 qsize。"""
    recording = RecordingEventQueue()
    bus = EventBus(base_event_registry, handler_registry, queue=recording)
    handler = CountingHandler()
    handler_registry.register(handler)

    async with bus:
        await bus.proxy('queue_test').publish('test.event', {'value': 1})
        await wait_for_condition(lambda: handler.count == 1)
        assert bus.queue_size == recording.qsize()
        assert bus.queue_size == 0


@pytest.mark.asyncio
async def test_bus_graceful_stop_with_custom_queue(
    base_event_registry: EventRegistry,
    handler_registry: EventHandlerRegistry,
) -> None:
    """停机时总线通过注入队列完成排空，事件不丢失。"""
    recording = RecordingEventQueue()
    bus = EventBus(base_event_registry, handler_registry, queue=recording)
    handler = CountingHandler()
    handler_registry.register(handler)

    async with bus:
        for i in range(5):
            await bus.proxy('queue_test').publish('test.event', {'value': i})

    assert handler.count == 5
    assert recording.qsize() == 0


@pytest.mark.asyncio
async def test_bus_accepts_bounded_injected_queue(
    base_event_registry: EventRegistry,
    handler_registry: EventHandlerRegistry,
    event_bus_factory: Callable[..., EventBus],
) -> None:
    """有界注入队列与总线协作不丢事件（等价旧 max_queue_size 语义）。"""
    bus = EventBus(
        base_event_registry,
        handler_registry,
        queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=2)),
    )
    handler = CountingHandler()
    handler_registry.register(handler)

    async with bus:
        for i in range(10):
            await bus.proxy('queue_test').publish('test.event', {'value': i})

    assert handler.count == 10
    assert bus.queue_size == 0
