"""simple_handler 装饰器的单元测试。

覆盖：无负载/有负载处理器、类型校验、同步/异步函数、
元数据传递、自定义超时、集成总线。
"""

import asyncio
from typing import AsyncGenerator

import pytest
from pydantic import BaseModel, Field

from infinity_bus import (
    EventBus,
    EventDeclaration,
    EventHandler,
    EventHandlerRegistry,
    EventRegistry,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
)
from infinity_bus.templates import handler


# ---------------------------------------------------------------------------
# 测试用事件与负载
# ---------------------------------------------------------------------------
class PayloadModel(BaseModel):
    value: int = Field(description='测试值')
    msg: str = Field(default='hello', description='测试消息')


class WithPayloadEvent(EventDeclaration):
    name = 'test.with_payload'
    payload_type = PayloadModel


class NoPayloadEvent(EventDeclaration):
    name = 'test.no_payload'


class OtherPayloadEvent(EventDeclaration):
    name = 'test.other_payload'
    payload_type = PayloadModel


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def event_registry() -> EventRegistry:
    reg = EventRegistry()
    reg.register(WithPayloadEvent)
    reg.register(NoPayloadEvent)
    reg.register(OtherPayloadEvent)
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
# 基础装饰：无负载处理器
# ---------------------------------------------------------------------------
class TestNoPayloadHandler:
    """无负载事件的处理器装饰器测试"""

    def test_no_payload_sync_function(self) -> None:
        """无负载事件 + 同步函数 → 正确生成 EventHandler 子类"""

        @handler(NoPayloadEvent)
        def my_handler() -> None:
            pass

        assert issubclass(my_handler, EventHandler)
        assert my_handler.__name__ == 'my_handler'

    def test_no_payload_async_function(self) -> None:
        """无负载事件 + 异步函数 → 正确生成 EventHandler 子类"""

        @handler(NoPayloadEvent)
        async def my_async_handler() -> None:
            pass

        assert issubclass(my_async_handler, EventHandler)

    def test_no_payload_handler_with_extra_param_raises(self) -> None:
        """无负载事件 + 带参数的处理器 → 抛出 TypeError"""
        with pytest.raises(TypeError, match='无负载'):

            @handler(NoPayloadEvent)
            def bad_handler(extra: int) -> None:  # type: ignore[unused-function]
                pass


# ---------------------------------------------------------------------------
# 基础装饰：有负载处理器
# ---------------------------------------------------------------------------
class TestWithPayloadHandler:
    """有负载事件的处理器装饰器测试"""

    def test_payload_sync_function(self) -> None:
        """有负载事件 + 同步函数 → 正确生成 EventHandler 子类"""

        @handler(WithPayloadEvent)
        def my_handler(payload: PayloadModel) -> None:
            pass

        assert issubclass(my_handler, EventHandler)
        assert my_handler.__name__ == 'my_handler'

    def test_payload_async_function(self) -> None:
        """有负载事件 + 异步函数 → 正确生成 EventHandler 子类"""

        @handler(WithPayloadEvent)
        async def my_async_handler(payload: PayloadModel) -> None:
            pass

        assert issubclass(my_async_handler, EventHandler)

    def test_payload_handler_missing_param_raises(self) -> None:
        """有负载事件 + 无参数处理器 → 抛出 TypeError"""
        with pytest.raises(TypeError, match='要求负载参数'):

            @handler(WithPayloadEvent)
            def bad_handler() -> None:  # type: ignore[unused-function]
                pass

    def test_payload_handler_wrong_type_raises(self) -> None:
        """有负载事件 + 错误类型的参数 → 抛出 TypeError"""

        class OtherModel(BaseModel):
            x: int

        class OtherEvent(EventDeclaration):
            name = 'test.other'
            payload_type = OtherModel

        with pytest.raises(TypeError, match='参数类型应为'):

            @handler(OtherEvent)
            def bad_handler(payload: PayloadModel) -> None:  # type: ignore[unused-function]
                pass

    def test_payload_handler_unannotated_param(self) -> None:
        """有负载事件 + 未注解参数 → 不校验类型（允许通过）"""

        @handler(WithPayloadEvent)
        def my_handler(payload) -> None:  # type: ignore[no-untyped-def]
            pass

        assert issubclass(my_handler, EventHandler)


# ---------------------------------------------------------------------------
# 元数据传递
# ---------------------------------------------------------------------------
class TestHandlerMetadata:
    """装饰器应正确传递函数元数据"""

    def test_name_preserved(self) -> None:
        """__name__ 应等于原函数名"""

        @handler(NoPayloadEvent)
        async def fetch_user_profile() -> None:
            """Fetch user profile from remote."""

        assert fetch_user_profile.__name__ == 'fetch_user_profile'

    def test_qualname_preserved(self) -> None:
        """__qualname__ 应包含原函数名"""

        @handler(NoPayloadEvent)
        async def process_order() -> None:
            """Process an order."""

        # 嵌套函数的 __qualname__ 包含封闭作用域前缀，
        # 装饰器保留原函数的值，确保以原函数名结尾
        assert process_order.__qualname__.endswith('process_order')

    def test_module_preserved(self) -> None:
        """__module__ 应等于原函数所在模块"""

        @handler(NoPayloadEvent)
        async def local_func() -> None:
            pass

        assert local_func.__module__ == __name__

    def test_doc_preserved(self) -> None:
        """__doc__ 应等于原函数文档字符串"""

        @handler(NoPayloadEvent)
        async def documented_func() -> None:
            """This is a custom docstring for testing."""

        assert documented_func.__doc__ == 'This is a custom docstring for testing.'


# ---------------------------------------------------------------------------
# 超时参数
# ---------------------------------------------------------------------------
class TestHandleTimeout:
    """handle_timeout 参数测试"""

    def test_default_timeout(self) -> None:
        """默认超时为 32.0"""

        @handler(NoPayloadEvent)
        async def default_timeout_handler() -> None:
            pass

        instance = default_timeout_handler()
        assert instance.handle_timeout == 32.0

    def test_custom_timeout(self) -> None:
        """自定义超时正确传递"""

        @handler(NoPayloadEvent, handle_timeout=10.0)
        async def custom_timeout_handler() -> None:
            pass

        instance = custom_timeout_handler()
        assert instance.handle_timeout == 10.0

    def test_none_timeout(self) -> None:
        """超时为 None 时正确传递"""

        @handler(NoPayloadEvent, handle_timeout=None)
        async def no_timeout_handler() -> None:
            pass

        instance = no_timeout_handler()
        assert instance.handle_timeout is None


# ---------------------------------------------------------------------------
# 集成测试：与 EventBus 一起运行
# ---------------------------------------------------------------------------
class TestHandlerIntegration:
    """handler 装饰器与 EventBus 集成测试"""

    @pytest.mark.asyncio
    async def test_no_payload_handler_receives_event(
        self, running_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """无负载处理器在实际总线中正确触发"""
        received: list[None] = []

        @handler(NoPayloadEvent)
        async def handle_no_payload() -> None:
            received.append(None)

        hid = handler_registry.register(handle_no_payload())
        proxy = running_bus.proxy('test')

        await proxy.publish('test.no_payload', None)
        # 等待异步处理
        await asyncio.sleep(0.1)

        assert len(received) == 1
        handler_registry.unregister(hid)

    @pytest.mark.asyncio
    async def test_payload_handler_receives_correct_payload(
        self, running_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """有负载处理器在实际总线中收到正确的负载数据"""
        received_payloads: list[PayloadModel] = []

        @handler(WithPayloadEvent)
        async def handle_with_payload(payload: PayloadModel) -> None:
            received_payloads.append(payload)

        hid = handler_registry.register(handle_with_payload())
        proxy = running_bus.proxy('test')

        await proxy.publish('test.with_payload', {'value': 42, 'msg': 'integration'})
        await asyncio.sleep(0.1)

        assert len(received_payloads) == 1
        assert received_payloads[0].value == 42
        assert received_payloads[0].msg == 'integration'
        handler_registry.unregister(hid)

    @pytest.mark.asyncio
    async def test_sync_handler_in_bus(self, running_bus: EventBus, handler_registry: EventHandlerRegistry) -> None:
        """同步处理器在实际总线中正确执行"""
        received: list[int] = []

        @handler(WithPayloadEvent)
        def sync_handler(payload: PayloadModel) -> None:
            received.append(payload.value)

        hid = handler_registry.register(sync_handler())
        proxy = running_bus.proxy('test')

        await proxy.publish('test.with_payload', {'value': 99, 'msg': 'sync'})
        await asyncio.sleep(0.1)

        assert received == [99]
        handler_registry.unregister(hid)

    @pytest.mark.asyncio
    async def test_multiple_handlers_same_event(
        self, running_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """同一事件多个装饰器生成的处理器全部触发"""
        results: list[str] = []

        @handler(WithPayloadEvent)
        async def handler_a(payload: PayloadModel) -> None:
            results.append('A')

        @handler(WithPayloadEvent)
        async def handler_b(payload: PayloadModel) -> None:
            results.append('B')

        hid_a = handler_registry.register(handler_a())
        hid_b = handler_registry.register(handler_b())
        proxy = running_bus.proxy('test')

        await proxy.publish('test.with_payload', {'value': 1, 'msg': 'multi'})
        await asyncio.sleep(0.1)

        assert set(results) == {'A', 'B'}
        handler_registry.unregister(hid_a)
        handler_registry.unregister(hid_b)


# ---------------------------------------------------------------------------
# 边界条件
# ---------------------------------------------------------------------------
class TestEdgeCases:
    """边界条件测试"""

    def test_handler_is_subclass_of_event_handler(self) -> None:
        """装饰器返回值是 EventHandler 的子类"""

        @handler(NoPayloadEvent)
        async def sub_check() -> None:
            pass

        assert issubclass(sub_check, EventHandler)

    def test_handler_instance_has_subscriptions(self) -> None:
        """生成的处理器实例包含正确的事件订阅"""

        @handler(WithPayloadEvent)
        async def sub_handler(payload: PayloadModel) -> None:
            pass

        instance = sub_handler()
        assert 'test.with_payload' in instance.subscriptions

    def test_handler_instance_has_no_other_subscriptions(self) -> None:
        """生成的处理器只订阅声明的事件"""

        @handler(NoPayloadEvent)
        async def single_sub() -> None:
            pass

        instance = single_sub()
        assert instance.subscriptions == ['test.no_payload']
