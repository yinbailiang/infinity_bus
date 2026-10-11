"""ServiceHandler 完整测试

覆盖：装饰器声明期校验、processor 收集与订阅生成、handle 分发与响应构造、
ServiceError 转换、单向事件、忽略规则、端到端集成（含 RPC 往返）。
"""

import asyncio
from typing import Any, AsyncGenerator

import pytest
from pydantic import BaseModel, Field

from infinity_bus import (
    Event,
    EventBus,
    EventDeclaration,
    EventHandlerRegistry,
    EventRegistry,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
)
from infinity_bus.templates.handlers.service import (
    ConflictError,
    InvalidRequestError,
    ServiceHandler,
    process,
)
from infinity_bus.templates.request import RequestProtocol, ResponseProtocol, request


# ---------------------------------------------------------------------------
# 测试用 Payload 与事件
# ---------------------------------------------------------------------------
class CalcReq(RequestProtocol):
    a: int = Field(description='被除数')
    b: int = Field(description='除数')


class CalcResp(ResponseProtocol):
    result: int = Field(default=0, description='计算结果')
    detail: str = Field(default='', description='附加信息')


class NotifyReq(RequestProtocol):
    message: str = Field(description='通知内容')


class RawPayload(BaseModel):
    """不继承任何协议的裸 Payload，用于声明期校验测试"""

    foo: str = Field(default='x')


class CalcReqEvent(EventDeclaration):
    name = 'svc.calc.request'
    payload_type = CalcReq


class CalcRespEvent(EventDeclaration):
    name = 'svc.calc.response'
    payload_type = CalcResp


class NotifyEvent(EventDeclaration):
    name = 'svc.notify'
    payload_type = NotifyReq


class NoPayloadEvent(EventDeclaration):
    name = 'svc.nopayload'


class BadRequestEvent(EventDeclaration):
    name = 'svc.bad.request'
    payload_type = RawPayload


class BadResponseEvent(EventDeclaration):
    name = 'svc.bad.response'
    payload_type = RawPayload


class NoPayloadResponseEvent(EventDeclaration):
    name = 'svc.nopayload.response'


# ---------------------------------------------------------------------------
# 测试用 ServiceHandler 子类
# ---------------------------------------------------------------------------
class CalcService(ServiceHandler):
    """除法服务：成功响应 / ServiceError 转换 / None 不回应"""

    @process(CalcReqEvent, CalcRespEvent)
    def divide(self, payload: CalcReq, proxy: EventBus.Proxy, raw: Event) -> dict[str, int] | None:
        if payload.b == 0:
            raise InvalidRequestError('除数为零', detail=f'b={payload.b}')
        if payload.b < 0:
            # 测试约定：负数请求静默丢弃，不回应
            return None
        return {'result': payload.a // payload.b}


class AsyncCalcService(ServiceHandler):
    """异步 processor"""

    @process(CalcReqEvent, CalcRespEvent)
    async def divide(self, payload: CalcReq, proxy: EventBus.Proxy, raw: Event) -> dict[str, int]:
        return {'result': payload.a * payload.b}


class NotifyService(ServiceHandler):
    """单向事件（无响应声明）：仅副作用"""

    def __init__(self) -> None:
        super().__init__()
        self.notified: list[str] = []

    @process(NotifyEvent)
    def notify(self, payload: NotifyReq, proxy: EventBus.Proxy, raw: Event) -> None:
        self.notified.append(payload.message)


class FailingNotifyService(ServiceHandler):
    """单向事件抛 ServiceError → 无响应可转，应向上传播"""

    @process(NotifyEvent)
    def notify(self, payload: NotifyReq, proxy: EventBus.Proxy, raw: Event) -> None:
        raise ConflictError('通知失败')


class BaseService(ServiceHandler):
    @process(CalcReqEvent, CalcRespEvent)
    def calculate(self, payload: CalcReq, proxy: EventBus.Proxy, raw: Event) -> dict[str, int]:
        return {'result': 0}


class OverrideService(BaseService):
    """子类以不同方法名重新登记同一请求事件"""

    @process(CalcReqEvent, CalcRespEvent)
    def calculate_v2(self, payload: CalcReq, proxy: EventBus.Proxy, raw: Event) -> dict[str, int]:
        return {'result': 1}


class EmptyService(ServiceHandler):
    """无任何 processor"""


# ---------------------------------------------------------------------------
# 辅助工具：捕获 proxy.publish 调用的 spy
# ---------------------------------------------------------------------------
class _PublishSpy:
    """替换 ``proxy.publish`` 的轻量捕获器（与被替换方法签名兼容）"""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any] | BaseModel | None]] = []

    async def __call__(self, name: str, data: dict[str, Any] | BaseModel | None = None) -> None:
        self.calls.append((name, data))


def _make_spy_proxy() -> tuple[EventBus.Proxy, _PublishSpy]:
    """用未启动的真实 EventBus 构造 Proxy，并将 publish 替换为捕获器"""
    bus = EventBus(EventRegistry(), EventHandlerRegistry())
    proxy = bus.proxy('test')
    spy = _PublishSpy()
    proxy.publish = spy
    return proxy, spy


def _calc_payload(a: int, b: int) -> CalcReq:
    return CalcReq(a=a, b=b, session_id='s-1', request_id='r-1')


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def event_registry() -> EventRegistry:
    reg = EventRegistry()
    reg.register(CalcReqEvent)
    reg.register(CalcRespEvent)
    reg.register(NotifyEvent)
    return reg


@pytest.fixture
def handler_registry() -> EventHandlerRegistry:
    return EventHandlerRegistry()


@pytest.fixture
async def running_bus(
    event_registry: EventRegistry, handler_registry: EventHandlerRegistry
) -> AsyncGenerator[EventBus, None]:
    bus = EventBus(event_registry, handler_registry, queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=64)))
    await bus.start()
    yield bus
    await bus.stop()


# ============================================================================
# 装饰器声明期校验
# ============================================================================
class TestProcessDecorator:
    """@process 在类定义期对请求/响应事件做协议校验"""

    def test_rejects_request_without_payload(self) -> None:
        """请求事件无负载 → TypeError"""
        with pytest.raises(TypeError, match='载荷不可为空'):
            process(NoPayloadEvent)

    def test_rejects_request_payload_not_request_protocol(self) -> None:
        """请求负载不继承 RequestProtocol → TypeError"""
        with pytest.raises(TypeError, match='必须是 RequestProtocol'):
            process(BadRequestEvent)

    def test_rejects_response_without_payload(self) -> None:
        """响应事件无负载 → TypeError"""
        with pytest.raises(TypeError, match='载荷不可为空'):
            process(CalcReqEvent, NoPayloadResponseEvent)

    def test_rejects_response_payload_not_response_protocol(self) -> None:
        """响应负载不继承 ResponseProtocol → TypeError"""
        with pytest.raises(TypeError, match='必须是 ResponseProtocol'):
            process(CalcReqEvent, BadResponseEvent)


# ============================================================================
# processor 收集与订阅生成
# ============================================================================
class TestProcessorCollection:
    """__init_subclass__ 收集 processor 表并据此生成订阅"""

    def test_collects_processors_from_decorated_methods(self) -> None:
        """被 @process 装饰的方法按请求事件名登记"""
        svc = CalcService()
        assert set(svc.processors.keys()) == {'svc.calc.request'}
        meta = svc.processors['svc.calc.request']
        assert meta.func_name == 'divide'
        assert meta.request is CalcReqEvent
        assert meta.response is CalcRespEvent

    def test_subscriptions_generated_from_processors(self) -> None:
        """订阅列表自动由 processor 表的键生成"""
        svc = CalcService()
        assert list(svc.subscriptions) == ['svc.calc.request']

    def test_multiple_processors_collected(self) -> None:
        """多个 processor 全部登记（双向 + 单向）"""

        class MultiService(CalcService):
            @process(NotifyEvent)
            def notify(self, payload: NotifyReq, proxy: EventBus.Proxy, raw: Event) -> None: ...

        svc = MultiService()
        assert set(svc.processors.keys()) == {'svc.calc.request', 'svc.notify'}
        assert set(svc.subscriptions) == {'svc.calc.request', 'svc.notify'}

    def test_parent_processor_inherited(self) -> None:
        """未覆写时继承父类的 processor 登记"""

        class ChildService(BaseService):
            pass

        svc = ChildService()
        assert svc.processors['svc.calc.request'].func_name == 'calculate'

    async def test_subclass_override_wins(self) -> None:
        """子类重新登记同一请求事件时，子类版本胜出（行为级验证）"""
        svc = OverrideService()
        assert svc.processors['svc.calc.request'].func_name == 'calculate_v2'

        proxy, spy = _make_spy_proxy()
        payload = _calc_payload(1, 1)
        await svc.handle(payload, proxy, Event(name=CalcReqEvent.name, data=payload))

        assert len(spy.calls) == 1
        resp = spy.calls[0][1]
        assert isinstance(resp, CalcResp)
        assert resp.result == 1

    def test_empty_service_has_no_processors(self) -> None:
        """无 processor 的服务：空表、空订阅"""
        svc = EmptyService()
        assert len(svc.processors) == 0
        assert list(svc.subscriptions) == []


# ============================================================================
# handle 分发与响应构造
# ============================================================================
class TestHandleDispatch:
    """handle：查表分发、响应构造、错误转换、忽略规则"""

    async def test_publishes_success_response(self) -> None:
        """正常返回 dict → 构造并发布 success 响应（session/request 回传）"""
        svc = CalcService()
        proxy, spy = _make_spy_proxy()
        payload = _calc_payload(10, 2)

        await svc.handle(payload, proxy, Event(name=CalcReqEvent.name, data=payload))

        assert len(spy.calls) == 1
        name, resp = spy.calls[0]
        assert name == CalcRespEvent.name
        assert isinstance(resp, CalcResp)
        assert resp.success is True
        assert resp.result == 5
        assert resp.session_id == 's-1'
        assert resp.request_id == 'r-1'

    async def test_service_error_translated_to_failed_response(self) -> None:
        """processor 抛 ServiceError → success=False + error_msg + body 字段合并"""
        svc = CalcService()
        proxy, spy = _make_spy_proxy()
        payload = _calc_payload(7, 0)

        await svc.handle(payload, proxy, Event(name=CalcReqEvent.name, data=payload))

        assert len(spy.calls) == 1
        _, resp = spy.calls[0]
        assert isinstance(resp, CalcResp)
        assert resp.success is False
        assert resp.error_msg == '除数为零'
        assert resp.detail == 'b=0'

    async def test_none_body_means_no_response(self) -> None:
        """返回 None → 不发布响应"""
        svc = CalcService()
        proxy, spy = _make_spy_proxy()
        payload = _calc_payload(1, -1)

        await svc.handle(payload, proxy, Event(name=CalcReqEvent.name, data=payload))

        assert spy.calls == []

    async def test_single_direction_event_side_effect_only(self) -> None:
        """单向事件（无响应声明）→ 执行副作用、不发布响应"""
        svc = NotifyService()
        proxy, spy = _make_spy_proxy()
        payload = NotifyReq(message='hello', session_id='s-1', request_id='r-1')

        await svc.handle(payload, proxy, Event(name=NotifyEvent.name, data=payload))

        assert spy.calls == []
        assert svc.notified == ['hello']

    async def test_single_direction_service_error_propagates(self) -> None:
        """单向事件抛 ServiceError → 无响应可转，向上抛出"""
        svc = FailingNotifyService()
        proxy, _spy = _make_spy_proxy()
        payload = NotifyReq(message='x', session_id='s-1', request_id='r-1')

        with pytest.raises(ConflictError, match='通知失败'):
            await svc.handle(payload, proxy, Event(name=NotifyEvent.name, data=payload))

    async def test_none_payload_ignored(self) -> None:
        """payload 为 None → 直接忽略"""
        svc = CalcService()
        proxy, spy = _make_spy_proxy()

        await svc.handle(None, proxy, Event(name=CalcReqEvent.name, data=None))

        assert spy.calls == []

    async def test_unknown_event_ignored(self) -> None:
        """未登记的事件名 → 忽略，不发布响应"""
        svc = CalcService()
        proxy, spy = _make_spy_proxy()
        payload = _calc_payload(1, 1)

        await svc.handle(payload, proxy, Event(name='svc.unknown', data=payload))

        assert spy.calls == []

    async def test_payload_type_mismatch_ignored(self) -> None:
        """事件名命中但 payload 类型不匹配 → 忽略，不发布响应"""
        svc = CalcService()
        proxy, spy = _make_spy_proxy()
        payload = NotifyReq(message='x', session_id='s-1', request_id='r-1')

        await svc.handle(payload, proxy, Event(name=CalcReqEvent.name, data=payload))

        assert spy.calls == []


# ============================================================================
# 端到端集成
# ============================================================================
class TestIntegration:
    """真实 EventBus 上的端到端行为"""

    async def test_request_response_roundtrip(
        self, running_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """request() 往返：同步 processor 返回成功响应"""
        handler_registry.register(CalcService())

        resp = await request(
            running_bus.proxy('cli'),
            CalcReqEvent.name,
            {'a': 9, 'b': 3},
            CalcRespEvent.name,
            timeout=2.0,
        )

        assert isinstance(resp, CalcResp)
        assert resp.success is True
        assert resp.result == 3

    async def test_async_processor_roundtrip(
        self, running_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """异步 processor 同样支持"""
        handler_registry.register(AsyncCalcService())

        resp = await request(
            running_bus.proxy('cli'),
            CalcReqEvent.name,
            {'a': 3, 'b': 4},
            CalcRespEvent.name,
            timeout=2.0,
        )

        assert isinstance(resp, CalcResp)
        assert resp.result == 12

    async def test_service_error_roundtrip(self, running_bus: EventBus, handler_registry: EventHandlerRegistry) -> None:
        """ServiceError 端到端：调用方收到 success=False 响应"""
        handler_registry.register(CalcService())

        resp = await request(
            running_bus.proxy('cli'),
            CalcReqEvent.name,
            {'a': 1, 'b': 0},
            CalcRespEvent.name,
            timeout=2.0,
        )

        assert isinstance(resp, CalcResp)
        assert resp.success is False
        assert resp.error_msg == '除数为零'
        assert resp.detail == 'b=0'

    async def test_single_direction_event_processed(
        self, running_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """单向事件经总线分发被执行"""
        svc = NotifyService()
        handler_registry.register(svc)

        await running_bus.proxy('cli').publish(
            'svc.notify', {'message': 'hello', 'session_id': 's-1', 'request_id': 'r-1'}
        )
        await asyncio.sleep(0.1)

        assert svc.notified == ['hello']
