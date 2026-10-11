import asyncio
from typing import List, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel, Field

from infinity_bus import (
    BusShuttingDown,
    Event,
    EventBus,
    EventDeclaration,
    EventHandler,
    EventHandlerRegistry,
    EventRegistry,
    Router,
)
from infinity_bus.templates.request import (
    RequestProtocol,
    ResponseProtocol,
    build_response,
    request,
)


class Matcher:
    """测试兼容：按已注册 handler 的订阅束求命中（Router 语义）。"""

    def __init__(self, event_registry: EventRegistry, handler_registry: EventHandlerRegistry) -> None:
        self._router = Router(event_registry)
        for hid, handler in handler_registry:
            self._router.subscribe(hid, handler.subscriptions)
        self._handlers = handler_registry

    def match(self, event_type: str) -> list[tuple[str, EventHandler]]:
        result: list[tuple[str, EventHandler]] = []
        for hid in self._router.match(event_type):
            handler = self._handlers.get(hid)
            if handler is not None:
                result.append((hid, handler))
        return result


# ---------------------------------------------------------------------------
# 测试用的请求/响应 Payload 与事件声明
# ---------------------------------------------------------------------------
class SimpleRequestPayload(RequestProtocol):
    data: str = Field(description='请求数据')


class SimpleResponsePayload(ResponseProtocol):
    result: str = Field(description='响应数据')


class TestRequestEvent(EventDeclaration):
    name = 'test.request'
    payload_type = SimpleRequestPayload


class TestResponseEvent(EventDeclaration):
    name = 'test.response'
    payload_type = SimpleResponsePayload


# 用于测试类型错误的响应（payload 不继承 ResponseProtocol）
class BadResponsePayload(BaseModel):
    session_id: str
    request_id: str
    some_field: int


class BadResponseEvent(EventDeclaration):
    name = 'test.bad_response'
    payload_type = BadResponsePayload


# ---------------------------------------------------------------------------
# Fixtures：构建模拟总线环境
# ---------------------------------------------------------------------------
@pytest.fixture
def event_registry() -> EventRegistry:
    reg = EventRegistry()
    reg.register(TestRequestEvent)
    reg.register(TestResponseEvent)
    reg.register(BadResponseEvent)
    return reg


@pytest.fixture
def handler_registry() -> EventHandlerRegistry:
    return EventHandlerRegistry()


@pytest.fixture
def mock_bus_proxy(event_registry: EventRegistry, handler_registry: EventHandlerRegistry) -> EventBus.Proxy:
    """创建一个模拟的 EventBus.Proxy，实际使用真实组件但可捕获发布调用"""
    bus = MagicMock(spec=EventBus)
    bus._events = event_registry
    bus._handlers = handler_registry

    proxy = EventBus.Proxy(bus, source='test_client')
    # 拦截 publish 方法以便断言
    proxy.publish = AsyncMock()
    return proxy


def _publish_mock(proxy: EventBus.Proxy) -> AsyncMock:
    """取回 mock_bus_proxy 上替换 publish 的 AsyncMock，供类型化断言。"""
    return cast(AsyncMock, proxy.publish)


# ---------------------------------------------------------------------------
# 辅助函数：模拟服务端响应触发
# ---------------------------------------------------------------------------
async def trigger_response(
    event_registry: EventRegistry, handler_registry: EventHandlerRegistry, resp_event: str, payload: BaseModel
) -> None:
    """手动触发已注册的响应处理器，模拟事件总线分发"""
    matcher = Matcher(event_registry, handler_registry)
    handlers: List[tuple[str, EventHandler]] = matcher.match(resp_event)
    for _, handler in handlers:
        # 构造一个模拟的 Event 和 Proxy
        event = Event(name=resp_event, data=payload)
        bus_proxy = MagicMock(spec=EventBus.Proxy)
        await handler.handle(payload, bus_proxy, event)


# ---------------------------------------------------------------------------
# 测试用例
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_request_success(
    mock_bus_proxy: EventBus.Proxy, event_registry: EventRegistry, handler_registry: EventHandlerRegistry
) -> None:
    """正常请求响应流程"""
    session_id = 'test-session-123'

    # 启动请求任务
    req_task: asyncio.Task[ResponseProtocol] = asyncio.create_task(
        request(
            bus_proxy=mock_bus_proxy,
            req_event='test.request',
            req_data={'data': 'hello'},
            resp_event='test.response',
            session_id=session_id,
            timeout=5.0,
        )
    )

    # 等待发布被调用
    await asyncio.sleep(0.01)
    publish_mock = _publish_mock(mock_bus_proxy)
    assert publish_mock.called
    call_args = publish_mock.call_args
    published_data = call_args[0][1]
    req_id_in_publish: str = published_data['request_id']
    assert published_data['session_id'] == session_id
    assert published_data['data'] == 'hello'

    # 模拟服务端响应
    response_payload = SimpleResponsePayload(
        session_id=session_id,
        request_id=req_id_in_publish,
        success=True,
        result='world',
    )
    await trigger_response(event_registry, handler_registry, 'test.response', response_payload)

    # 获取结果
    result: ResponseProtocol = await req_task
    assert isinstance(result, SimpleResponsePayload)
    assert result.success is True
    assert result.result == 'world'


@pytest.mark.asyncio
async def test_request_timeout(mock_bus_proxy: EventBus.Proxy) -> None:
    """请求超时应抛出 TimeoutError"""
    with pytest.raises(asyncio.TimeoutError):
        await request(
            bus_proxy=mock_bus_proxy,
            req_event='test.request',
            req_data={'data': 'test'},
            resp_event='test.response',
            timeout=0.1,
        )


@pytest.mark.asyncio
async def test_request_bus_shutting_down(mock_bus_proxy: EventBus.Proxy) -> None:
    """总线关闭时发布请求应抛出 BusShuttingDown"""
    publish_mock = _publish_mock(mock_bus_proxy)
    publish_mock.side_effect = BusShuttingDown('bus is down')

    with pytest.raises(BusShuttingDown):
        await request(
            bus_proxy=mock_bus_proxy,
            req_event='test.request',
            req_data={'data': 'test'},
            resp_event='test.response',
        )


@pytest.mark.asyncio
async def test_request_cancelled(
    mock_bus_proxy: EventBus.Proxy, event_registry: EventRegistry, handler_registry: EventHandlerRegistry
) -> None:
    """外部取消请求任务应正确传播 CancelledError 并清理资源"""
    req_task: asyncio.Task[ResponseProtocol] = asyncio.create_task(
        request(
            bus_proxy=mock_bus_proxy,
            req_event='test.request',
            req_data={'data': 'test'},
            resp_event='test.response',
        )
    )

    await asyncio.sleep(0.01)
    req_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await req_task

    # 验证临时处理器已注销（通过检查 handler_registry 中无处理器）
    assert handler_registry.handlers_count == 0


@pytest.mark.asyncio
async def test_request_fails_immediately_if_response_event_not_compliant(
    mock_bus_proxy: EventBus.Proxy,
) -> None:
    """如果响应事件的 payload_type 不继承 ResponseProtocol，应在发布前立即抛出 TypeError"""
    with pytest.raises(TypeError):
        await request(
            bus_proxy=mock_bus_proxy,
            req_event='test.request',
            req_data={'data': 'test'},
            resp_event='test.bad_response',  # BadResponsePayload 不继承 ResponseProtocol
        )
    # 确保从未尝试发布事件
    _publish_mock(mock_bus_proxy).assert_not_called()


@pytest.mark.asyncio
async def test_handler_rejects_malformed_response_payload_at_runtime(
    mock_bus_proxy: EventBus.Proxy,
    event_registry: EventRegistry,
    handler_registry: EventHandlerRegistry,
) -> None:
    """当收到 payload 不是 ResponseProtocol 实例时（尽管声明合法），请求应失败"""
    req_task = asyncio.create_task(
        request(
            bus_proxy=mock_bus_proxy,
            req_event='test.request',
            req_data={'data': 'test'},
            resp_event='test.response',  # 声明合法（TestResponsePayload 继承 ResponseProtocol）
        )
    )

    await asyncio.sleep(0.1)
    # 提取发布时的 session_id 和 request_id
    call_args = _publish_mock(mock_bus_proxy).call_args
    published_payload = call_args[0][1]
    session_id = published_payload['session_id']
    request_id = published_payload['request_id']

    # 构造一个不继承 ResponseProtocol 的 payload（类型错误）
    bad_payload = BadResponsePayload(
        session_id=session_id,
        request_id=request_id,
        some_field=42,
    )
    # 手动触发响应，绕过总线类型检查
    await trigger_response(event_registry, handler_registry, 'test.response', bad_payload)

    # 请求任务应抛出 TypeError
    with pytest.raises(TypeError):
        await req_task


@pytest.mark.asyncio
async def test_request_session_mismatch_ignored(
    mock_bus_proxy: EventBus.Proxy, event_registry: EventRegistry, handler_registry: EventHandlerRegistry
) -> None:
    """响应中的 session_id 或 request_id 不匹配时应被忽略，请求继续等待（直至超时）"""
    req_task: asyncio.Task[ResponseProtocol] = asyncio.create_task(
        request(
            bus_proxy=mock_bus_proxy,
            req_event='test.request',
            req_data={'data': 'test'},
            resp_event='test.response',
            timeout=0.2,
        )
    )

    await asyncio.sleep(0.01)
    call_args = _publish_mock(mock_bus_proxy).call_args
    published_data = call_args[0][1]
    real_session = published_data['session_id']
    real_req_id = published_data['request_id']

    # 发送一个 session_id 不匹配的响应
    bad_resp1 = SimpleResponsePayload(
        session_id='wrong-session',
        request_id=real_req_id,
        success=True,
        result='ignored',
    )
    await trigger_response(event_registry, handler_registry, 'test.response', bad_resp1)

    # 发送一个 request_id 不匹配的响应
    bad_resp2 = SimpleResponsePayload(
        session_id=real_session,
        request_id='wrong-request-id',
        success=True,
        result='ignored',
    )
    await trigger_response(event_registry, handler_registry, 'test.response', bad_resp2)

    # 请求应超时，因为正确的响应未到达
    with pytest.raises(asyncio.TimeoutError):
        await req_task


@pytest.mark.asyncio
async def test_request_unregistered_event(mock_bus_proxy: EventBus.Proxy) -> None:
    """使用未注册的事件应抛出 ValueError"""
    with pytest.raises(ValueError):
        await request(
            bus_proxy=mock_bus_proxy,
            req_event='nonexistent.request',
            req_data={'data': 'test'},
            resp_event='test.response',
        )
    with pytest.raises(ValueError):
        await request(
            bus_proxy=mock_bus_proxy,
            req_event='test.request',
            req_data={'data': 'test'},
            resp_event='nonexistent.response',
        )


@pytest.mark.asyncio
async def test_request_no_timeout(
    mock_bus_proxy: EventBus.Proxy,
    event_registry: EventRegistry,
    handler_registry: EventHandlerRegistry,
) -> None:
    """timeout=None 时直接 await future，不经过 asyncio.wait_for 包装"""
    req_task: asyncio.Task[ResponseProtocol] = asyncio.create_task(
        request(
            bus_proxy=mock_bus_proxy,
            req_event='test.request',
            req_data={'data': 'no_timeout_test'},
            resp_event='test.response',
            timeout=None,
        )
    )

    await asyncio.sleep(0.01)
    call_args = _publish_mock(mock_bus_proxy).call_args
    published_data = call_args[0][1]
    session_id = published_data['session_id']
    request_id = published_data['request_id']

    response_payload = SimpleResponsePayload(
        session_id=session_id,
        request_id=request_id,
        success=True,
        result='no_timeout_ok',
    )
    await trigger_response(event_registry, handler_registry, 'test.response', response_payload)

    result: ResponseProtocol = await req_task
    assert isinstance(result, SimpleResponsePayload)
    assert result.result == 'no_timeout_ok'


@pytest.mark.asyncio
async def test_request_payload_not_request_protocol(
    mock_bus_proxy: EventBus.Proxy, event_registry: EventRegistry
) -> None:
    """请求事件负载不继承 RequestProtocol 应抛出 TypeError"""

    # 注册一个不继承 RequestProtocol 的事件
    class BadRequestPayload(BaseModel):
        foo: str

    class BadRequestEvent(EventDeclaration):
        name = 'bad.request'
        payload_type = BadRequestPayload

    event_registry.register(BadRequestEvent)

    with pytest.raises(TypeError, match='必须继承 RequestProtocol'):
        await request(
            bus_proxy=mock_bus_proxy,
            req_event='bad.request',
            req_data={'foo': 'bar'},
            resp_event='test.response',
        )


@pytest.mark.asyncio
async def test_response_failure_raise_if_failed(
    mock_bus_proxy: EventBus.Proxy, event_registry: EventRegistry, handler_registry: EventHandlerRegistry
) -> None:
    """响应中 success=False 时，调用 raise_if_failed 应抛出 RuntimeError"""
    req_task: asyncio.Task[ResponseProtocol] = asyncio.create_task(
        request(
            bus_proxy=mock_bus_proxy,
            req_event='test.request',
            req_data={'data': 'test'},
            resp_event='test.response',
        )
    )

    await asyncio.sleep(0.01)
    call_args = _publish_mock(mock_bus_proxy).call_args
    published_data = call_args[0][1]
    session_id = published_data['session_id']
    request_id = published_data['request_id']

    # 发送失败响应
    fail_resp = SimpleResponsePayload(
        session_id=session_id, request_id=request_id, success=False, error_msg='something went wrong', result='Error!'
    )
    await trigger_response(event_registry, handler_registry, 'test.response', fail_resp)

    result: ResponseProtocol = await req_task
    assert result.success is False
    with pytest.raises(RuntimeError, match='something went wrong'):
        result.raise_if_failed()


def test_build_response_echoes_request_ids_and_extra_fields() -> None:
    """build_response 应带回 session_id/request_id 并填充业务字段（回归：pydantic v2 model_validate 收字典）"""
    req = SimpleRequestPayload(session_id='s1', request_id='r1', data='hello')
    resp = build_response(req, SimpleResponsePayload, result='world')

    assert isinstance(resp, SimpleResponsePayload)
    assert resp.session_id == 's1'
    assert resp.request_id == 'r1'
    assert resp.success is True
    assert resp.result == 'world'


def test_build_response_overrides_conflicting_ids_from_request() -> None:
    """业务字段携带冲突的 session/request id 时，应以请求中的为准"""
    req = SimpleRequestPayload(session_id='s1', request_id='r1', data='hello')
    resp = build_response(req, SimpleResponsePayload, result='world', session_id='wrong', request_id='wrong2')

    assert resp.session_id == 's1'
    assert resp.request_id == 'r1'
