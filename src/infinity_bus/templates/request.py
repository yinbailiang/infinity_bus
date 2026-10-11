"""请求-响应模板：在事件总线上实现同步风格的异步 RPC 调用。"""

import asyncio
import logging
import uuid
from typing import Any, Dict, Optional, Type, cast

from pydantic import BaseModel, Field

from .. import Event, EventBus, EventDeclaration
from .expect import expect

logger = logging.getLogger(__name__)


# ---------- 基础协议 ----------
class RequestProtocol(BaseModel):
    """请求协议基类，业务请求 Payload 须继承此类。"""

    session_id: str = Field(description='会话ID')
    request_id: str = Field(description='请求ID')


class ResponseProtocol(BaseModel):
    """响应协议基类，业务响应 Payload 须继承此类。"""

    session_id: str = Field(description='会话ID')
    request_id: str = Field(description='请求ID')
    success: bool = Field(default=True, description='操作是否成功')
    error_msg: Optional[str] = Field(default=None, description='失败时的错误信息')

    def raise_if_failed(self) -> None:
        """若响应失败则抛出 RuntimeError。"""
        if not self.success and self.error_msg:
            raise RuntimeError(self.error_msg)


def build_response(request: RequestProtocol, response_type: type[ResponseProtocol], **kargs: Any) -> ResponseProtocol:
    """基于请求构造响应负载：自动带回 session_id/request_id，其余字段由 kargs 提供。

    ``kargs`` 中若携带冲突的 session_id/request_id 会被请求值覆盖（请求是唯一事实源）。
    """
    kargs = kargs.copy()
    kargs['session_id'] = request.session_id
    kargs['request_id'] = request.request_id
    return response_type.model_validate(kargs)


async def request(
    bus_proxy: EventBus.Proxy,
    req_event: str,
    req_data: Dict[str, Any],
    resp_event: str,
    session_id: Optional[str] = None,
    timeout: Optional[float] = 60.0,
) -> ResponseProtocol:
    """发布请求事件并等待匹配的响应，实现事件总线上的 RPC 调用。

    自动注入 session_id 和 request_id，通过 ``expect`` 等待匹配的响应事件。
    """
    req_decl: Optional[Type[EventDeclaration]] = bus_proxy.events_registry.get(req_event)
    if req_decl is None:
        raise ValueError(f"请求事件 '{req_event}' 未注册")
    if req_decl.payload_type is None or not issubclass(req_decl.payload_type, RequestProtocol):
        raise TypeError(f"请求事件 '{req_event}' 负载必须继承 RequestProtocol")

    resp_decl: Optional[Type[EventDeclaration]] = bus_proxy.events_registry.get(resp_event)
    if resp_decl is None:
        raise ValueError(f"响应事件 '{resp_event}' 未注册")
    if resp_decl.payload_type is None or not issubclass(resp_decl.payload_type, ResponseProtocol):
        raise TypeError(f"响应事件 '{resp_event}' 负载必须继承 ResponseProtocol")

    payload_data: Dict[str, Any] = req_data.copy()
    session_id = session_id if session_id is not None else uuid.uuid4().hex
    request_id: str = uuid.uuid4().hex
    payload_data['session_id'] = session_id
    payload_data['request_id'] = request_id

    def response_filter(event: Event) -> bool:
        """按 session_id + request_id 精确匹配响应事件。

        若 payload 类型不匹配，抛出 TypeError 以便通过 ``expect`` 的 future
        立即传播错误，而非让调用方等待超时。
        """
        payload: Optional[BaseModel] = event.data
        if not isinstance(payload, ResponseProtocol):
            # 此分支不应当发生，防御，防止总线带病
            raise TypeError(
                f'响应 payload 应为 ResponseProtocol 子类，实际为 {type(payload).__name__}。'
                f'请确认 resp_event="{resp_event}" 的 payload_type 继承自 ResponseProtocol。'
            )
        return payload.session_id == session_id and payload.request_id == request_id

    async with expect(
        bus_proxy=bus_proxy,
        event_patterns=resp_event,
        filter_func=response_filter,
    ) as future:
        await bus_proxy.publish(req_event, payload_data)

        if timeout is None:
            resp: Event = await future
        else:
            resp: Event = await asyncio.wait_for(future, timeout=timeout)

    assert resp.data is not None, 'Unexpected None response'
    return cast(ResponseProtocol, resp.data)
