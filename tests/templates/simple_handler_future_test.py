"""PEP 563 前向注解下的 @handler 回归测试。"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from infinity_bus import EventDeclaration, EventHandler
from infinity_bus.templates import handler


class FuturePayload(BaseModel):
    message: str


class FutureEvent(EventDeclaration):
    name = 'test.future_annotations'
    payload_type = FuturePayload


class OtherPayload(BaseModel):
    other: int


def test_handler_accepts_future_annotations() -> None:
    """`from __future__ import annotations` 下，正确注解不应被字符串比较误伤。"""

    @handler(FutureEvent)
    async def on_event(payload: FuturePayload) -> None:
        assert payload.message

    assert issubclass(on_event, EventHandler)
    assert on_event.__name__ == 'on_event'


def test_handler_rejects_mismatched_future_annotation() -> None:
    """字符串注解解析后仍应执行类型校验，错误类型要报出具体类名。"""

    async def bad_handler(payload: OtherPayload) -> None:
        pass

    with pytest.raises(TypeError, match='FuturePayload'):
        handler(FutureEvent)(bad_handler)


def test_handler_accepts_local_future_annotation() -> None:
    """函数内定义的局部负载类也能通过名称回退匹配。"""

    class LocalPayload(BaseModel):
        message: str

    class LocalEvent(EventDeclaration):
        name = 'test.local_future_annotations'
        payload_type = LocalPayload

    @handler(LocalEvent)
    async def on_event(payload: LocalPayload) -> None:
        assert payload.message

    assert issubclass(on_event, EventHandler)
    assert on_event.__name__ == 'on_event'
