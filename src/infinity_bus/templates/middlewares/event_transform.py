import logging
from typing import Any, Callable, Dict

from pydantic import BaseModel

from infinity_bus import (
    BeforePublishNext,
    Event,
    EventBus,
    EventRegistry,
    Middleware,
    OnPublishNext,
)

TransformFunc = Callable[
    [str, Dict[str, Any] | BaseModel | None],
    tuple[str, Dict[str, Any] | BaseModel | None],
]
"""事件转换函数签名：(name, data) -> (new_name, new_data)。"""

logger = logging.getLogger(__name__)


class EventTransformMiddleware(Middleware):
    """在 ``before_publish`` 阶段对事件名和/或负载数据进行转换。

    典型场景
    --------
    - **事件重命名**：将旧版事件名映射到新版。
    - **数据脱敏**：在持久化前移除敏感字段。
    - **数据补全**：自动注入通用字段（如 ``trace_id``、``timestamp``）。
    - **协议适配**：将外部系统的事件格式转换为内部格式。

    参数
    ----
    transform:
        转换函数，签名为 ``(name, data) -> (new_name, new_data)``。
    """

    def __init__(self, transform: TransformFunc) -> None:
        self._transform = transform

    async def on_setup(self, bus: EventBus) -> None:
        """No-op."""
        pass

    async def on_teardown(self, bus: EventBus) -> None:
        """No-op."""
        pass

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: Dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        """转换事件名和/或负载，然后调用 next。"""
        new_name, new_data = self._transform(name, data)
        await next(event_registry, new_name, source, new_data, old_event)

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        """Propagate to next."""
        await next(event)


def make_rename_transform(
    mapping: Dict[str, str],
) -> TransformFunc:
    """创建一个简单的事件重命名转换。

    Example::

        transform = make_rename_transform({"old.event": "new.event"})
        EventTransformMiddleware(transform)
    """

    def _rename(
        name: str,
        data: Dict[str, Any] | BaseModel | None,
    ) -> tuple[str, Dict[str, Any] | BaseModel | None]:
        return mapping.get(name, name), data

    return _rename


def make_field_inject_transform(
    **static_fields: Any,
) -> TransformFunc:
    """创建一个自动注入静态字段的转换。

    Example::

        transform = make_field_inject_transform(env="prod", version="1.0")
        EventTransformMiddleware(transform)
    """

    def _inject(
        name: str,
        data: Dict[str, Any] | BaseModel | None,
    ) -> tuple[str, Dict[str, Any] | BaseModel | None]:
        if isinstance(data, BaseModel):
            data = {**static_fields, **data.model_dump()}
            return name, data
        if isinstance(data, dict):
            merged = {**static_fields, **data}
            return name, merged
        return name, data

    return _inject


def make_field_redact_transform(
    *fields: str,
    replacement: str = '***',
) -> TransformFunc:
    """创建一个脱敏转换，将指定字段替换为 ``replacement``。

    Example::

        transform = make_field_redact_transform("password", "token")
        EventTransformMiddleware(transform)
    """

    def _redact(
        name: str,
        data: Dict[str, Any] | BaseModel | None,
    ) -> tuple[str, Dict[str, Any] | BaseModel | None]:
        if isinstance(data, BaseModel):
            data = data.model_dump()
        if isinstance(data, dict):
            data = data.copy()  # 避免修改调用方的原始数据
            for field in fields:
                if field in data:
                    data[field] = replacement
        return name, data

    return _redact
