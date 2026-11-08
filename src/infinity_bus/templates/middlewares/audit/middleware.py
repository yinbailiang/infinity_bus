"""审计中间件门面：把发布事件转交 AuditWriter。"""

from typing import Any

from pydantic import BaseModel

from infinity_bus import (
    BeforePublishNext,
    Event,
    EventBus,
    EventRegistry,
    Middleware,
    OnPublishNext,
)

from .models import AuditState, PendingError, PendingEvent
from .sinks.base import AuditSink
from .writer import AuditWriter


class AuditMiddleware(Middleware):
    """审计门面中间件。

    只负责把发布事件 / 发布失败包装成 :class:`PendingEvent` /
    :class:`PendingError` 并交给 :class:`AuditWriter`；队列、批处理、
    重试、状态机与 Sink 生命周期全部由 writer 负责。

    由调用方显式组装：

    .. code-block:: python

        writer = AuditWriter(sink, renderer, writer_config)
        middleware = AuditMiddleware(writer)
    """

    def __init__(self, writer: AuditWriter) -> None:
        self._writer = writer

    async def on_setup(self, bus: EventBus) -> None:
        """启动底层 writer。"""
        await self._writer.start()

    async def on_teardown(self, bus: EventBus) -> None:
        """停止底层 writer。"""
        await self._writer.stop()

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        """原样传递到下一个中间件（审计在 ``on_publish`` 中记录）。"""
        await next(event_registry, name, source, data, old_event)

    async def on_publish(self, event: Event, next: OnPublishNext) -> None:
        """把已入队事件交给 writer。"""
        self._writer.enqueue(PendingEvent(event))
        await next(event)

    async def on_publish_error(
        self,
        error: Exception,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> None:
        """把发布失败也记入审计流。"""
        self._writer.enqueue(PendingError(error=error, name=name, source=source, data=data))

    async def flush(self) -> None:
        """等待 writer 队列中的记录全部写入或降级。"""
        await self._writer.flush()

    @property
    def writer(self) -> AuditWriter:
        """底层写入器。"""
        return self._writer

    @property
    def sink(self) -> AuditSink:
        """底层存储后端。"""
        return self._writer.sink

    @property
    def state(self) -> AuditState:
        """当前 writer 状态对象（计数器与降级信息都在其中）。"""
        return self._writer.state
