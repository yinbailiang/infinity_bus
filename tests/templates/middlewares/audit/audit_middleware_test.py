"""AuditMiddleware 队列、攒批、降级与生命周期测试。"""

import asyncio
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

import pytest
from conftest import MiddlewareTestPayload, SimplePingHandler
from pydantic import BaseModel

from infinity_bus import (
    Event,
    EventBus,
    EventHandlerRegistry,
    EventRegistry,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
    MiddlewareChain,
)
from infinity_bus.templates.middlewares.audit import (
    AuditMiddleware,
    AuditPayloadConfig,
    AuditRecord,
    AuditRecordRenderer,
    AuditSink,
    AuditWriter,
    AuditWriterConfig,
)
from infinity_bus.templates.middlewares.audit.models import ClosedState, DegradedState, RunningState


class RecordingSink(AuditSink):
    """可注入失败的内存 Sink。"""

    def __init__(self) -> None:
        self.records: list[AuditRecord] = []
        self.batches: list[list[AuditRecord]] = []
        self.opened = False
        self.open_calls = 0
        self.close_calls = 0
        self.maintain_calls = 0
        self.reclaim_calls = 0
        self.fail_open = False
        self.fail_maintain = False
        self.fail_reclaim = False
        self.fail_write = 0

    @property
    def is_open(self) -> bool:
        return self.opened

    async def open(self) -> None:
        self.open_calls += 1
        if self.fail_open:
            raise OSError('open failed')
        self.opened = True

    async def write(self, records: Sequence[AuditRecord]) -> None:
        if self.fail_write > 0:
            self.fail_write -= 1
            raise OSError('write failed')
        batch = list(records)
        self.batches.append(batch)
        self.records.extend(batch)

    async def maintain(self) -> None:
        self.maintain_calls += 1
        if self.fail_maintain:
            raise OSError('maintain failed')

    async def reclaim(self) -> None:
        self.reclaim_calls += 1
        if self.fail_reclaim:
            raise OSError('reclaim failed')

    async def close(self) -> None:
        self.close_calls += 1
        self.opened = False


async def _noop_next(event: Event) -> None:
    pass


def _event(name: str = 'mw.ping') -> Event:
    return Event(
        name=name,
        data=MiddlewareTestPayload(key='k', count=1),
        sources=['src'],
        timestamps=[datetime.now(timezone.utc)],
        event_ids=['parent'],
    )


def _middleware(
    sink: AuditSink,
    *,
    writer_config: AuditWriterConfig | None = None,
    payload_config: AuditPayloadConfig | None = None,
) -> AuditMiddleware:
    """按显式组装方式创建 writer + 门面中间件。"""
    writer = AuditWriter(sink, AuditRecordRenderer(payload_config), writer_config)
    return AuditMiddleware(writer)


@pytest.fixture
def bus(base_event_registry: EventRegistry, handler_registry: EventHandlerRegistry) -> EventBus:
    """未启动的总线实例，仅用于调用中间件生命周期钩子。"""
    return EventBus(
        base_event_registry,
        handler_registry,
        queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
    )


@pytest.mark.asyncio
async def test_published_event_is_recorded(
    base_event_registry: EventRegistry,
    handler_registry: EventHandlerRegistry,
) -> None:
    """总线发布的事件以 digest 模式进入审计流。"""
    sink = RecordingSink()
    middleware = _middleware(sink, writer_config=AuditWriterConfig(flush_interval=0))
    chain = MiddlewareChain()
    await chain.add(middleware)
    event_bus = EventBus(
        base_event_registry,
        handler_registry,
        queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
        middleware_chain=chain,
    )
    async with event_bus:
        await event_bus.proxy('src').publish('mw.ping', {'key': 'k', 'count': 1})
        await middleware.flush()
        records = [record for record in sink.records if record.name == 'mw.ping']
        assert len(records) == 1
        assert records[0].service == 'src'
        assert records[0].payload_sha256 is not None
        assert records[0].data is None
        assert middleware.sink is sink
        assert middleware.state.written_count >= 1


@pytest.mark.asyncio
async def test_config_controls_payload_mode(bus: EventBus) -> None:
    """中间件使用 config 中的 payload_mode。"""
    sink = RecordingSink()
    middleware = _middleware(
        sink,
        writer_config=AuditWriterConfig(flush_interval=0),
        payload_config=AuditPayloadConfig(payload_mode='full'),
    )
    await middleware.on_setup(bus)
    await middleware.on_publish(_event(), _noop_next)
    await middleware.flush()
    assert len(sink.records) == 1
    assert sink.records[0].data is not None
    await middleware.on_teardown(bus)


@pytest.mark.asyncio
async def test_publish_error_is_recorded(
    base_event_registry: EventRegistry,
    handler_registry: EventHandlerRegistry,
) -> None:
    """发布失败写入带 error 的审计记录。"""
    sink = RecordingSink()
    middleware = _middleware(sink, writer_config=AuditWriterConfig(flush_interval=0))
    chain = MiddlewareChain()
    await chain.add(middleware)
    event_bus = EventBus(
        base_event_registry,
        handler_registry,
        queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
        middleware_chain=chain,
    )
    async with event_bus:
        with pytest.raises(ValueError):
            await event_bus.proxy('src').publish('missing.event')
        await middleware.flush()
        errors = [record for record in sink.records if record.error]
        assert len(errors) == 1
        assert 'ValueError' in (errors[0].error or '')
        assert errors[0].event_id == 'ERROR'


@pytest.mark.asyncio
async def test_batching_collects_records(bus: EventBus) -> None:
    """在 flush_interval 内攒满 batch_size 后一次写入。"""
    sink = RecordingSink()
    middleware = _middleware(sink, writer_config=AuditWriterConfig(batch_size=3, flush_interval=0.05))
    await middleware.on_setup(bus)
    for _ in range(3):
        await middleware.on_publish(_event(), _noop_next)
    await middleware.flush()
    assert len(sink.batches) == 1
    assert len(sink.batches[0]) == 3
    await middleware.on_teardown(bus)


@pytest.mark.asyncio
async def test_teardown_drains_queue(bus: EventBus) -> None:
    """停机前排空队列并关闭 Sink。"""
    sink = RecordingSink()
    middleware = _middleware(sink, writer_config=AuditWriterConfig(batch_size=10, flush_interval=0))
    await middleware.on_setup(bus)
    for _ in range(5):
        await middleware.on_publish(_event(), _noop_next)
    await middleware.on_teardown(bus)
    assert len(sink.records) == 5
    assert sink.close_calls == 1
    assert not sink.is_open
    assert isinstance(middleware.state, ClosedState)


@pytest.mark.asyncio
async def test_publish_after_teardown_is_dropped(bus: EventBus) -> None:
    """teardown 之后仍在飞行的调用被丢弃并计数。"""
    sink = RecordingSink()
    middleware = _middleware(sink)
    await middleware.on_setup(bus)
    await middleware.on_teardown(bus)
    await middleware.on_publish(_event(), _noop_next)
    assert middleware.state.dropped_count == 1
    assert isinstance(middleware.state, ClosedState)


@pytest.mark.asyncio
async def test_queue_full_drops_newest() -> None:
    """队列满时丢弃最新记录并计数。"""
    sink = RecordingSink()
    middleware = _middleware(sink, writer_config=AuditWriterConfig(queue_maxsize=1))
    await middleware.on_publish(_event(), _noop_next)
    await middleware.on_publish(_event(), _noop_next)
    assert middleware.writer.queue_size == 1
    assert middleware.state.dropped_count == 1


@pytest.mark.asyncio
async def test_write_failure_falls_back_and_recovers(
    base_event_registry: EventRegistry,
    handler_registry: EventHandlerRegistry,
) -> None:
    """写失败走 fallback，退避后自动重连恢复。"""
    sink = RecordingSink()
    sink.fail_write = 1
    middleware = _middleware(
        sink,
        writer_config=AuditWriterConfig(flush_interval=0, reconnect_backoff=0.01),
    )
    chain = MiddlewareChain()
    await chain.add(middleware)
    event_bus = EventBus(
        base_event_registry,
        handler_registry,
        queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
        middleware_chain=chain,
    )
    async with event_bus:
        await event_bus.proxy('src').publish('mw.ping', {'key': 'k', 'count': 1})
        await middleware.flush()
        assert isinstance(middleware.state, DegradedState)
        assert middleware.state.write_error_count == 1
        assert middleware.state.fallback_count == 1
        assert sink.reclaim_calls == 1

        await asyncio.sleep(0.02)
        await event_bus.proxy('src').publish('mw.ping', {'key': 'k', 'count': 2})
        await middleware.flush()
        assert isinstance(middleware.state, RunningState)
        assert len([record for record in sink.records if record.name == 'mw.ping']) == 1
        assert sink.open_calls >= 2


@pytest.mark.asyncio
async def test_open_failure_degrades_then_recovers(
    base_event_registry: EventRegistry,
    handler_registry: EventHandlerRegistry,
) -> None:
    """启动打开失败不阻断总线，退避后恢复。"""
    sink = RecordingSink()
    sink.fail_open = True
    middleware = _middleware(
        sink,
        writer_config=AuditWriterConfig(flush_interval=0, reconnect_backoff=0.01),
    )
    chain = MiddlewareChain()
    await chain.add(middleware)
    event_bus = EventBus(
        base_event_registry,
        handler_registry,
        queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
        middleware_chain=chain,
    )
    async with event_bus:
        assert isinstance(middleware.state, DegradedState)
        await event_bus.proxy('src').publish('mw.ping', {'key': 'k', 'count': 1})
        await middleware.flush()
        assert middleware.state.fallback_count == 1

        sink.fail_open = False
        await asyncio.sleep(0.02)
        await event_bus.proxy('src').publish('mw.ping', {'key': 'k', 'count': 2})
        await middleware.flush()
        assert isinstance(middleware.state, RunningState)
        assert len([record for record in sink.records if record.name == 'mw.ping']) == 1


@pytest.mark.asyncio
async def test_reclaim_failure_still_degrades(
    base_event_registry: EventRegistry,
    handler_registry: EventHandlerRegistry,
) -> None:
    """强制回收失败不影响降级与 fallback。"""
    sink = RecordingSink()
    sink.fail_write = 1
    sink.fail_reclaim = True
    middleware = _middleware(sink, writer_config=AuditWriterConfig(flush_interval=0))
    chain = MiddlewareChain()
    await chain.add(middleware)
    event_bus = EventBus(
        base_event_registry,
        handler_registry,
        queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
        middleware_chain=chain,
    )
    async with event_bus:
        await event_bus.proxy('src').publish('mw.ping', {'key': 'k', 'count': 1})
        await middleware.flush()
        assert isinstance(middleware.state, DegradedState)
        assert middleware.state.fallback_count == 1
        assert sink.reclaim_calls == 1


@pytest.mark.asyncio
async def test_periodic_maintenance_runs_without_events(bus: EventBus) -> None:
    """没有事件时也按 maintenance_interval 执行清理。"""
    sink = RecordingSink()
    middleware = _middleware(sink, writer_config=AuditWriterConfig(maintenance_interval=0.01))
    await middleware.on_setup(bus)
    await asyncio.sleep(0.05)
    assert sink.maintain_calls >= 1
    await middleware.on_teardown(bus)


@pytest.mark.asyncio
async def test_periodic_maintenance_reopens_closed_sink(bus: EventBus) -> None:
    """写失败关闭 Sink 后，维护任务可独立重开并继续清理。"""
    sink = RecordingSink()
    sink.fail_open = True
    middleware = _middleware(sink, writer_config=AuditWriterConfig(maintenance_interval=0.01))
    await middleware.on_setup(bus)
    assert not sink.is_open
    sink.fail_open = False
    await asyncio.sleep(0.05)
    assert sink.is_open
    assert sink.maintain_calls >= 1
    await middleware.on_teardown(bus)


@pytest.mark.asyncio
async def test_maintain_failure_is_isolated(bus: EventBus) -> None:
    """维护失败只记录日志，不影响写入。"""
    sink = RecordingSink()
    sink.fail_maintain = True
    middleware = _middleware(sink, writer_config=AuditWriterConfig(flush_interval=0))
    await middleware.on_setup(bus)
    await middleware.on_publish(_event(), _noop_next)
    await middleware.flush()
    assert middleware.state.written_count == 1
    assert sink.maintain_calls == 1
    await middleware.on_teardown(bus)


@pytest.mark.asyncio
async def test_flush_before_setup_returns() -> None:
    """未启动时 flush 直接返回。"""
    middleware = _middleware(RecordingSink())
    await middleware.flush()


@pytest.mark.asyncio
async def test_before_publish_propagates(
    base_event_registry: EventRegistry,
) -> None:
    """before_publish 原样传递到下一个中间件。"""
    middleware = _middleware(RecordingSink())
    called: list[str] = []

    async def final(
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
    ) -> None:
        called.append(name)

    await middleware.before_publish(base_event_registry, 'mw.ping', 'src', {'key': 'k'}, None, final)
    assert called == ['mw.ping']


def test_middleware_exposes_writer() -> None:
    """门面中间件暴露显式组装的 writer / sink。"""
    sink = RecordingSink()
    writer = AuditWriter(sink, AuditRecordRenderer())
    middleware = AuditMiddleware(writer)
    assert middleware.writer is writer
    assert middleware.sink is sink


@pytest.mark.asyncio
async def test_simple_ping_handler_is_unaffected(
    base_event_registry: EventRegistry,
    handler_registry: EventHandlerRegistry,
) -> None:
    """审计中间件不改变正常发布 / 处理流程。"""
    sink = RecordingSink()
    middleware = _middleware(sink, writer_config=AuditWriterConfig(flush_interval=0))
    chain = MiddlewareChain()
    await chain.add(middleware)
    handler = SimplePingHandler()
    await handler_registry.register(handler)
    event_bus = EventBus(
        base_event_registry,
        handler_registry,
        queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
        middleware_chain=chain,
    )
    async with event_bus:
        await event_bus.proxy('src').publish('mw.ping', {'key': 'k', 'count': 1})
        await handler.wait_received(timeout=2.0)
        await middleware.flush()
    assert len(handler.received) == 1
