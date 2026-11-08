"""AuditWriter 直接测试：显式组装的写入器生命周期。"""

from collections.abc import Sequence
from datetime import datetime, timezone

import pytest

from infinity_bus import Event
from infinity_bus.templates.middlewares.audit import (
    AuditRecord,
    AuditRecordRenderer,
    AuditSink,
    AuditWriter,
    AuditWriterConfig,
)
from infinity_bus.templates.middlewares.audit.models import ClosedState, PendingEvent


class _Sink(AuditSink):
    """最小内存 Sink。"""

    def __init__(self) -> None:
        self.opened = False
        self.records: list[AuditRecord] = []

    @property
    def is_open(self) -> bool:
        return self.opened

    async def open(self) -> None:
        self.opened = True

    async def write(self, records: Sequence[AuditRecord]) -> None:
        self.records.extend(records)

    async def close(self) -> None:
        self.opened = False


def _event() -> Event:
    return Event(
        name='mw.ping',
        sources=['src'],
        timestamps=[datetime.now(timezone.utc)],
        event_ids=['parent'],
    )


@pytest.mark.asyncio
async def test_writer_enqueue_and_flush() -> None:
    """Writer 独立完成 enqueue、flush 与关闭。"""
    sink = _Sink()
    writer = AuditWriter(sink, AuditRecordRenderer(), AuditWriterConfig(flush_interval=0))
    await writer.start()
    writer.enqueue(PendingEvent(_event()))
    await writer.flush()
    assert len(sink.records) == 1
    assert writer.state.written_count == 1

    await writer.stop()
    assert isinstance(writer.state, ClosedState)
    assert not sink.is_open


@pytest.mark.asyncio
async def test_writer_drops_after_stop() -> None:
    """Writer 关闭后拒绝新条目并计数。"""
    sink = _Sink()
    writer = AuditWriter(sink, AuditRecordRenderer())
    await writer.start()
    await writer.stop()
    writer.enqueue(PendingEvent(_event()))
    assert writer.state.dropped_count == 1
