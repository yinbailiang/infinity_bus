"""AuditWriter 直接测试：显式组装的写入器生命周期。"""

import asyncio
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
from infinity_bus.templates.middlewares.audit.models import ClosedState, Pending, PendingEvent


class _Sink(AuditSink):
    """最小内存 Sink。"""

    def __init__(self) -> None:
        self.opened = False
        self.records: list[AuditRecord] = []
        self.maintain_calls = 0

    @property
    def is_open(self) -> bool:
        return self.opened

    async def open(self) -> None:
        self.opened = True

    async def write(self, records: Sequence[AuditRecord]) -> None:
        self.records.extend(records)

    async def maintain(self) -> None:
        self.maintain_calls += 1

    async def close(self) -> None:
        self.opened = False


def _event() -> Event:
    return Event(
        name='mw.ping',
        sources=['src'],
        timestamps=[datetime.now(timezone.utc)],
        event_ids=['parent'],
    )


def _broken_renderer(item: Pending) -> AuditRecord:
    raise ValueError('render failed')


@pytest.mark.asyncio
async def test_writer_enqueue_and_flush() -> None:
    """Writer 独立完成 enqueue、flush 与关闭。"""
    sink = _Sink()
    writer = AuditWriter(sink, AuditRecordRenderer(), AuditWriterConfig(flush_interval=0))
    await writer.start()
    writer.enqueue(PendingEvent(_event()))
    assert await writer.flush() == 0
    assert len(sink.records) == 1
    assert writer.state.written_count == 1

    await writer.stop()
    assert isinstance(writer.state, ClosedState)
    assert not sink.is_open


@pytest.mark.asyncio
async def test_flush_before_start_reports_pending_count() -> None:
    """writer 未启动时 flush 返回未处理记录数，而不是静默成功。"""
    sink = _Sink()
    writer = AuditWriter(sink, AuditRecordRenderer(), AuditWriterConfig(flush_interval=0))
    writer.enqueue(PendingEvent(_event()))
    assert await writer.flush() == 1
    assert not sink.records

    await writer.start()
    assert await writer.flush() == 0
    assert len(sink.records) == 1
    await writer.stop()


@pytest.mark.asyncio
async def test_flush_after_writer_task_done_reports_pending_count() -> None:
    """writer 任务意外结束后，flush 仍会报告队列中的未完成记录。"""
    sink = _Sink()
    writer = AuditWriter(
        sink,
        AuditRecordRenderer(),
        AuditWriterConfig(flush_interval=0, maintenance_interval=None),
    )
    await writer.start()
    task = writer._writer_task  # pyright: ignore[reportPrivateUsage]
    assert task is not None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    writer.enqueue(PendingEvent(_event()))
    assert await writer.flush() == 1
    assert not sink.records
    await writer.stop()
    assert writer.queue_size == 0


@pytest.mark.asyncio
async def test_writer_drops_after_stop() -> None:
    """Writer 关闭后拒绝新条目并计数。"""
    sink = _Sink()
    writer = AuditWriter(sink, AuditRecordRenderer())
    await writer.start()
    await writer.stop()
    writer.enqueue(PendingEvent(_event()))
    assert writer.state.dropped_count == 1


@pytest.mark.asyncio
async def test_writer_skips_maintenance_when_disabled() -> None:
    """maintenance_interval=None 时写路径不调用 maintain。"""
    sink = _Sink()
    writer = AuditWriter(
        sink,
        AuditRecordRenderer(),
        AuditWriterConfig(flush_interval=0, maintenance_interval=None),
    )
    await writer.start()
    writer.enqueue(PendingEvent(_event()))
    await writer.flush()
    assert sink.maintain_calls == 0
    await writer.stop()


@pytest.mark.asyncio
async def test_writer_counts_render_failure_as_dropped() -> None:
    """渲染失败的条目计入 dropped_count。"""
    sink = _Sink()
    writer = AuditWriter(sink, _broken_renderer, AuditWriterConfig(flush_interval=0))
    await writer.start()
    writer.enqueue(PendingEvent(_event()))
    await writer.flush()
    assert not sink.records
    assert writer.state.dropped_count == 1
    await writer.stop()


@pytest.mark.asyncio
async def test_writer_reuse_after_stop_timeout() -> None:
    """停机超时后清空残留队列与哨兵，重新启动仍可正常写入。"""
    sink = _Sink()
    writer = AuditWriter(
        sink,
        AuditRecordRenderer(),
        AuditWriterConfig(
            batch_size=100,
            flush_interval=0.05,
            queue_maxsize=1,
            close_timeout=0.01,
            maintenance_interval=None,
        ),
    )
    await writer.start()
    writer.enqueue(PendingEvent(_event()))
    await asyncio.sleep(0)  # writer 取走第一条并进入批收集
    writer.enqueue(PendingEvent(_event()))  # 队满，迫使 stop 时 STOP 入队失败
    await writer.stop()
    assert writer.queue_size == 0

    await writer.start()
    writer.enqueue(PendingEvent(_event()))
    await writer.flush()
    assert len(sink.records) == 1
    await writer.stop()
