"""统一审计日志中间件：有界队列 + 攒批 + 退避重连。"""

import asyncio
import json
import logging
import time
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final

from pydantic import BaseModel

from infinity_bus import (
    BeforePublishNext,
    Event,
    EventBus,
    EventRegistry,
    Middleware,
    OnPublishNext,
)

from .record import (
    AuditRecord,
    PayloadMode,
    build_error_record,
    build_record,
    validate_extra_fields,
)
from .sink import AuditSink, LogFallback

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _PendingEvent:
    event: Event


@dataclass(slots=True)
class _PendingError:
    error: Exception
    name: str
    source: str
    data: dict[str, Any] | BaseModel | None


class _StopSentinel:
    __slots__ = ()


_STOP: Final = _StopSentinel()

_Pending = _PendingEvent | _PendingError
_QueueItem = _Pending | _StopSentinel


class AuditMiddleware(Middleware):
    """把事件发布记录写入可插拔的 :class:`AuditSink`。

    必须挂在 ``on_publish``（发射侧）：每次发布恰好记录一次；挂到
    ``before_dispatch`` 会按 fanout 成员数重复记录。

    发布路径只做一次 ``put_nowait``；记录构建、攒批、写库与保留策略
    全部在后台任务中执行。队列满时丢弃并计数，绝不阻塞发布。
    """

    def __init__(
        self,
        sink: AuditSink,
        *,
        batch_size: int = 100,
        flush_interval: float = 0.5,
        queue_maxsize: int = 10_000,
        payload_mode: PayloadMode = 'digest',
        payload_allowlist: Collection[str] = (),
        payload_max_bytes: int | None = 64 * 1024,
        fallback: LogFallback | None = None,
        extra_fields: Mapping[str, Any] | None = None,
        reconnect_backoff: float = 1.0,
        reconnect_backoff_max: float = 30.0,
        close_timeout: float = 5.0,
    ) -> None:
        if batch_size < 1:
            raise ValueError('batch_size 必须为正整数')
        if flush_interval < 0:
            raise ValueError('flush_interval 不能为负')
        if queue_maxsize < 1:
            raise ValueError('queue_maxsize 必须为正整数')
        if payload_max_bytes is not None and payload_max_bytes < 1:
            raise ValueError('payload_max_bytes 必须为正整数或 None')
        if reconnect_backoff <= 0:
            raise ValueError('reconnect_backoff 必须为正数')
        if reconnect_backoff_max < reconnect_backoff:
            raise ValueError('reconnect_backoff_max 不能小于 reconnect_backoff')
        if close_timeout <= 0:
            raise ValueError('close_timeout 必须为正数')
        extra = dict(extra_fields or {})
        validate_extra_fields(extra)

        self._sink = sink
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._payload_mode: PayloadMode = payload_mode
        self._payload_allowlist: frozenset[str] = frozenset(payload_allowlist)
        self._payload_max_bytes = payload_max_bytes
        self._fallback: LogFallback = fallback or self._default_fallback
        self._extra_fields = extra
        self._reconnect_backoff = reconnect_backoff
        self._reconnect_backoff_max = reconnect_backoff_max
        self._close_timeout = close_timeout

        self._queue: asyncio.Queue[_QueueItem] = asyncio.Queue(maxsize=queue_maxsize)
        self._writer_task: asyncio.Task[None] | None = None
        self._closing = False
        self._degraded = False
        self._next_retry_at = 0.0
        self._backoff = reconnect_backoff
        self._written_count = 0
        self._dropped_count = 0
        self._write_error_count = 0
        self._fallback_count = 0

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def on_setup(self, bus: EventBus) -> None:
        """打开 Sink 并启动后台写入任务；打开失败时降级并等待重连。"""
        self._closing = False
        try:
            await self._sink.open()
        except ImportError:
            raise
        except Exception:
            logger.exception('审计 Sink 打开失败，进入降级模式: %s', type(self._sink).__name__)
            self._degraded = True
            self._schedule_retry()
        else:
            self._degraded = False
            self._next_retry_at = 0.0
            self._backoff = self._reconnect_backoff
        if self._writer_task is None or self._writer_task.done():
            self._writer_task = asyncio.create_task(self._writer_loop())

    async def on_teardown(self, bus: EventBus) -> None:
        """排空队列并关闭 Sink；可重复调用。"""
        self._closing = True
        task = self._writer_task
        self._writer_task = None
        if task is not None and not task.done():
            try:
                self._queue.put_nowait(_STOP)
            except asyncio.QueueFull:
                pass
            try:
                await asyncio.wait_for(self._queue.join(), timeout=self._close_timeout)
            except TimeoutError:
                logger.warning(
                    '审计队列在 %.1fs 内未排空，仍有 %d 条待写入',
                    self._close_timeout,
                    self._queue.qsize(),
                )
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception('审计写入任务异常退出')
        try:
            await self._sink.close()
        except Exception:
            logger.exception('关闭审计 Sink 失败')

    # ------------------------------------------------------------------
    # 钩子
    # ------------------------------------------------------------------

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
        """把已入队事件交给后台写入队列。"""
        self._enqueue(_PendingEvent(event))
        await next(event)

    async def on_publish_error(
        self,
        error: Exception,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> None:
        """把发布失败也记入审计流。"""
        self._enqueue(_PendingError(error=error, name=name, source=source, data=data))

    # ------------------------------------------------------------------
    # 观测
    # ------------------------------------------------------------------

    async def flush(self) -> None:
        """等待当前队列中的记录全部写入或降级。"""
        task = self._writer_task
        if task is None or task.done():
            return
        await self._queue.join()

    @property
    def sink(self) -> AuditSink:
        """底层存储后端。"""
        return self._sink

    @property
    def degraded(self) -> bool:
        """当前是否处于「存储不可用、只走 fallback」状态。"""
        return self._degraded

    @property
    def written_count(self) -> int:
        """已成功写入 Sink 的记录数。"""
        return self._written_count

    @property
    def dropped_count(self) -> int:
        """因队列满或关闭后调用而丢弃的记录数。"""
        return self._dropped_count

    @property
    def write_error_count(self) -> int:
        """Sink 写入失败次数（按批次计）。"""
        return self._write_error_count

    @property
    def fallback_count(self) -> int:
        """转交 fallback 的记录数。"""
        return self._fallback_count

    @property
    def queue_size(self) -> int:
        """当前待写入队列长度。"""
        return self._queue.qsize()

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------

    @staticmethod
    def _default_fallback(line: str) -> None:
        logger.warning('Audit fallback: %s', line)

    def _enqueue(self, item: _Pending) -> None:
        if self._closing:
            self._dropped_count += 1
            return
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            self._dropped_count += 1
            if self._dropped_count == 1 or self._dropped_count % 1000 == 0:
                logger.warning('审计队列已满，累计丢弃 %d 条记录', self._dropped_count)

    async def _writer_loop(self) -> None:
        while True:
            if self._closing and self._queue.empty():
                return
            item = await self._queue.get()
            if isinstance(item, _StopSentinel):
                self._queue.task_done()
                return
            batch: list[_Pending] = [item]
            stop = False
            try:
                stop = await self._collect(batch)
            except Exception:
                logger.exception('审计批次收集失败')
            try:
                await self._deliver(batch)
            except Exception:
                logger.exception('审计批次写入失败')
            finally:
                for _ in batch:
                    self._queue.task_done()
            if stop:
                return

    async def _collect(self, batch: list[_Pending]) -> bool:
        """在 ``flush_interval`` 内把队列中的条目并入当前批次。"""
        deadline = asyncio.get_running_loop().time() + self._flush_interval
        while len(batch) < self._batch_size:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=remaining)
            except TimeoutError:
                break
            if isinstance(item, _StopSentinel):
                self._queue.task_done()
                return True
            batch.append(item)
        return False

    async def _deliver(self, batch: Sequence[_Pending]) -> None:
        records: list[AuditRecord] = []
        for item in batch:
            try:
                records.append(self._render(item))
            except Exception:
                logger.exception('构建审计记录失败，丢弃该条')
        if not records:
            return
        if self._degraded:
            if time.monotonic() < self._next_retry_at:
                self._fallback_records(records)
                return
            try:
                await self._sink.open()
            except Exception:
                logger.warning('审计 Sink 重连失败，继续退避', exc_info=True)
                self._schedule_retry()
                self._fallback_records(records)
                return
            self._degraded = False
            self._backoff = self._reconnect_backoff
            self._next_retry_at = 0.0
            logger.info('审计 Sink 已恢复: %s', type(self._sink).__name__)
        try:
            await self._sink.write(records)
        except Exception:
            self._write_error_count += 1
            logger.exception('审计 Sink 写入失败，降级本批 %d 条记录', len(records))
            try:
                await self._sink.close()
            except Exception:
                logger.exception('写入失败后关闭审计 Sink 失败')
            self._degraded = True
            self._schedule_retry()
            self._fallback_records(records)
            return
        self._written_count += len(records)
        self._backoff = self._reconnect_backoff
        try:
            await self._sink.maintain()
        except Exception:
            logger.exception('审计 Sink 维护失败')

    def _render(self, item: _Pending) -> AuditRecord:
        if isinstance(item, _PendingEvent):
            return build_record(
                item.event,
                payload_mode=self._payload_mode,
                payload_allowlist=self._payload_allowlist,
                payload_max_bytes=self._payload_max_bytes,
                extra_fields=self._extra_fields,
            )
        return build_error_record(
            item.error,
            name=item.name,
            source=item.source,
            data=item.data,
            payload_mode=self._payload_mode,
            payload_allowlist=self._payload_allowlist,
            payload_max_bytes=self._payload_max_bytes,
            extra_fields=self._extra_fields,
        )

    def _schedule_retry(self) -> None:
        self._next_retry_at = time.monotonic() + self._backoff
        self._backoff = min(self._backoff * 2, self._reconnect_backoff_max)

    def _fallback_records(self, records: Sequence[AuditRecord]) -> None:
        for record in records:
            self._fallback_count += 1
            try:
                self._fallback(json.dumps(record.to_json_dict(), ensure_ascii=False))
            except Exception:
                logger.exception('审计 fallback 回调失败')
