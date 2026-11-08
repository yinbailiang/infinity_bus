"""审计写入器：有界队列 + 攒批 + 退避重连。"""

import asyncio
import json
import logging
from collections.abc import Callable, Sequence

from .config import AuditWriterConfig
from .models import STOP, AuditState, DegradedState, Pending, QueueItem, StopSentinel
from .record import AuditRecord
from .sinks.base import AuditSink, LogFallback
from .state_machine import AuditStateMachine

logger = logging.getLogger(__name__)


class AuditWriter:
    """把待写入条目批量、可靠地写入可插拔的 :class:`AuditSink`。

    由调用方显式组装：Sink、渲染器与写入配置分别注入；发布侧中间件只负责
    :meth:`enqueue`。队列、攒批、重试、状态机与维护任务都在本类中。
    """

    def __init__(
        self,
        sink: AuditSink,
        renderer: Callable[[Pending], AuditRecord],
        config: AuditWriterConfig | None = None,
    ) -> None:
        """绑定 Sink、渲染器与写入配置。"""
        self._sink = sink
        self._renderer = renderer
        self._config: AuditWriterConfig = config or AuditWriterConfig()
        self._fallback: LogFallback = self._config.fallback or self._default_fallback

        self._queue: asyncio.Queue[QueueItem] = asyncio.Queue(maxsize=self._config.queue_maxsize)
        self._sink_lock = asyncio.Lock()
        self._writer_task: asyncio.Task[None] | None = None
        self._maintenance_task: asyncio.Task[None] | None = None
        self._machine = AuditStateMachine(self._config)

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """打开 Sink 并启动后台写入 / 维护任务；打开失败时降级并等待重连。"""
        self._machine.begin_setup()
        try:
            async with self._sink_lock:
                await self._sink.open()
        except ImportError:
            raise
        except Exception:
            logger.exception('审计 Sink 打开失败，进入降级模式: %s', type(self._sink).__name__)
            self._machine.setup_failed()
        else:
            self._machine.setup_opened()
        if self._writer_task is None or self._writer_task.done():
            self._writer_task = asyncio.create_task(self._writer_loop())
        if self._config.maintenance_interval is not None and (
            self._maintenance_task is None or self._maintenance_task.done()
        ):
            self._maintenance_task = asyncio.create_task(self._maintenance_loop())

    async def stop(self) -> None:
        """排空队列并关闭 Sink；可重复调用。"""
        self._machine.begin_teardown()
        try:
            await self._stop_maintenance()
            await self._stop_writer()
            await self._close_sink()
        finally:
            self._machine.teardown_finished()

    async def _stop_maintenance(self) -> None:
        """取消并等待维护任务。"""
        maintenance = self._maintenance_task
        self._maintenance_task = None
        if maintenance is None or maintenance.done():
            return
        maintenance.cancel()
        try:
            await maintenance
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception('审计维护任务异常退出')

    async def _stop_writer(self) -> None:
        """发送停止信号、排空队列、取消写入任务并清理残留条目。"""
        task = self._writer_task
        self._writer_task = None
        if task is not None and not task.done():
            try:
                self._queue.put_nowait(STOP)
            except asyncio.QueueFull:
                pass
            await self._drain_queue()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception('审计写入任务异常退出')
        self._discard_queue()

    def _discard_queue(self) -> None:
        """丢弃停机后残留的条目，结清队列计数并清理哨兵。"""
        while True:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                if not isinstance(item, StopSentinel):
                    self._machine.record_dropped()
            finally:
                self._queue.task_done()

    async def _drain_queue(self) -> None:
        """等待队列排空，超时只记录告警。"""
        try:
            await asyncio.wait_for(self._queue.join(), timeout=self._config.close_timeout)
        except TimeoutError:
            logger.warning(
                '审计队列在 %.1fs 内未排空，仍有 %d 条待写入',
                self._config.close_timeout,
                self._queue.qsize(),
            )

    async def _close_sink(self) -> None:
        """关闭 Sink，失败只记录日志。"""
        try:
            async with self._sink_lock:
                await self._sink.close()
        except Exception:
            logger.exception('关闭审计 Sink 失败')

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
    def renderer(self) -> Callable[[Pending], AuditRecord]:
        """当前条目渲染器。"""
        return self._renderer

    @property
    def config(self) -> AuditWriterConfig:
        """当前写入配置（不可变）。"""
        return self._config

    @property
    def state(self) -> AuditState:
        """当前状态对象（计数器与降级信息都在其中）。"""
        return self._machine.state

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

    def enqueue(self, item: Pending) -> None:
        """把一条待写入条目放入有界队列；停机或队列满时丢弃并计数。"""
        if not self._machine.accepts_records():
            self._machine.record_dropped()
            return
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            self._machine.record_dropped()
            if self._machine.state.dropped_count == 1 or self._machine.state.dropped_count % 1000 == 0:
                logger.warning('审计队列已满，累计丢弃 %d 条记录', self._machine.state.dropped_count)

    async def _writer_loop(self) -> None:
        while True:
            if self._machine.writer_should_stop(queue_empty=self._queue.empty()):
                return
            item = await self._queue.get()
            if isinstance(item, StopSentinel):
                self._queue.task_done()
                return
            if await self._process_batch(item):
                return

    async def _process_batch(self, item: Pending) -> bool:
        """收集并写入一批条目；返回是否收到停止信号。"""
        batch: list[Pending] = [item]
        stop = False
        try:
            try:
                stop = await self._collect(batch)
            except Exception:
                logger.exception('审计批次收集失败')
            await self._deliver(batch)
        except Exception:
            logger.exception('审计批次写入失败')
        finally:
            for _ in batch:
                self._queue.task_done()
        return stop

    async def _maintenance_loop(self) -> None:
        """与写入解耦的周期维护：即使没有事件或写入持续失败也会运行。"""
        interval = self._config.maintenance_interval
        if interval is None:
            return
        while True:
            await asyncio.sleep(interval)
            try:
                async with self._sink_lock:
                    if not self._sink.is_open:
                        await self._sink.open()
                        if isinstance(self._machine.state, DegradedState):
                            self._machine.retry_ready()
                    await self._sink.maintain()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception('审计 Sink 定期维护失败')

    async def _collect(self, batch: list[Pending]) -> bool:
        """在 ``flush_interval`` 内把队列中的条目并入当前批次。"""
        deadline = asyncio.get_running_loop().time() + self._config.flush_interval
        while len(batch) < self._config.batch_size:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=remaining)
            except TimeoutError:
                break
            if isinstance(item, StopSentinel):
                self._queue.task_done()
                return True
            batch.append(item)
        return False

    async def _deliver(self, batch: Sequence[Pending]) -> None:
        records = self._render_batch(batch)
        if not records:
            return
        if not await self._ensure_sink_ready(records):
            return
        if not await self._write_batch(records):
            return
        if isinstance(self._machine.state, DegradedState):
            self._machine.retry_succeeded()
            logger.info('审计 Sink 已恢复: %s', type(self._sink).__name__)
        self._machine.record_written(len(records))
        if self._config.maintenance_interval is not None:
            await self._maintain_sink()

    def _render_batch(self, batch: Sequence[Pending]) -> list[AuditRecord]:
        """构建本批审计记录；单条失败只丢弃该条。"""
        records: list[AuditRecord] = []
        for item in batch:
            try:
                records.append(self._renderer(item))
            except Exception:
                self._machine.record_dropped()
                logger.exception('构建审计记录失败，丢弃该条')
        return records

    async def _ensure_sink_ready(self, records: Sequence[AuditRecord]) -> bool:
        """确保 Sink 可写；降级时负责重试或转 fallback。"""
        if not isinstance(self._machine.state, DegradedState):
            return True
        if not self._machine.retry_due():
            self._fallback_records(records)
            return False
        try:
            async with self._sink_lock:
                await self._sink.open()
        except Exception:
            logger.warning('审计 Sink 重连失败，继续退避', exc_info=True)
            self._machine.retry_failed()
            self._fallback_records(records)
            return False
        logger.info('审计 Sink 已重连，等待写入确认: %s', type(self._sink).__name__)
        return True

    async def _write_batch(self, records: Sequence[AuditRecord]) -> bool:
        """写入一批记录；失败时回收/关闭 Sink 并转 fallback。"""
        try:
            async with self._sink_lock:
                await self._sink.write(records)
        except Exception:
            logger.exception('审计 Sink 写入失败，尝试强制回收空间后降级本批 %d 条记录', len(records))
            await self._reclaim_and_close_sink()
            self._machine.write_failed()
            self._fallback_records(records)
            return False
        return True

    async def _reclaim_and_close_sink(self) -> None:
        """写入失败后的空间回收与关闭，单步失败不阻断后续降级。"""
        try:
            async with self._sink_lock:
                await self._sink.reclaim()
        except Exception:
            logger.exception('写入失败后回收审计 Sink 空间失败')
        try:
            async with self._sink_lock:
                await self._sink.close()
        except Exception:
            logger.exception('写入失败后关闭审计 Sink 失败')

    async def _maintain_sink(self) -> None:
        """写入成功后的维护，失败只记录日志。"""
        try:
            async with self._sink_lock:
                await self._sink.maintain()
        except Exception:
            logger.exception('审计 Sink 维护失败')

    def _fallback_records(self, records: Sequence[AuditRecord]) -> None:
        for record in records:
            self._machine.record_fallback()
            try:
                self._fallback(json.dumps(record.to_json_dict(), ensure_ascii=False, default=str))
            except Exception:
                logger.exception('审计 fallback 回调失败')
