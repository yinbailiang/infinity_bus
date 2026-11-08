"""审计中间件状态机：纯状态迁移与决策，不执行 I/O。"""

import time
from collections.abc import Callable

from .config import AuditWriterConfig
from .models import (
    AuditState,
    ClosedState,
    ClosingState,
    DegradedState,
    RunningState,
)


class AuditStateMachine:
    """审计写入状态机。

    只负责状态迁移、计数器归属与重试决策；队列、Sink I/O 与任务生命周期
    由 :class:`~infinity_bus.templates.middlewares.audit.writer.AuditWriter`
    负责。时钟可注入，便于同步测试退避行为。
    """

    def __init__(
        self,
        config: AuditWriterConfig,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._config = config
        self._clock = clock
        self._state: AuditState = RunningState()

    @property
    def state(self) -> AuditState:
        """当前状态对象。"""
        return self._state

    def begin_setup(self) -> None:
        """开始 setup：恢复为可接受记录的运行状态。"""
        self._state = self._state.inherit(RunningState())

    def setup_opened(self) -> None:
        """Sink 打开成功：进入运行状态。"""
        self._state = self._state.inherit(RunningState())

    def setup_failed(self) -> None:
        """Sink 打开失败：进入降级状态并安排首次重试。"""
        self._state = self._schedule_retry(self._state)

    def begin_teardown(self) -> None:
        """开始停机：拒绝新记录。"""
        self._state = self._state.inherit(ClosingState())

    def teardown_finished(self) -> None:
        """停机完成。"""
        self._state = self._state.inherit(ClosedState())

    def accepts_records(self) -> bool:
        """是否仍接受新记录。"""
        match self._state:
            case ClosingState() | ClosedState():
                return False
            case _:
                return True

    def writer_should_stop(self, *, queue_empty: bool) -> bool:
        """写入任务是否应退出。"""
        match self._state:
            case ClosingState() | ClosedState():
                return queue_empty
            case _:
                return False

    def retry_due(self) -> bool:
        """降级状态下是否已到重试时刻；非降级状态恒为可写。"""
        match self._state:
            case DegradedState(next_retry_at=retry_at):
                return self._clock() >= retry_at
            case _:
                return True

    def record_dropped(self) -> None:
        """记录一条因停机或队列满而丢弃的记录。"""
        self._state.dropped_count += 1

    def record_written(self, count: int) -> None:
        """记录成功写入的记录数。"""
        self._state.written_count += count

    def record_fallback(self, count: int = 1) -> None:
        """记录转交 fallback 的记录数。"""
        self._state.fallback_count += count

    def write_failed(self) -> None:
        """写入失败：累计错误并进入降级状态。"""
        self._state.write_error_count += 1
        self._state = self._schedule_retry(self._state)

    def retry_succeeded(self) -> None:
        """重连成功：恢复运行状态。"""
        self._state = self._state.inherit(RunningState())

    def retry_failed(self) -> None:
        """重连失败：继续退避。"""
        self._state = self._schedule_retry(self._state)

    def retry_ready(self) -> None:
        """维护任务重开 Sink 后，让降级状态立即可重试但保留退避值。"""
        match self._state:
            case DegradedState(backoff=backoff):
                self._state = self._state.inherit(DegradedState(next_retry_at=0.0, backoff=backoff))
            case _:
                pass

    def _schedule_retry(self, state: AuditState) -> DegradedState:
        match state:
            case DegradedState(backoff=backoff):
                wait = backoff
            case _:
                wait = self._config.reconnect_backoff
        next_backoff = min(wait * 2, self._config.reconnect_backoff_max)
        return state.inherit(
            DegradedState(
                next_retry_at=self._clock() + wait,
                backoff=next_backoff,
            )
        )
