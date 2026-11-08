"""审计中间件的类型定义与状态模型。"""

from dataclasses import dataclass
from typing import Any, Final, Literal, TypeVar

from pydantic import BaseModel

from infinity_bus import Event

PayloadMode = Literal['digest', 'full']
"""负载记录模式：``digest`` 只存大小与摘要，``full`` 额外存正文。"""

_StateT = TypeVar('_StateT', bound='AuditState')


@dataclass(slots=True)
class AuditState:
    """审计中间件的共享状态：跨状态保留的计数器。"""

    written_count: int = 0
    dropped_count: int = 0
    write_error_count: int = 0
    fallback_count: int = 0

    def inherit(self, state: _StateT) -> _StateT:
        """把当前共享计数复制到新状态实例。"""
        state.written_count = self.written_count
        state.dropped_count = self.dropped_count
        state.write_error_count = self.write_error_count
        state.fallback_count = self.fallback_count
        return state


@dataclass(slots=True)
class RunningState(AuditState):
    """Sink 可用，正常写入。"""


@dataclass(slots=True)
class DegradedState(AuditState):
    """Sink 不可用：记录走 fallback，并按退避重连。"""

    next_retry_at: float = 0.0
    backoff: float = 0.0


@dataclass(slots=True)
class ClosingState(AuditState):
    """停机中：拒绝新记录并排空队列。"""


@dataclass(slots=True)
class ClosedState(AuditState):
    """已关闭。"""


@dataclass(slots=True)
class PendingEvent:
    """待构建审计记录的事件。"""

    event: Event


@dataclass(slots=True)
class PendingError:
    """待构建审计记录的发布失败。"""

    error: Exception
    name: str
    source: str
    data: dict[str, Any] | BaseModel | None


class StopSentinel:
    """写入任务停止信号。"""

    __slots__ = ()


STOP: Final = StopSentinel()

Pending = PendingEvent | PendingError
QueueItem = Pending | StopSentinel
