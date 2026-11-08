"""审计日志存储后端接口。"""

from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence

from ..record import AuditRecord

LogFallback = Callable[[str], None]
"""降级回调：接收一条 JSON 序列化的审计记录。"""


class AuditSink(ABC):
    """审计日志存储后端抽象。

    实现方只需关心单批记录的持久化；重连、退避、队列与降级由
    :class:`~infinity_bus.templates.middlewares.audit.middleware.AuditMiddleware` 统一负责。
    """

    @property
    @abstractmethod
    def is_open(self) -> bool:
        """后端是否处于可用状态。"""

    @abstractmethod
    async def open(self) -> None:
        """打开后端；必须幂等。"""

    @abstractmethod
    async def write(self, records: Sequence[AuditRecord]) -> None:
        """持久化一批记录；返回即表示已落库，失败时抛错。"""

    async def maintain(self) -> None:
        """执行周期性维护（保留策略 / 清理）；默认无操作。"""
        pass

    async def reclaim(self) -> None:
        """立即回收存储空间，调用方期望跳过内部限频；默认无操作。

        写入失败时由中间件调用，用于处理磁盘已满等场景。
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """关闭后端并释放资源；必须幂等，关闭后 ``is_open`` 为 ``False``。"""
