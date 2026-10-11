import logging
from typing import Any, Callable, Dict, Set

from pydantic import BaseModel

from infinity_bus import (
    BeforePublishNext,
    Event,
    EventBus,
    EventRegistry,
    Middleware,
    OnPublishNext,
)

BlockPredicate = Callable[[str, Dict[str, Any] | BaseModel | None], bool]
"""屏蔽判定函数签名：(name, data) -> bool。返回 ``True`` 表示屏蔽该事件。"""

logger = logging.getLogger(__name__)


class EventBlockMiddleware(Middleware):
    """根据规则屏蔽（丢弃）特定事件，不调用下游中间件也不入队。

    典型场景
    --------
    - **功能开关**：通过配置动态开启/关闭某类事件。
    - **A/B 测试**：按用户分组过滤事件。
    - **环境隔离**：在开发环境中屏蔽外部通知类事件。
    - **噪音过滤**：屏蔽高频但无业务价值的调试事件。

    参数
    ----
    block_predicate:
        判定函数，签名为 ``(name, data) -> bool``。
    block_reason:
        屏蔽时日志中包含的原因描述。
    """

    def __init__(
        self,
        block_predicate: BlockPredicate,
        *,
        block_reason: str = 'blocked by predicate',
    ) -> None:
        self._predicate = block_predicate
        self._reason = block_reason
        self._blocked_count: int = 0

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
        """判定是否屏蔽事件，屏蔽时直接返回不调用 next。"""
        if self._predicate(name, data):
            self._blocked_count += 1
            logger.debug(
                'EventBlock: 屏蔽事件 %s (reason=%s, total_blocked=%d)',
                name,
                self._reason,
                self._blocked_count,
            )
            return  # 不调用 next，事件被丢弃
        await next(event_registry, name, source, data, old_event)

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        """Propagate to next."""
        await next(event)

    @property
    def blocked_count(self) -> int:
        """累计已屏蔽事件数。"""
        return self._blocked_count


# ============================================================================
# 预置屏蔽函数
# ============================================================================


def make_blocklist_predicate(
    *event_names: str,
) -> BlockPredicate:
    """创建一个基于事件名黑名单的屏蔽判定。

    Example::

        pred = make_blocklist_predicate("debug.heartbeat", "debug.ping")
        EventBlockMiddleware(pred, block_reason="debug events disabled")
    """

    blocked: Set[str] = set(event_names)

    def _predicate(
        name: str,
        data: Dict[str, Any] | BaseModel | None,
    ) -> bool:
        return name in blocked

    return _predicate


def make_allowlist_predicate(
    *event_names: str,
) -> BlockPredicate:
    """创建一个基于事件名白名单的屏蔽判定 —— 仅允许白名单中的事件通过。

    Example::

        pred = make_allowlist_predicate("user.login", "user.logout")
        EventBlockMiddleware(pred, block_reason="not in allowlist")
    """

    allowed: Set[str] = set(event_names)

    def _predicate(
        name: str,
        data: Dict[str, Any] | BaseModel | None,
    ) -> bool:
        return name not in allowed

    return _predicate
