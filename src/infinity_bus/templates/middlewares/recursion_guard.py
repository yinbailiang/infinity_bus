"""递归防护中间件 —— 双重检测，防止自递归和互递归。"""

import logging
from typing import Any, Dict, Optional, Set

from pydantic import BaseModel

from infinity_bus import (
    BeforePublishNext,
    Event,
    EventBus,
    EventRegistry,
    Middleware,
    OnPublishNext,
)

logger = logging.getLogger(__name__)


class RecursionDetectedError(RuntimeError):
    """事件发布递归调用被检测并拦截。"""

    pass


class RecursionGuardMiddleware(Middleware):
    """递归调用防护中间件：双重检测，防止自递归和互递归。

    检测逻辑在 ``before_publish`` 阶段执行，不消耗队列资源。

    **第一层：per-source 计数**
        同一 ``source`` 在事件链的 ``sources`` 中出现次数 ≥ ``max_depth`` 时拒绝。
        防范单模块自身递归。

    **第二层：绝对链长**
        事件链 ``event_ids`` 长度 ≥ ``max_chain_length`` 时拒绝，无论各 source
        计数如何。防范多模块互递归（K 个模块互递归可达 ``max_depth × K`` 轮）。

    参数
    ----
    max_depth:
        同一 ``source`` 在事件链中允许出现的最大次数。默认 3。
    max_chain_length:
        事件链绝对最大长度。默认 50。设为 ``None`` 禁用此层检测。
    ignore_sources:
        不参与 **per-source 计数** 检查的发布者名称集合。
        注意：不影响绝对链长检测。
    """

    def __init__(
        self,
        max_depth: int = 3,
        max_chain_length: Optional[int] = 50,
        ignore_sources: Optional[Set[str]] = None,
    ) -> None:
        self.max_depth = max_depth
        self.max_chain_length = max_chain_length  # None → 禁用链长检查
        self._ignore = ignore_sources or set()

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
        """双重递归检测：per-source 计数 + 绝对链长。"""
        if old_event is not None:
            if self.max_chain_length is not None:
                chain_len = len(old_event.event_ids) + 1  # +1 计入当前事件
                if chain_len > self.max_chain_length:
                    raise RecursionDetectedError(
                        f'Chain length exceeded: {chain_len} > {self.max_chain_length} '
                        f'(max_chain_length={self.max_chain_length})'
                    )

            if source not in self._ignore:
                count = old_event.sources.count(source) + 1
                if count > self.max_depth:
                    raise RecursionDetectedError(
                        f"Recursion detected: source '{source}' appears "
                        f'{count} times in the event chain '
                        f'(max_depth={self.max_depth})'
                    )

        await next(event_registry, name, source, data, old_event)

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        """Propagate to next."""
        await next(event)
