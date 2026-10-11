import asyncio
import logging
import time
from collections import defaultdict, deque
from typing import Any, Dict

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


class RateLimitMiddleware(Middleware):
    """基于**滑动窗口**的速率限制中间件。

    特性
    ----
    - 支持**全局限流**和**按事件名限流**两种模式。
    - 超过限制时自动丢弃事件（不调用 ``next``），并记录警告日志。
    - 纯内存实现，无外部依赖。

    参数
    ----
    max_requests:
        时间窗口内允许的最大请求数。
    window_seconds:
        滑动窗口大小（秒）。
    per_event:
        若为 ``True``，按事件名独立计数；否则全局共享一个窗口。
    """

    def __init__(
        self,
        max_requests: int = 100,
        window_seconds: float = 1.0,
        per_event: bool = False,
    ) -> None:
        self._max = max_requests
        self._window = window_seconds
        self._per_event = per_event

        # name → deque[float]
        self._buckets: Dict[str, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

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
        """滑动窗口限流检查，超限时丢弃事件。"""
        key = name if self._per_event else '__global__'
        now = time.monotonic()

        async with self._lock:
            bucket = self._buckets[key]
            # 清理过期时间戳
            cutoff = now - self._window
            while bucket and bucket[0] < cutoff:
                bucket.popleft()

            if len(bucket) >= self._max:
                logger.warning(
                    'RateLimit 触发: event=%s, limit=%d/%ds',
                    name,
                    self._max,
                    self._window,
                )
                return  # 丢弃事件

            bucket.append(now)

        await next(event_registry, name, source, data, old_event)

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        """Propagate to next."""
        await next(event)

    @property
    def current_rate(self) -> Dict[str, int]:
        """返回当前各窗口的请求计数快照。"""
        now = time.monotonic()
        cutoff = now - self._window
        return {k: sum(1 for t in v if t >= cutoff) for k, v in self._buckets.items()}
