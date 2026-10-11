"""速率限制中间件测试：RateLimitMiddleware。"""

import asyncio

import pytest
from conftest import SimplePingHandler

from infinity_bus import (
    EventBus,
    EventHandlerRegistry,
    EventRegistry,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
    MiddlewareChain,
)
from infinity_bus.templates.middlewares import (
    EventTransformMiddleware,
    RateLimitMiddleware,
    make_field_inject_transform,
)

# ============================================================================
# RateLimitMiddleware
# ============================================================================


class TestRateLimitMiddleware:
    """滑动窗口速率限制"""

    @pytest.mark.asyncio
    async def test_allows_within_limit(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """在限制内的事件正常通过"""
        mw = RateLimitMiddleware(max_requests=10, window_seconds=1.0)
        chain = MiddlewareChain()
        await chain.add(mw)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            for i in range(5):
                await bus.proxy('src').publish('mw.ping', {'key': f'k{i}', 'count': i})
                await asyncio.sleep(0.01)

        # 所有事件都应被处理
        assert len(handler.received) == 5

    @pytest.mark.asyncio
    async def test_blocks_when_exceeded(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """超出限制时事件被丢弃"""
        mw = RateLimitMiddleware(max_requests=3, window_seconds=10.0)
        chain = MiddlewareChain()
        await chain.add(mw)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            for i in range(10):
                await bus.proxy('src').publish('mw.ping', {'key': f'k{i}', 'count': i})
                await asyncio.sleep(0.01)

        # 仅前 3 个被处理（其余被丢弃）
        assert mw.current_rate.get('__global__', 0) <= 3
        # 由于丢弃发生于 before_publish，handler 只会收到 ≤3 条
        assert len(handler.received) <= 3

    @pytest.mark.asyncio
    async def test_per_event_rate_limit(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """按事件名独立限流"""
        mw = RateLimitMiddleware(max_requests=2, window_seconds=10.0, per_event=True)
        chain = MiddlewareChain()
        await chain.add(mw)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=20)),
            middleware_chain=chain,
        )
        async with bus:
            # 发布 mw.ping 事件（最多 2 个通过）
            for i in range(5):
                await bus.proxy('src').publish('mw.ping', {'key': f'ping{i}', 'count': i})
                await asyncio.sleep(0.01)

            # 发布 user.login（无负载事件，另一个窗口）
            for i in range(5):
                await bus.proxy('src').publish('user.login', None)
                await asyncio.sleep(0.01)

        # mw.ping 窗口限制为 2
        ping_count = mw.current_rate.get('mw.ping', 0)
        assert ping_count <= 2

    @pytest.mark.asyncio
    async def test_sliding_window_expires_old_timestamps(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """窗口滑动后旧时间戳被清理，新事件可继续通过"""
        mw = RateLimitMiddleware(max_requests=3, window_seconds=0.1)
        chain = MiddlewareChain()
        await chain.add(mw)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            # 第一轮：打满窗口
            for i in range(3):
                await bus.proxy('src').publish('mw.ping', {'key': f'k{i}', 'count': i})
                await asyncio.sleep(0.01)

            # 等待窗口过期
            await asyncio.sleep(0.15)

            # 第二轮：旧时间戳被清理，新事件可再次通过
            await bus.proxy('src').publish('mw.ping', {'key': 'k_after', 'count': 99})
            await asyncio.sleep(0.05)

        # 两轮共 4 个事件通过（3 + 1）
        assert len(handler.received) == 4


# ============================================================================
# 组合测试：rate_limit + transform
# ============================================================================


class TestRateLimitBeforeTransform:
    """先限流再转换"""

    @pytest.mark.asyncio
    async def test_rate_limit_before_transform(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """先限流再转换"""
        rate_mw = RateLimitMiddleware(max_requests=3, window_seconds=10.0)
        transform = make_field_inject_transform(source='test')
        trans_mw = EventTransformMiddleware(transform)

        chain = MiddlewareChain()
        await chain.add(rate_mw)
        await chain.add(trans_mw)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            for i in range(10):
                await bus.proxy('src').publish('mw.ping', {'key': f'k{i}', 'count': i})
                await asyncio.sleep(0.01)

        # 限制后仅 ≤3 条通过
        assert len(handler.received) <= 3
