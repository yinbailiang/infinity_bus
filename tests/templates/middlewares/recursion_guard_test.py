"""递归防护中间件测试：RecursionGuardMiddleware。"""

import asyncio
from typing import Any, Dict, List, Optional

import pytest
from conftest import SimplePingHandler
from pydantic import BaseModel

from infinity_bus import (
    Event,
    EventBus,
    EventHandler,
    EventHandlerRegistry,
    EventRegistry,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
    MiddlewareChain,
    Regex,
)
from infinity_bus.templates import (
    RecursionGuardMiddleware,
)

# ============================================================================
# 辅助类
# ============================================================================


class ChainPublishingHandler(EventHandler):
    """在 handler 中二次发布事件的处理器，用于测试递归防护。"""

    def __init__(
        self,
        publish_event: str,
        publish_data: Optional[Dict[str, Any]] = None,
        subscriptions: Optional[List[str | Regex]] = None,
    ):
        super().__init__(subscriptions=subscriptions or ['mw.ping'])
        self._pub_event = publish_event
        self._pub_data = publish_data or {}
        self.publish_count = 0

    async def handle(
        self,
        payload: Optional[BaseModel],
        bus_proxy: EventBus.Proxy,
        raw_event: Event,
    ) -> None:
        self.publish_count += 1
        await bus_proxy.publish(self._pub_event, self._pub_data)


# ============================================================================
# RecursionGuardMiddleware
# ============================================================================


class TestRecursionGuardMiddleware:
    """递归调用防护"""

    @pytest.mark.asyncio
    async def test_allows_normal_chain(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """正常链式发布不被拦截"""
        guard = RecursionGuardMiddleware(max_depth=3)
        chain = MiddlewareChain()
        await chain.add(guard)

        handler = ChainPublishingHandler('mw.ping', {'key': 'nested', 'count': 1})
        await handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            for _ in range(3):
                await bus.proxy('test').publish('mw.ping', {'key': 'start', 'count': 0})
                await asyncio.sleep(0.05)

        # 链长 3 在阈值内，应全部通过
        assert handler.publish_count >= 3

    @pytest.mark.asyncio
    async def test_blocks_recursive_loop(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """超过阈值时抛出 RecursionDetectedError"""
        guard = RecursionGuardMiddleware(max_depth=2)
        chain = MiddlewareChain()
        await chain.add(guard)

        handler = ChainPublishingHandler('mw.ping', {'key': 'loop', 'count': 1})
        await handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            # 发布 → handler 二次发布 → handler 三次发布 → 应被拦截
            await bus.proxy('test').publish('mw.ping', {'key': 'start', 'count': 0})
            await asyncio.sleep(0.1)

        # 第 3 次 handler 执行后，再发布时链中 source 出现第 3 次 → 被拦截
        assert handler.publish_count == 3

    @pytest.mark.asyncio
    async def test_root_event_always_allowed(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """根事件（无 old_event）始终允许"""
        guard = RecursionGuardMiddleware(
            max_depth=0,
            max_chain_length=None,  # 禁用链路长检查，仅测 per-source
        )
        chain = MiddlewareChain()
        await chain.add(guard)

        handler = SimplePingHandler()
        await handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy('test').publish('mw.ping', {'key': 'root', 'count': 1})
            await handler.wait_received(timeout=2.0)

        assert len(handler.received) == 1

    @pytest.mark.asyncio
    async def test_ignore_sources_not_counted(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """ignore_sources 中的发布者不参与计数"""
        guard = RecursionGuardMiddleware(
            max_depth=0,
            max_chain_length=None,  # 禁用链长检查
            ignore_sources={'ChainPublishingHandler'},
        )
        chain = MiddlewareChain()
        await chain.add(guard)

        handler = ChainPublishingHandler('mw.ping', {'key': 'ignored', 'count': 1})
        await handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            for _ in range(3):
                await bus.proxy('test').publish('mw.ping', {'key': 'start', 'count': 0})
                await asyncio.sleep(0.05)

        # 被忽略的 source 不计数，全部通过
        assert handler.publish_count >= 3

    @pytest.mark.asyncio
    async def test_different_sources_not_affected(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """不同 source 之间的链式调用不互相影响"""
        guard = RecursionGuardMiddleware(max_depth=2)
        chain = MiddlewareChain()
        await chain.add(guard)

        class HandlerA(EventHandler):
            def __init__(self):
                super().__init__(['mw.ping'])

            async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
                await bus_proxy.publish('mw.ping', {'key': 'from_a', 'count': 1})

        class HandlerB(EventHandler):
            def __init__(self):
                super().__init__(['mw.ping'])

            async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
                await bus_proxy.publish('mw.ping', {'key': 'from_b', 'count': 2})

        await handler_registry.register(HandlerA())
        await handler_registry.register(HandlerB())

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=50)),
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy('test').publish('mw.ping', {'key': 'start', 'count': 0})
            await asyncio.sleep(0.2)

        # 不同 handler 交替发布，各自不超限

    @pytest.mark.asyncio
    async def test_absolute_chain_length_blocks_mutual_recursion(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """绝对链长上限防止互递归无限增长"""
        guard = RecursionGuardMiddleware(
            max_depth=100,  # per-source 设很高，不触发
            max_chain_length=5,  # 链长 5 就拦截
        )
        chain = MiddlewareChain()
        await chain.add(guard)

        handler = ChainPublishingHandler('mw.ping', {'key': 'loop', 'count': 1})
        await handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=100)),
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy('test').publish('mw.ping', {'key': 'start', 'count': 0})
            await asyncio.sleep(0.2)

        # 链长上限 5，handler 最多执行 5 次（第 6 次发布被拦截）
        assert handler.publish_count <= 5
