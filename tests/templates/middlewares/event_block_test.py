"""事件屏蔽中间件测试：EventBlockMiddleware。"""

import asyncio
from typing import Any

import pytest
from conftest import (
    SimplePingHandler,
)
from pydantic import BaseModel

from infinity_bus import (
    EventBus,
    EventHandlerRegistry,
    EventRegistry,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
    MiddlewareChain,
)
from infinity_bus.templates.middlewares import (
    EventBlockMiddleware,
    EventTransformMiddleware,
    make_allowlist_predicate,
    make_blocklist_predicate,
    make_rename_transform,
)

# ============================================================================
# EventBlockMiddleware
# ============================================================================


class TestEventBlockMiddleware:
    """事件屏蔽中间件"""

    @pytest.mark.asyncio
    async def test_blocklist_blocks_specified_events(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """黑名单中的事件被屏蔽"""
        pred = make_blocklist_predicate('mw.ping')
        mw = EventBlockMiddleware(pred, block_reason='test block')
        chain = MiddlewareChain()
        await chain.add(mw)

        handler = SimplePingHandler()
        await handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy('src').publish('mw.ping', {'key': 'k', 'count': 1})
            await asyncio.sleep(0.1)

        # 被屏蔽，handler 不应收到
        assert len(handler.received) == 0
        assert mw.blocked_count == 1

    @pytest.mark.asyncio
    async def test_blocklist_allows_other_events(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """非黑名单事件正常通过"""
        pred = make_blocklist_predicate('some.other.event')
        mw = EventBlockMiddleware(pred)
        chain = MiddlewareChain()
        await chain.add(mw)

        handler = SimplePingHandler()
        await handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy('src').publish('mw.ping', {'key': 'k', 'count': 1})
            await handler.wait_received(timeout=2.0)

        assert len(handler.received) >= 1
        assert mw.blocked_count == 0

    @pytest.mark.asyncio
    async def test_allowlist_only_allows_whitelisted(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """白名单模式：仅允许指定事件"""
        pred = make_allowlist_predicate('user.login')
        mw = EventBlockMiddleware(pred, block_reason='not in allowlist')
        chain = MiddlewareChain()
        await chain.add(mw)

        handler = SimplePingHandler()
        await handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            # mw.ping 不在白名单中 → 被屏蔽
            await bus.proxy('src').publish('mw.ping', {'key': 'k', 'count': 1})
            # user.login 在白名单中 → 通过
            await bus.proxy('src').publish('user.login', None)
            await asyncio.sleep(0.1)

            # mw.ping 被屏蔽（关闭时 __shutdown__ 也可能被屏蔽，所以 ≥1）
            assert mw.blocked_count >= 1

        assert len(handler.received) == 0  # mw.ping 被屏蔽

    @pytest.mark.asyncio
    async def test_custom_block_predicate(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """自定义屏蔽判定"""

        def block_sensitive(
            name: str,
            data: dict[str, Any] | BaseModel | None,
        ) -> bool:
            if isinstance(data, dict):
                return data.get('count', 0) < 0
            return False

        mw = EventBlockMiddleware(block_sensitive)
        chain = MiddlewareChain()
        await chain.add(mw)

        handler = SimplePingHandler()
        await handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            # count < 0 → 屏蔽
            await bus.proxy('src').publish('mw.ping', {'key': 'bad', 'count': -1})
            # count >= 0 → 通过
            await bus.proxy('src').publish('mw.ping', {'key': 'good', 'count': 1})
            await handler.wait_received(timeout=2.0)

        assert mw.blocked_count >= 1
        assert len(handler.received) == 1
        assert handler.received[0].key == 'good'


# ============================================================================
# 组合测试：transform + block
# ============================================================================


class TestTransformThenBlock:
    """先转换事件名，再基于新名称屏蔽"""

    @pytest.mark.asyncio
    async def test_transform_then_block(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """先转换事件名，再基于新名称屏蔽"""
        # 1. 将 mw.ping → blocked.event
        transform = make_rename_transform({'mw.ping': 'blocked.event'})
        trans_mw = EventTransformMiddleware(transform)

        # 2. 屏蔽 blocked.event
        pred = make_blocklist_predicate('blocked.event')
        block_mw = EventBlockMiddleware(pred)

        chain = MiddlewareChain()
        await chain.add(trans_mw)
        await chain.add(block_mw)  # transform 在外层，先执行

        handler = SimplePingHandler()
        await handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy('src').publish('mw.ping', {'key': 'k', 'count': 1})
            await asyncio.sleep(0.1)

        # 转换后的事件被屏蔽，handler 不应收到
        assert len(handler.received) == 0
        assert block_mw.blocked_count == 1
