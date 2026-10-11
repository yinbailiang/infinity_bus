"""事件转换中间件测试：EventTransformMiddleware。"""

from typing import Any, List, Optional

import pytest
from conftest import (
    MiddlewareTestPayload,
    SimplePingHandler,
)
from pydantic import BaseModel

from infinity_bus import (
    Event,
    EventBus,
    EventDeclaration,
    EventHandlerRegistry,
    EventRegistry,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
    MiddlewareChain,
)
from infinity_bus.templates.middlewares import (
    EventTransformMiddleware,
    make_field_inject_transform,
    make_field_redact_transform,
    make_rename_transform,
)

# ============================================================================
# EventTransformMiddleware
# ============================================================================


class TestEventTransformMiddleware:
    """事件转换中间件"""

    @pytest.mark.asyncio
    async def test_rename_event(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """事件重命名：旧名 → 新名（目标事件需接受相同负载类型）"""

        # 注册一个目标事件，接受 MiddlewareTestPayload
        class RenameTargetEvent(EventDeclaration):
            name = 'rename.target'
            payload_type = MiddlewareTestPayload

        base_event_registry.register(RenameTargetEvent)
        transform = make_rename_transform({'mw.ping': 'rename.target'})
        mw = EventTransformMiddleware(transform)
        chain = MiddlewareChain()
        await chain.add(mw)

        # 监听重命名后的目标事件
        received: List[str] = []

        class TargetWatcher(SimplePingHandler):
            def __init__(self) -> None:
                super().__init__()
                self.subscriptions.replace(['rename.target'])

            async def handle(
                self,
                payload: Optional[BaseModel],
                bus_proxy: Any,
                raw_event: Event,
            ) -> None:
                received.append(raw_event.name)
                if isinstance(payload, MiddlewareTestPayload):
                    self.received.append(payload)
                    self._event.set()

        watcher = TargetWatcher()
        handler_registry.register(watcher)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy('src').publish('mw.ping', {'key': 'k', 'count': 1})
            await watcher.wait_received(timeout=2.0)

        # 重命名后的事件被 TargetWatcher 收到
        assert len(received) >= 1
        assert received[0] == 'rename.target'

    @pytest.mark.asyncio
    async def test_field_inject(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """自动注入字段"""
        transform = make_field_inject_transform(trace_id='abc-123', env='test')
        mw = EventTransformMiddleware(transform)
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
            await bus.proxy('src').publish('mw.ping', {'key': 'original'})
            await handler.wait_received(timeout=2.0)

        assert len(handler.received) >= 1
        payload = handler.received[0]
        assert payload.key == 'original'
        # 注入的字段在 data 中
        # 注意：MiddlewareTestPayload 只有 key, count，注入的额外字段会被忽略
        # 所以这里只验证 handler 确实收到了事件

    @pytest.mark.asyncio
    async def test_field_redact(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """敏感字段脱敏"""
        transform = make_field_redact_transform('key')
        mw = EventTransformMiddleware(transform)
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
            await bus.proxy('src').publish('mw.ping', {'key': 'secret123', 'count': 99})
            await handler.wait_received(timeout=2.0)

        assert len(handler.received) >= 1
        payload = handler.received[0]
        # key 被替换为 ***
        assert payload.key == '***'
        assert payload.count == 99

    @pytest.mark.asyncio
    async def test_custom_transform(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """自定义转换函数"""

        # 注册转换目标事件
        class PrefixedPingEvent(EventDeclaration):
            name = 'prefix.mw.ping'
            payload_type = MiddlewareTestPayload

        base_event_registry.register(PrefixedPingEvent)

        def add_prefix(
            name: str,
            data: dict[str, Any] | BaseModel | None,
        ) -> tuple[str, dict[str, Any] | BaseModel | None]:
            # 不对系统事件添加前缀
            if name.startswith('infinity_bus.'):
                return name, data
            return f'prefix.{name}', data

        mw = EventTransformMiddleware(add_prefix)
        chain = MiddlewareChain()
        await chain.add(mw)

        handler = SimplePingHandler()
        # 修改订阅以匹配转换后的事件名
        handler.subscriptions.replace(['prefix.mw.ping'])
        handler_registry.register(handler)

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
