"""事件转发中间件测试：EventForwardMiddleware。"""

import asyncio
from typing import Any, List, Optional

import pytest
from conftest import (
    BASE_EVENT_DECLS,
    MiddlewareTestPayload,
    SimplePingHandler,
    create_event_registry,
)
from pydantic import BaseModel

from infinity_bus import (
    Event,
    EventBus,
    EventDeclaration,
    EventHandler,
    EventHandlerRegistry,
    EventRegistry,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
    MiddlewareChain,
    Regex,
)
from infinity_bus.templates import (
    EventForwardMiddleware,
    make_bidirectional_forward,
    make_event_name_filter,
)

# ============================================================================
# 辅助：用于验证转发结果的 Handler
# ============================================================================


class ForwardSpyHandler(EventHandler):
    """记录所有接收到的事件名和来源，用于验证转发结果。"""

    def __init__(self, subscriptions: List[str | Regex]) -> None:
        super().__init__(subscriptions)
        self.received_names: List[str] = []
        self.received_sources: List[str] = []
        self.received_payloads: List[Optional[BaseModel]] = []
        self._event = asyncio.Event()

    async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
        self.received_names.append(raw_event.name)
        self.received_sources.append(raw_event.sources[-1] if raw_event.sources else '')
        self.received_payloads.append(payload)
        self._event.set()

    async def wait_received(self, timeout: float = 2.0) -> None:
        await asyncio.wait_for(self._event.wait(), timeout)


# ============================================================================
# EventForwardMiddleware
# ============================================================================


class TestEventForwardMiddleware:
    """事件转发中间件"""

    @pytest.mark.asyncio
    async def test_basic_forward(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """源总线事件自动转发到目标总线"""
        # 1. 创建目标总线（独立注册表 + 处理器）
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(['mw.ping'])
        target_handlers.register(spy)

        target_bus = EventBus(
            target_registry, target_handlers, queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10))
        )

        # 2. 源总线装配转发中间件
        fw = EventForwardMiddleware(target=target_bus, source_name='forward-test')
        chain = MiddlewareChain()
        await chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy('src').publish('mw.ping', {'key': 'hello', 'count': 42})
                await spy.wait_received(timeout=3.0)

        # 目标总线收到了转发的事件
        assert len(spy.received_names) >= 1
        assert spy.received_names[0] == 'mw.ping'
        assert spy.received_sources[0] == 'forward-test'

    @pytest.mark.asyncio
    async def test_forward_with_payload(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """转发事件的负载数据完整传递"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(['mw.ping'])
        target_handlers.register(spy)

        target_bus = EventBus(
            target_registry, target_handlers, queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10))
        )

        fw = EventForwardMiddleware(target=target_bus, source_name='payload-test')
        chain = MiddlewareChain()
        await chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy('src').publish('mw.ping', {'key': 'data-check', 'count': 99})
                await spy.wait_received(timeout=3.0)

        assert len(spy.received_payloads) >= 1
        payload = spy.received_payloads[0]
        assert isinstance(payload, MiddlewareTestPayload)
        assert payload.key == 'data-check'
        assert payload.count == 99

    @pytest.mark.asyncio
    async def test_event_filter_whitelist(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """白名单过滤：仅转发指定事件"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(['mw.ping'])
        target_handlers.register(spy)

        target_bus = EventBus(
            target_registry, target_handlers, queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10))
        )

        # 仅转发 mw.ping
        event_filter = make_event_name_filter('mw.ping', mode='white')
        fw = EventForwardMiddleware(
            target=target_bus,
            source_name='filter-test',
            event_filter=event_filter,
        )
        chain = MiddlewareChain()
        await chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                # 发布 mw.ping → 应被转发
                await source_bus.proxy('src').publish('mw.ping', {'key': 'forwarded', 'count': 1})
                # 发布 user.login → 不应被转发
                await source_bus.proxy('src').publish('user.login', None)
                await spy.wait_received(timeout=3.0)

        # 仅 mw.ping 被转发
        assert len(spy.received_names) == 1
        assert spy.received_names[0] == 'mw.ping'

    @pytest.mark.asyncio
    async def test_event_filter_blacklist(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """黑名单过滤：排除指定事件，转发其余"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(['mw.ping', 'user.login'])
        target_handlers.register(spy)

        target_bus = EventBus(
            target_registry, target_handlers, queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10))
        )

        # 排除 mw.ping，其余全部转发
        event_filter = make_event_name_filter('mw.ping', mode='black')
        fw = EventForwardMiddleware(
            target=target_bus,
            source_name='blacklist-test',
            event_filter=event_filter,
        )
        chain = MiddlewareChain()
        await chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy('src').publish('mw.ping', {'key': 'blocked', 'count': 1})
                await source_bus.proxy('src').publish('user.login', None)
                # 等待足够时间让两个事件都被处理
                await asyncio.sleep(0.3)

        # mw.ping 被排除，仅 user.login 被转发
        forwarded = [n for n in spy.received_names if n == 'user.login']
        assert len(forwarded) >= 1
        assert 'mw.ping' not in spy.received_names

    @pytest.mark.asyncio
    async def test_system_events_not_forwarded_by_default(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """系统事件（infinity_bus.*）默认不转发"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()

        # 同时订阅用户事件和系统事件
        spy = ForwardSpyHandler(['mw.ping', 'infinity_bus.__shutdown__', 'infinity_bus.__task_error__'])
        target_handlers.register(spy)

        target_bus = EventBus(
            target_registry, target_handlers, queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10))
        )

        fw = EventForwardMiddleware(
            target=target_bus,
            source_name='no-sys-test',
            forward_system_events=False,  # 默认值
        )
        chain = MiddlewareChain()
        await chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy('src').publish('mw.ping', {'key': 'user-event', 'count': 1})
                await spy.wait_received(timeout=3.0)

        # 用户事件被转发
        assert 'mw.ping' in spy.received_names
        # 来自转发源的事件中没有系统事件（目标总线自身的生命周期事件除外）
        for i, name in enumerate(spy.received_names):
            if spy.received_sources[i] == 'no-sys-test':
                assert not name.startswith('infinity_bus.'), f"system event '{name}' was forwarded but should not be"

    @pytest.mark.asyncio
    async def test_system_events_forwarded_when_enabled(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """设置 forward_system_events=True 时系统事件也被转发"""

        # 注册一个自定义系统事件用于测试（无负载）
        class CustomSysEvent(EventDeclaration):
            name = 'infinity_bus.custom_test'

        # 源和目标都需要注册该事件
        source_registry = create_event_registry(BASE_EVENT_DECLS)
        source_registry.register(CustomSysEvent)

        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_registry.register(CustomSysEvent)

        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(['infinity_bus.custom_test'])
        target_handlers.register(spy)

        target_bus = EventBus(
            target_registry, target_handlers, queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10))
        )

        fw = EventForwardMiddleware(
            target=target_bus,
            source_name='sys-fwd-test',
            forward_system_events=True,
        )
        chain = MiddlewareChain()
        await chain.add(fw)

        source_bus = EventBus(
            source_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy('src').publish('infinity_bus.custom_test', None)
                await spy.wait_received(timeout=3.0)

        # 系统事件被转发
        sys_events = [n for n in spy.received_names if n.startswith('infinity_bus.')]
        assert len(sys_events) >= 1

    @pytest.mark.asyncio
    async def test_dynamic_target_provider(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """动态目标总线提供者：每次转发时获取最新实例"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(['mw.ping'])
        target_handlers.register(spy)

        target_bus = EventBus(
            target_registry, target_handlers, queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10))
        )

        # 使用工厂函数提供目标总线
        def get_target() -> EventBus:
            return target_bus

        fw = EventForwardMiddleware(target=get_target, source_name='dynamic-test')
        chain = MiddlewareChain()
        await chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy('src').publish('mw.ping', {'key': 'dynamic', 'count': 7})
                await spy.wait_received(timeout=3.0)

        assert len(spy.received_names) >= 1
        assert spy.received_names[0] == 'mw.ping'
        assert spy.received_sources[0] == 'dynamic-test'

    @pytest.mark.asyncio
    async def test_custom_sync_filter(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """自定义同步过滤回调"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(['mw.ping'])
        target_handlers.register(spy)

        target_bus = EventBus(
            target_registry, target_handlers, queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10))
        )

        def count_filter(event: Event) -> bool:
            if event.data is not None:
                data_dict = event.data.model_dump()
                return data_dict.get('count', 0) > 10
            return False

        fw = EventForwardMiddleware(
            target=target_bus,
            source_name='custom-filter',
            event_filter=count_filter,
        )
        chain = MiddlewareChain()
        await chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                # count=5 → 不转发
                await source_bus.proxy('src').publish('mw.ping', {'key': 'low', 'count': 5})
                # count=100 → 转发
                await source_bus.proxy('src').publish('mw.ping', {'key': 'high', 'count': 100})
                await spy.wait_received(timeout=3.0)

        assert len(spy.received_payloads) >= 1
        payload = spy.received_payloads[0]
        assert isinstance(payload, MiddlewareTestPayload)
        assert payload.key == 'high'
        assert payload.count == 100

    @pytest.mark.asyncio
    async def test_custom_async_filter(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """自定义异步过滤回调"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(['mw.ping'])
        target_handlers.register(spy)

        target_bus = EventBus(
            target_registry, target_handlers, queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10))
        )

        async def async_filter(event: Event) -> bool:
            await asyncio.sleep(0.01)  # 模拟异步检查
            if event.data is not None:
                data_dict = event.data.model_dump()
                return data_dict.get('key', '') == 'async-ok'
            return False

        fw = EventForwardMiddleware(
            target=target_bus,
            source_name='async-filter',
            event_filter=async_filter,
        )
        chain = MiddlewareChain()
        await chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy('src').publish('mw.ping', {'key': 'async-no', 'count': 1})
                await source_bus.proxy('src').publish('mw.ping', {'key': 'async-ok', 'count': 2})
                await spy.wait_received(timeout=3.0)

        assert len(spy.received_payloads) >= 1
        payload = spy.received_payloads[0]
        assert isinstance(payload, MiddlewareTestPayload)
        assert payload.key == 'async-ok'

    @pytest.mark.asyncio
    async def test_error_isolation_target_unreachable(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """转发失败不影响源总线正常运行 —— 目标总线未启动"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()

        # 目标总线不启动
        target_bus = EventBus(
            target_registry, target_handlers, queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10))
        )

        fw = EventForwardMiddleware(target=target_bus, source_name='error-iso')
        chain = MiddlewareChain()
        await chain.add(fw)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )

        # 源总线正常启动，目标总线未启动
        async with source_bus:
            await source_bus.proxy('src').publish('mw.ping', {'key': 'still-works', 'count': 1})
            await handler.wait_received(timeout=3.0)

        # 源总线的 handler 仍然正常收到事件
        assert len(handler.received) >= 1
        assert handler.received[0].key == 'still-works'

    @pytest.mark.asyncio
    async def test_filter_exception_does_not_block(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """过滤回调异常时事件被跳过，不影响后续事件"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(['mw.ping'])
        target_handlers.register(spy)

        target_bus = EventBus(
            target_registry, target_handlers, queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10))
        )

        call_count = 0

        def faulty_filter(event: Event) -> bool:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError('filter exploded')
            return True

        fw = EventForwardMiddleware(
            target=target_bus,
            source_name='filter-error',
            event_filter=faulty_filter,
        )
        chain = MiddlewareChain()
        await chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                # 第一次：过滤回调抛异常 → 跳过
                await source_bus.proxy('src').publish('mw.ping', {'key': 'first', 'count': 1})
                # 第二次：过滤回调正常 → 转发
                await source_bus.proxy('src').publish('mw.ping', {'key': 'second', 'count': 2})
                await spy.wait_received(timeout=3.0)

        # 第二个事件被正常转发
        assert len(spy.received_payloads) >= 1
        assert isinstance(spy.received_payloads[0], MiddlewareTestPayload)
        assert spy.received_payloads[0].key == 'second'
        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_no_filter_forwards_all_non_system(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """无过滤器时转发所有非系统事件"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(['mw.ping', 'user.login'])
        target_handlers.register(spy)

        target_bus = EventBus(
            target_registry, target_handlers, queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10))
        )

        fw = EventForwardMiddleware(target=target_bus, source_name='no-filter')
        chain = MiddlewareChain()
        await chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy('src').publish('mw.ping', {'key': 'all', 'count': 1})
                await source_bus.proxy('src').publish('user.login', None)
                await asyncio.sleep(0.3)

        # 两个非系统事件都被转发
        assert len(spy.received_names) >= 2
        assert 'mw.ping' in spy.received_names
        assert 'user.login' in spy.received_names

    @pytest.mark.asyncio
    async def test_source_name_default(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """默认 source_name 为 'event_forward'"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(['mw.ping'])
        target_handlers.register(spy)

        target_bus = EventBus(
            target_registry, target_handlers, queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10))
        )

        # 不指定 source_name，使用默认值
        fw = EventForwardMiddleware(target=target_bus)
        chain = MiddlewareChain()
        await chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy('src').publish('mw.ping', {'key': 'default', 'count': 1})
                await spy.wait_received(timeout=3.0)

        assert len(spy.received_sources) >= 1
        assert spy.received_sources[0] == 'event_forward'


# ============================================================================
# make_bidirectional_forward
# ============================================================================


class TestMakeBidirectionalForward:
    """make_bidirectional_forward 双向转发中间件对"""

    @pytest.mark.asyncio
    async def test_basic_bidirectional(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """A→B 和 B→A 两个方向均能成功转发"""
        registry_a = create_event_registry(BASE_EVENT_DECLS)
        registry_b = create_event_registry(BASE_EVENT_DECLS)

        handlers_a = EventHandlerRegistry()
        handlers_b = EventHandlerRegistry()

        spy_a = ForwardSpyHandler(['mw.ping', 'user.login'])
        spy_b = ForwardSpyHandler(['mw.ping', 'user.login'])
        handlers_a.register(spy_a)
        handlers_b.register(spy_b)

        # 先创建链和总线，再生成中间件对并追加到链中
        chain_a = MiddlewareChain()
        chain_b = MiddlewareChain()
        bus_a = EventBus(
            registry_a,
            handlers_a,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_a,
        )
        bus_b = EventBus(
            registry_b,
            handlers_b,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_b,
        )

        a_to_b, b_to_a = make_bidirectional_forward(
            bus_a,
            bus_b,
            source_a_to_b='a→b',
            source_b_to_a='b→a',
        )
        await chain_a.add(a_to_b)
        await chain_b.add(b_to_a)

        async with bus_a:
            async with bus_b:
                # A 发布事件 → 应转发到 B
                await bus_a.proxy('a-src').publish('mw.ping', {'key': 'from-a', 'count': 1})
                await spy_b.wait_received(timeout=3.0)

                # B 发布事件 → 应转发到 A
                await bus_b.proxy('b-src').publish('user.login', None)
                await spy_a.wait_received(timeout=3.0)

        # B 收到了来自 A 的事件
        b_pings = [n for n in spy_b.received_names if n == 'mw.ping']
        assert len(b_pings) >= 1
        b_sources = [spy_b.received_sources[i] for i, n in enumerate(spy_b.received_names) if n == 'mw.ping']
        assert 'a→b' in b_sources

        # A 收到了来自 B 的事件
        a_logins = [n for n in spy_a.received_names if n == 'user.login']
        assert len(a_logins) >= 1
        a_sources = [spy_a.received_sources[i] for i, n in enumerate(spy_a.received_names) if n == 'user.login']
        assert 'b→a' in a_sources

    @pytest.mark.asyncio
    async def test_anti_recursion_prevents_loop(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """反递归过滤阻止 A→B→A 回环"""
        registry_a = create_event_registry(BASE_EVENT_DECLS)
        registry_b = create_event_registry(BASE_EVENT_DECLS)

        handlers_a = EventHandlerRegistry()
        handlers_b = EventHandlerRegistry()

        spy_a = ForwardSpyHandler(['mw.ping'])
        spy_b = ForwardSpyHandler(['mw.ping'])
        handlers_a.register(spy_a)
        handlers_b.register(spy_b)

        chain_a = MiddlewareChain()
        chain_b = MiddlewareChain()
        bus_a = EventBus(
            registry_a,
            handlers_a,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_a,
        )
        bus_b = EventBus(
            registry_b,
            handlers_b,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_b,
        )

        a_to_b, b_to_a = make_bidirectional_forward(
            bus_a,
            bus_b,
            source_a_to_b='a→b',
            source_b_to_a='b→a',
            anti_recursion=True,
        )
        await chain_a.add(a_to_b)
        await chain_b.add(b_to_a)

        async with bus_a:
            async with bus_b:
                # A 发布事件
                await bus_a.proxy('a-src').publish('mw.ping', {'key': 'loop-test', 'count': 1})
                await spy_b.wait_received(timeout=3.0)
                # 给足够时间让可能的回环发生
                await asyncio.sleep(0.3)

        # B 收到了 1 次（来自 A 的转发）
        assert len(spy_b.received_names) == 1
        # A 的 spy 不应该收到来自 B 的回环转发（反递归生效）
        b_to_a_sources = [s for s in spy_a.received_sources if s == 'b→a']
        assert len(b_to_a_sources) == 0

    @pytest.mark.asyncio
    async def test_anti_recursion_disabled_allows_loop(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """关闭反递归时 A→B→A 可以发生"""
        registry_a = create_event_registry(BASE_EVENT_DECLS)
        registry_b = create_event_registry(BASE_EVENT_DECLS)

        handlers_a = EventHandlerRegistry()
        handlers_b = EventHandlerRegistry()

        spy_a = ForwardSpyHandler(['mw.ping'])
        spy_b = ForwardSpyHandler(['mw.ping'])
        handlers_a.register(spy_a)
        handlers_b.register(spy_b)

        chain_a = MiddlewareChain()
        chain_b = MiddlewareChain()
        bus_a = EventBus(
            registry_a,
            handlers_a,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_a,
        )
        bus_b = EventBus(
            registry_b,
            handlers_b,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_b,
        )

        a_to_b, b_to_a = make_bidirectional_forward(
            bus_a,
            bus_b,
            source_a_to_b='a→b',
            source_b_to_a='b→a',
            anti_recursion=False,
        )
        await chain_a.add(a_to_b)
        await chain_b.add(b_to_a)

        async with bus_a:
            async with bus_b:
                await bus_a.proxy('a-src').publish('mw.ping', {'key': 'no-guard', 'count': 1})
                # B 也会回环转发到 A
                await asyncio.sleep(0.5)

        # B 收到来自 A 的转发
        assert len(spy_b.received_names) >= 1
        # A 也收到了来自 B 的回环（无防护）
        b_to_a_sources = [s for s in spy_a.received_sources if s == 'b→a']
        assert len(b_to_a_sources) >= 1

    @pytest.mark.asyncio
    async def test_custom_event_filter_with_pair(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """自定义过滤器与反递归组合生效"""
        registry_a = create_event_registry(BASE_EVENT_DECLS)
        registry_b = create_event_registry(BASE_EVENT_DECLS)

        handlers_a = EventHandlerRegistry()
        handlers_b = EventHandlerRegistry()

        spy_a = ForwardSpyHandler(['mw.ping', 'user.login'])
        spy_b = ForwardSpyHandler(['mw.ping', 'user.login'])
        handlers_a.register(spy_a)
        handlers_b.register(spy_b)

        chain_a = MiddlewareChain()
        chain_b = MiddlewareChain()
        bus_a = EventBus(
            registry_a,
            handlers_a,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_a,
        )
        bus_b = EventBus(
            registry_b,
            handlers_b,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_b,
        )

        a_to_b, b_to_a = make_bidirectional_forward(
            bus_a,
            bus_b,
            source_a_to_b='a→b',
            source_b_to_a='b→a',
            event_filter=make_event_name_filter('mw.ping', mode='white'),
            anti_recursion=True,
        )
        await chain_a.add(a_to_b)
        await chain_b.add(b_to_a)

        async with bus_a:
            async with bus_b:
                # A 发布 mw.ping → 应转发到 B
                await bus_a.proxy('a-src').publish('mw.ping', {'key': 'filtered', 'count': 1})
                # A 发布 user.login → 不应转发（白名单仅含 mw.ping）
                await bus_a.proxy('a-src').publish('user.login', None)
                await spy_b.wait_received(timeout=3.0)
                await asyncio.sleep(0.2)

        # B 仅收到 mw.ping
        b_names = spy_b.received_names
        assert 'mw.ping' in b_names
        assert 'user.login' not in b_names

    @pytest.mark.asyncio
    async def test_dynamic_bus_providers(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """支持动态总线提供者（工厂回调）"""
        registry_a = create_event_registry(BASE_EVENT_DECLS)
        registry_b = create_event_registry(BASE_EVENT_DECLS)

        handlers_a = EventHandlerRegistry()
        handlers_b = EventHandlerRegistry()

        spy_b = ForwardSpyHandler(['mw.ping'])
        handlers_b.register(spy_b)

        # bus_b 先创建并持有链，再生成中间件对
        chain_b = MiddlewareChain()
        bus_b = EventBus(
            registry_b,
            handlers_b,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_b,
        )

        # bus_a 尚未创建，使用工厂回调
        _bus_a: Optional[EventBus] = None

        def get_bus_a() -> EventBus:
            assert _bus_a is not None
            return _bus_a

        a_to_b, b_to_a = make_bidirectional_forward(
            get_bus_a,
            bus_b,
            source_a_to_b='a→b',
            source_b_to_a='b→a',
        )

        chain_a = MiddlewareChain()
        await chain_a.add(a_to_b)
        _bus_a = EventBus(
            registry_a,
            handlers_a,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_a,
        )
        await chain_b.add(b_to_a)

        async with _bus_a:
            async with bus_b:
                await _bus_a.proxy('a-src').publish('mw.ping', {'key': 'dynamic', 'count': 42})
                await spy_b.wait_received(timeout=3.0)

        assert len(spy_b.received_names) >= 1
        assert spy_b.received_names[0] == 'mw.ping'
        assert spy_b.received_sources[0] == 'a→b'

    @pytest.mark.asyncio
    async def test_default_source_names(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """默认 source 名称为 'a→b' 和 'b→a'"""
        registry_a = create_event_registry(BASE_EVENT_DECLS)
        registry_b = create_event_registry(BASE_EVENT_DECLS)

        handlers_a = EventHandlerRegistry()
        handlers_b = EventHandlerRegistry()

        spy_b = ForwardSpyHandler(['mw.ping'])
        handlers_b.register(spy_b)

        chain_a = MiddlewareChain()
        chain_b = MiddlewareChain()
        bus_a = EventBus(
            registry_a,
            handlers_a,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_a,
        )
        bus_b = EventBus(
            registry_b,
            handlers_b,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_b,
        )

        a_to_b, b_to_a = make_bidirectional_forward(bus_a, bus_b)
        await chain_a.add(a_to_b)
        await chain_b.add(b_to_a)

        async with bus_a:
            async with bus_b:
                await bus_a.proxy('a-src').publish('mw.ping', {'key': 'default', 'count': 1})
                await spy_b.wait_received(timeout=3.0)

        assert len(spy_b.received_sources) >= 1
        assert spy_b.received_sources[0] == 'a→b'

        async with bus_a:
            async with bus_b:
                await bus_a.proxy('a-src').publish('mw.ping', {'key': 'default', 'count': 1})
                await spy_b.wait_received(timeout=3.0)

        assert len(spy_b.received_sources) >= 1
        assert spy_b.received_sources[0] == 'a→b'
