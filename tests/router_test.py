"""Router / Subscriptions 单元测试

覆盖：订阅束容器语义、Router 登记/撤销、预计算分派、版本感知重建、
激活期束改动自动同步。
"""

import asyncio
from typing import Callable, Optional, Type

from pydantic import BaseModel

from infinity_bus import (
    Event,
    EventBus,
    EventDeclaration,
    EventHandler,
    EventHandlerRegistry,
    EventRegistry,
    Regex,
    Router,
    Subscriptions,
)

# ============================================================================
# 测试用事件声明
# ============================================================================


class _EventAlpha(EventDeclaration):
    name = 'test.alpha'


class _EventBeta(EventDeclaration):
    name = 'test.beta'


class _EventGamma(EventDeclaration):
    name = 'test.gamma'


def _make_registry(*decls: Type[EventDeclaration]) -> EventRegistry:
    reg = EventRegistry()
    for d in decls:
        reg.register(d)
    return reg


# ============================================================================
# Subscriptions 容器语义
# ============================================================================


class TestSubscriptionsContainer:
    def test_init_len_iter_patterns(self) -> None:
        r = Regex(r'test\..*')
        s = Subscriptions(['a', r])
        assert len(s) == 2
        assert list(s) == ['a', r]
        assert s.patterns == ('a', r)
        assert not s.is_bound

    def test_empty_is_falsy(self) -> None:
        assert not Subscriptions()
        assert bool(Subscriptions(['a']))

    def test_add_dedup(self) -> None:
        s = Subscriptions(['a'])
        s.add('b')
        s.add('a')  # 重复忽略
        assert len(s) == 2
        assert 'a' in s
        assert 'b' in s

    def test_remove(self) -> None:
        s = Subscriptions(['a', 'b'])
        assert s.remove('a') is True
        assert s.remove('x') is False
        assert list(s) == ['b']

    def test_replace(self) -> None:
        s = Subscriptions(['a'])
        s.replace(['b', 'c'])
        assert list(s) == ['b', 'c']

    def test_eq_with_list_and_tuple(self) -> None:
        s = Subscriptions(['a', 'b'])
        assert s == ['a', 'b']
        assert s == ('a', 'b')
        assert s != ['a']
        assert s != 42

    def test_eq_with_other_subscriptions(self) -> None:
        assert Subscriptions(['a', 'b']) == Subscriptions(['a', 'b'])
        assert Subscriptions(['a']) != Subscriptions(['b'])


# ============================================================================
# Router 登记 / 撤销 / 分派
# ============================================================================


class TestRouterSubscribeMatch:
    def test_subscribe_exact_match(self) -> None:
        events = _make_registry(_EventAlpha, _EventBeta)
        router = Router(events)
        router.subscribe('h1', Subscriptions(['test.alpha']))

        assert router.match('test.alpha') == ['h1']
        assert router.match('test.beta') == []

    def test_subscribe_regex_match(self) -> None:
        events = _make_registry(_EventAlpha, _EventBeta)
        router = Router(events)
        router.subscribe('h1', Subscriptions([Regex(r'test\..*')]))

        assert router.match('test.alpha') == ['h1']
        assert router.match('test.beta') == ['h1']
        assert router.match('test.gamma') == []  # 未注册事件不再分派表

    def test_order_preserved_and_handler_dedup(self) -> None:
        events = _make_registry(_EventAlpha)
        router = Router(events)
        router.subscribe('h1', Subscriptions(['test.alpha']))
        router.subscribe('h2', Subscriptions(['test.alpha', Regex(r'test\..*')]))

        # h2 精确+正则双命中 → 去重一次；h1 在 h2 前（订阅序）
        assert router.match('test.alpha') == ['h1', 'h2']

    def test_unsubscribe_removes_and_unknown_false(self) -> None:
        events = _make_registry(_EventAlpha)
        router = Router(events)
        router.subscribe('h1', Subscriptions(['test.alpha']))
        assert router.unsubscribe('h1') is True
        assert router.match('test.alpha') == []
        assert router.unsubscribe('h1') is False

    def test_resubscribe_same_bundle_is_noop(self) -> None:
        events = _make_registry(_EventAlpha)
        router = Router(events)
        subs = Subscriptions(['test.alpha'])
        router.subscribe('h1', subs)
        version = router.version
        router.subscribe('h1', subs)
        assert router.version == version

    def test_routes_and_dispatch_table_views(self) -> None:
        events = _make_registry(_EventAlpha, _EventBeta)
        router = Router(events)
        router.subscribe('h1', Subscriptions(['test.alpha']))

        assert router.routes[0][0] == 'h1'
        assert router.dispatch_table['test.alpha'] == ['h1']
        assert router.dispatch_table['test.beta'] == []


# ============================================================================
# 版本感知自动重建
# ============================================================================


class TestVersionAwareRebuild:
    def test_late_event_registration_triggers_rebuild(self) -> None:
        reg = EventRegistry()
        router = Router(reg)
        router.subscribe('h1', Subscriptions(['test.alpha']))
        assert router.match('test.alpha') == []  # 事件尚未注册

        reg.register(_EventAlpha)
        assert router.match('test.alpha') == ['h1']  # 事件注册后重建命中

    def test_clear_unbinds_all(self) -> None:
        events = _make_registry(_EventAlpha)
        router = Router(events)
        subs1 = Subscriptions(['test.alpha'])
        subs2 = Subscriptions(['test.alpha'])
        router.subscribe('h1', subs1)
        router.subscribe('h2', subs2)

        router.clear()

        assert router.match('test.alpha') == []
        assert not subs1.is_bound
        assert not subs2.is_bound


# ============================================================================
# 激活期束改动自动同步（核心动态自订阅）
# ============================================================================


class TestBoundChangeSync:
    def test_bound_replace_auto_updates_routes(self) -> None:
        events = _make_registry(_EventAlpha, _EventBeta)
        router = Router(events)
        subs = Subscriptions(['test.alpha'])
        router.subscribe('h1', subs)
        assert subs.is_bound

        subs.replace(['test.beta'])  # 状态机换阶段

        assert router.match('test.alpha') == []
        assert router.match('test.beta') == ['h1']

    def test_bound_add_auto_updates_routes(self) -> None:
        events = _make_registry(_EventAlpha, _EventBeta)
        router = Router(events)
        subs = Subscriptions(['test.alpha'])
        router.subscribe('h1', subs)

        subs.add('test.beta')

        assert router.match('test.alpha') == ['h1']
        assert router.match('test.beta') == ['h1']

    def test_bound_remove_auto_updates_routes(self) -> None:
        events = _make_registry(_EventAlpha, _EventBeta)
        router = Router(events)
        subs = Subscriptions(['test.alpha', 'test.beta'])
        router.subscribe('h1', subs)

        subs.remove('test.alpha')

        assert router.match('test.alpha') == []
        assert router.match('test.beta') == ['h1']

    def test_unbound_change_applies_on_next_subscribe(self) -> None:
        """未激活时改动只更新束内容，订阅（激活）时整体生效"""
        events = _make_registry(_EventAlpha, _EventBeta)
        subs = Subscriptions(['test.alpha'])
        subs.replace(['test.beta'])  # 未激活时改

        router = Router(events)
        router.subscribe('h1', subs)

        assert router.match('test.beta') == ['h1']
        assert router.match('test.alpha') == []

    def test_unbind_on_unsubscribe(self) -> None:
        events = _make_registry(_EventAlpha)
        router = Router(events)
        subs = Subscriptions(['test.alpha'])
        router.subscribe('h1', subs)
        assert subs.is_bound

        router.unsubscribe('h1')
        assert not subs.is_bound


# ============================================================================
# 总线集成：激活即路由 + 运行期动态自订阅
# ============================================================================


class _RecordNameHandler(EventHandler):
    """记录收到的事件名（用于集成断言）"""

    def __init__(self, subscriptions: list[str | Regex]) -> None:
        super().__init__(subscriptions=subscriptions)
        self.received: list[str] = []

    async def handle(self, payload: Optional[BaseModel], bus_proxy: EventBus.Proxy, raw_event: Event) -> None:
        self.received.append(raw_event.name)


async def _wait_until(condition: Callable[[], bool], timeout: float = 1.0) -> None:
    """轮询等待条件成立"""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if condition():
            return
        await asyncio.sleep(0.005)
    raise TimeoutError('condition not met')


class TestBusIntegration:
    async def test_register_to_running_bus_routes_immediately(
        self, event_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """注册到已运行总线：on_activate 自动登记订阅，事件即被路由"""
        handler = _RecordNameHandler(subscriptions=['test.event'])
        handler_registry.register(handler)

        await event_bus.proxy('pub').publish('test.event', {'value': 1})
        await _wait_until(lambda: len(handler.received) == 1)

        assert handler.received == ['test.event']
        active_ids = {hid for hid, _ in event_bus.router.routes}
        assert handler.handler_id in active_ids

    async def test_runtime_replace_subscriptions_changes_dispatch(
        self, event_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """运行期 replace 自身订阅：live 路由立即切换（动态自订阅）"""
        handler = _RecordNameHandler(subscriptions=['test.event'])
        handler_registry.register(handler)

        await event_bus.proxy('pub').publish('test.event', {'value': 1})
        await _wait_until(lambda: len(handler.received) == 1)

        handler.subscriptions.replace(['user.login'])  # 状态机换阶段

        await event_bus.proxy('pub').publish('test.event', {'value': 2})
        await event_bus.proxy('pub').publish('user.login', None)
        await _wait_until(lambda: len(handler.received) == 2)
        await asyncio.sleep(0.05)  # 给多余投递留出暴露窗口

        assert handler.received == ['test.event', 'user.login']  # 第二个 test.event 不再到达

    async def test_unregister_removes_routing(
        self, event_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """注销处理器：on_deactivate 撤销订阅，后续事件不再投递"""
        handler = _RecordNameHandler(subscriptions=['test.event'])
        hid = handler_registry.register(handler)

        await event_bus.proxy('pub').publish('test.event', {'value': 1})
        await _wait_until(lambda: len(handler.received) == 1)

        handler_registry.unregister(hid)

        await event_bus.proxy('pub').publish('test.event', {'value': 2})
        await asyncio.sleep(0.1)

        assert handler.received == ['test.event']
        active_ids = {hid for hid, _ in event_bus.router.routes}
        assert handler.handler_id not in active_ids

    async def test_active_empty_subscription_handler_runtime_add_routes(self) -> None:
        """激活中的空订阅处理器也被登记（激活 ⟺ 已路由）；运行期 add() 立即生效（回归 exp1）"""
        reg = _make_registry(_EventAlpha)
        hreg = EventHandlerRegistry()
        handler = _RecordNameHandler(subscriptions=[])
        bus = EventBus(reg, hreg)
        try:
            await bus.start()
            hid = hreg.register(handler)  # 运行中注册：空订阅束也应被登记
            assert handler.handler_id == hid
            assert handler.subscriptions.is_bound  # 激活 ⟺ 已登记恒成立

            handler.subscriptions.add(_EventAlpha.name)  # 运行期动态自订阅
            await bus.proxy('pub').publish(_EventAlpha.name, None)
            await _wait_until(lambda: len(handler.received) == 1)

            assert handler.received == [_EventAlpha.name]
            active_ids = {hid for hid, _ in bus.router.routes}
            assert hid in active_ids
        finally:
            await bus.stop()
