from typing import Any, Optional, cast
from unittest.mock import MagicMock

import pytest
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
)

# ============================================================================
# EventHandlerRegistry CRUD
# ============================================================================


@pytest.mark.asyncio
async def test_handler_register_crud(handler_registry: EventHandlerRegistry):
    """测试 Handler 注册表的增删查功能"""

    class SampleHandler(EventHandler):
        def __init__(self):
            super().__init__(['sample.event'])

        async def handle(
            self,
            payload: Optional[BaseModel],
            bus_proxy: Any,
            raw_event: Event,
        ) -> None:
            pass

    handler = SampleHandler()
    hid = handler_registry.register(handler)

    assert handler_registry.handlers_count == 1
    assert handler_registry.all_handlers == {hid: handler}
    assert handler_registry.get(hid) == handler
    assert hid in handler_registry

    assert handler_registry.unregister(hid) is None
    assert handler_registry.get(hid) is None
    assert hid not in handler_registry
    assert handler_registry.handlers_count == 0

    assert handler_registry.unregister('invalid_id') is None


@pytest.mark.asyncio
async def test_handler_subscriptions_copy():
    """subscriptions 应为独立副本，外部修改不影响 Handler"""

    subs: list[str | Regex] = ['a.event']

    class MyHandler(EventHandler):
        def __init__(self):
            super().__init__(subs)

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            pass

    handler = MyHandler()
    subs.append('b.event')
    assert handler.subscriptions == ['a.event']


# ============================================================================
# Regex 协议方法
# ============================================================================


def test_regex_str_and_repr():
    """Regex.__str__ 返回原始模式，__repr__ 返回可求值形式"""
    r = Regex(r'user\..*')
    assert str(r) == r'user\..*'
    assert repr(r) == f'Regex({r.pattern!r})'


def test_regex_eq():
    """Regex 可与 Regex 或 str 进行相等比较"""
    a = Regex(r'a\..*')
    b = Regex(r'a\..*')
    c = Regex(r'b\..*')

    assert a == b
    assert a != c
    assert a == r'a\..*'
    assert a != r'b\..*'
    assert a != 42  # 非 Regex/str 返回 NotImplemented


def test_regex_hash():
    """相同模式的 Regex 哈希值相同，可放入 set"""
    a = Regex(r'a\..*')
    b = Regex(r'a\..*')
    c = Regex(r'b\..*')

    assert hash(a) == hash(b)
    assert hash(a) != hash(c)

    s = {a, b, c}
    assert len(s) == 2


def test_regex_pattern_property():
    """pattern 属性返回原始正则字符串"""
    r = Regex(r'user\.\w+')
    assert r.pattern == r'user\.\w+'


def test_regex_fullmatch():
    """fullmatch 正确匹配完整字符串"""
    r = Regex(r'user\.\w+')
    assert r.fullmatch('user.login') is not None
    assert r.fullmatch('user.login.extra') is None
    assert r.fullmatch('admin.login') is None


# ============================================================================
# EventHandler 协议方法
# ============================================================================


@pytest.mark.asyncio
async def test_handler_call_delegates_to_handle(handler_registry: EventHandlerRegistry):
    """EventHandler.__call__ 应解包 event.data 并委托给 handle()"""
    received_payload: Optional[BaseModel] = None

    class CallTestHandler(EventHandler):
        def __init__(self):
            super().__init__(['test.call'])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            nonlocal received_payload
            received_payload = payload

    handler = CallTestHandler()
    handler_registry.register(handler)

    # 直接调用 __call__，模拟总线分发
    from pydantic import BaseModel as PydanticBaseModel

    class DummyPayload(PydanticBaseModel):
        value: int

    payload = DummyPayload(value=42)
    event = Event(name='test.call', data=payload)
    # 需要一个 mock proxy
    from unittest.mock import MagicMock

    mock_proxy = MagicMock()
    await handler(mock_proxy, event)

    assert received_payload is not None
    assert received_payload.value == 42


# ============================================================================
# EventHandlerRegistry 协议方法
# ============================================================================


def test_registry_len(handler_registry: EventHandlerRegistry):
    """__len__ 返回已注册处理器数量"""
    assert len(handler_registry) == 0

    class LenHandler(EventHandler):
        def __init__(self):
            super().__init__(['len.event'])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            pass

    handler_registry.register(LenHandler())
    assert len(handler_registry) == 1

    handler_registry.register(LenHandler())
    assert len(handler_registry) == 2


def test_registry_contains(handler_registry: EventHandlerRegistry):
    """__contains__ 检查 handler ID 是否已注册"""

    class ContainsHandler(EventHandler):
        def __init__(self):
            super().__init__(['contains.event'])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            pass

    hid = handler_registry.register(ContainsHandler())
    assert hid in handler_registry
    assert 'nonexistent' not in handler_registry


def test_registry_iter(handler_registry: EventHandlerRegistry):
    """__iter__ 迭代所有 (handler_id, handler) 对"""

    class IterHandler(EventHandler):
        def __init__(self):
            super().__init__(['iter.event'])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            pass

    h1 = IterHandler()
    h2 = IterHandler()
    hid1 = handler_registry.register(h1)
    hid2 = handler_registry.register(h2)

    items = dict(handler_registry)
    assert items[hid1] is h1
    assert items[hid2] is h2
    assert len(items) == 2


def test_registry_clear(handler_registry: EventHandlerRegistry):
    """clear() 清除所有已注册处理器"""

    class ClearHandler(EventHandler):
        def __init__(self):
            super().__init__(['clear.event'])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            pass

    handler_registry.register(ClearHandler())
    handler_registry.register(ClearHandler())
    assert len(handler_registry) == 2

    handler_registry.clear()
    assert len(handler_registry) == 0
    assert handler_registry.handlers_count == 0


# ============================================================================
# EventHandler 生命周期钩子（处理器可感知自己的 id）
# ============================================================================


class IdAwareHandler(EventHandler):
    """测试用：注册后可感知自己 id 的处理器"""

    def __init__(self):
        super().__init__(['id.event'])

    async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
        pass


def test_handler_gets_id_on_register(handler_registry: EventHandlerRegistry):
    """register 应通过 on_registered 钩子把分配的 id 回填给处理器"""
    handler = IdAwareHandler()
    assert handler.handler_id is None  # 未注册时无 id

    hid = handler_registry.register(handler)

    assert handler.handler_id == hid  # 注册后感知自己的 id
    assert handler_registry.get(hid) is handler


def test_handler_loses_id_on_unregister(handler_registry: EventHandlerRegistry):
    """unregister 应通过 on_unregistered 钩子清空处理器的 id"""
    handler = IdAwareHandler()
    hid = handler_registry.register(handler)
    assert handler.handler_id == hid

    assert handler_registry.unregister(hid) is None
    assert handler.handler_id is None

    # 未注册的 id 视为幂等空操作（返回 None），不触发任何钩子副作用
    handler2 = IdAwareHandler()
    assert handler_registry.unregister('invalid_id') is None
    assert handler2.handler_id is None


def test_handler_on_registered_override_receives_id(handler_registry: EventHandlerRegistry):
    """子类覆写 on_registered 可感知注册事件与 id"""
    observed: list[str] = []

    class OverridingHandler(EventHandler):
        def __init__(self):
            super().__init__(['id.event'])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            pass

        def on_registered(self, handler_id: str) -> None:
            observed.append(handler_id)
            super().on_registered(handler_id)

    hid = handler_registry.register(OverridingHandler())
    assert observed == [hid]


def test_handler_on_unregistered_override_observes(handler_registry: EventHandlerRegistry):
    """子类覆写 on_unregistered 可感知注销事件"""
    observed: list[Optional[str]] = []

    class OverridingHandler(EventHandler):
        def __init__(self):
            super().__init__(['id.event'])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            pass

        def on_unregistered(self) -> None:
            observed.append(self.handler_id)
            super().on_unregistered()

    handler = OverridingHandler()
    hid = handler_registry.register(handler)
    assert handler_registry.unregister(hid) is None

    assert observed == [hid]
    assert handler.handler_id is None


def test_registry_clear_notifies_all_handlers(handler_registry: EventHandlerRegistry):
    """clear() 应逐一向在册处理器触发下线钩子（清空 id）"""
    h1 = IdAwareHandler()
    h2 = IdAwareHandler()
    handler_registry.register(h1)
    handler_registry.register(h2)

    handler_registry.clear()

    assert h1.handler_id is None
    assert h2.handler_id is None
    assert len(handler_registry) == 0


def test_registry_rejects_duplicate_instance(handler_registry: EventHandlerRegistry):
    """同一处理器实例禁止重复注册（保证 id 与实例一一对应）"""
    handler = IdAwareHandler()
    handler_registry.register(handler)

    with pytest.raises(ValueError):
        handler_registry.register(handler)

    # 不同实例允许注册
    handler_registry.register(IdAwareHandler())
    assert len(handler_registry) == 2


# ============================================================================
# 原子注册回退 & 兜底注销（回滚覆盖总线侧副作用：补发 on_deactivate / on_unregistered）
# ============================================================================


class RaisingRegisterHandler(EventHandler):
    """on_registered 抛错的处理器"""

    def __init__(self):
        super().__init__(['id.event'])

    async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
        pass

    def on_registered(self, handler_id: str) -> None:
        raise RuntimeError('register hook boom')  # 失败是处理器自己的事


class RaisingUnregisterHandler(EventHandler):
    """on_unregistered 抛错的处理器"""

    def __init__(self):
        super().__init__(['id.event'])

    async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
        pass

    def on_unregistered(self) -> None:
        raise RuntimeError('unregister hook boom')  # 失败是处理器自己的事


def test_register_is_atomic_on_hook_failure(handler_registry: EventHandlerRegistry):
    """on_registered 抛错时 register 应原子回退：撤销入库、版本不变、向上抛出"""
    version_before = handler_registry.version

    with pytest.raises(RuntimeError, match='register hook boom'):
        handler_registry.register(RaisingRegisterHandler())

    assert len(handler_registry) == 0  # 不残留半注册状态
    assert handler_registry.version == version_before  # 版本未递增

    # 回退后注册表可正常复用
    ok = IdAwareHandler()
    hid = handler_registry.register(ok)
    assert ok.handler_id == hid


def test_unregister_guarantees_removal_and_reports_hook_failure(handler_registry: EventHandlerRegistry):
    """on_unregistered 抛错时 unregister 仍保证移除，并把失败显式返回（不静默）"""
    handler = RaisingUnregisterHandler()
    hid = handler_registry.register(handler)
    version_after_register = handler_registry.version

    err = handler_registry.unregister(hid)  # 移除成功且失败显式上报
    assert isinstance(err, RuntimeError)
    assert 'unregister hook boom' in str(err)
    assert hid not in handler_registry
    assert handler_registry.get(hid) is None
    assert handler_registry.version == version_after_register + 1


def test_unregister_unknown_id_is_idempotent_noop(handler_registry: EventHandlerRegistry):
    """未知 id 视为幂等空操作：返回 None、不触发任何钩子、版本不变"""
    assert handler_registry.unregister('invalid_id') is None
    assert handler_registry.version == 0


def test_clear_removes_all_and_reports_hook_failures(handler_registry: EventHandlerRegistry):
    """clear 逐个触发 on_unregistered：单个失败不中断、全部移除，且失败显式上报（带实体）"""
    bad = RaisingUnregisterHandler()
    good = IdAwareHandler()
    bad_id = handler_registry.register(bad)
    handler_registry.register(good)

    failures = handler_registry.clear()

    assert len(handler_registry) == 0
    assert good.handler_id is None
    assert len(failures) == 1
    assert failures[0][0] == bad_id
    assert 'unregister hook boom' in str(failures[0][1])


# ============================================================================
# __call__ 生成的 proxy source 格式：类名:id / 类名:unregister
# ============================================================================


class SourceProbeHandler(EventHandler):
    """用于探测 __call__ 传给 bus.proxy 的 source"""

    def __init__(self):
        super().__init__(['probe.event'])

    async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
        pass


async def test_call_proxy_source_includes_id_when_registered(handler_registry: EventHandlerRegistry):
    """注册后 __call__ 生成 proxy 的 source 应为 类名:id"""
    handler = SourceProbeHandler()
    hid = handler_registry.register(handler)

    bus = MagicMock()
    await handler(bus, Event(name='probe.event'))

    source = bus.proxy.call_args[0][0]
    assert source == f'{SourceProbeHandler.__name__}:{hid}'


async def test_call_proxy_source_shows_unregister_when_not_registered():
    """未注册（无 id）时 __call__ 生成 proxy 的 source 应为 类名:unregister"""
    handler = SourceProbeHandler()

    bus = MagicMock()
    await handler(bus, Event(name='probe.event'))

    source = bus.proxy.call_args[0][0]
    assert source == f'{SourceProbeHandler.__name__}:unregister'


def test_registry_destroy_triggers_clear():
    """注册表被销毁时应触发全部在册处理器的下线钩子（__del__ 兜底 clear）"""
    import gc

    unregistered: list[str] = []

    class DelHandler(EventHandler):
        def __init__(self):
            super().__init__(['del.event'])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
            pass

        def on_unregistered(self) -> None:
            if self.handler_id is not None:
                unregistered.append(self.handler_id)
            super().on_unregistered()

    registry = EventHandlerRegistry()
    registry.register(DelHandler())
    registry.register(DelHandler())

    del registry
    gc.collect()

    assert len(unregistered) == 2  # 两个在册处理器均收到下线钩子


# ============================================================================
# 注册表-总线绑定生命周期：activate / deactivate / on_activate / on_deactivate
# ============================================================================


class BusLifecycleHandler(EventHandler):
    """记录完整生命周期事件序（含 id 可用性）的处理器"""

    def __init__(self, fail_active: bool = False, fail_deactivate: bool = False):
        super().__init__(['lc.event'])
        self.events: list[str] = []
        self.fail_active = fail_active
        self.fail_deactivate = fail_deactivate

    async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
        pass

    def on_registered(self, handler_id: str) -> None:
        self.events.append(f'registered:{handler_id}')
        super().on_registered(handler_id)

    def on_unregistered(self) -> None:
        self.events.append('unregistered')
        super().on_unregistered()

    def on_activate(self, bus: Any) -> None:
        self.events.append(f'active:{self.handler_id}')
        if self.fail_active:
            raise RuntimeError('active boom')

    def on_deactivate(self, bus: Any) -> None:
        self.events.append(f'deactivate:{self.handler_id}')
        if self.fail_deactivate:
            raise RuntimeError('deactivate boom')


def test_registry_activate_fires_on_activate_for_all(handler_registry: EventHandlerRegistry):
    """activate(bus) 应对全部在册处理器触发 on_activate（此时 id 已可用）"""
    h1 = BusLifecycleHandler()
    h2 = BusLifecycleHandler()
    handler_registry.register(h1)
    handler_registry.register(h2)

    handler_registry.activate(MagicMock())

    assert h1.events == [f'registered:{h1.handler_id}', f'active:{h1.handler_id}']
    assert h2.events == [f'registered:{h2.handler_id}', f'active:{h2.handler_id}']


def test_registry_activate_rejects_second_bus(handler_registry: EventHandlerRegistry):
    """注册表已绑定总线时再次 activate 应抛 RuntimeError（禁止跨总线共用）"""
    handler_registry.register(BusLifecycleHandler())
    handler_registry.activate(MagicMock())

    with pytest.raises(RuntimeError, match='已绑定总线'):
        handler_registry.activate(MagicMock())


def test_registry_register_while_active_fires_on_activate(handler_registry: EventHandlerRegistry):
    """已绑定总线时 register 应在 on_registered 后触发 on_activate"""
    handler_registry.activate(MagicMock())

    h = BusLifecycleHandler()
    hid = handler_registry.register(h)

    assert h.events == [f'registered:{hid}', f'active:{hid}']


def test_registry_register_while_active_rolls_back_when_on_activate_fails(
    handler_registry: EventHandlerRegistry,
):
    """已绑定总线时 on_activate 抛错应原子回退：撤销入库、版本不变、向上抛出"""
    handler_registry.activate(MagicMock())
    version_before = handler_registry.version

    with pytest.raises(RuntimeError, match='active boom'):
        handler_registry.register(BusLifecycleHandler(fail_active=True))

    assert len(handler_registry) == 0
    assert handler_registry.version == version_before


def test_registry_deactivate_fires_on_deactivate(handler_registry: EventHandlerRegistry):
    """deactivate() 应对全部在册处理器触发 on_deactivate（id 仍可用、不清空）"""
    h = BusLifecycleHandler()
    hid = handler_registry.register(h)
    handler_registry.activate(MagicMock())

    handler_registry.deactivate()

    assert h.events == [f'registered:{hid}', f'active:{hid}', f'deactivate:{hid}']
    assert h.handler_id == hid  # deactivate 不清 id（仍注册在册）


def test_registry_deactivate_idempotent_when_not_bound(handler_registry: EventHandlerRegistry):
    """未绑定时 deactivate() 为空操作（幂等）"""
    h = BusLifecycleHandler()
    handler_registry.register(h)

    handler_registry.deactivate()

    assert len(handler_registry) == 1
    assert h.handler_id is not None


def test_registry_unregister_while_active_orders_deactivate_then_unregistered(
    handler_registry: EventHandlerRegistry,
):
    """注销时应先 on_deactivate（id 可用）再 on_unregistered（清 id）"""
    h = BusLifecycleHandler()
    hid = handler_registry.register(h)
    handler_registry.activate(MagicMock())

    assert handler_registry.unregister(hid) is None

    assert h.events == [f'registered:{hid}', f'active:{hid}', f'deactivate:{hid}', 'unregistered']
    assert h.handler_id is None


def test_registry_clear_while_active_deactivates_all(handler_registry: EventHandlerRegistry):
    """clear() 应先统一 deactivate（全部下线）再逐个 unregistered，保证全部移除"""
    h1 = BusLifecycleHandler()
    h2 = BusLifecycleHandler()
    handler_registry.register(h1)
    handler_registry.register(h2)
    handler_registry.activate(MagicMock())

    handler_registry.clear()

    assert len(handler_registry) == 0
    assert h1.events[-2].startswith('deactivate:')
    assert h1.events[-1] == 'unregistered'
    assert h2.events[-2].startswith('deactivate:')
    assert h2.events[-1] == 'unregistered'
    assert h1.handler_id is None
    assert h2.handler_id is None


def test_registry_activate_all_or_nothing_rolls_back_on_failure(handler_registry: EventHandlerRegistry):
    """任一 on_activate 失败 → activate 整体回退：解绑、已激活者补 on_deactivate、抛错；不丢弃处理器"""
    good = BusLifecycleHandler()  # 先注册 good（将成功激活）
    bad = BusLifecycleHandler(fail_active=True)
    handler_registry.register(good)
    handler_registry.register(bad)
    version_before = handler_registry.version

    with pytest.raises(RuntimeError, match='active boom'):
        handler_registry.activate(MagicMock())

    # 不丢弃：两者仍在册、仍持有 id；版本未变
    assert len(handler_registry) == 2
    assert good.handler_id is not None
    assert bad.handler_id is not None
    assert handler_registry.version == version_before

    # 回卷覆盖全部已尝试者：good 与失败者 bad 都收到 on_deactivate（撤销副作用）
    assert good.events[-1].startswith('deactivate:')
    assert bad.events[-1].startswith('deactivate:')

    # 回退后注册表未绑定；修复（移除坏者）后可重新激活
    handler_registry.unregister(bad.handler_id or '')
    handler_registry.activate(MagicMock())
    assert good.events[-1].startswith('active:')


def test_registry_activate_supports_nested_register(handler_registry: EventHandlerRegistry):
    """_bus 前置绑定：on_activate 内嵌套 register 的新处理器立即收到 on_activate 且不重复激活"""

    class ActivateRegistersHandler(BusLifecycleHandler):
        def __init__(self):
            super().__init__()
            self.sibling: Optional[BusLifecycleHandler] = None

        def on_activate(self, bus: Any) -> None:
            super().on_activate(bus)
            sibling = BusLifecycleHandler()
            self.sibling = sibling
            handler_registry.register(sibling)  # 嵌套注册：应触发 sibling.on_activate

    outer = ActivateRegistersHandler()
    handler_registry.register(outer)

    handler_registry.activate(MagicMock())

    sibling = outer.sibling
    assert sibling is not None
    hid = sibling.handler_id
    assert hid is not None
    assert sibling.events == [f'registered:{hid}', f'active:{hid}']
    assert sibling.events.count(f'active:{hid}') == 1  # 快照遍历不会重复激活


def test_registry_deactivate_keeps_failing_handlers(handler_registry: EventHandlerRegistry):
    """deactivate() 中 on_deactivate 抛错的处理器应保留在册（不静默移除）"""
    bad = BusLifecycleHandler(fail_deactivate=True)
    good = BusLifecycleHandler()
    handler_registry.register(bad)
    handler_registry.register(good)
    version_before = handler_registry.version
    handler_registry.activate(MagicMock())

    handler_registry.deactivate()

    # 失败者与正常者都保留在册、id 不变；版本不变（无移除）
    assert len(handler_registry) == 2
    assert bad.handler_id is not None
    assert good.handler_id is not None
    assert handler_registry.version == version_before
    # 两者都收到 on_deactivate
    assert bad.events[-1].startswith('deactivate:')
    assert good.events[-1].startswith('deactivate:')


# ============================================================================
# 总线侧副作用回滚回归（exp2/exp3）：幽灵路由 / 残留 id / 束绑定
# ============================================================================


class _GhostEvent(EventDeclaration):
    name = 'lc.ghost.event'


class _SubscribeThenRaiseHandler(EventHandler):
    """on_activate 先 super() 订阅再抛错：验证回滚能撤销已登记的订阅"""

    def __init__(self, fail_after_subscribe: bool = False):
        super().__init__([_GhostEvent.name])
        self.fail_after_subscribe = fail_after_subscribe
        self.deactivate_calls = 0

    async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
        pass

    def on_activate(self, bus: Any) -> None:
        super().on_activate(bus)  # 先登记订阅
        if self.fail_after_subscribe:
            raise RuntimeError('boom after subscribe')

    def on_deactivate(self, bus: Any) -> None:
        super().on_deactivate(bus)
        self.deactivate_calls += 1


class _RouterBus:
    """暴露 router 的最小总线替身（用于单测总线侧副作用）"""

    def __init__(self, event_registry: EventRegistry) -> None:
        self.router = Router(event_registry)


def test_register_rollback_cleans_partial_activation_side_effects():
    """register：on_activate 订阅后抛错 → 回滚应撤销订阅、解绑束、清空 id（无幽灵路由）"""
    events = EventRegistry()
    events.register(_GhostEvent)
    bus = _RouterBus(events)
    hreg = EventHandlerRegistry()
    hreg.activate(cast(Any, bus))  # 绑定到总线（空注册表）

    h = _SubscribeThenRaiseHandler(fail_after_subscribe=True)
    with pytest.raises(RuntimeError, match='boom after subscribe'):
        hreg.register(h)

    assert len(hreg) == 0
    assert h.handler_id is None
    assert not h.subscriptions.is_bound
    assert bus.router.match(_GhostEvent.name) == []  # 无幽灵路由
    assert h.deactivate_calls == 1  # 回滚补发 on_deactivate


def test_activate_rollback_includes_failing_handler_side_effects():
    """activate：on_activate 订阅后抛错 → 回滚覆盖失败者本身，路由表无幽灵条目"""
    events = EventRegistry()
    events.register(_GhostEvent)
    bus = _RouterBus(events)
    hreg = EventHandlerRegistry()
    good = _SubscribeThenRaiseHandler()
    bad = _SubscribeThenRaiseHandler(fail_after_subscribe=True)
    hreg.register(good)
    hreg.register(bad)

    with pytest.raises(RuntimeError, match='boom after subscribe'):
        hreg.activate(cast(Any, bus))

    assert len(hreg) == 2  # 处理器保留在册
    assert bus.router.routes == ()  # 路由表干净
    assert good.deactivate_calls == 1
    assert bad.deactivate_calls == 1  # 失败者也收到回卷


def test_deactivate_returns_failures_and_strict_raises(handler_registry: EventHandlerRegistry):
    """deactivate() 返回失败清单；strict=True 时抛出首个失败（调用方感知清理不完整）"""
    bad = BusLifecycleHandler(fail_deactivate=True)
    good = BusLifecycleHandler()
    handler_registry.register(bad)
    handler_registry.register(good)
    handler_registry.activate(MagicMock())

    failures = handler_registry.deactivate()
    assert isinstance(failures, list)
    assert len(failures) == 1
    assert failures[0][0] == bad.handler_id
    assert 'deactivate boom' in str(failures[0][1])
    assert len(handler_registry) == 2  # 失败者保留在册

    # 再次激活后 strict=True 应抛出首个失败
    handler_registry.activate(MagicMock())
    with pytest.raises(RuntimeError, match='deactivate boom'):
        handler_registry.deactivate(strict=True)
