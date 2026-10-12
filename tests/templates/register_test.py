"""ModuleEventRegister 和 ModuleHandlerRegister 的单元测试"""

from typing import Any, Dict, Optional, cast

import pytest
from pydantic import BaseModel

from infinity_bus import (
    EventDeclaration,
    EventHandler,
    EventHandlerRegistry,
    EventRegistry,
    Router,
)
from infinity_bus.templates.register import ModuleEventRegister, ModuleHandlerRegister


class Matcher:
    """测试兼容：按已注册 handler 的订阅束求命中（Router 语义）。"""

    def __init__(self, event_registry: EventRegistry, handler_registry: EventHandlerRegistry) -> None:
        self._router = Router(event_registry)
        for hid, handler in handler_registry:
            self._router.subscribe(hid, handler.subscriptions)
        self._handlers = handler_registry

    def match(self, event_type: str) -> list[tuple[str, EventHandler]]:
        result: list[tuple[str, EventHandler]] = []
        for hid in self._router.match(event_type):
            handler = self._handlers.get(hid)
            if handler is not None:
                result.append((hid, handler))
        return result


# ============================================================================
# 测试用事件声明
# ============================================================================
class _TestPayload(BaseModel):
    value: int


class _EventA(EventDeclaration):
    name = 'test.module.a'
    payload_type = _TestPayload


class _EventB(EventDeclaration):
    name = 'test.module.b'
    payload_type = None


class _EventC(EventDeclaration):
    name = 'test.module.c'
    payload_type = _TestPayload


class _TestEvent(EventDeclaration):
    name = 'test.event'


class _TestAnotherEvent(EventDeclaration):
    name = 'test.another'


_EXTRA_EVENT_DECLS = (_TestEvent, _TestAnotherEvent)


# ============================================================================
# 测试用 Handler
# ============================================================================
class _SimpleHandler(EventHandler):
    def __init__(self, extra: str = 'default'):
        super().__init__(subscriptions=['test.event'])
        self.extra = extra

    async def handle(self, payload, bus_proxy, raw_event) -> None:  # type: ignore
        pass


class _AnotherHandler(EventHandler):
    def __init__(self, db: Optional[Any] = None):
        super().__init__(subscriptions=['test.another'])
        self.db = db

    async def handle(self, payload, bus_proxy, raw_event) -> None:  # type: ignore
        pass


# ============================================================================
# Fixtures
# ============================================================================
@pytest.fixture
def empty_event_registry() -> EventRegistry:
    return EventRegistry()


@pytest.fixture
def empty_handler_registry() -> EventHandlerRegistry:
    return EventHandlerRegistry()


@pytest.fixture
def module_events() -> ModuleEventRegister:
    return ModuleEventRegister('test_module')


@pytest.fixture
def module_handlers() -> ModuleHandlerRegister:
    return ModuleHandlerRegister('test_module')


# ============================================================================
# ModuleEventRegister 测试
# ============================================================================
class TestModuleEventRegister:
    """测试模块事件注册器"""

    # ---- 初始化 ----
    def test_init_creates_empty_set(self) -> None:
        reg = ModuleEventRegister('my_mod')
        assert reg.name == 'my_mod'
        assert reg.events == set()

    # ---- add_event ----
    def test_add_event_adds_to_set(self, module_events: ModuleEventRegister) -> None:
        module_events.add_event(_EventA)
        assert _EventA in module_events.events

    def test_add_event_idempotent(self, module_events: ModuleEventRegister) -> None:
        """重复添加同一事件声明不会产生重复项"""
        module_events.add_event(_EventA)
        module_events.add_event(_EventA)
        assert len(module_events.events) == 1

    # ---- event 装饰器 ----
    def test_event_decorator_registers_class(self, module_events: ModuleEventRegister) -> None:
        @module_events.event
        class _DecoratedEvent(EventDeclaration):
            name = 'test.decorated'
            payload_type = None

        assert _DecoratedEvent in module_events.events

    def test_event_decorator_returns_class_unchanged(self, module_events: ModuleEventRegister) -> None:
        """装饰器应原样返回类（不改变类型）"""
        result = module_events.event(_EventA)
        assert result is _EventA

    # ---- register_all_events ----
    def test_register_all_events_populates_registry(
        self,
        module_events: ModuleEventRegister,
        empty_event_registry: EventRegistry,
    ) -> None:
        module_events.add_event(_EventA)
        module_events.add_event(_EventB)

        module_events.register_all_events(empty_event_registry)

        assert empty_event_registry.get('test.module.a') is _EventA
        assert empty_event_registry.get('test.module.b') is _EventB

    def test_register_all_events_duplicate_name_raises(
        self,
        module_events: ModuleEventRegister,
        empty_event_registry: EventRegistry,
    ) -> None:
        """当两个事件声明同名时，第二次 register 应抛出 ValueError"""
        # 手动注册一个同名事件
        empty_event_registry.register(_EventA)
        module_events.add_event(_EventA)

        with pytest.raises(ValueError, match='重复的事件声明'):
            module_events.register_all_events(empty_event_registry)

    def test_register_all_events_empty_noop(
        self,
        module_events: ModuleEventRegister,
        empty_event_registry: EventRegistry,
    ) -> None:
        """空注册器调用 register_all_events 不应报错"""
        module_events.register_all_events(empty_event_registry)
        assert empty_event_registry.list_names() == []

    # ---- get_all_event_names ----
    def test_get_all_event_names_returns_names(self, module_events: ModuleEventRegister) -> None:
        module_events.add_event(_EventA)
        module_events.add_event(_EventB)
        module_events.add_event(_EventC)

        names = module_events.get_all_event_names()
        assert set(names) == {'test.module.a', 'test.module.b', 'test.module.c'}

    def test_get_all_event_names_empty(self, module_events: ModuleEventRegister) -> None:
        assert module_events.get_all_event_names() == []

    # ---- __repr__ ----
    def test_repr(self, module_events: ModuleEventRegister) -> None:
        module_events.add_event(_EventA)
        r = repr(module_events)
        assert 'test_module' in r
        assert '1' in r

    def test_repr_empty(self, module_events: ModuleEventRegister) -> None:
        r = repr(module_events)
        assert 'test_module' in r
        assert '0' in r


# ============================================================================
# ModuleHandlerRegister 测试
# ============================================================================
class TestModuleHandlerRegister:
    """测试模块处理器注册器"""

    # ---- 初始化 ----
    def test_init_creates_empty_set(self) -> None:
        reg = ModuleHandlerRegister('my_mod')
        assert reg.name == 'my_mod'
        assert reg.handlers == set()

    # ---- add_handler ----
    def test_add_handler_adds_to_set(self, module_handlers: ModuleHandlerRegister) -> None:
        module_handlers.add_handler(_SimpleHandler, depends=lambda: {'extra': 'hi'})
        assert len(module_handlers.handlers) == 1

    def test_add_handler_same_entry_idempotent(self, module_handlers: ModuleHandlerRegister) -> None:
        """同一个 (handler_type, depends) 重复添加不应产生重复"""

        def dep() -> Dict[str, Any]:
            return {'extra': 'hi'}

        module_handlers.add_handler(_SimpleHandler, depends=dep)
        module_handlers.add_handler(_SimpleHandler, depends=dep)
        assert len(module_handlers.handlers) == 1

    def test_add_handler_different_depends_not_deduped(self, module_handlers: ModuleHandlerRegister) -> None:
        """不同 depends 的同类型 handler 应视为不同注册项"""
        module_handlers.add_handler(_SimpleHandler, depends=lambda: {'extra': 'a'})
        module_handlers.add_handler(_SimpleHandler, depends=lambda: {'extra': 'b'})
        assert len(module_handlers.handlers) == 2

    # ---- handler 装饰器 ----
    def test_handler_decorator_registers_class(self, module_handlers: ModuleHandlerRegister) -> None:
        @module_handlers.handler()
        class _DecoratedHandler(EventHandler):  # type: ignore
            def __init__(self):
                super().__init__(subscriptions=['test.x'])

            async def handle(self, payload, bus_proxy, raw_event):  # type: ignore
                pass

        assert len(module_handlers.handlers) == 1

    def test_handler_decorator_returns_class_unchanged(self, module_handlers: ModuleHandlerRegister) -> None:
        """装饰器应原样返回类"""
        decorator = module_handlers.handler()
        result = decorator(_SimpleHandler)
        assert result is _SimpleHandler

    def test_handler_decorator_with_custom_depends(self, module_handlers: ModuleHandlerRegister) -> None:
        """装饰器接受 depends 参数并存储"""

        def factory() -> Dict[str, Any]:
            return {'extra': 'injected'}

        decorator = module_handlers.handler(depends=factory)
        decorator(_SimpleHandler)

        # 验证存储了正确的 depends
        entry = next(iter(module_handlers.handlers))
        assert entry[0] is _SimpleHandler
        assert entry[1] is factory

    # ---- register_all_handlers ----
    @pytest.mark.asyncio
    async def test_register_all_handlers_instantiates_and_registers(
        self,
        module_handlers: ModuleHandlerRegister,
        empty_event_registry: EventRegistry,
        empty_handler_registry: EventHandlerRegistry,
    ) -> None:
        for evt in _EXTRA_EVENT_DECLS:
            empty_event_registry.register(evt)
        module_handlers.add_handler(_SimpleHandler, depends=lambda: {'extra': 'test'})
        await module_handlers.register_all_handlers(empty_handler_registry)

        assert empty_handler_registry.handlers_count == 1
        handlers = Matcher(empty_event_registry, empty_handler_registry).match('test.event')
        assert len(handlers) == 1
        assert isinstance(handlers[0][1], _SimpleHandler)
        assert handlers[0][1].extra == 'test'

    @pytest.mark.asyncio
    async def test_register_all_handlers_passes_dependencies(
        self,
        module_handlers: ModuleHandlerRegister,
        empty_event_registry: EventRegistry,
        empty_handler_registry: EventHandlerRegistry,
    ) -> None:
        """验证依赖工厂的返回值正确传递给处理器构造器"""
        for evt in _EXTRA_EVENT_DECLS:
            empty_event_registry.register(evt)
        fake_db = object()
        module_handlers.add_handler(_AnotherHandler, depends=lambda: {'db': fake_db})
        await module_handlers.register_all_handlers(empty_handler_registry)

        handlers = Matcher(empty_event_registry, empty_handler_registry).match('test.another')
        assert cast(_AnotherHandler, handlers[0][1]).db is fake_db

    @pytest.mark.asyncio
    async def test_register_all_handlers_multiple_handlers(
        self,
        module_handlers: ModuleHandlerRegister,
        empty_event_registry: EventRegistry,
        empty_handler_registry: EventHandlerRegistry,
    ) -> None:
        """注册多个处理器，全部应出现在注册表中"""
        for evt in _EXTRA_EVENT_DECLS:
            empty_event_registry.register(evt)
        module_handlers.add_handler(_SimpleHandler, depends=lambda: {'extra': 'a'})
        module_handlers.add_handler(_AnotherHandler, depends=lambda: {'db': None})

        await module_handlers.register_all_handlers(empty_handler_registry)

        assert empty_handler_registry.handlers_count == 2
        assert len(Matcher(empty_event_registry, empty_handler_registry).match('test.event')) == 1
        assert len(Matcher(empty_event_registry, empty_handler_registry).match('test.another')) == 1

    @pytest.mark.asyncio
    async def test_register_all_handlers_empty_noop(
        self,
        module_handlers: ModuleHandlerRegister,
        empty_handler_registry: EventHandlerRegistry,
    ) -> None:
        """空注册器调用 register_all_handlers 不应报错"""
        await module_handlers.register_all_handlers(empty_handler_registry)
        assert empty_handler_registry.handlers_count == 0

    @pytest.mark.asyncio
    async def test_register_all_handlers_default_depends_empty_dict(
        self,
        empty_event_registry: EventRegistry,
        empty_handler_registry: EventHandlerRegistry,
    ) -> None:
        """默认 depends (lambda: {}) 时处理器应使用默认参数构造"""
        for evt in _EXTRA_EVENT_DECLS:
            empty_event_registry.register(evt)
        reg = ModuleHandlerRegister('test')
        reg.add_handler(_SimpleHandler, depends=lambda: {})
        await reg.register_all_handlers(empty_handler_registry)

        handlers = Matcher(empty_event_registry, empty_handler_registry).match('test.event')
        assert cast(_SimpleHandler, handlers[0][1]).extra == 'default'

    @pytest.mark.asyncio
    async def test_register_all_handlers_depends_factory_called_per_registration(
        self,
        empty_event_registry: EventRegistry,
        empty_handler_registry: EventHandlerRegistry,
    ) -> None:
        """每次 register_all_handlers 调用时 depends 工厂被重新调用"""
        for evt in _EXTRA_EVENT_DECLS:
            empty_event_registry.register(evt)
        call_count = 0

        def counting_factory() -> Dict[str, Any]:
            nonlocal call_count
            call_count += 1
            return {'extra': str(call_count)}

        reg = ModuleHandlerRegister('test')
        reg.add_handler(_SimpleHandler, depends=counting_factory)

        # 两次调用 register_all_handlers（第二次注册到新 registry）
        await reg.register_all_handlers(empty_handler_registry)
        reg2 = EventHandlerRegistry()
        await reg.register_all_handlers(reg2)

        assert call_count == 2
        # 第二次调用时 extra 应为 "2"
        handlers = Matcher(empty_event_registry, reg2).match('test.event')
        assert cast(_SimpleHandler, handlers[0][1]).extra == '2'

    @pytest.mark.asyncio
    async def test_register_all_handlers_one_failure_does_not_block_others(
        self,
        empty_event_registry: EventRegistry,
        empty_handler_registry: EventHandlerRegistry,
    ) -> None:
        """单个处理器注册失败（depends 工厂抛异常）不应阻止其余处理器正常注册"""
        for evt in _EXTRA_EVENT_DECLS:
            empty_event_registry.register(evt)

        def _failing_factory() -> Dict[str, Any]:
            raise RuntimeError('依赖工厂故意失败')

        reg = ModuleHandlerRegister('test')
        # 第一个会失败，第二个和第三个应正常注册
        reg.add_handler(_SimpleHandler, depends=_failing_factory)
        reg.add_handler(_SimpleHandler, depends=lambda: {'extra': 'ok-1'})
        reg.add_handler(_AnotherHandler, depends=lambda: {'db': 'ok-db'})

        # 不应抛出异常
        await reg.register_all_handlers(empty_handler_registry)

        # 只有 2 个成功注册（失败的被跳过）
        assert empty_handler_registry.handlers_count == 2

        # 验证成功的处理器可以正常匹配
        matcher = Matcher(empty_event_registry, empty_handler_registry)
        event_handlers = matcher.match('test.event')
        another_handlers = matcher.match('test.another')

        assert len(event_handlers) == 1
        assert isinstance(event_handlers[0][1], _SimpleHandler)
        assert event_handlers[0][1].extra == 'ok-1'

        assert len(another_handlers) == 1
        assert isinstance(another_handlers[0][1], _AnotherHandler)
        assert another_handlers[0][1].db == 'ok-db'

    @pytest.mark.asyncio
    async def test_register_all_handlers_all_fail_still_no_exception(
        self,
        empty_handler_registry: EventHandlerRegistry,
    ) -> None:
        """所有处理器注册均失败时也不应抛出异常，注册表保持为空"""

        def _failing_factory() -> Dict[str, Any]:
            raise RuntimeError('全部失败')

        reg = ModuleHandlerRegister('test')
        reg.add_handler(_SimpleHandler, depends=_failing_factory)
        reg.add_handler(_AnotherHandler, depends=_failing_factory)

        # 不应抛出异常
        await reg.register_all_handlers(empty_handler_registry)

        assert empty_handler_registry.handlers_count == 0

    # ---- register_all_handlers (atomic=True) ----
    @pytest.mark.asyncio
    async def test_register_all_handlers_atomic_all_succeed(
        self,
        empty_event_registry: EventRegistry,
        empty_handler_registry: EventHandlerRegistry,
    ) -> None:
        """atomic=True 且全部成功时应正常注册所有处理器"""
        for evt in _EXTRA_EVENT_DECLS:
            empty_event_registry.register(evt)

        reg = ModuleHandlerRegister('test')
        reg.add_handler(_SimpleHandler, depends=lambda: {'extra': 'a'})
        reg.add_handler(_AnotherHandler, depends=lambda: {'db': 'x'})

        await reg.register_all_handlers(empty_handler_registry, atomic=True)

        assert empty_handler_registry.handlers_count == 2

    @pytest.mark.asyncio
    async def test_register_all_handlers_atomic_rollback_on_failure(
        self,
        empty_event_registry: EventRegistry,
        empty_handler_registry: EventHandlerRegistry,
    ) -> None:
        """atomic=True 时任一处理器失败应回滚已注册的并抛出异常"""
        for evt in _EXTRA_EVENT_DECLS:
            empty_event_registry.register(evt)

        def _failing_factory() -> Dict[str, Any]:
            raise RuntimeError('依赖工厂故意失败')

        reg = ModuleHandlerRegister('test')
        # 第 1 个成功，第 2 个失败 → 第 1 个应被回滚
        reg.add_handler(_SimpleHandler, depends=lambda: {'extra': 'ok'})
        reg.add_handler(_SimpleHandler, depends=_failing_factory)

        with pytest.raises(RuntimeError, match='依赖工厂故意失败'):
            await reg.register_all_handlers(empty_handler_registry, atomic=True)

        # 回滚后注册表应为空
        assert empty_handler_registry.handlers_count == 0

    @pytest.mark.asyncio
    async def test_register_all_handlers_atomic_first_fails_no_rollback_needed(
        self,
        empty_event_registry: EventRegistry,
        empty_handler_registry: EventHandlerRegistry,
    ) -> None:
        """atomic=True 且第一个就失败时直接抛异常，注册表为空"""
        for evt in _EXTRA_EVENT_DECLS:
            empty_event_registry.register(evt)

        def _failing_factory() -> Dict[str, Any]:
            raise RuntimeError('第一个就失败')

        reg = ModuleHandlerRegister('test')
        reg.add_handler(_SimpleHandler, depends=_failing_factory)
        reg.add_handler(_AnotherHandler, depends=lambda: {'db': 'never-reached'})

        with pytest.raises(RuntimeError, match='第一个就失败'):
            await reg.register_all_handlers(empty_handler_registry, atomic=True)

        assert empty_handler_registry.handlers_count == 0

    # ---- __repr__ ----
    def test_repr(self, module_handlers: ModuleHandlerRegister) -> None:
        module_handlers.add_handler(_SimpleHandler, depends=lambda: {})
        r = repr(module_handlers)
        assert 'test_module' in r
        assert '1' in r

    def test_repr_empty(self, module_handlers: ModuleHandlerRegister) -> None:
        r = repr(module_handlers)
        assert 'test_module' in r
        assert '0' in r
