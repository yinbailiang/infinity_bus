"""EventBus 集成测试：发布/订阅、并发、超时、停机等。"""

import asyncio
import time
from typing import Any, Awaitable, Callable, List, Optional

import pytest
from conftest import (
    BlockingHandler,
    ConcurrentTrackingHandler,
    CountingHandler,
    ErrorSpyHandler,
    PatternSpyHandler,
    SlowHandler,
    publish_many,
    wait_for_condition,
)
from pydantic import BaseModel, ValidationError

from infinity_bus import (
    BusError,
    BusShuttingDown,
    BusStartFailed,
    BusStartRollbackError,
    DispatchNext,
    Event,
    EventBus,
    EventHandler,
    EventHandlerRegistry,
    EventRegistry,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
    Middleware,
    MiddlewareChain,
    ShutdownConfig,
    ShutdownEvent,
    TaskErrorEvent,
    TaskRecord,
)
from infinity_bus.middleware import BeforePublishNext, OnPublishNext

# ============================================================================
# 发布校验
# ============================================================================


@pytest.mark.asyncio
async def test_unknown_event_publish(event_bus: EventBus):
    """验证发布未注册事件时抛出 ValueError"""
    with pytest.raises(ValueError):
        await event_bus.proxy('test_pub').publish('unknown.event', None)


@pytest.mark.asyncio
async def test_payload_check(event_bus: EventBus):
    """验证发布事件时负载类型检查"""

    # 声明需要负载但未提供
    with pytest.raises(ValueError):
        await event_bus.proxy('test_pub').publish('test.event', None)

    # 声明需要负载但提供了错误类型
    with pytest.raises(ValidationError):
        await event_bus.proxy('test_pub').publish('test.event', {'value': 'not an int'})

    class ErrorPayload(BaseModel):
        pass

    # 声明需要负载且提供了错误 BaseModel 子类
    with pytest.raises(TypeError):
        await event_bus.proxy('test_pub').publish('test.event', ErrorPayload())

    # 声明不需要负载但提供了负载
    with pytest.raises(ValueError):
        await event_bus.proxy('test_pub').publish('test.slow', {'unexpected': 'data'})


# ============================================================================
# 生命周期
# ============================================================================


@pytest.mark.asyncio
async def test_double_start_stop(event_bus_factory: Callable[..., EventBus]):
    """验证重复启动或停止幂等"""
    bus = event_bus_factory()
    await bus.start()
    assert bus.is_running
    await bus.start()
    assert bus.is_running
    await bus.stop()
    assert not bus.is_running
    await bus.stop()
    assert not bus.is_running


@pytest.mark.asyncio
async def test_context_manager(event_bus_factory: Callable[..., EventBus]):
    """验证 async with 上下文管理器"""
    bus = event_bus_factory()
    async with bus:
        assert bus.is_running
    assert not bus.is_running


@pytest.mark.asyncio
async def test_publish_before_start(
    event_bus_factory: Callable[..., EventBus],
):
    """总线未启动时发布事件应抛出 RuntimeError"""
    bus = event_bus_factory()
    with pytest.raises(RuntimeError, match='EventBus is not running'):
        await bus.proxy('test').publish('test.event', {'value': 1})


# ============================================================================
# 并发与背压
# ============================================================================


@pytest.mark.slow
@pytest.mark.asyncio
async def test_high_concurrency_throughput(
    event_bus_factory: Callable[..., EventBus],
    handler_registry: EventHandlerRegistry,
) -> None:
    """验证高并发下无事件丢失"""
    event_bus = event_bus_factory(max_queue_size=1024, max_handler_semaphore=256)
    handler = CountingHandler()
    await handler_registry.register(handler)

    N = 65536
    async with event_bus:
        tasks = [event_bus.proxy('pub').publish('test.event', {'value': i}) for i in range(N)]
        await asyncio.gather(*tasks)

        await wait_for_condition(lambda: handler.count == N, timeout=2.0)
        assert handler.count == N, f'吞吐量丢失: 期望 {N}, 实际 {handler.count}'


@pytest.mark.asyncio
async def test_backpressure_no_deadlock(
    event_bus_factory: Callable[..., EventBus],
    handler_registry: EventHandlerRegistry,
) -> None:
    """并发限流：超过 Semaphore 限制时，新任务应等待"""
    bus = event_bus_factory(max_handler_semaphore=2)
    handler = ConcurrentTrackingHandler(subscriptions=['test.block'])
    await handler_registry.register(handler)

    N = 10
    async with bus:
        tasks = [asyncio.create_task(bus.proxy('bp_pub').publish('test.block', None)) for _ in range(N)]

        await handler.wait_started()
        await asyncio.sleep(0.05)
        assert handler.max_seen == 2, f'Semaphore 限流失效: max_seen={handler.max_seen}'

        handler.release_all()
        await asyncio.gather(*tasks)

    assert handler.active_count == 0


# ============================================================================
# 错误处理
# ============================================================================


@pytest.mark.asyncio
async def test_error_isolation_and_propagation(event_bus: EventBus, handler_registry: EventHandlerRegistry) -> None:
    """单个 Handler 错误不影响其他 Handler，且错误被正确上报"""
    counter = CountingHandler()
    spy = ErrorSpyHandler()
    await handler_registry.register(counter)
    await handler_registry.register(spy)

    tasks: List[Awaitable[Any]] = await publish_many(
        event_bus,
        'test.event',
        [{'value': 1}, {'value': -1}, {'value': 2}],
    )
    await asyncio.gather(*tasks)
    await wait_for_condition(lambda: len(spy.captured) >= 1)

    assert counter.count == 2, '有效事件应被正常计数'
    assert len(spy.captured) == 1
    assert spy.captured[0]['type'] == 'ValueError'


@pytest.mark.asyncio
async def test_handler_timeout_handling(event_bus: EventBus, handler_registry: EventHandlerRegistry) -> None:
    """Handler 超时后触发错误事件，且不影响总线运行"""
    slow = SlowHandler(delay=0.5, subscriptions=['test.slow'])
    spy = ErrorSpyHandler()
    await handler_registry.register(slow)
    await handler_registry.register(spy)

    await event_bus.proxy('timeout_pub').publish('test.slow', None)

    await wait_for_condition(lambda: len(spy.captured) >= 1, timeout=1.0)
    assert slow.completed == 0, '超时任务不应完成计数'
    assert spy.captured[0]['type'] == 'TimeoutError'


# ============================================================================
# 路由
# ============================================================================


@pytest.mark.asyncio
async def test_pattern_matching_routing(event_bus: EventBus, handler_registry: EventHandlerRegistry) -> None:
    """正则表达式订阅的路由正确性"""
    spy = PatternSpyHandler(pattern=r'user\..*')
    await handler_registry.register(spy)

    await event_bus.proxy('route_pub').publish('user.login', None)
    await event_bus.proxy('route_pub').publish('user.logout', None)
    await event_bus.proxy('route_pub').publish('admin.login', None)

    await wait_for_condition(lambda: len(spy.triggered) >= 2)
    assert set(spy.triggered) == {'user.login', 'user.logout'}


@pytest.mark.asyncio
async def test_multi_handler_same_event(
    event_bus: EventBus,
    handler_registry: EventHandlerRegistry,
) -> None:
    """同一事件可被多个 Handler 处理"""
    results: List[str] = []

    class HandlerA(EventHandler):
        def __init__(self):
            super().__init__(['test.event'])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            results.append('A')

    class HandlerB(EventHandler):
        def __init__(self):
            super().__init__(['test.event'])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            results.append('B')

    await handler_registry.register(HandlerA())
    await handler_registry.register(HandlerB())

    await event_bus.proxy('test').publish('test.event', {'value': 1})
    await wait_for_condition(lambda: len(results) == 2)
    assert set(results) == {'A', 'B'}


# ============================================================================
# 停机
# ============================================================================


@pytest.mark.asyncio
async def test_graceful_shutdown_and_cleanup(event_bus: EventBus, handler_registry: EventHandlerRegistry) -> None:
    """总线优雅停止后资源清理完全"""
    handler = CountingHandler()
    await handler_registry.register(handler)

    await event_bus.proxy('shut_pub').publish('test.event', {'value': 1})
    await wait_for_condition(lambda: handler.count == 1)

    await event_bus.stop()

    assert not event_bus.is_running
    assert event_bus.active_task_count == 0
    assert event_bus.queue_size == 0


@pytest.mark.asyncio
async def test_shutdown_cancels_pending_tasks(
    handler_registry: EventHandlerRegistry,
    base_event_registry: EventRegistry,
) -> None:
    """关闭时若 handler 执行超时，应取消 pending 任务并完成关闭"""
    bus = EventBus(
        base_event_registry,
        handler_registry,
        queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
        max_handler_semaphore=2,
        shutdown=ShutdownConfig(tasks_timeout=0.3),
    )
    handler = BlockingHandler(subscriptions=['test.block'])
    await handler_registry.register(handler)

    async with bus:
        await bus.proxy('test').publish('test.block', None)
        await handler.wait_started()

    assert not bus.is_running
    assert bus.active_task_count == 0


@pytest.mark.asyncio
async def test_shutting_down_exception(event_bus: EventBus, handler_registry: EventHandlerRegistry) -> None:
    """总线停止过程中新发布事件应抛出 BusShuttingDown"""
    slow = SlowHandler(delay=2.0, subscriptions=['test.slow'])
    await handler_registry.register(slow)

    asyncio.create_task(event_bus.proxy('slow_pub').publish('test.slow', None))
    await slow.wait_started()

    stop_task = asyncio.create_task(event_bus.stop())

    with pytest.raises(BusShuttingDown):
        for _ in range(20):
            await event_bus.proxy('probe').publish('test.event', {'value': 1})
            await asyncio.sleep(0.05)

    await stop_task
    assert not event_bus.is_running

    with pytest.raises(RuntimeError, match='EventBus is not running'):
        await event_bus.proxy('after_stop').publish('test.event', {'value': 2})


# ============================================================================
# 属性与内部路径覆盖
# ============================================================================


@pytest.mark.asyncio
async def test_proxy_handlers_registry(event_bus: EventBus):
    """Proxy.handlers_registry 返回处理器注册表"""
    proxy = event_bus.proxy('test')
    assert proxy.handlers_registry is not None


@pytest.mark.asyncio
async def test_is_publishing_enabled(event_bus: EventBus):
    """is_publishing_enabled 反映发布开关状态"""
    assert event_bus.is_publishing_enabled is True
    await event_bus.stop()
    assert event_bus.is_publishing_enabled is False


@pytest.mark.asyncio
async def test_system_events_auto_registered():
    """未预注册 TaskErrorEvent 时，总线自动补注册"""
    reg = EventRegistry()
    hreg = EventHandlerRegistry()
    _ = EventBus(reg, hreg)
    assert reg.get(TaskErrorEvent.name) is not None
    assert reg.get(ShutdownEvent.name) is not None


class _BusWithBrokenStop(EventBus):
    """stop() 会抛出异常的 EventBus 子类，用于测试 __aexit__ 异常路径"""

    async def stop(self) -> None:
        await super().stop()
        raise RuntimeError('stop boom')


@pytest.mark.asyncio
async def test_context_manager_stop_error_propagates(
    base_event_registry: EventRegistry,
    handler_registry: EventHandlerRegistry,
):
    """__aexit__: stop() 异常且无上下文异常时，应传播 stop 错误"""
    bus = _BusWithBrokenStop(base_event_registry, handler_registry)
    await bus.start()

    with pytest.raises(RuntimeError, match='stop boom'):
        async with bus:
            pass


@pytest.mark.asyncio
async def test_context_manager_stop_error_with_context_exception(
    base_event_registry: EventRegistry,
    handler_registry: EventHandlerRegistry,
):
    """__aexit__: stop() 异常 + 上下文异常 → 传播原异常，stop 错误仅记日志"""
    bus = _BusWithBrokenStop(base_event_registry, handler_registry)
    await bus.start()

    with pytest.raises(ValueError, match='context error'):
        async with bus:
            raise ValueError('context error')


# ============================================================================
# 长稳测试（慢）
# ============================================================================


@pytest.mark.slow
@pytest.mark.asyncio
async def test_long_running_stability(
    event_bus_factory: Callable[..., EventBus],
    handler_registry: EventHandlerRegistry,
) -> None:
    """长时间运行压力测试：验证无资源泄漏和队列溢出"""
    bus: EventBus = event_bus_factory(max_queue_size=10, max_handler_semaphore=25)
    handler = CountingHandler()
    await handler_registry.register(handler)

    duration = 10
    rate = 512
    expected = duration * rate

    async with bus:
        start = time.time()
        while time.time() - start < duration:
            batch = [bus.proxy('stress_pub').publish('test.event', {'value': i}) for i in range(rate)]
            await asyncio.gather(*batch)
            await asyncio.sleep(1.0)

            active = bus.active_task_count
            qsize = bus.queue_size
            assert active <= 25, f'任务泄漏: active={active}'
            assert qsize <= 10, f'队列溢出: qsize={qsize}'

    assert handler.count >= expected * 0.95
    assert bus.active_task_count == 0


# ============================================================================
# start() 失败回滚（回归 exp2）：不泄漏调度任务、可重试
# ============================================================================


class _StartBoomHandler(EventHandler):
    """on_activate 抛错：用于验证 start() 失败回滚与可重试"""

    def __init__(self):
        super().__init__(['test.event'])
        self.activated = 0

    async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
        pass

    async def on_activate(self, bus: EventBus) -> None:
        self.activated += 1
        raise RuntimeError('start boom')


@pytest.mark.asyncio
async def test_start_failure_is_retryable_and_no_dispatch_leak(event_bus_factory: Callable[..., EventBus]):
    """start() 激活失败 → 回滚到未启动态、不泄漏调度任务；修复后可成功重试"""
    hreg = EventHandlerRegistry()
    await hreg.register(_StartBoomHandler())

    bus = event_bus_factory(h_registry=hreg)
    with pytest.raises(BusStartFailed, match='start boom') as excinfo:
        await bus.start()
    assert isinstance(excinfo.value, BusError)
    assert isinstance(excinfo.value.original_error, RuntimeError)

    # 已回滚：未运行、未就绪、无泄漏的调度任务
    assert not bus.is_running
    assert not bus.is_publishing_enabled
    assert bus._dispatch_task is None

    # 移除坏处理器后可成功重试，事件正常路由分发
    await hreg.clear()
    good = CountingHandler()
    await hreg.register(good)

    await bus.start()
    await bus.proxy('pub').publish('test.event', {'value': 1})
    await wait_for_condition(lambda: good.count == 1)
    await bus.stop()
    assert good.count == 1


@pytest.mark.asyncio
async def test_start_rolls_back_when_middleware_setup_fails(event_bus_factory: Callable[..., EventBus]):
    """首阶段（中间件 setup）冲突失败 → 整体回滚：链解绑、无调度泄漏，可重试（覆盖 _abort_start）"""
    hreg = EventHandlerRegistry()
    good = CountingHandler()
    await hreg.register(good)
    chain = MiddlewareChain()
    bus = event_bus_factory(h_registry=hreg, middleware_chain=chain)
    await chain.setup(bus)  # 预绑定 → start 内的 mw_chain.setup 抛 RuntimeError（走回滚）

    with pytest.raises(BusStartFailed, match='already been setup'):
        await bus.start()

    # 已回滚：未运行、无泄漏调度任务、注册表与中间件链均解绑
    assert not bus.is_running
    assert not bus.is_publishing_enabled
    assert bus._dispatch_task is None
    assert hreg._bus is None

    # 无需人工清理即可成功重试，事件正常路由分发
    await bus.start()
    await bus.proxy('pub').publish('test.event', {'value': 1})
    await wait_for_condition(lambda: good.count == 1)
    await bus.stop()
    assert good.count == 1


# ============================================================================
# start() 生命周期顺序（回归：处理器在激活阶段发布的消息须经过完整中间件管道）
# ============================================================================


@pytest.mark.asyncio
async def test_start_orders_middleware_setup_before_handler_activation(
    event_bus_factory: Callable[..., EventBus],
) -> None:
    """启动顺序：先初始化中间件、再激活处理器；停机顺序与之互为镜像——
    处理器在激活阶段发布的消息因此经过完整可用的中间件管道。"""
    order: List[str] = []

    class _OrderMiddleware(Middleware):
        async def before_publish(
            self,
            event_registry: EventRegistry,
            name: str,
            source: str,
            data: dict[str, Any] | BaseModel | None,
            old_event: Event | None,
            next: BeforePublishNext,
        ) -> None:
            await next(event_registry, name, source, data, old_event)

        async def on_publish(self, event: Event, next: OnPublishNext) -> None:
            await next(event)

        async def on_setup(self, bus: EventBus) -> None:
            order.append('mw.on_setup')

        async def on_teardown(self, bus: EventBus) -> None:
            order.append('mw.on_teardown')

    class _OrderHandler(EventHandler):
        def __init__(self) -> None:
            super().__init__(['test.event'])

        async def on_activate(self, bus: EventBus) -> None:
            await super().on_activate(bus)
            order.append('handler.on_activate')

        async def on_deactivate(self, bus: EventBus) -> None:
            await super().on_deactivate(bus)
            order.append('handler.on_deactivate')

        async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
            pass

    chain = MiddlewareChain()
    await chain.add(_OrderMiddleware())
    hreg = EventHandlerRegistry()
    await hreg.register(_OrderHandler())
    bus = event_bus_factory(h_registry=hreg, middleware_chain=chain)

    await bus.start()
    assert order == ['mw.on_setup', 'handler.on_activate']

    await bus.stop()
    assert order == ['mw.on_setup', 'handler.on_activate', 'handler.on_deactivate', 'mw.on_teardown']


@pytest.mark.asyncio
async def test_start_rolls_back_middleware_when_handler_activation_fails(
    event_bus_factory: Callable[..., EventBus],
) -> None:
    """中间件 setup 成功但处理器激活失败 → 中间件被整体回滚（on_teardown）且链解绑，可重试"""
    teardowns: List[str] = []

    class _TeardownSpyMiddleware(Middleware):
        async def before_publish(
            self,
            event_registry: EventRegistry,
            name: str,
            source: str,
            data: dict[str, Any] | BaseModel | None,
            old_event: Event | None,
            next: BeforePublishNext,
        ) -> None:
            await next(event_registry, name, source, data, old_event)

        async def on_publish(self, event: Event, next: OnPublishNext) -> None:
            await next(event)

        async def on_teardown(self, bus: EventBus) -> None:
            teardowns.append('mw.on_teardown')

    chain = MiddlewareChain()
    await chain.add(_TeardownSpyMiddleware())
    hreg = EventHandlerRegistry()
    await hreg.register(_StartBoomHandler())
    bus = event_bus_factory(h_registry=hreg, middleware_chain=chain)

    with pytest.raises(BusStartFailed, match='start boom'):
        await bus.start()

    # 已 setup 的中间件被回滚下线；未运行、无泄漏调度任务
    assert teardowns == ['mw.on_teardown']
    assert not bus.is_running
    assert not bus.is_publishing_enabled
    assert bus._dispatch_task is None

    # 无需人工清理即可成功重试，事件正常路由分发
    await hreg.clear()
    good = CountingHandler()
    await hreg.register(good)
    await bus.start()
    await bus.proxy('pub').publish('test.event', {'value': 1})
    await wait_for_condition(lambda: good.count == 1)
    await bus.stop()
    assert good.count == 1


# ============================================================================
# 任务管道钩子（before_dispatch / on_task_start / on_task_done）— 微时序锁定
# ============================================================================


class _TaskPipelineSpy(Middleware):
    """任务管道钩子的可配置探针中间件（记录调用 + 可注错 / 阻断 / 过滤）。"""

    def __init__(self, order: Optional[List[str]] = None) -> None:
        self._bus: Optional[EventBus] = None
        self._order: Optional[List[str]] = order
        self.bus_at_dispatch: List[int] = []  # before_dispatch 进入时的 active_task_count
        self.active_after_next: List[int] = []  # next 返回后的 active_task_count
        self.dispatch_calls: List[tuple[str, List[str]]] = []
        self.starts: List[TaskRecord] = []
        self.dones: List[TaskRecord] = []
        self.block_for: set[str] = set()  # 阻断这些事件的派发（不调用 next）
        self.raise_for: set[str] = set()  # 在这些事件上抛异常（短路 + 记录）
        self.filter_keep: Optional[str] = None  # 仅保留该处理器类名对应的 id
        self.raise_on_start = False
        self.raise_on_done = False

    async def on_setup(self, bus: EventBus) -> None:
        self._bus = bus

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        await next(event_registry, name, source, data, old_event)

    async def on_publish(self, event: Event, next: OnPublishNext) -> None:
        await next(event)

    async def before_dispatch(self, event: Event, handler_ids: List[str], next: DispatchNext) -> None:
        self.dispatch_calls.append((event.name, list(handler_ids)))
        if self._bus is not None:
            self.bus_at_dispatch.append(self._bus.active_task_count)
        if self._order is not None:
            self._order.append(f'dispatch:in:{event.name}')
        if event.name in self.raise_for:
            raise RuntimeError(f'dispatch hook boom: {event.name}')
        if event.name in self.block_for:
            return  # 阻断：不调用 next（事件被丢弃）
        if self.filter_keep is not None and self._bus is not None:
            registry = self._bus.proxy('spy').handlers_registry
            handler_ids = [
                hid
                for hid in handler_ids
                if (h := registry.get(hid)) is not None and h.__class__.__name__ == self.filter_keep
            ]
        await next(event, handler_ids)
        if self._bus is not None:
            self.active_after_next.append(self._bus.active_task_count)
        if self._order is not None:
            self._order.append(f'dispatch:out:{event.name}')

    async def on_task_start(self, record: TaskRecord) -> None:
        if self.raise_on_start:
            raise RuntimeError('on_task_start boom')
        self.starts.append(record)
        if self._order is not None:
            self._order.append(f'start:{record.handler_name}')

    async def on_task_done(self, record: TaskRecord) -> None:
        if self.raise_on_done:
            raise RuntimeError('on_task_done boom')
        self.dones.append(record)
        if self._order is not None:
            self._order.append(f'done:{record.handler_name}')


class _DispatchOrderSpy(Middleware):
    """记录 before_dispatch 洋葱顺序（共享 log）。"""

    def __init__(self, name: str, log: List[str]) -> None:
        self.name = name
        self._log = log

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        await next(event_registry, name, source, data, old_event)

    async def on_publish(self, event: Event, next: OnPublishNext) -> None:
        await next(event)

    async def before_dispatch(self, event: Event, handler_ids: List[str], next: DispatchNext) -> None:
        self._log.append(f'{self.name}:in:{event.name}')
        await next(event, handler_ids)
        self._log.append(f'{self.name}:out:{event.name}')


@pytest.mark.asyncio
async def test_task_pipeline_micro_timing_order(event_bus_factory: Callable[..., EventBus]) -> None:
    """微时序：dispatch in → （核心创建任务）→ dispatch out → start（信号量后）→ handle → done。"""
    order: List[str] = []
    spy = _TaskPipelineSpy(order=order)
    chain = MiddlewareChain()
    await chain.add(spy)

    class ProbeHandler(EventHandler):
        def __init__(self) -> None:
            super().__init__(['mw.ping'])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
            order.append('handle')

    hreg = EventHandlerRegistry()
    hid = await hreg.register(ProbeHandler())
    bus = event_bus_factory(h_registry=hreg, middleware_chain=chain)

    async with bus:
        await bus.proxy('p').publish('mw.ping', {'key': 'k', 'count': 1})
        await wait_for_condition(lambda: len(spy.dones) == 1)

        assert order == [
            'dispatch:in:mw.ping',
            'dispatch:out:mw.ping',
            'start:ProbeHandler',
            'handle',
            'done:ProbeHandler',
        ]
        assert spy.bus_at_dispatch[0] == 0  # 钩子进入时任务尚未创建
        assert spy.active_after_next[0] == 1  # next 返回后任务已创建并登记
        assert spy.dispatch_calls[0] == ('mw.ping', [hid])
        assert spy.dones[0] is spy.starts[0]  # done 收到同一记录实例
        assert spy.dones[0].outcome == 'success'


@pytest.mark.asyncio
async def test_before_dispatch_fires_for_unmatched_event(event_bus: EventBus) -> None:
    """零匹配事件同样触发 before_dispatch（ids 为空）——可观测「无人认领」事件。"""
    spy = _TaskPipelineSpy()
    await event_bus.proxy('admin').middleware.add(spy)

    await event_bus.proxy('p').publish('test.event', {'value': 1})
    await wait_for_condition(lambda: len(spy.dispatch_calls) >= 1)

    assert spy.dispatch_calls[0] == ('test.event', [])
    assert spy.starts == []


@pytest.mark.asyncio
async def test_before_dispatch_onion_order(event_bus_factory: Callable[..., EventBus]) -> None:
    """洋葱模型：外层先入后出；handler 执行严格在所有 dispatch 钩子退出之后。"""
    order: List[str] = []
    chain = MiddlewareChain()
    await chain.add(_DispatchOrderSpy('a', order))
    await chain.add(_DispatchOrderSpy('b', order))

    class ProbeHandler(EventHandler):
        def __init__(self) -> None:
            super().__init__(['mw.ping'])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
            order.append('handle')

    hreg = EventHandlerRegistry()
    await hreg.register(ProbeHandler())
    bus = event_bus_factory(h_registry=hreg, middleware_chain=chain)

    async with bus:
        await bus.proxy('p').publish('mw.ping', {'key': 'k', 'count': 1})
        await wait_for_condition(lambda: 'handle' in order)
        assert order == [
            'a:in:mw.ping',
            'b:in:mw.ping',
            'b:out:mw.ping',
            'a:out:mw.ping',
            'handle',
        ]


@pytest.mark.asyncio
async def test_before_dispatch_can_filter_handlers(event_bus_factory: Callable[..., EventBus]) -> None:
    """经 next 传入过滤后的 id 列表：仅保留的处理器创建任务（其余不执行）。"""
    spy = _TaskPipelineSpy()
    spy.filter_keep = 'HandlerX'
    chain = MiddlewareChain()
    await chain.add(spy)

    received: List[str] = []

    class HandlerX(EventHandler):
        def __init__(self) -> None:
            super().__init__(['test.event'])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
            received.append('X')

    class HandlerY(EventHandler):
        def __init__(self) -> None:
            super().__init__(['test.event'])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
            received.append('Y')

    hreg = EventHandlerRegistry()
    await hreg.register(HandlerX())
    await hreg.register(HandlerY())
    bus = event_bus_factory(h_registry=hreg, middleware_chain=chain)

    async with bus:
        await bus.proxy('p').publish('test.event', {'value': 1})
        await wait_for_condition(lambda: len(spy.dones) == 1)
        await asyncio.sleep(0.05)  # 给错误派发的 Y 留出暴露窗口

        assert received == ['X']
        assert spy.active_after_next[0] == 1
        assert [r.handler_name for r in spy.starts] == ['HandlerX']


@pytest.mark.asyncio
async def test_before_dispatch_blocking_drops_event(event_bus_factory: Callable[..., EventBus]) -> None:
    """阻断（不调用 next）：事件被丢弃、总线无异常；解除阻断后恢复派发。"""
    spy = _TaskPipelineSpy()
    spy.block_for = {'test.event'}
    chain = MiddlewareChain()
    await chain.add(spy)

    hreg = EventHandlerRegistry()
    handler = CountingHandler()
    await hreg.register(handler)
    bus = event_bus_factory(h_registry=hreg, middleware_chain=chain)

    async with bus:
        await bus.proxy('p').publish('test.event', {'value': 1})
        await asyncio.sleep(0.05)
        assert handler.count == 0
        assert spy.starts == []

        spy.block_for = set()
        await bus.proxy('p').publish('test.event', {'value': 2})
        await wait_for_condition(lambda: handler.count == 1)
        assert bus.is_running


@pytest.mark.asyncio
async def test_before_dispatch_exception_drops_event(event_bus_factory: Callable[..., EventBus]) -> None:
    """钩子异常 = 短路 + 记录：事件被丢弃，总线保持运行，后续事件正常。"""
    spy = _TaskPipelineSpy()
    spy.raise_for = {'test.event'}
    chain = MiddlewareChain()
    await chain.add(spy)

    hreg = EventHandlerRegistry()
    handler = CountingHandler()
    await hreg.register(handler)
    bus = event_bus_factory(h_registry=hreg, middleware_chain=chain)

    async with bus:
        await bus.proxy('p').publish('test.event', {'value': 1})
        await asyncio.sleep(0.05)
        assert handler.count == 0
        assert bus.is_running

        spy.raise_for = set()
        await bus.proxy('p').publish('test.event', {'value': 2})
        await wait_for_condition(lambda: handler.count == 1)


@pytest.mark.asyncio
async def test_on_task_start_after_semaphore_acquired(event_bus_factory: Callable[..., EventBus]) -> None:
    """微时序：start 在获取信号量后才触发——占满信号量时，已创建但排队的任务不触发 start。"""
    spy = _TaskPipelineSpy()
    chain = MiddlewareChain()
    await chain.add(spy)

    hreg = EventHandlerRegistry()
    blocker = BlockingHandler(subscriptions=['test.block'])
    await hreg.register(blocker)
    bus = event_bus_factory(h_registry=hreg, middleware_chain=chain, max_handler_semaphore=1)

    async with bus:
        await bus.proxy('p').publish('test.block', None)
        await bus.proxy('p').publish('test.block', None)
        await wait_for_condition(lambda: bus.active_task_count == 2)
        await asyncio.sleep(0.05)  # 让可能的错误 start 暴露

        # 两个任务均已创建，但仅第一个获取到信号量并触发 start
        assert len(spy.starts) == 1
        assert spy.dones == []
        assert spy.starts[0].outcome is None  # start 时结束字段未填
        assert spy.starts[0].started_at >= spy.starts[0].dispatched_at

        blocker.release()
        await wait_for_condition(lambda: len(spy.dones) == 2)

    assert len(spy.starts) == 2
    assert [r.outcome for r in spy.dones] == ['success', 'success']
    assert spy.dones[0] is spy.starts[0]
    assert spy.dones[1] is spy.starts[1]
    assert all(r.duration_seconds is not None and r.duration_seconds >= 0 for r in spy.dones)


@pytest.mark.asyncio
async def test_on_task_done_error_outcome(event_bus_factory: Callable[..., EventBus]) -> None:
    """异常任务：done 携带 error 结果；TaskErrorEvent 上报机制不受影响。"""
    spy = _TaskPipelineSpy()
    chain = MiddlewareChain()
    await chain.add(spy)

    hreg = EventHandlerRegistry()
    counter = CountingHandler()
    err_spy = ErrorSpyHandler()
    await hreg.register(counter)
    await hreg.register(err_spy)
    bus = event_bus_factory(h_registry=hreg, middleware_chain=chain)

    async with bus:
        await bus.proxy('p').publish('test.event', {'value': -1})
        await wait_for_condition(
            lambda: any(r.handler_name == 'CountingHandler' and r.outcome == 'error' for r in spy.dones)
        )
        await wait_for_condition(lambda: len(err_spy.captured) >= 1)

        done = next(r for r in spy.dones if r.handler_name == 'CountingHandler')
        start = next(r for r in spy.starts if r.handler_name == 'CountingHandler')
        assert done is start  # done 收到同一记录实例
        assert done.outcome == 'error'
        assert done.error_type == 'ValueError'
        assert done.error_message is not None and 'Invalid value' in done.error_message
        assert err_spy.captured[0]['type'] == 'ValueError'


@pytest.mark.asyncio
async def test_task_hooks_error_isolation(event_bus_factory: Callable[..., EventBus]) -> None:
    """钩子异常被逐中间件隔离：后续中间件仍被通知，任务结果不受影响。"""
    bad = _TaskPipelineSpy()
    bad.raise_on_start = True
    bad.raise_on_done = True
    good = _TaskPipelineSpy()
    chain = MiddlewareChain()
    await chain.add(bad)
    await chain.add(good)

    hreg = EventHandlerRegistry()
    counter = CountingHandler()
    await hreg.register(counter)
    bus = event_bus_factory(h_registry=hreg, middleware_chain=chain)

    async with bus:
        await bus.proxy('p').publish('test.event', {'value': 1})
        await wait_for_condition(lambda: len(good.dones) == 1)

        assert counter.count == 1  # 任务正常完成
        assert good.dones[0].outcome == 'success'  # 结果不受钩子异常影响
        assert len(good.starts) == 1
        assert bad.starts == []  # bad 的钩子抛错前未记录
        assert bad.dones == []


@pytest.mark.asyncio
async def test_task_hook_pairing_on_shutdown_cancel(
    base_event_registry: EventRegistry, handler_registry: EventHandlerRegistry
) -> None:
    """微时序：等待信号量时被取消的任务不触发钩子（与 start 严格配对）；执行中的任务以 cancelled 回收。"""
    spy = _TaskPipelineSpy()
    chain = MiddlewareChain()
    await chain.add(spy)

    hreg = handler_registry
    blocker = BlockingHandler(subscriptions=['test.block'])
    await hreg.register(blocker)
    bus = EventBus(
        base_event_registry,
        hreg,
        queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
        max_handler_semaphore=1,
        shutdown=ShutdownConfig(tasks_timeout=0.3),
        middleware_chain=chain,
    )

    await bus.start()
    await bus.proxy('p').publish('test.block', None)
    await bus.proxy('p').publish('test.block', None)
    await wait_for_condition(lambda: bus.active_task_count == 2)
    await wait_for_condition(lambda: len(spy.starts) == 1)

    await bus.stop()

    assert bus.active_task_count == 0
    assert len(spy.starts) == 1  # 第二个任务从未开始
    assert len(spy.dones) == 1
    assert spy.dones[0].outcome == 'cancelled'
    assert spy.dones[0] is spy.starts[0]


@pytest.mark.asyncio
async def test_task_pipeline_hot_add_remove(event_bus: EventBus, handler_registry: EventHandlerRegistry) -> None:
    """运行时增删中间件：任务管道钩子对后续任务即时生效/失效（链缓存失效）。"""
    spy = _TaskPipelineSpy()
    counter = CountingHandler()
    await handler_registry.register(counter)

    await event_bus.proxy('p').publish('test.event', {'value': 1})
    await wait_for_condition(lambda: counter.count == 1)
    assert spy.dispatch_calls == [] and spy.starts == []  # 未加入：无记录

    await event_bus.proxy('admin').middleware.add(spy)
    await event_bus.proxy('p').publish('test.event', {'value': 2})
    await wait_for_condition(lambda: len(spy.dones) == 1)
    assert len(spy.starts) == 1  # 热添加后生效

    await event_bus.proxy('admin').middleware.remove(spy)
    await event_bus.proxy('p').publish('test.event', {'value': 3})
    await wait_for_condition(lambda: counter.count == 3)
    await asyncio.sleep(0.05)
    assert len(spy.starts) == 1  # 移除后不再通知
    assert len(spy.dispatch_calls) == 1


# ============================================================================
# 总线启停微时序锁定（start / stop 阶段标志与顺序）
# ============================================================================


class _LifecycleSpyMiddleware(Middleware):
    """记录启停各阶段总线状态快照的中间件（微时序锁定用）。"""

    def __init__(self, order: Optional[List[str]] = None) -> None:
        self._bus: Optional[EventBus] = None
        self._order = order
        self.setup_calls = 0
        self.teardown_calls = 0
        self.shutdown_publish_count = 0
        self.snapshots: dict[str, tuple[bool, bool, int]] = {}  # phase -> (running, publish, active_tasks)
        self.teardown_dispatch_task_done: Optional[bool] = None
        self.teardown_publish_error: Optional[str] = None

    async def on_setup(self, bus: EventBus) -> None:
        self._bus = bus
        self.setup_calls += 1
        self.snapshots['mw_on_setup'] = (bus.is_running, bus.is_publishing_enabled, bus.active_task_count)
        if self._order is not None:
            self._order.append('mw_setup')

    async def on_teardown(self, bus: EventBus) -> None:
        self.teardown_calls += 1
        self.snapshots['mw_on_teardown'] = (bus.is_running, bus.is_publishing_enabled, bus.active_task_count)
        self.teardown_dispatch_task_done = bus._dispatch_task is not None and bus._dispatch_task.done()
        try:
            await bus.proxy('teardown_probe').publish('test.event', {'value': 1})
        except Exception as e:
            self.teardown_publish_error = type(e).__name__
        if self._order is not None:
            self._order.append('mw_teardown')

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        await next(event_registry, name, source, data, old_event)

    async def on_publish(self, event: Event, next: OnPublishNext) -> None:
        if event.name == 'infinity_bus.__shutdown__':
            self.shutdown_publish_count += 1
            if self._bus is not None:
                self.snapshots['shutdown_on_publish'] = (
                    self._bus.is_running,
                    self._bus.is_publishing_enabled,
                    self._bus.active_task_count,
                )
            if self._order is not None:
                self._order.append('shutdown_on_publish')
        await next(event)


class _LifecycleSpyHandler(EventHandler):
    """记录激活/下线阶段状态与处理计数的处理器（微时序锁定用）。"""

    def __init__(self, order: Optional[List[str]] = None) -> None:
        super().__init__(['test.event'])
        self._order = order
        self.handled = 0
        self.activate_calls = 0
        self.deactivate_calls = 0
        self.activate_snapshot: Optional[tuple[bool, bool, bool]] = None  # running, publish, dispatch_created
        self.deactivate_snapshot: Optional[tuple[bool, bool, int, int]] = None  # running, publish, active, queue
        self.deactivate_publish_error: Optional[str] = None

    async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
        self.handled += 1

    async def on_activate(self, bus: EventBus) -> None:
        await super().on_activate(bus)
        self.activate_calls += 1
        self.activate_snapshot = (bus.is_running, bus.is_publishing_enabled, bus._dispatch_task is not None)
        if self._order is not None:
            self._order.append('handler_activate')

    async def on_deactivate(self, bus: EventBus) -> None:
        await super().on_deactivate(bus)
        self.deactivate_calls += 1
        self.deactivate_snapshot = (
            bus.is_running,
            bus.is_publishing_enabled,
            bus.active_task_count,
            bus.queue_size,
        )
        try:
            await bus.proxy('deact_probe').publish('test.event', {'value': 1})
        except Exception as e:
            self.deactivate_publish_error = type(e).__name__
        if self._order is not None:
            self._order.append('handler_deactivate')


@pytest.mark.asyncio
async def test_start_stop_micro_timing_phase_locks(event_bus_factory: Callable[..., EventBus]) -> None:
    """启停微时序：setup/激活时双闸关且调度未建；__shutdown__ 发布时双闸仍开；
    排空完成先于下线；running 清零先于下线/拆中间件；拆中间件时调度已终结。"""
    order: List[str] = []
    mw = _LifecycleSpyMiddleware(order=order)
    chain = MiddlewareChain()
    await chain.add(mw)
    hreg = EventHandlerRegistry()
    handler = _LifecycleSpyHandler(order=order)
    await hreg.register(handler)
    bus = event_bus_factory(h_registry=hreg, middleware_chain=chain)

    await bus.start()
    assert order == ['mw_setup', 'handler_activate']
    assert mw.snapshots['mw_on_setup'] == (False, False, 0)  # 中间件 setup 阶段：双闸关
    assert handler.activate_snapshot == (False, False, False)  # 激活阶段：双闸关、调度任务尚未创建
    assert bus.is_running and bus.is_publishing_enabled  # 阶段三之后：双闸开

    for i in range(3):
        await bus.proxy('p').publish('test.event', {'value': i})
    await wait_for_condition(lambda: handler.handled >= 3 and bus.active_task_count == 0)

    await bus.stop()

    # 严格顺序：setup → 激活 → shutdown 发布 → 下线 → 拆中间件
    assert order == ['mw_setup', 'handler_activate', 'shutdown_on_publish', 'handler_deactivate', 'mw_teardown']
    # ① __shutdown__ 发布时双闸仍开（先发通知、后关闸）
    assert mw.snapshots['shutdown_on_publish'] == (True, True, 0)
    # ② 下线时：双闸已关、任务清零、队列排空（排空先于下线）；发布被拒为 RuntimeError（running 已清零）
    assert handler.deactivate_snapshot == (False, False, 0, 0)
    assert handler.handled == 3
    assert handler.deactivate_publish_error == 'RuntimeError'
    # ③ 拆中间件时：调度循环已终结、发布仍被拒、状态干净
    assert mw.snapshots['mw_on_teardown'] == (False, False, 0)
    assert mw.teardown_dispatch_task_done is True
    assert mw.teardown_publish_error == 'RuntimeError'


@pytest.mark.asyncio
async def test_start_micro_timing_concurrent_double_start(event_bus_factory: Callable[..., EventBus]) -> None:
    """并发重复 start：锁串行化且仅初始化一次（中间件 setup / 处理器激活各一次）。"""
    mw = _LifecycleSpyMiddleware()
    chain = MiddlewareChain()
    await chain.add(mw)
    hreg = EventHandlerRegistry()
    handler = _LifecycleSpyHandler()
    await hreg.register(handler)
    bus = event_bus_factory(h_registry=hreg, middleware_chain=chain)

    await asyncio.gather(bus.start(), bus.start(), bus.start())

    assert mw.setup_calls == 1
    assert handler.activate_calls == 1
    assert bus.is_running and bus.is_publishing_enabled

    await bus.proxy('p').publish('test.event', {'value': 1})
    await wait_for_condition(lambda: handler.handled == 1)
    await bus.stop()
    assert mw.teardown_calls == 1


@pytest.mark.asyncio
async def test_stop_micro_timing_concurrent_double_stop(event_bus_factory: Callable[..., EventBus]) -> None:
    """并发重复 stop：__shutdown__ 仅发布一次、处理器下线与中间件拆卸各一次。"""
    mw = _LifecycleSpyMiddleware()
    chain = MiddlewareChain()
    await chain.add(mw)
    hreg = EventHandlerRegistry()
    handler = _LifecycleSpyHandler()
    await hreg.register(handler)
    bus = event_bus_factory(h_registry=hreg, middleware_chain=chain)

    await bus.start()
    await asyncio.gather(bus.stop(), bus.stop(), bus.stop())

    assert mw.shutdown_publish_count == 1
    assert mw.teardown_calls == 1
    assert handler.deactivate_calls == 1
    assert not bus.is_running and not bus.is_publishing_enabled


@pytest.mark.asyncio
async def test_restart_micro_timing_hooks_refire(event_bus_factory: Callable[..., EventBus]) -> None:
    """start → stop → start → stop：每轮生命周期钩子精确重放，计数不泄漏、重启后可正常分发。"""
    mw = _LifecycleSpyMiddleware()
    chain = MiddlewareChain()
    await chain.add(mw)
    hreg = EventHandlerRegistry()
    handler = _LifecycleSpyHandler()
    await hreg.register(handler)
    bus = event_bus_factory(h_registry=hreg, middleware_chain=chain)

    await bus.start()
    await bus.stop()
    await bus.start()
    assert bus.is_running and bus.is_publishing_enabled

    await bus.proxy('p').publish('test.event', {'value': 1})
    await wait_for_condition(lambda: handler.handled == 1)
    await bus.stop()

    assert mw.setup_calls == 2 and mw.teardown_calls == 2
    assert handler.activate_calls == 2 and handler.deactivate_calls == 2
    assert mw.shutdown_publish_count == 2
    assert not bus.is_running and not bus.is_publishing_enabled


# ============================================================================
# 启动失败聚合异常（BusStartRollbackError）— 回滚不完整时一次聚合两类失败
# ============================================================================


class _FailingTeardownMiddleware(Middleware):
    """on_teardown 失败一次的中间件（聚合异常测试用，可修复重试）。"""

    def __init__(self) -> None:
        self.fail_once = True
        self.teardown_calls = 0

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        await next(event_registry, name, source, data, old_event)

    async def on_publish(self, event: Event, next: OnPublishNext) -> None:
        await next(event)

    async def on_teardown(self, bus: EventBus) -> None:
        self.teardown_calls += 1
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError('teardown boom')


class _FailingDeactivateHandler(EventHandler):
    """on_deactivate 抛错的处理器（聚合异常测试用）。"""

    def __init__(self) -> None:
        super().__init__(['test.event'])

    async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
        pass

    async def on_deactivate(self, bus: EventBus) -> None:
        await super().on_deactivate(bus)
        raise RuntimeError('deactivate boom')


@pytest.mark.asyncio
async def test_start_error_aggregates_rollback_failures(
    event_bus_factory: Callable[..., EventBus],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """启动后期失败且回滚不完整：BusStartRollbackError 一次聚合处理器下线与中间件拆卸失败。"""
    failing_mw = _FailingTeardownMiddleware()
    chain = MiddlewareChain()
    await chain.add(failing_mw)
    hreg = EventHandlerRegistry()
    handler = _FailingDeactivateHandler()
    hid = await hreg.register(handler)
    bus = event_bus_factory(h_registry=hreg, middleware_chain=chain)

    real_create_task = asyncio.create_task

    def _fail_dispatch_create(coro: Any, **kwargs: Any) -> Any:
        if getattr(coro, '__qualname__', '').endswith('_dispatch_loop'):
            coro.close()  # 防止 never-awaited 告警
            raise RuntimeError('dispatch create boom')
        return real_create_task(coro, **kwargs)

    monkeypatch.setattr(asyncio, 'create_task', _fail_dispatch_create)
    try:
        with pytest.raises(BusStartRollbackError) as excinfo:
            await bus.start()
    finally:
        monkeypatch.setattr(asyncio, 'create_task', real_create_task)

    err = excinfo.value
    assert isinstance(err, BusError)
    # 原始异常完整保留并挂链
    assert isinstance(err.original_error, RuntimeError)
    assert 'dispatch create boom' in str(err.original_error)
    assert err.__cause__ is err.original_error
    # 一次聚合两类回滚失败
    assert len(err.handler_failures) == 1
    assert err.handler_failures[0][0] == hid
    assert 'deactivate boom' in str(err.handler_failures[0][1])
    assert len(err.middleware_failures) == 1
    assert err.middleware_failures[0][0] is failing_mw
    assert 'teardown boom' in str(err.middleware_failures[0][1])
    # 回滚到可重试的未启动态（处理器因下线失败按文档保留在册）
    assert not bus.is_running and not bus.is_publishing_enabled
    assert bus._dispatch_task is None
    assert handler.handler_id == hid  # deactivate 失败不清 id


@pytest.mark.asyncio
async def test_start_error_aggregates_middleware_teardown_failure(
    event_bus_factory: Callable[..., EventBus],
) -> None:
    """激活失败 + 中间件拆卸失败：BusStartRollbackError 携带失败清单，修复后可整体重试。"""
    failing_mw = _FailingTeardownMiddleware()  # 仅失败一次
    chain = MiddlewareChain()
    await chain.add(failing_mw)
    hreg = EventHandlerRegistry()
    await hreg.register(_StartBoomHandler())
    bus = event_bus_factory(h_registry=hreg, middleware_chain=chain)

    with pytest.raises(BusStartRollbackError) as excinfo:
        await bus.start()

    err = excinfo.value
    assert isinstance(err, BusError)
    assert isinstance(err.original_error, RuntimeError)
    assert 'start boom' in str(err.original_error)
    assert err.handler_failures == []
    assert len(err.middleware_failures) == 1
    assert err.middleware_failures[0][0] is failing_mw
    assert 'teardown boom' in str(err.middleware_failures[0][1])

    # 中间件仅失败一次：清理坏处理器后可整体重试成功
    await hreg.clear()
    good = CountingHandler()
    await hreg.register(good)
    await bus.start()
    await bus.proxy('pub').publish('test.event', {'value': 1})
    await wait_for_condition(lambda: good.count == 1)
    await bus.stop()
    assert good.count == 1
