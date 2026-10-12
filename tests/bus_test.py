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
    BusShuttingDown,
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
    with pytest.raises(RuntimeError, match='start boom'):
        await bus.start()

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

    with pytest.raises(RuntimeError, match='already been setup'):
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

    with pytest.raises(RuntimeError, match='start boom'):
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
