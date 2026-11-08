import asyncio
import logging
import types
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from .event import Event, EventDeclaration, EventRegistry
from .handler import EventHandler, EventHandlerRegistry
from .middleware import Middleware, MiddlewareChain, TaskRecord
from .queue import EventQueue, InMemoryEventQueue
from .router import Router

logger = logging.getLogger(__name__)


class BusError(Exception):
    """总线异常基类：``infinity_bus`` 抛出的所有总线级异常均为其子类。"""

    pass


class BusShuttingDown(BusError):
    """总线正在停止，拒绝新发布，请求处理器执行清理并退出"""

    pass


class BusStartFailed(BusError):
    """总线启动失败（回滚完整）时抛出。

    由 :meth:`EventBus.start` 在任一启动环节失败、且回滚已把总线恢复到
    可重试的「未启动」态时抛出；携带触发回滚的原始启动异常（同步挂在
    ``__cause__`` 链上）。

    - :attr:`original_error`：原始启动异常。
    """

    def __init__(self, original_error: BaseException) -> None:
        super().__init__(f'EventBus start failed: {type(original_error).__name__}: {original_error}')
        self.original_error: BaseException = original_error


class BusStartRollbackError(BusError):
    """总线启动失败且回滚不完整时抛出：聚合原始启动异常与回滚阶段的清理失败。

    仅在 ``start()`` 失败、且回滚过程存在清理失败时抛出（回滚干净时原样重抛
    原始异常）；原始异常同步挂在 ``__cause__`` 链上。

    - :attr:`original_error`：触发回滚的原始启动异常；
    - :attr:`handler_failures`：``[(handler_id, 异常), ...]``——回滚中
      ``on_deactivate`` 失败（按文档保留在册）的处理器清单；
    - :attr:`middleware_failures`：``[(中间件实例, 异常), ...]``——回滚中
      ``on_teardown`` 失败的清单。
    """

    def __init__(
        self,
        original_error: BaseException,
        handler_failures: list[tuple[str, Exception]],
        middleware_failures: list[tuple['Middleware', Exception]],
    ) -> None:
        message = (
            f'EventBus start failed and rollback was incomplete: '
            f'{type(original_error).__name__}: {original_error}; '
            f'{len(handler_failures)} handler deactivation failure(s), '
            f'{len(middleware_failures)} middleware teardown failure(s)'
        )
        super().__init__(message)
        self.original_error: BaseException = original_error
        self.handler_failures: list[tuple[str, Exception]] = handler_failures
        self.middleware_failures: list[tuple['Middleware', Exception]] = middleware_failures


class ShutdownEvent(EventDeclaration):
    """总线停机前发布的系统事件，通知所有处理器准备退出。"""

    name = 'infinity_bus.__shutdown__'


class TaskErrorPayload(BaseModel):
    """处理器执行异常时携带的负载。"""

    error_event: Event = Field(description='发生异常的事件')
    handler_id: str | None = Field(default=None, description='发生异常的处理器内部ID')
    handler_name: str = Field(description='发生异常的处理器类名')
    error_type: str = Field(description='异常类型')
    error_message: str = Field(description='异常消息')


class TaskErrorEvent(EventDeclaration):
    """处理器执行异常时发布的系统事件，用于错误监控与故障排查。"""

    name = 'infinity_bus.__task_error__'
    payload_type = TaskErrorPayload


class ShutdownConfig(BaseModel):
    """总线停机配置"""

    queue_timeout_min: float = Field(default=1.0, description='等待事件全部派发（处理器任务全部启动）的最短时间（秒）')
    queue_timeout_max: float = Field(default=15.0, description='等待事件全部派发（处理器任务全部启动）的最长时间（秒）')
    tasks_timeout: float = Field(default=15.0, description='停机时等待活跃处理器任务收尾的时间（秒）')
    task_reclaim_delay: float = Field(
        default=0.0,
        description='任务回收前等待完成回调（含任务异常日志）落定的时长（秒）；0 表示仅让出一次事件循环',
    )
    avg_wait_time: float = Field(default=0.05, description='单事件处理耗时估算（秒），用于按积压量推导派发等待超时')


class _DispatchFence:
    """一批处理器任务的启动闸门：全部开始执行后 ack 一次。

    每个任务在获得并发信号量（真正开始执行）时 ``arrive``；最后一个到达者触发
    ``on_open``。闸门**不等待**、不观察成败——它只回答「这条事件是否已完整交给
    执行者」，因此不会入侵任务自身的生命周期。

    未启动的任务（停机时仍在信号量队列中被取消）不会到达，闸门保持关闭：事件
    因此不被 ack，持久队列会在通道关闭时重投——这正是「至少派发一次」的边界。
    """

    __slots__ = ('event', '_remaining', '_loop', '_on_open')

    def __init__(self, event: Event, parties: int, on_open: Callable[['_DispatchFence'], None]) -> None:
        self.event = event
        self._remaining = parties
        self._loop = asyncio.get_running_loop()
        self._on_open = on_open

    def arrive(self) -> None:
        """标记一个处理器任务已获得执行权；最后一个到达时异步触发 ack。"""
        if self._remaining <= 0:  # 防御重复到达：ack 只触发一次
            return
        self._remaining -= 1
        if self._remaining == 0:
            # 不在处理器任务栈上执行 ack：避免 ack 异常污染任务结果
            self._loop.call_soon(self._open)

    def _open(self) -> None:
        try:
            self._on_open(self)
        except Exception:
            logger.exception('Dispatch fence failed to acknowledge event %s', self.event.id)


class EventBus:
    """
    异步事件总线，支持订阅/发布模式

    系统内置事件:
    infinity_bus.__task_error__ 任务执行失败时发送，载荷为 TaskErrorPayload，发布者为 EventBusErrorReporter
    infinity_bus.__shutdown__ 总线将要关闭时发送, 无载荷, 发布者为 EventBus
    """

    class Proxy:
        """事件总线代理，提供给处理器调用以访问总线功能"""

        def __init__(self, bus: 'EventBus', source: str, raw_event: Event | None = None) -> None:
            self._bus: EventBus = bus
            self._source: str = source
            self._raw_event: Event | None = raw_event

        async def publish(self, name: str, data: dict[str, Any] | BaseModel | None = None) -> None:
            """发布事件到总线，自动校验负载类型并触发中间件管道。"""
            await self._bus._publish(name, self._source, data, self._raw_event)

        @property
        def handlers_registry(self) -> EventHandlerRegistry:
            """访问处理器注册表。"""
            return self._bus._handlers

        @property
        def events_registry(self) -> EventRegistry:
            """访问事件注册表。"""
            return self._bus._events

        @property
        def middleware(self) -> MiddlewareChain:
            """访问中间件链，支持运行时动态增删中间件。"""
            return self._bus._mw_chain

    def __init__(
        self,
        event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
        queue: EventQueue | None = None,
        router: Router | None = None,
        max_handler_semaphore: int = 256,
        shutdown: ShutdownConfig = ShutdownConfig(),
        middleware_chain: MiddlewareChain | None = None,
    ) -> None:
        self._events: EventRegistry = event_registry
        self._handlers: EventHandlerRegistry = handler_registry
        self._router: Router = router or Router(event_registry)
        self._mw_chain: MiddlewareChain = middleware_chain or MiddlewareChain()

        if self._events.get(ShutdownEvent.name) is None:
            self._events.register(ShutdownEvent)
        if self._events.get(TaskErrorEvent.name) is None:
            self._events.register(TaskErrorEvent)

        self._state_lock: asyncio.Lock = asyncio.Lock()
        self._enable_publish: asyncio.Event = asyncio.Event()
        self._running: asyncio.Event = asyncio.Event()
        self._queue: EventQueue = queue or InMemoryEventQueue()
        self._dispatch_task: asyncio.Task[None] | None = None

        self._handler_semaphore = asyncio.Semaphore(max_handler_semaphore)
        self._active_tasks: set[asyncio.Task[Any]] = set()
        # 尚未打开的启动闸门：事件已 get 但还有处理器任务没开始执行
        self._pending_fences: set[_DispatchFence] = set()
        # 分发循环串行执行：标记本次事件是否已进入 _core_dispatch（异常路径用）
        self._claimed_by_core: bool = False

        self._shutdown: ShutdownConfig = shutdown

    async def _publish(
        self,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None = None,
        old_event: Event | None = None,
    ) -> None:
        """发布事件到总线（经过中间件链）"""
        if not self._enable_publish.is_set():
            if self._running.is_set():
                logger.warning('EventBus is stopping, cannot publish new events')
                raise BusShuttingDown('EventBus is stopping, cannot publish new events')
            else:
                logger.warning('EventBus is not running, cannot publish events')
                raise RuntimeError('EventBus is not running, cannot publish events')

        try:
            await self._mw_chain.build_before_publish(self._core_publish)(self._events, name, source, data, old_event)
        except Exception as e:
            await self._mw_chain.build_on_publish_error(self._noop_on_publish_error)(e, name, source, data)
            raise

    async def _core_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
    ) -> None:
        """before_publish 链的末端处理器：事件声明校验 → 负载校验 → 构造 Event → 入队 → 触发 on_publish 链

        此方法仅在所有中间件的 ``before_publish`` 钩子均调用 ``await next(...)`` 后执行。
        """
        event_declaration: type[EventDeclaration] | None = event_registry.get(name)
        if not event_declaration:
            logger.error(f'Unknown event type: {name}')
            raise ValueError(f'Unknown event type: {name}')

        payload: BaseModel | None = None
        if event_declaration.payload_type:
            if data is None:
                raise ValueError(f'Event {name} requires payload data, but none provided')

            elif isinstance(data, BaseModel):
                if not isinstance(data, event_declaration.payload_type):
                    raise TypeError(
                        f"Payload type mismatch for event '{name}': "
                        f'expected {event_declaration.payload_type.__name__}, got {type(data).__name__}'
                    )
                payload = data.model_copy()

            else:
                payload = event_declaration.payload_type(**data)
        else:
            if data is not None:
                raise ValueError(f'Event {name} does not accept payload data')

        event = Event(
            name=name,
            data=payload,
            sources=old_event.sources.copy() if old_event else [],
            timestamps=old_event.timestamps.copy() if old_event else [],
            event_ids=old_event.event_ids.copy() if old_event else [],
        )
        event.sources.append(source)
        event.timestamps.append(datetime.now(timezone.utc))
        event.event_ids.append(event.id)
        await self._queue.put(event)
        logger.debug(f'Event published: {event.name} (id={event.id})')

        # 发布成功后，运行 on_publish 中间件链
        await self._mw_chain.build_on_publish(EventBus._noop_on_publish)(event)

    @staticmethod
    async def _noop_on_publish_error(
        error: Exception,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> None:
        """on_publish_error 链的末端处理器（空操作）"""

    @staticmethod
    async def _noop_on_publish(
        event: Event,
    ) -> None:
        """on_publish 链的末端处理器（空操作）"""

    @staticmethod
    async def _noop_task_notify(
        record: TaskRecord,
    ) -> None:
        """task 通知链（on_task_start / on_task_done）的末端处理器（空操作）"""

    @staticmethod
    async def _noop_dispatch_abandoned(
        event: Event,
    ) -> None:
        """on_dispatch_abandoned 链的末端处理器（空操作）"""

    @property
    def router(self) -> Router:
        """事件路由器：订阅管理与分派（handler 在 on_activate 时经此登记订阅）。"""
        return self._router

    async def start(self) -> None:
        """启动事件分发循环（**失败可重试**）。

        启动顺序与 :meth:`stop` 的清理顺序互为镜像：**先初始化中间件、再激活
        处理器**（处理器在激活阶段发布的消息也因此经过完整的中间件管道），
        最后启动调度循环并开放发布。任一环节失败或取消都会整体回滚——撤销调度任务、
        下线处理器、回滚中间件——使总线回到可安全重试的「未启动」态，
        不会泄漏半启动的后台任务。
        """
        async with self._state_lock:
            if self._running.is_set():
                return
            try:
                # 阶段一：初始化中间件。必须先于处理器激活——处理器（及其后台任务）
                # 在激活/启动阶段发布的消息依赖已就绪的中间件管道。
                await self._mw_chain.setup(self)
                # 阶段二：激活处理器（订阅登记）。
                await self._handlers.activate(self)
                # 阶段三：启动调度循环、对外就绪。
                self._dispatch_task = asyncio.create_task(self._dispatch_loop())
                self._running.set()
                self._enable_publish.set()
            except asyncio.CancelledError:
                logger.warning('EventBus start cancelled; rolling back')
                await self._abort_start()
                raise
            except Exception as e:
                logger.exception('Error occurred while starting event bus')
                handler_failures, middleware_failures = await self._abort_start()
                if handler_failures or middleware_failures:
                    raise BusStartRollbackError(
                        original_error=e,
                        handler_failures=handler_failures,
                        middleware_failures=middleware_failures,
                    ) from e
                raise BusStartFailed(e) from e
            logger.info('EventBus started')

    async def _abort_start(self) -> tuple[list[tuple[str, Exception]], list[tuple[Middleware, Exception]]]:
        """启动失败回滚（尽力而为，不掩盖原始错误）。

        撤销调度循环、清理进行中的处理器任务、下线处理器、回滚中间件，使总线回到
        可重试的「未启动」态。返回回滚中收集的清理失败清单
        ``(处理器下线失败, 中间件拆卸失败)``，供 :meth:`start` 聚合为 :class:`BusStartRollbackError`。
        """
        try:
            task = self._dispatch_task
            self._dispatch_task = None
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            self._enable_publish.clear()
            self._running.clear()
            await self._wait_all_tasks_done()
            await self._clear_fences()
        except Exception:
            logger.exception('Failed to roll back dispatch during start')
        handler_failures: list[tuple[str, Exception]] = []
        try:
            handler_failures = await self._handlers.deactivate()
        except Exception:
            logger.exception('Handler deactivation failed during start rollback')
        middleware_failures: list[tuple[Middleware, Exception]] = []
        try:
            middleware_failures = await self._mw_chain.teardown(self)
        except Exception:
            logger.exception('Middleware teardown failed during start rollback')
        return handler_failures, middleware_failures

    async def stop(self) -> None:
        """停止事件总线（**尽力而为**：单个清理步骤失败不会中断其余清理）。

        - 停机事件发布失败、队列排空失败只记录日志，仍继续清理；
        - 调度循环、活跃任务、未完成派发闸门、处理器与中间件在 ``finally``
          中按顺序回收，保证 ``is_running`` 最终为 ``False``；
        - 协程被取消时也会执行清理路径，然后继续向上传播取消。
        """
        async with self._state_lock:
            if not self._running.is_set():
                return

            try:
                try:
                    await self._publish(ShutdownEvent.name, source='EventBus', data=None)
                except Exception:
                    logger.exception('Failed to publish shutdown event; continuing shutdown')

                self._enable_publish.clear()  # 阻止新消息入队

                try:
                    timeout: float = max(
                        self._shutdown.queue_timeout_min,
                        min(self._shutdown.queue_timeout_max, self._queue.qsize() * self._shutdown.avg_wait_time),
                    )
                    await asyncio.wait_for(self._queue.join(), timeout=timeout)  # 等事件全部派发，避免未确认消息被重投
                except asyncio.TimeoutError:
                    logger.warning('Timeout while waiting for event queue to drain during shutdown')
                except Exception:
                    logger.exception('Failed to drain event queue during shutdown')
            finally:
                try:
                    await self._cancel_dispatch_best_effort()
                    await self._wait_all_tasks_done_best_effort()
                    await self._clear_fences_best_effort()
                finally:
                    self._running.clear()
                    await self._deactivate_handlers_best_effort()
                    await self._teardown_middlewares_best_effort()
                    self._dispatch_task = None
                    logger.info('EventBus stopped')

    async def _cancel_dispatch_best_effort(self) -> None:
        """取消并等待调度任务（幂等，异常只记录）。"""
        task = self._dispatch_task
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception('Dispatch task failed during shutdown')

    async def _wait_all_tasks_done_best_effort(self) -> None:
        """等待活跃处理器任务收尾（异常只记录）。"""
        try:
            await self._wait_all_tasks_done()
        except Exception:
            logger.exception('Failed to wait for handler tasks during shutdown')

    async def _clear_fences_best_effort(self) -> None:
        """结清未完成派发闸门（异常只记录）。"""
        try:
            await self._clear_fences()
        except Exception:
            logger.exception('Failed to clear dispatch fences during shutdown')

    async def _deactivate_handlers_best_effort(self) -> None:
        """下线处理器（异常只记录，失败者保留在册）。"""
        try:
            failures = await self._handlers.deactivate()
        except Exception:
            logger.exception('Handler deactivation failed during shutdown')
            return
        if failures:
            logger.warning(
                '%d handler(s) failed to deactivate during stop; they remain registered',
                len(failures),
            )

    async def _teardown_middlewares_best_effort(self) -> None:
        """拆卸中间件（异常只记录）。"""
        try:
            mw_failures = await self._mw_chain.teardown(self)
        except Exception:
            logger.exception('Middleware teardown failed during shutdown')
            return
        if mw_failures:
            logger.warning('%d middleware(s) failed to tear down during stop', len(mw_failures))

    async def __aenter__(self) -> 'EventBus':
        """异步上下文管理器入口"""
        await self.start()
        return self

    async def __aexit__(
        self, exc_type: type | None, exc_val: BaseException | None, exc_tb: types.TracebackType | None
    ) -> bool | None:
        """异步上下文管理器出口，确保总线资源被释放且不吞没异常"""
        try:
            await self.stop()
        except Exception as stop_err:
            if exc_val is not None:
                logger.error(f'Error during EventBus shutdown (original exception will propagate): {stop_err}')
            else:
                raise
        return None  # 不抑制上下文体中的异常

    def proxy(self, source: str, raw_event: Event | None = None) -> Proxy:
        """创建一个事件总线代理实例，供事件处理器调用"""
        return EventBus.Proxy(self, source, raw_event)

    async def _handler_wrapper(
        self,
        handler: EventHandler,
        handler_id: str,
        bus: 'EventBus',
        event: Event,
        dispatched_at: datetime,
        fence: _DispatchFence | None = None,
    ) -> None:
        """事件处理器包装器。

        接收原始 ``EventBus`` 实例，传递给 ``handler.__call__``。
        任务生命周期钩子在此触达：获取并发信号量后触发 ``on_task_start``；
        任务结束（成功 / 异常 / 超时 / 取消 / 停机跳过）时在 ``finally``
        路径触发 ``on_task_done``（与 ``on_task_start`` 严格配对）。

        ``fence`` 是本次派发的启动闸门：获得信号量（真正开始执行）时到达，
        全部到达才 ack —— 与任务成败无关（见 :class:`_DispatchFence`）。
        """
        record: TaskRecord | None = None
        try:
            async with self._handler_semaphore:  # 控制并发处理器数量，避免过载
                if fence is not None:
                    fence.arrive()  # 获得执行权即视为派发成功，与任务成败无关
                record = TaskRecord(
                    handler_id=handler_id,
                    handler_name=handler.__class__.__name__,
                    event=event,
                    dispatched_at=dispatched_at,
                    started_at=datetime.now(timezone.utc),
                )
                await self._mw_chain.build_task_start(self._noop_task_notify)(record)
                try:
                    async with asyncio.timeout(handler.handle_timeout):
                        await handler(bus, event)
                except BusShuttingDown:
                    logger.debug(
                        'Handler %s skipped publish during shutdown (event=%s)', handler.__class__.__name__, event.name
                    )
                    record.outcome = 'skipped'
                except asyncio.CancelledError:
                    record.outcome = 'cancelled'
                    raise
                except Exception as e:
                    record.outcome = 'error'
                    record.error_type = type(e).__name__
                    record.error_message = str(e)
                    if 'EventBusErrorReporter' not in event.sources:
                        try:
                            await self._publish(
                                name=TaskErrorEvent.name,
                                source='EventBusErrorReporter',
                                data=TaskErrorPayload(
                                    error_event=event,
                                    handler_id=handler_id,
                                    handler_name=handler.__class__.__name__,
                                    error_type=type(e).__name__,
                                    error_message=str(e),
                                ),
                                old_event=event,
                            )
                        except BusShuttingDown as err:
                            logger.warning(f'Skipping task_error publish during shutdown: {err}')
                        except Exception:
                            logger.exception('Failed to publish task_error event')
                    raise
                else:
                    record.outcome = 'success'
                finally:
                    record.duration_seconds = (datetime.now(timezone.utc) - record.started_at).total_seconds()
                    await self._mw_chain.build_task_done(self._noop_task_notify)(record)
        except asyncio.CancelledError:
            # 取消发生在 on_task_start 之后、内部 try 之前：补齐回收通知（严格配对）
            if record is not None and record.outcome is None:
                record.outcome = 'cancelled'
                record.duration_seconds = (datetime.now(timezone.utc) - record.started_at).total_seconds()
                await self._mw_chain.build_task_done(self._noop_task_notify)(record)
            raise

    async def _core_dispatch(self, event: Event, handler_ids: list[str]) -> None:
        """``before_dispatch`` 链末端：按最终处理器 id 列表逐个创建任务并登记。

        本次派发的所有任务共享同一 ``dispatched_at``（分发时刻），供任务记录
        推导信号量排队时长；同时创建一个**启动闸门**——所有任务都获得执行权时
        才 ``task_done``（见 :class:`_DispatchFence`）。任务成败不参与 ack 判定：
        总线只保证事件被派发，业务结果由 ``TaskErrorEvent`` 与使用方策略负责。
        """
        self._claimed_by_core = True
        dispatched_at = datetime.now(timezone.utc)
        targets: list[tuple[str, EventHandler]] = []
        for handler_id in handler_ids:
            handler = self._handlers.get(handler_id)
            if handler is not None:
                targets.append((handler_id, handler))
        if not targets:
            self._acknowledge(event)  # 没人接手：当场确认，避免队列被坏事件卡住
            return
        fence = _DispatchFence(event, len(targets), self._open_fence)
        self._pending_fences.add(fence)
        for handler_id, handler in targets:
            self._register_task(
                asyncio.create_task(self._handler_wrapper(handler, handler_id, self, event, dispatched_at, fence))
            )

    async def _dispatch_loop(self) -> None:
        """事件分发主循环。

        ack 由 :class:`_DispatchFence` 在「该事件的所有处理器任务都开始执行」时
        触发，不在这里无条件 ``task_done``；只有没人接手的事件（无处理器 /
        被中间件短路 / 派发链在进入核心前失败）才当场确认。
        """
        await self._running.wait()  # 等待事件总线启动
        while self._running.is_set():
            try:
                event = await self._queue.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception('Unexpected error in dispatch loop')
                await asyncio.sleep(0)  # 别在持续失败时空转
                continue
            try:
                await self._dispatch_event(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception('Unexpected error dispatching event %s', event.name)
                if not self._claimed_by_core:
                    self._acknowledge(event)

    async def _dispatch_event(self, event: Event) -> None:
        """派发单条事件；进入核心前的失败当场确认，进入后由闸门负责 ack。"""
        self._claimed_by_core = False
        try:
            handler_ids = self._router.match(event.name)
        except Exception:
            logger.exception('Router match failed (event=%s); event dropped', event.name)
            self._acknowledge(event)
            return
        try:
            await self._mw_chain.build_dispatch(self._core_dispatch)(event, handler_ids)
        except Exception:
            logger.exception('before_dispatch chain failed (event=%s); event dropped', event.name)
            if not self._claimed_by_core:
                self._acknowledge(event)

    def _register_task(self, task: asyncio.Task[Any]) -> None:
        self._active_tasks.add(task)
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task: asyncio.Task[Any]) -> None:
        """任务完成时的回调（在任务完成后立即触发）"""
        self._active_tasks.discard(task)

        try:
            if exc := task.exception():
                raise exc
        except BusShuttingDown:
            logger.debug('BusShuttingDown Error')
        except asyncio.CancelledError:
            logger.debug(f'Handler task cancelled: {task.get_name()}')
        except asyncio.InvalidStateError:
            logger.warning(f'Task {task.get_name()} callback triggered in invalid state')
        except Exception:
            logger.exception(f'Handler task failed: {task.get_name()}')

    def _open_fence(self, fence: _DispatchFence) -> None:
        """闸门全部到达：移出待确认集合并 ack 该事件。"""
        self._pending_fences.discard(fence)
        self._acknowledge(fence.event)

    def _acknowledge(self, event: Event) -> None:
        """向队列确认「这条事件已派发完成」，携带事件身份以精确定位消息。"""
        self._queue.task_done(event)

    async def _clear_fences(self) -> None:
        """处理未打开的闸门（停机收尾）。

        未打开 = 仍有处理器任务没开始执行 = 事件没有完整派发。先给中间件撤回认领的
        机会（``on_dispatch_abandoned``），再通知队列停止等待该事件：进程内队列结清
        计数（无重投能力），持久队列保持未 ack（由 broker 在通道关闭时重投）。
        """
        for fence in list(self._pending_fences):
            try:
                await self._mw_chain.build_dispatch_abandoned(self._noop_dispatch_abandoned)(fence.event)
            except Exception:
                logger.exception('Middleware chain failed to handle abandoned dispatch (event=%s)', fence.event.id)
            try:
                self._queue.task_abandoned(fence.event)
            except Exception:
                logger.exception('Failed to abandon event %s during shutdown', fence.event.id)
        self._pending_fences.clear()

    async def _wait_all_tasks_done(self) -> None:
        """等待所有未完成的任务完成，适用于事件总线停止时调用。

        对被取消任务**先整体取消、再统一等待**（避免逐个取消时，先取消的任务
        释放并发信号量、让仍在排队的任务抢先开始执行）；随后让出事件循环
        （可配置延时 ``ShutdownConfig.task_reclaim_delay``）使完成回调与任务异常
        日志先行落定，最后同步摘除已完成任务作为确定性兜底（回调仍会幂等执行）。
        """
        if self._active_tasks:
            try:
                logger.info(f'Waiting for {len(self._active_tasks)} active handler tasks to complete...')
                done, pending = await asyncio.wait(
                    self._active_tasks.copy(), return_when=asyncio.ALL_COMPLETED, timeout=self._shutdown.tasks_timeout
                )
                logger.info(f'All handler tasks completed. Total: {len(done)}')
                if pending:
                    logger.warning(f'Timeout: {len(pending)} tasks pending, cancelling...')
                    # 先整体取消、再统一等待：避免逐个取消时，先取消的任务释放并发信号量，
                    # 让仍在排队的任务在轮到其取消前抢先开始执行
                    for task in pending:
                        if not task.done():
                            task.cancel()
                    for task in pending:
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                # 让完成回调（含任务异常日志）先行落定：默认仅让出一次事件循环，
                # task_reclaim_delay > 0 时额外等待（可配置）
                await asyncio.sleep(self._shutdown.task_reclaim_delay)
                for task in list(self._active_tasks):
                    if task.done():
                        self._active_tasks.discard(task)
            except Exception as e:
                logger.exception('Unexpected error in wait all task done')
                raise e

    @property
    def is_running(self) -> bool:
        """总线是否在运行。"""
        return self._running.is_set()

    @property
    def is_publishing_enabled(self) -> bool:
        """是否允许发布新事件（停止过程中为 False）。"""
        return self._enable_publish.is_set()

    @property
    def active_task_count(self) -> int:
        """当前活跃的处理器任务数。"""
        return len(self._active_tasks)

    @property
    def pending_ack_count(self) -> int:
        """尚未确认（还有处理器任务未开始执行）的事件数。"""
        return len(self._pending_fences)

    @property
    def queue_size(self) -> int:
        """事件队列中待处理的事件数。"""
        return self._queue.qsize()
