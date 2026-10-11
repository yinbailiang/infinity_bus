import asyncio
import logging
import types
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set, Type, Union

from pydantic import BaseModel, Field

from .event import Event, EventDeclaration, EventRegistry
from .handler import EventHandler, EventHandlerRegistry
from .middleware import MiddlewareChain
from .queue import EventQueue, InMemoryEventQueue
from .router import Router

logger = logging.getLogger(__name__)


class BusShuttingDown(Exception):
    """总线正在停止，拒绝新发布，请求处理器执行清理并退出"""

    pass


class ShutdownEvent(EventDeclaration):
    """总线停机前发布的系统事件，通知所有处理器准备退出。"""

    name = 'infinity_bus.__shutdown__'


class TaskErrorPayload(BaseModel):
    """处理器执行异常时携带的负载。"""

    error_event: Event = Field(description='发生异常的事件')
    handler_id: Optional[str] = Field(default=None, description='发生异常的处理器内部ID')
    handler_name: str = Field(description='发生异常的处理器类名')
    error_type: str = Field(description='异常类型')
    error_message: str = Field(description='异常消息')


class TaskErrorEvent(EventDeclaration):
    """处理器执行异常时发布的系统事件，用于错误监控与故障排查。"""

    name = 'infinity_bus.__task_error__'
    payload_type = TaskErrorPayload


class ShutdownConfig(BaseModel):
    """总线停机配置"""

    queue_timeout_min: float = Field(default=1.0, description='队列排空最小等待时间（秒）')
    queue_timeout_max: float = Field(default=15.0, description='队列排空最大等待时间（秒）')
    tasks_timeout: float = Field(default=15.0, description='活跃任务完成等待时间（秒）')
    avg_wait_time: float = Field(default=0.05, description='每个事件平均处理时间估算（秒）')


class EventBus:
    """
    异步事件总线，支持订阅/发布模式

    系统内置事件:
    infinity_bus.__task_error__ 任务执行失败时发送，载荷为 TaskErrorPayload，发布者为 EventBusErrorReporter
    infinity_bus.__shutdown__ 总线将要关闭时发送, 无载荷, 发布者为 EventBus
    """

    class Proxy:
        """事件总线代理，提供给处理器调用以访问总线功能"""

        def __init__(self, bus: 'EventBus', source: str, raw_event: Optional[Event] = None) -> None:
            self._bus: EventBus = bus
            self._source: str = source
            self._raw_event: Optional[Event] = raw_event

        async def publish(self, name: str, data: Optional[Union[Dict[str, Any], BaseModel]] = None) -> None:
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
        queue: Optional[EventQueue] = None,
        router: Optional[Router] = None,
        max_handler_semaphore: int = 256,
        shutdown: ShutdownConfig = ShutdownConfig(),
        middleware_chain: Optional[MiddlewareChain] = None,
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
        self._dispatch_task: Optional[asyncio.Task[None]] = None

        self._handler_semaphore = asyncio.Semaphore(max_handler_semaphore)
        self._active_tasks: Set[asyncio.Task[Any]] = set()

        self._shutdown: ShutdownConfig = shutdown

    async def _publish(
        self,
        name: str,
        source: str,
        data: Optional[Union[Dict[str, Any], BaseModel]] = None,
        old_event: Optional[Event] = None,
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
        data: Optional[Union[Dict[str, Any], BaseModel]],
        old_event: Optional[Event],
    ) -> None:
        """before_publish 链的末端处理器：事件声明校验 → 负载校验 → 构造 Event → 入队 → 触发 on_publish 链

        此方法仅在所有中间件的 ``before_publish`` 钩子均调用 ``await next(...)`` 后执行。
        """
        event_declaration: Optional[Type[EventDeclaration]] = event_registry.get(name)
        if not event_declaration:
            logger.error(f'Unknown event type: {name}')
            raise ValueError(f'Unknown event type: {name}')

        payload: Optional[BaseModel] = None
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
        data: Optional[Union[Dict[str, Any], BaseModel]],
    ) -> None:
        """on_publish_error 链的末端处理器（空操作）"""

    @staticmethod
    async def _noop_on_publish(
        event: Event,
    ) -> None:
        """on_publish 链的末端处理器（空操作）"""

    @property
    def router(self) -> Router:
        """事件路由器：订阅管理与分派（handler 在 on_activate 时经此登记订阅）。"""
        return self._router

    async def start(self) -> None:
        """启动事件分发循环（**失败可重试**）。

        先激活处理器（订阅登记）再启动调度循环：任一环节失败都会整体回滚——
        撤销调度任务、下线处理器、回滚中间件——使总线回到可安全重试的
        「未启动」态，不会泄漏半启动的后台任务。
        """
        async with self._state_lock:
            if self._running.is_set():
                return
            # 阶段一：激活处理器（订阅登记）。失败时不产生任何后台任务，天然可重试。
            self._handlers.activate(self)
            # 阶段二：启动调度循环、对外就绪、初始化中间件。失败则回滚已获得资源。
            try:
                self._dispatch_task = asyncio.create_task(self._dispatch_loop())
                self._running.set()
                self._enable_publish.set()
                await self._mw_chain.setup(self)
            except Exception:
                logger.exception('Error occurred while starting event bus')
                await self._abort_start()
                raise
            logger.info('EventBus started')

    async def _abort_start(self) -> None:
        """启动失败回滚（尽力而为，不掩盖原始错误）。

        撤销调度循环、清理进行中的处理器任务、下线处理器、回滚中间件，
        使总线回到可重试的「未启动」态。
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
            self._running.clear()
            self._enable_publish.clear()
            await self._wait_all_tasks_done()
        except Exception:
            logger.exception('Failed to roll back dispatch during start')
        try:
            self._handlers.deactivate()
        except Exception:
            logger.exception('Handler deactivation failed during start rollback')
        try:
            await self._mw_chain.teardown(self)
        except Exception:
            logger.exception('Middleware teardown failed during start rollback')

    async def stop(self) -> None:
        """停止事件总线"""
        async with self._state_lock:
            if not self._running.is_set():
                return

            await self._publish(ShutdownEvent.name, source='EventBus', data=None)

            self._enable_publish.clear()  # 阻止新消息入队

            try:
                timeout: float = max(
                    self._shutdown.queue_timeout_min,
                    min(self._shutdown.queue_timeout_max, self._queue.qsize() * self._shutdown.avg_wait_time),
                )
                await asyncio.wait_for(self._queue.join(), timeout=timeout)  # 等待队列处理完毕，避免丢失事件
            except asyncio.TimeoutError:
                logger.warning('Timeout while waiting for event queue to drain during shutdown')

            if self._dispatch_task:
                self._dispatch_task.cancel()
                try:
                    await self._dispatch_task
                except asyncio.CancelledError:
                    pass

            await self._wait_all_tasks_done()  # 等待所有处理器任务完成

            self._running.clear()
            failures = self._handlers.deactivate()
            if failures:
                logger.warning(
                    '%d handler(s) failed to deactivate during stop; they remain registered',
                    len(failures),
                )
            mw_failures = await self._mw_chain.teardown(self)
            if mw_failures:
                logger.warning('%d middleware(s) failed to tear down during stop', len(mw_failures))
            logger.info('EventBus stopped')

    async def __aenter__(self) -> 'EventBus':
        """异步上下文管理器入口"""
        await self.start()
        return self

    async def __aexit__(
        self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: Optional[types.TracebackType]
    ) -> Optional[bool]:
        """异步上下文管理器出口，确保总线资源被释放且不吞没异常"""
        try:
            await self.stop()
        except Exception as stop_err:
            if exc_val is not None:
                logger.error(f'Error during EventBus shutdown (original exception will propagate): {stop_err}')
            else:
                raise
        return None  # 不抑制上下文体中的异常

    def proxy(self, source: str, raw_event: Optional[Event] = None) -> Proxy:
        """创建一个事件总线代理实例，供事件处理器调用"""
        return EventBus.Proxy(self, source, raw_event)

    async def _handler_wrapper(self, handler: EventHandler, handler_id: str, bus: 'EventBus', event: Event) -> None:
        """事件处理器包装器。

        接收原始 ``EventBus`` 实例，传递给 ``handler.__call__``。
        ``__call__`` 内部自行调用 ``bus.proxy()`` 创建代理。
        """
        try:
            async with self._handler_semaphore:  # 控制并发处理器数量，避免过载
                async with asyncio.timeout(handler.handle_timeout):
                    await handler(bus, event)
        except BusShuttingDown:
            logger.debug(
                'Handler %s skipped publish during shutdown (event=%s)', handler.__class__.__name__, event.name
            )
        except Exception as e:
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

    async def _dispatch_loop(self) -> None:
        """事件分发主循环"""
        await self._running.wait()  # 等待事件总线启动
        while self._running.is_set():
            event: Optional[Event] = None
            try:
                event = await self._queue.get()
                for handler_id in self._router.match(event.name):
                    handler = self._handlers.get(handler_id)
                    if handler is not None:
                        self._register_task(
                            asyncio.create_task(self._handler_wrapper(handler, handler_id, self, event))
                        )
            except Exception:
                logger.exception('Unexpected error in dispatch loop')
            finally:
                if event:
                    self._queue.task_done()

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

    async def _wait_all_tasks_done(self) -> None:
        """等待所有未完成的任务完成，适用于事件总线停止时调用"""
        if self._active_tasks:
            try:
                logger.info(f'Waiting for {len(self._active_tasks)} active handler tasks to complete...')
                done, pending = await asyncio.wait(
                    self._active_tasks.copy(), return_when=asyncio.ALL_COMPLETED, timeout=self._shutdown.tasks_timeout
                )
                logger.info(f'All handler tasks completed. Total: {len(done)}')
                if pending:
                    logger.warning(f'Timeout: {len(pending)} tasks pending, cancelling...')
                    for task in pending:
                        if not task.done():
                            task.cancel()
                            try:
                                await task
                            except asyncio.CancelledError:
                                pass
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
    def queue_size(self) -> int:
        """事件队列中待处理的事件数。"""
        return self._queue.qsize()
