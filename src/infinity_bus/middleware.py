from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, Literal, Optional

from pydantic import BaseModel, Field

from .event import Event, EventRegistry

if TYPE_CHECKING:
    from .bus import EventBus

logger = logging.getLogger(__name__)


BeforePublishNext = Callable[
    [EventRegistry, str, str, Dict[str, Any] | BaseModel | None, Event | None],
    Awaitable[None],
]
"""before_publish 链中 next 回调的签名"""

OnPublishNext = Callable[
    [Event],
    Awaitable[None],
]
"""on_publish 链中 next 回调的签名"""

OnPublishErrorNext = Callable[
    [Exception, str, str, Dict[str, Any] | BaseModel | None],
    Awaitable[None],
]
"""on_publish_error 链中 next 回调的签名"""

TaskOutcome = Literal['success', 'error', 'cancelled', 'skipped']
"""任务结果：成功 / 异常 / 取消 / 停机跳过（BusShuttingDown）。"""


class TaskRecord(BaseModel):
    """处理器任务的生命周期记录（``on_task_start`` / ``on_task_done`` 钩子载荷）。

    同一实例先传给 ``on_task_start``（此时 ``outcome`` / ``duration_seconds`` /
    ``error_type`` / ``error_message`` 为 ``None``）；任务结束时传给 ``on_task_done``
    时已补齐上述字段。信号量排队时长可由 ``started_at - dispatched_at`` 推导
    （``started_at`` 为获取信号量、真正开始的时刻）。
    """

    handler_id: str = Field(description='注册表分配的处理器 id')
    handler_name: str = Field(description='处理器类名')
    event: Event = Field(description='触发本次任务的原始事件')
    dispatched_at: datetime = Field(description='任务创建（派发）时刻（UTC）')
    started_at: datetime = Field(description='获取并发信号量、开始执行的时刻（UTC）')
    outcome: Optional[TaskOutcome] = Field(default=None, description='任务结果（on_task_done 时补齐）')
    duration_seconds: Optional[float] = Field(default=None, description='开始执行到结束的时长（秒）')
    error_type: Optional[str] = Field(default=None, description='异常类型名（outcome=error 时）')
    error_message: Optional[str] = Field(default=None, description='异常消息（outcome=error 时）')


DispatchNext = Callable[[Event, list[str]], Awaitable[None]]
"""before_dispatch 链中 next 回调的签名：事件 + 最终派发集合（可过滤/重排）。"""

TaskNotifyNext = Callable[[TaskRecord], Awaitable[None]]
"""on_task_start / on_task_done 链中 next 回调的签名"""


class Middleware(ABC):
    """事件总线中间件基类。"""

    async def on_setup(self, bus: EventBus) -> None:
        """总线启动后回调。"""
        pass

    async def on_teardown(self, bus: EventBus) -> None:
        """总线停止前或运行时被移除时回调。

        .. warning::

           运行时 ``remove()`` 不会等待正在执行的钩子完成。
           ``on_teardown`` 被调用后，已进入 ``before_publish`` / ``on_publish``
           的中间件实例仍可能继续执行 —— 中间件作者应确保自身状态清理
           不影响这些残留调用。
        """
        pass

    @abstractmethod
    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        """发布**前**钩子 —— 在 publish 的任何逻辑（含事件校验）开始前执行。

        调用 ``await next(...)`` 将进入下一个中间件或核心发布流程
        （事件声明校验 → 构造 ``Event`` → 入队 → 触发 ``on_publish`` 链）。
        """
        ...

    @abstractmethod
    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        """发布**后**钩子 —— 在事件成功入队后执行。

        通过 ``event.name``、``event.data``、``event.sources`` 等
        可获取完整的运行时事件信息。
        """
        ...

    async def on_publish_error(
        self,
        error: Exception,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> None:
        """发布流程中发生异常时回调。"""
        pass

    async def before_dispatch(
        self,
        event: Event,
        handler_ids: list[str],
        next: DispatchNext,
    ) -> None:
        """分发**前**钩子 —— 路由匹配完成后、创建处理器任务前执行。

        调用 ``await next(event, handler_ids)`` 进入下一个中间件（链末端按传入的
        ``handler_ids`` 顺序逐个创建任务）：可传入过滤 / 重排后的新列表；不调用
        ``next`` 即**阻断**本次派发（事件被丢弃，由中间件自行补偿）。

        .. warning::

           本钩子在分发循环协程内执行，处于关键路径——应快速返回或自行
           ``asyncio.create_task`` 卸载耗时工作，否则将拖慢整个分发节奏。
        """
        await next(event, handler_ids)

    async def on_task_start(self, record: TaskRecord) -> None:
        """任务**开始**钩子 —— 处理器任务获取并发信号量后、执行 ``handle`` 前调用（通知式）。

        触发时 ``record.outcome`` 等结束字段尚未填充。钩子异常被逐中间件隔离
        记录，不影响任务执行。与 :meth:`on_task_done` **严格配对**：本钩子触发过
        的任务必然收到对应的 ``on_task_done``。
        """
        pass

    async def on_task_done(self, record: TaskRecord) -> None:
        """任务**回收**钩子 —— 任务结束时（成功 / 异常 / 超时 / 取消 / 停机跳过）调用（通知式）。

        收到的是 ``on_task_start`` 的同一记录实例，已补齐 ``outcome`` /
        ``duration_seconds`` / ``error_type`` / ``error_message``。在任务
        ``finally`` 路径保证触发（含被取消的任务）；等待信号量期间即被取消、
        从未开始的任务不会触发（与 :meth:`on_task_start` 严格配对）。
        钩子异常被逐中间件隔离记录，不改变任务结果。
        """
        pass


class MiddlewareChain:
    """中间件链管理器。"""

    def __init__(self) -> None:
        self._bus: EventBus | None = None
        self._middlewares: list[Middleware] = []
        self._cached_before: BeforePublishNext | None = None
        self._cached_on: OnPublishNext | None = None
        self._cached_on_error: OnPublishErrorNext | None = None
        self._cached_dispatch: DispatchNext | None = None
        self._cached_task_start: TaskNotifyNext | None = None
        self._cached_task_done: TaskNotifyNext | None = None

    def _invalidate(self) -> None:
        """清空缓存（中间件列表变更时调用）。"""
        self._cached_before = None
        self._cached_on = None
        self._cached_on_error = None
        self._cached_dispatch = None
        self._cached_task_start = None
        self._cached_task_done = None

    async def add(self, middleware: Middleware) -> 'MiddlewareChain':
        """在链**末尾**追加一个中间件。"""
        if middleware in self._middlewares:
            raise ValueError(f'Middleware {middleware.__class__.__name__} is already in the chain')
        self._middlewares.append(middleware)
        if self._bus is not None:  # 已绑定总线 → 立即 on_setup（失败回滚副作用后移除）
            try:
                await middleware.on_setup(self._bus)
            except Exception:
                try:
                    await middleware.on_teardown(self._bus)  # 撤销 on_setup 已产生的副作用
                except Exception:
                    logger.exception(
                        'Middleware %s on_teardown failed during add rollback', middleware.__class__.__name__
                    )
                self._middlewares.remove(middleware)
                raise RuntimeError(f'Middleware {middleware.__class__.__name__} on_setup failed')
        self._invalidate()
        return self

    async def insert(self, index: int, middleware: Middleware) -> 'MiddlewareChain':
        """在指定位置插入中间件。"""
        if middleware in self._middlewares:
            raise ValueError(f'Middleware {middleware.__class__.__name__} is already in the chain')
        self._middlewares.insert(index, middleware)
        if self._bus is not None:  # 已绑定总线 → 立即 on_setup（失败回滚副作用后移除）
            try:
                await middleware.on_setup(self._bus)
            except Exception:
                try:
                    await middleware.on_teardown(self._bus)  # 撤销 on_setup 已产生的副作用
                except Exception:
                    logger.exception(
                        'Middleware %s on_teardown failed during insert rollback', middleware.__class__.__name__
                    )
                self._middlewares.remove(middleware)
                raise RuntimeError(f'Middleware {middleware.__class__.__name__} on_setup failed')
        self._invalidate()
        return self

    async def remove(self, middleware: Middleware) -> Exception | None:
        """移除指定中间件实例（**保证移除，失败显式上报**）。

        - 不存在时视为幂等空操作，返回 ``None``（成员判断用 ``in chain.middlewares``）；
        - 已绑定总线时先触发 ``on_teardown``，再立即从链中移除；
        - ``on_teardown`` 失败不阻断移除，但**返回该异常**；
        - 已在飞行的链引用不会撤销 —— 中间件作者应自行处理 ``on_teardown``
          后仍被调用的情况（参见 :meth:`Middleware.on_teardown`）。
        """
        if middleware not in self._middlewares:
            return None
        error: Exception | None = None
        if self._bus is not None:  # 已绑定总线 → 立即 on_teardown
            try:
                await middleware.on_teardown(self._bus)
            except Exception as exc:
                error = exc
                logger.exception('Middleware %s on_teardown failed', middleware.__class__.__name__)
        self._middlewares.remove(middleware)
        self._invalidate()
        return error

    async def clear(self) -> list[tuple[Middleware, Exception]]:
        """清空所有中间件（**保证清空，失败显式上报**）。

        已绑定总线时逐项触发 ``on_teardown``；单个失败不中断其余清理，
        返回 ``[(middleware, 异常), ...]`` 失败清单。
        """
        failures: list[tuple[Middleware, Exception]] = []
        for middleware in list(self._middlewares):
            if self._bus is not None:  # 已绑定总线 → 立即 on_teardown
                try:
                    await middleware.on_teardown(self._bus)
                except Exception as exc:
                    failures.append((middleware, exc))
                    logger.exception('Middleware %s on_teardown failed', middleware.__class__.__name__)
        self._middlewares.clear()
        self._invalidate()
        return failures

    @property
    def middlewares(self) -> list[Middleware]:
        """返回当前注册的中间件列表"""
        return list(self._middlewares)

    async def setup(self, bus: EventBus) -> None:
        """按注册顺序初始化全部中间件（**原子，含总线侧副作用回滚**）。

        - 先绑定总线，再逐个触发 ``on_setup``；
        - 任一 ``on_setup`` 抛错即**整体回退**：解除绑定，并对**已尝试的中间件
          （含当前失败者）**补发 ``on_teardown``（撤销其部分副作用），然后向上
          抛出首个错误；
        - 中间件**保留在链中**（失败后修复可重新 ``setup``）——语义与处理器
          ``activate`` 一致；
        - 重复调用（已绑定）抛 ``RuntimeError``。
        """
        if self._bus is not None:
            raise RuntimeError('MiddlewareChain has already been setup')
        self._bus = bus
        attempted: list[Middleware] = []
        try:
            for mw in list(self._middlewares):  # 快照：允许 on_setup 内嵌套变更
                attempted.append(mw)
                await mw.on_setup(bus)
        except Exception:
            self._bus = None
            for mw in reversed(attempted):
                try:
                    await mw.on_teardown(bus)
                except Exception:
                    logger.exception('Middleware %s on_teardown failed during setup rollback', mw.__class__.__name__)
            raise

    async def teardown(self, bus: EventBus, strict: bool = False) -> list[tuple[Middleware, Exception]]:
        """按注册**逆序**通知所有中间件执行清理，重复调用安全（幂等）。

        - 逐项触发 ``on_teardown``，单个失败不中断其余清理（记录日志并记入返回值）；
        - 返回 ``[(middleware, 异常), ...]`` 失败清单，调用方（如 :meth:`EventBus.stop`）
          可感知清理不完整；``strict=True`` 在全部尝试后抛出首个失败；
        - 未绑定时返回 ``[]``。
        """
        if self._bus is None:
            return []
        self._bus = None
        failures: list[tuple[Middleware, Exception]] = []
        for mw in reversed(list(self._middlewares)):
            try:
                await mw.on_teardown(bus)
            except Exception as exc:
                failures.append((mw, exc))
                logger.exception('Middleware %s on_teardown failed', mw.__class__.__name__)
        if strict and failures:
            raise failures[0][1]
        return failures

    def build_before_publish(
        self,
        final_handler: BeforePublishNext,
    ) -> BeforePublishNext:
        """构建 ``before_publish`` 责任链（带缓存，中间件变更后自动重建）。"""
        if self._cached_before is None:
            handler = final_handler
            for mw in reversed(self._middlewares):
                handler = self._wrap_before(mw, handler)
            self._cached_before = handler
        return self._cached_before

    def build_on_publish(
        self,
        final_handler: OnPublishNext,
    ) -> OnPublishNext:
        """构建 ``on_publish`` 责任链（带缓存，中间件变更后自动重建）。"""
        if self._cached_on is None:
            handler = final_handler
            for mw in reversed(self._middlewares):
                handler = self._wrap_on(mw, handler)
            self._cached_on = handler
        return self._cached_on

    def build_on_publish_error(
        self,
        final_handler: OnPublishErrorNext,
    ) -> OnPublishErrorNext:
        """构建 ``on_publish_error`` 责任链（带缓存，中间件变更后自动重建）。"""
        if self._cached_on_error is None:
            handler = final_handler
            for mw in reversed(self._middlewares):
                handler = self._wrap_on_error(mw, handler)
            self._cached_on_error = handler
        return self._cached_on_error

    def build_dispatch(
        self,
        final_handler: DispatchNext,
    ) -> DispatchNext:
        """构建 ``before_dispatch`` 责任链（带缓存，中间件变更后自动重建）。"""
        if self._cached_dispatch is None:
            handler = final_handler
            for mw in reversed(self._middlewares):
                handler = self._wrap_dispatch(mw, handler)
            self._cached_dispatch = handler
        return self._cached_dispatch

    def build_task_start(
        self,
        final_handler: TaskNotifyNext,
    ) -> TaskNotifyNext:
        """构建 ``on_task_start`` 通知链（带缓存，逐中间件错误隔离后继续）。"""
        if self._cached_task_start is None:
            handler = final_handler
            for mw in reversed(self._middlewares):
                handler = self._wrap_task_start(mw, handler)
            self._cached_task_start = handler
        return self._cached_task_start

    def build_task_done(
        self,
        final_handler: TaskNotifyNext,
    ) -> TaskNotifyNext:
        """构建 ``on_task_done`` 通知链（带缓存，逐中间件错误隔离后继续）。"""
        if self._cached_task_done is None:
            handler = final_handler
            for mw in reversed(self._middlewares):
                handler = self._wrap_task_done(mw, handler)
            self._cached_task_done = handler
        return self._cached_task_done

    @staticmethod
    def _wrap_before(
        middleware: Middleware,
        next_handler: BeforePublishNext,
    ) -> BeforePublishNext:
        """将一个中间件的 before_publish 包裹在 next_handler 之外。"""

        async def wrapped(
            event_registry: EventRegistry,
            name: str,
            source: str,
            data: dict[str, Any] | BaseModel | None,
            old_event: Event | None,
        ) -> None:
            await middleware.before_publish(
                event_registry,
                name,
                source,
                data,
                old_event,
                next_handler,
            )

        return wrapped

    @staticmethod
    def _wrap_on(
        middleware: Middleware,
        next_handler: OnPublishNext,
    ) -> OnPublishNext:
        """将一个中间件的 on_publish 包裹在 next_handler 之外。"""

        async def wrapped(
            event: Event,
        ) -> None:
            await middleware.on_publish(
                event,
                next_handler,
            )

        return wrapped

    @staticmethod
    def _wrap_on_error(
        middleware: Middleware,
        next_handler: OnPublishErrorNext,
    ) -> OnPublishErrorNext:
        """将一个中间件的 on_publish_error 包裹在 next_handler 之外。"""

        async def wrapped(
            error: Exception,
            name: str,
            source: str,
            data: dict[str, Any] | BaseModel | None,
        ) -> None:
            try:
                await middleware.on_publish_error(
                    error,
                    name,
                    source,
                    data,
                )
            except Exception:
                logger.exception(
                    'Middleware %s on_publish_error failed',
                    middleware.__class__.__name__,
                )
            await next_handler(error, name, source, data)

        return wrapped

    @staticmethod
    def _wrap_dispatch(
        middleware: Middleware,
        next_handler: DispatchNext,
    ) -> DispatchNext:
        """将一个中间件的 before_dispatch 包裹在 next_handler 之外。"""

        async def wrapped(
            event: Event,
            handler_ids: list[str],
        ) -> None:
            await middleware.before_dispatch(
                event,
                handler_ids,
                next_handler,
            )

        return wrapped

    @staticmethod
    def _wrap_task_start(
        middleware: Middleware,
        next_handler: TaskNotifyNext,
    ) -> TaskNotifyNext:
        """将一个中间件的 on_task_start 包裹在 next_handler 之外（错误隔离，链路继续）。"""

        async def wrapped(
            record: TaskRecord,
        ) -> None:
            try:
                await middleware.on_task_start(record)
            except Exception:
                logger.exception(
                    'Middleware %s on_task_start failed',
                    middleware.__class__.__name__,
                )
            await next_handler(record)

        return wrapped

    @staticmethod
    def _wrap_task_done(
        middleware: Middleware,
        next_handler: TaskNotifyNext,
    ) -> TaskNotifyNext:
        """将一个中间件的 on_task_done 包裹在 next_handler 之外（错误隔离，链路继续）。"""

        async def wrapped(
            record: TaskRecord,
        ) -> None:
            try:
                await middleware.on_task_done(record)
            except Exception:
                logger.exception(
                    'Middleware %s on_task_done failed',
                    middleware.__class__.__name__,
                )
            await next_handler(record)

        return wrapped
