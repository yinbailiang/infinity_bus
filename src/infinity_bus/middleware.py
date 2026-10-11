from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict

from pydantic import BaseModel

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


class MiddlewareChain:
    """中间件链管理器。"""

    def __init__(self) -> None:
        self._bus: EventBus | None = None
        self._middlewares: list[Middleware] = []
        self._cached_before: BeforePublishNext | None = None
        self._cached_on: OnPublishNext | None = None
        self._cached_on_error: OnPublishErrorNext | None = None

    def _invalidate(self) -> None:
        """清空缓存（中间件列表变更时调用）。"""
        self._cached_before = None
        self._cached_on = None
        self._cached_on_error = None

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
