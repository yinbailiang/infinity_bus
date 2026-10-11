import logging
import re
import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Dict, List, Optional

from pydantic import BaseModel

from .event import Event
from .router import Subscriptions

if TYPE_CHECKING:
    from . import EventBus

logger = logging.getLogger(__name__)


class Regex:
    """编译后的正则表达式包装器"""

    def __init__(self, pattern: str) -> None:
        self._pattern: str = pattern
        self._compile: re.Pattern[str] = re.compile(pattern)

    @property
    def pattern(self) -> str:
        """返回原始正则表达式字符串。"""
        return self._pattern

    def fullmatch(self, string: str) -> Optional[re.Match[str]]:
        """委托给已编译正则的 ``fullmatch``。"""
        return self._compile.fullmatch(string)

    def __str__(self) -> str:
        return self._pattern

    def __repr__(self) -> str:
        return f'Regex({self._pattern!r})'

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Regex):
            return self._pattern == other._pattern
        if isinstance(other, str):
            return self._pattern == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._pattern)


class EventHandler(ABC):
    """事件处理器基类，所有具体事件处理器应继承此类"""

    def __init__(
        self, subscriptions: Optional[List[Regex | str]] = None, handle_timeout: Optional[float] = 32.0
    ) -> None:
        self.subscriptions: Subscriptions = Subscriptions(subscriptions)
        self.handle_timeout: Optional[float] = handle_timeout
        self._handler_id: Optional[str] = None

    async def __call__(self, bus: 'EventBus', event: Event) -> None:
        """事件处理器入口，自动组装代理并解包事件数据。

        总线内部调用，接收原始 ``EventBus`` 实例，
        内部调用 ``bus.proxy(handler_name, event)`` 创建代理后
        传递给 ``handle()``。

        ``bus.proxy`` 的 source 形如 ``类名:id``——id 为注册表分配的
        :attr:`handler_id`；处理器未注册（无 id）时显示 ``unregister``。
        """
        source = f'{self.__class__.__name__}:{self.handler_id or "unregister"}'
        bus_proxy = bus.proxy(source, event)
        await self.handle(event.data, bus_proxy, event)

    @abstractmethod
    async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None: ...

    # ------------------------------------------------------------------
    # 生命周期钩子：处理器可感知自己的 id
    # ------------------------------------------------------------------

    @property
    def handler_id(self) -> Optional[str]:
        """注册后返回注册表分配的 id；未注册时为 ``None``。"""
        return self._handler_id

    def on_registered(self, handler_id: str) -> None:
        """被注册进注册表时由注册表调用：回填处理器自己的 id。

        子类可覆写以感知注册事件，需调用 ``super().on_registered(handler_id)``
        保留 id 回填行为。
        """
        self._handler_id = handler_id

    def on_unregistered(self) -> None:
        """被注销时由注册表调用：清空处理器自己的 id。

        子类可覆写以感知注销事件，需调用 ``super().on_unregistered()``
        保留 id 清空行为。
        """
        self._handler_id = None

    def on_activate(self, bus: 'EventBus') -> None:
        """所属注册表绑定到总线时调用（见 :meth:`EventHandlerRegistry.activate`）。

        触发时机：注册表 ``activate`` 时对全部在册处理器触发；已绑定的注册表内
        新注册的处理器在 ``register`` 时触发。调用时 :attr:`handler_id` 已可用。

        **基类默认把本处理器的订阅束登记到 ``bus.router``（激活即路由）**——空订阅束
        也会登记，使「激活 ⟺ 已登记」恒成立：之后运行期 ``add`` 立即生效；
        子类覆写需调用 ``super().on_activate(bus)`` 保留该行为（如 mailbox
        需同时启动后台任务）。
        """
        if self.handler_id is not None:
            bus.router.subscribe(self.handler_id, self.subscriptions)

    def on_deactivate(self, bus: 'EventBus') -> None:
        """所属注册表与总线解绑或被注销时调用，与 :meth:`on_activate` 对称。

        触发时机：注册表 ``deactivate`` 时对全部在册处理器触发；或从已绑定
        注册表中注销单个处理器时触发。调用时 :attr:`handler_id` 仍可用。

        **基类默认从 ``bus.router`` 撤销本处理器的订阅束**；子类覆写需调用
        ``super().on_deactivate(bus)`` 保留该行为。
        """
        if self.handler_id is not None:
            bus.router.unsubscribe(self.handler_id)


class EventHandlerRegistry:
    """事件处理器注册表，负责管理事件类型与处理器的映射关系。"""

    def __init__(self) -> None:
        self._handlers: Dict[str, EventHandler] = {}
        self._bus: 'EventBus | None' = None
        self._version: int = 0

    @property
    def version(self) -> int:
        """注册表版本号，每次变更递增。"""
        return self._version

    def __len__(self) -> int:
        """返回已注册处理器数量。"""
        return len(self._handlers)

    def __contains__(self, handler_id: str) -> bool:
        """检查处理器 ID 是否已注册。"""
        return handler_id in self._handlers

    def __iter__(self):
        """迭代所有 (handler_id, handler) 对。"""
        return iter(self._handlers.items())

    def activate(self, bus: 'EventBus') -> None:
        """把注册表绑定到一个总线并激活全部在册处理器（**原子，含总线侧副作用回滚**）。

        - 同一注册表**禁止跨总线共用**：已绑定时再次调用抛 ``RuntimeError``；
        - 先绑定总线，再逐个触发 :meth:`EventHandler.on_activate`——激活过程中
          嵌套 ``register`` 的新处理器会立即收到 ``on_activate``；
        - 任一 ``on_activate`` 抛错即**整体回退**：解除绑定，并对**已尝试激活的
          处理器（含当前失败者）**补发 :meth:`EventHandler.on_deactivate`，撤销其
          on_activate 已产生的订阅登记 / 后台任务（避免总线侧幽灵路由），并向上
          抛出首个错误；处理器保持注册，修复后可重新 ``activate``。
        """
        if self._bus is not None:
            raise RuntimeError('注册表已绑定总线，禁止跨总线共用')
        self._bus = bus
        activated: list[EventHandler] = []
        try:
            for handler in list(self._handlers.values()):  # 快照：允许嵌套 register
                activated.append(handler)
                handler.on_activate(bus)
        except Exception:
            self._bus = None
            for handler in reversed(activated):
                try:
                    handler.on_deactivate(bus)
                except Exception:
                    logger.exception(
                        'Handler %s on_deactivate failed during activate rollback',
                        handler.__class__.__name__,
                    )
            raise

    def deactivate(self, strict: bool = False) -> list[tuple[str, Exception]]:
        """解除总线绑定并使全部在册处理器下线（**尽力而为，保留失败者**）。

        - 遍历在册处理器触发 :meth:`EventHandler.on_deactivate`（此时
          :attr:`EventHandler.handler_id` 仍可用）；
        - ``on_deactivate`` 抛错的处理器**保留在册**（避免静默移除的意外行为），
          记录日志并记入返回值；其余处理器照常下线；
        - 返回 ``[(handler_id, 异常), ...]`` 失败清单，调用方（如 :meth:`EventBus.stop`）
          可据此感知清理不完整；``strict=True`` 时在全部尝试后抛出首个失败异常；
        - 未绑定时返回 ``[]``（幂等）。
        """
        if self._bus is None:
            return []
        bus: 'EventBus' = self._bus
        failures: list[tuple[str, Exception]] = []
        for hid, handler in list(self._handlers.items()):  # 快照：允许 on_deactivate 内嵌套变更
            try:
                handler.on_deactivate(bus)
            except Exception as exc:
                failures.append((hid, exc))
                logger.exception('Handler %s:%s on_deactivate failed', handler.__class__.__name__, hid)
        self._bus = None
        if strict and failures:
            raise failures[0][1]
        return failures

    def clear(self) -> list[tuple[str, Exception]]:
        """清除所有已注册处理器（**兜底：保证全部移除，失败显式上报**）。

        若已绑定总线，先 :meth:`deactivate` 使全部在册处理器下线；随后逐个
        触发 :meth:`EventHandler.on_unregistered`。单个钩子失败不中断其余清理，
        全部处理器必然被移除；返回 ``[(handler_id, 异常), ...]`` 失败清单
        （含 deactivate 与 on_unregistered 两阶段）。
        """
        failures: list[tuple[str, Exception]] = []
        if self._bus is not None:
            failures.extend(self.deactivate())
        for handler in list(self._handlers.values()):
            try:
                handler.on_unregistered()
            except Exception as exc:
                failures.append((handler.handler_id or '', exc))
                logger.exception('Handler %s on_unregistered failed', handler.__class__.__name__)
        self._handlers.clear()
        self._version += 1
        return failures

    def __del__(self) -> None:
        """注册表销毁时兜底清理：触发全部在册处理器的下线钩子。

        ``__del__`` 中禁止抛异常（解释器会忽略并告警），故整体包在
        ``try/except`` 内，且仅在解释器仍可正常记录日志时执行。
        """
        try:
            if self._handlers:
                self.clear()
        except Exception:
            pass

    def register(self, handler: EventHandler) -> str:
        """注册一个事件处理器实例（**原子，含总线侧副作用回滚**）。

        同一实例不可重复注册（抛 ``ValueError``）。注册后触发
        :meth:`EventHandler.on_registered` 钩子回填处理器 id；若注册表已绑定
        总线，还触发 :meth:`EventHandler.on_activate` 激活处理器。任一钩子抛异常，
        回退本次注册——撤销入库、不递增版本号——并向上抛出：

        - ``on_registered`` 失败：补发 :meth:`EventHandler.on_unregistered` 清空 id；
        - ``on_activate`` 失败：先补发 :meth:`EventHandler.on_deactivate`，撤销其已产生的
          订阅登记 / 后台任务（避免总线侧幽灵路由），再补发 ``on_unregistered`` 清空 id。
        """
        if handler in self._handlers.values():
            raise ValueError(f'处理器实例 {handler.__class__.__name__} 已注册，禁止重复注册')
        id = uuid.uuid4().hex
        self._handlers[id] = handler
        try:
            handler.on_registered(id)
        except Exception:
            del self._handlers[id]
            try:
                handler.on_unregistered()
            except Exception:
                logger.exception(
                    'Handler %s on_unregistered failed during register rollback',
                    handler.__class__.__name__,
                )
            raise
        try:
            if self._bus is not None:
                handler.on_activate(self._bus)
        except Exception:
            del self._handlers[id]
            if self._bus is not None:
                try:
                    handler.on_deactivate(self._bus)
                except Exception:
                    logger.exception(
                        'Handler %s on_deactivate failed during register rollback',
                        handler.__class__.__name__,
                    )
            try:
                handler.on_unregistered()
            except Exception:
                logger.exception(
                    'Handler %s on_unregistered failed during register rollback',
                    handler.__class__.__name__,
                )
            raise
        self._version += 1
        return id

    def get(self, handler_id: str) -> Optional[EventHandler]:
        """根据ID获取事件处理器实例"""
        return self._handlers.get(handler_id)

    def unregister(self, handler_id: str) -> Exception | None:
        """注销一个事件处理器实例（**兜底：保证移除，失败显式上报**）。

        - 处理器必然被移除；id 不存在时视为幂等空操作，返回 ``None``
          （存在性请用 ``handler_id in registry`` 判断）；
        - 若注册表已绑定总线，先触发 :meth:`EventHandler.on_deactivate` 使处理器
          下线（此时 :attr:`EventHandler.handler_id` 仍可用）；随后触发
          :meth:`EventHandler.on_unregistered` 清空 id；
        - 钩子失败不阻断移除，但**返回首个失败异常**（其余记录日志），
          调用方据此感知清理不完整。
        """
        handler: Optional[EventHandler] = self._handlers.pop(handler_id, None)
        if handler is None:
            return None
        first_error: Exception | None = None
        if self._bus is not None:
            try:
                handler.on_deactivate(self._bus)
            except Exception as exc:
                first_error = exc
                logger.exception('Handler %s on_deactivate failed', handler.__class__.__name__)
        try:
            handler.on_unregistered()
        except Exception as exc:
            if first_error is None:
                first_error = exc
            logger.exception('Handler %s on_unregistered failed', handler.__class__.__name__)
        self._version += 1
        return first_error

    @property
    def handlers_count(self) -> int:
        """（兼容属性）返回已注册处理器数量，等价于 ``len(registry)``。"""
        return len(self._handlers)

    @property
    def all_handlers(self) -> Dict[str, EventHandler]:
        """获取所有注册的事件处理器实例"""
        return self._handlers.copy()
