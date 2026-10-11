"""事件路由器 —— 订阅束 + 激活路由表（分派真相）。

路由语义对齐消息中间件：路由表**只含「已激活」的订阅束**
（broker 的绑定随连接存续）。处理器在 :meth:`EventHandler.on_activate`
时经 ``bus.router.subscribe(handler_id, self.subscriptions)`` 登记上线，
在 :meth:`EventHandler.on_deactivate` 时 ``unsubscribe`` 撤销下线。

- 运行期改订阅：直接对 :class:`Subscriptions` 调 add / remove / replace——
  激活中改动经绑定通知 Router（版本自增），分派表惰性重建自动生效；
- 未激活时改动仅更新束内容，下次 ``subscribe`` 时整体生效；
- 分派只返回命中的 ``handler_id``，handler 实例由总线从注册表解析。
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, Iterable, Iterator, List, Optional, Set, Tuple, cast

from .event import EventRegistry

if TYPE_CHECKING:
    from .handler import Regex

logger = logging.getLogger(__name__)


class Subscriptions:
    """订阅束：一组订阅模式的托管容器（替代裸露列表）。

    - 处理器构造时持有实例作为其订阅声明；
    - 激活时经 :meth:`Router.subscribe` 登记进路由表并绑定本 Router；
    - 运行期通过 :meth:`add` / :meth:`remove` / :meth:`replace` 修改——
      激活中改动自动同步 live 路由（Router 版本自增），未激活仅更新声明。
    """

    def __init__(self, patterns: Iterable[str | Regex] | None = None) -> None:
        """构造订阅束（patterns 可为 str/Regex 的可迭代，复制保存）。"""
        self._patterns: list[str | Regex] = list(patterns) if patterns is not None else []
        self._router: Optional[Router] = None
        self._handler_id: Optional[str] = None

    # ------------------------------------------------------------------
    # 变更（激活中自动同步 live 路由）
    # ------------------------------------------------------------------

    def add(self, pattern: str | Regex) -> None:
        """追加一个订阅模式（重复添加被忽略）。"""
        if pattern not in self._patterns:
            self._patterns.append(pattern)
            self._notify()

    def remove(self, pattern: str | Regex) -> bool:
        """移除一个订阅模式；存在并移除返回 True，否则 False。"""
        try:
            self._patterns.remove(pattern)
        except ValueError:
            return False
        self._notify()
        return True

    def replace(self, patterns: Iterable[str | Regex]) -> None:
        """整体替换订阅模式（状态机切换阶段等场景）。"""
        self._patterns = list(patterns)
        self._notify()

    # ------------------------------------------------------------------
    # 绑定（Router 内部调用；激活中改动经此通知 Router）
    # ------------------------------------------------------------------

    def bind(self, router: 'Router', handler_id: str) -> None:
        """绑定到 Router（Router.subscribe 内部调用）。"""
        self._router = router
        self._handler_id = handler_id

    def unbind(self) -> None:
        """解除与 Router 的绑定（Router.unsubscribe 内部调用）。"""
        self._router = None
        self._handler_id = None

    def _notify(self) -> None:
        if self._router is not None:
            self._router.version += 1

    # ------------------------------------------------------------------
    # 只读视图
    # ------------------------------------------------------------------

    @property
    def patterns(self) -> tuple[str | Regex, ...]:
        """当前订阅模式的只读元组。"""
        return tuple(self._patterns)

    @property
    def is_bound(self) -> bool:
        """是否已登记到某个 Router（激活中）。"""
        return self._router is not None

    def __iter__(self) -> Iterator[str | Regex]:
        return iter(self._patterns)

    def __len__(self) -> int:
        return len(self._patterns)

    def __bool__(self) -> bool:
        return bool(self._patterns)

    def __contains__(self, pattern: object) -> bool:
        return pattern in self._patterns

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Subscriptions):
            return self._patterns == other._patterns
        if isinstance(other, (list, tuple)):
            other_seq: list[str | Regex] | tuple[str | Regex, ...] = cast(
                'list[str | Regex] | tuple[str | Regex, ...]', other
            )
            return self._patterns == list(other_seq)
        return NotImplemented

    def __repr__(self) -> str:
        return f'Subscriptions({self._patterns!r})'


class Router:
    """事件路由器：订阅束登记 + 预计算分派表（版本感知）。

    ``subscribe`` 把处理器持有的订阅束登记为激活路由；``unsubscribe``
    撤销。路由表是分派的**唯一真相**，任何登记/撤销/束变更都递增版本号，
    分派表在 ``match`` 时惰性重建（精确 ``str`` 反向索引 + ``Regex`` 扫描）。
    """

    def __init__(self, event_registry: EventRegistry) -> None:
        """构造路由器（需事件注册表以预计算分派表）。"""
        self._events: EventRegistry = event_registry
        self._route_table: Dict[str, Subscriptions] = {}
        self.version: int = 0
        self._table: Dict[str, List[str]] = {}
        self._event_version: int = -1
        self._route_version: int = -1

    # ------------------------------------------------------------------
    # 登记 / 撤销（激活期调用；幂等）
    # ------------------------------------------------------------------

    def subscribe(self, handler_id: str, subs: Subscriptions) -> None:
        """把订阅束登记为激活路由（幂等替换：重复订阅同一束为空操作）。

        登记后束绑定本 Router——之后对束的 add/remove/replace 会自动同步。
        """
        old = self._route_table.get(handler_id)
        if old is subs:
            return
        if old is not None:
            old.unbind()
        self._route_table[handler_id] = subs
        subs.bind(self, handler_id)
        self.version += 1

    def unsubscribe(self, handler_id: str) -> bool:
        """撤销该 handler 的激活订阅（不存在返回 False）。"""
        subs = self._route_table.pop(handler_id, None)
        if subs is None:
            return False
        subs.unbind()
        self.version += 1
        return True

    def clear(self) -> None:
        """撤销全部激活订阅（解绑所有束）。"""
        for subs in self._route_table.values():
            subs.unbind()
        self._route_table.clear()
        self.version += 1

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    @property
    def routes(self) -> Tuple[Tuple[str, Subscriptions], ...]:
        """当前激活路由的只读快照 ``((handler_id, 订阅束), ...)``。"""
        return tuple(self._route_table.items())

    @property
    def dispatch_table(self) -> Dict[str, List[str]]:
        """预计算分派表的只读副本（自动感知版本变更）。"""
        self._ensure_fresh()
        return {name: list(ids) for name, ids in self._table.items()}

    def match(self, event_name: str) -> List[str]:
        """返回命中该事件名的 handler_id 列表（保持订阅登记顺序）。

        自动感知事件注册表/路由版本变更并重建缓存；只出 id，
        handler 实例由总线从处理器注册表解析。
        """
        self._ensure_fresh()
        return list(self._table.get(event_name, ()))

    # ------------------------------------------------------------------
    # 版本感知重建
    # ------------------------------------------------------------------

    def _ensure_fresh(self) -> None:
        if self._event_version != self._events.version or self._route_version != self.version:
            self._rebuild()

    def _rebuild(self) -> None:
        """重建分派表：精确 str → 反向索引；非 str（Regex）→ 扫描；按 handler 去重。"""
        exact_index: Dict[str, List[str]] = {}
        regex_list: List[Tuple[str, 'Regex']] = []
        for handler_id, subs in self._route_table.items():
            for pattern in subs.patterns:
                if isinstance(pattern, str):
                    exact_index.setdefault(pattern, []).append(handler_id)
                else:
                    regex_list.append((handler_id, pattern))

        self._table.clear()
        for event_name in self._events.list_names():
            matched: List[str] = []
            seen: Set[str] = set()
            for hid in exact_index.get(event_name, ()):
                if hid not in seen:
                    matched.append(hid)
                    seen.add(hid)
            for hid, regex in regex_list:
                if hid not in seen and regex.fullmatch(event_name) is not None:
                    matched.append(hid)
                    seen.add(hid)
            self._table[event_name] = matched

        self._event_version = self._events.version
        self._route_version = self.version
