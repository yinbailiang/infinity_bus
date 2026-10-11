import logging
from inspect import isawaitable
from typing import Any, Awaitable, Callable, Dict, Literal, Optional, Set, Union

from pydantic import BaseModel

from infinity_bus import (
    BeforePublishNext,
    Event,
    EventBus,
    EventRegistry,
    Middleware,
    OnPublishNext,
)

logger = logging.getLogger(__name__)

EventFilter = Callable[[Event], Union[bool, Awaitable[bool]]]
"""事件过滤回调签名：接收 Event，返回是否转发。"""

TargetBusProvider = Callable[[], Union[EventBus, Awaitable[EventBus]]]
"""目标总线提供者签名：返回目标 EventBus 实例。"""


class EventForwardMiddleware(Middleware):
    """单向跨总线事件转发中间件。

    在 ``on_publish`` 阶段将事件转发到另一个 EventBus 实例。
    转发过程完全异步且失败隔离——目标总线异常不会影响源总线的正常运行。

    参数
    ----
    target:
        目标总线或返回目标总线的回调。支持：
        - 直接传入 ``EventBus`` 实例（总线已启动且持续运行）
        - 传入 ``Callable[[], EventBus]`` 工厂回调（每次转发时获取最新实例）
    source_name:
        在目标总线上发布时使用的来源标识，默认 ``"event_forward"``。
    event_filter:
        事件过滤回调，返回 ``True`` 表示需要转发。
        为 ``None`` 时转发所有非系统事件。
        支持同步或异步回调。
    forward_system_events:
        是否转发 ``infinity_bus.*`` 系统事件，默认 ``False``。
    """

    _SYSTEM_EVENT_PREFIX = 'infinity_bus.'

    def __init__(
        self,
        target: Union[EventBus, TargetBusProvider],
        source_name: str = 'event_forward',
        event_filter: Optional[EventFilter] = None,
        forward_system_events: bool = False,
    ) -> None:
        self._target: EventBus | TargetBusProvider = target
        self._source_name: str = source_name
        self._filter: EventFilter | None = event_filter
        self._forward_system: bool = forward_system_events

        # 可观测性计数器
        self._forwarded_count: int = 0
        self._failed_count: int = 0
        self._skipped_count: int = 0

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: Dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        """透传"""
        await next(event_registry, name, source, data, old_event)

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        """在事件成功入队后，将其转发到目标总线。"""
        try:
            await self._do_forward(event)
        except Exception:
            logger.exception(
                'EventForward: 转发事件 %s (id=%s) 时发生未预期异常',
                event.name,
                event.id,
            )
        finally:
            await next(event)

    async def _do_forward(self, event: Event) -> None:
        """执行实际的转发逻辑。"""
        if not self._forward_system and event.name.startswith(self._SYSTEM_EVENT_PREFIX):
            self._skipped_count += 1
            return

        if self._filter is not None:
            try:
                result = self._filter(event)
                if isawaitable(result):
                    result = await result
                if not result:
                    self._skipped_count += 1
                    return
            except Exception:
                self._skipped_count += 1
                logger.warning(
                    'EventForward: 过滤回调异常，跳过事件 %s (id=%s)',
                    event.name,
                    event.id,
                    exc_info=True,
                )
                return

        try:
            target_bus = await self._resolve_target()
        except Exception:
            self._failed_count += 1
            logger.exception('EventForward: 获取目标总线失败，事件 %s (id=%s)', event.name, event.id)
            return

        if event.data is not None:
            forward_data: Optional[Dict[str, Any]] = event.data.model_dump()
        else:
            forward_data = None

        try:
            await target_bus.proxy(self._source_name, event).publish(event.name, forward_data)
            self._forwarded_count += 1
            logger.debug(
                'EventForward: 已转发 %s (id=%s) → target_bus',
                event.name,
                event.id,
            )
            return
        except Exception as e:
            self._failed_count += 1
            logger.warning(
                'EventForward: 转发 %s 失败 %s',
                event.name,
                e,
            )

    async def _resolve_target(self) -> EventBus:
        """解析目标总线实例"""
        if isinstance(self._target, EventBus):
            raw: Union[EventBus, Awaitable[EventBus]] = self._target
        else:
            raw = self._target()
        if isawaitable(raw):
            return await raw
        return raw

    @property
    def forwarded_count(self) -> int:
        """累计成功转发的事件数。"""
        return self._forwarded_count

    @property
    def failed_count(self) -> int:
        """累计转发失败的事件数（含目标不可达、publish 异常）。"""
        return self._failed_count

    @property
    def skipped_count(self) -> int:
        """累计被跳过的事件数（系统事件、过滤器拒绝、过滤器异常）。"""
        return self._skipped_count


def make_event_name_filter(
    *event_names: str,
    mode: Literal['white', 'black'] = 'white',
) -> EventFilter:
    """创建一个基于事件名的过滤回调。"""

    name_set: Set[str] = set(event_names)

    match mode:
        case 'white':

            async def _allow_filter(event: Event) -> bool:
                return event.name in name_set

            return _allow_filter
        case 'black':

            async def _block_filter(event: Event) -> bool:
                return event.name not in name_set

            return _block_filter


def make_bidirectional_forward(
    bus_a: Union[EventBus, TargetBusProvider],
    bus_b: Union[EventBus, TargetBusProvider],
    *,
    source_a_to_b: str = 'a→b',
    source_b_to_a: str = 'b→a',
    event_filter: Optional[EventFilter] = None,
    anti_recursion: bool = True,
    forward_system_events: bool = False,
) -> tuple[EventForwardMiddleware, EventForwardMiddleware]:
    """创建一对单向转发中间件，实现两条总线之间的双向事件同步。

    返回 ``(a_to_b, b_to_a)``：
    - ``a_to_b`` 挂载到总线 A 的中间件链，将事件从 A 转发到 B
    - ``b_to_a`` 挂载到总线 B 的中间件链，将事件从 B 转发到 A

    参数
    ----
    bus_a:
        总线 A（或返回总线 A 的回调）。
    bus_b:
        总线 B（或返回总线 B 的回调）。
    source_a_to_b:
        A→B 方向在目标总线上使用的来源标识，默认 ``"a→b"``。
    source_b_to_a:
        B→A 方向在目标总线上使用的来源标识，默认 ``"b→a"``。
    event_filter:
        共享的事件过滤回调，为 ``None`` 时转发所有非系统事件。
        若需两个方向使用不同过滤，请手动创建 ``EventForwardMiddleware``。
    anti_recursion:
        是否启用反递归过滤，默认 ``True``。
        启用后，每个方向自动跳过已由对向中间件转发过来的事件，
        防止 A→B→A→B… 无限循环。
    forward_system_events:
        是否转发 ``infinity_bus.*`` 系统事件，默认 ``False``。

    Example::

        a_to_b, b_to_a = make_bidirectional_forward(bus_a, bus_b)

        chain_a = MiddlewareChain()
        chain_a.add(a_to_b)
        bus_a = EventBus(..., middleware_chain=chain_a)

        chain_b = MiddlewareChain()
        chain_b.add(b_to_a)
        bus_b = EventBus(..., middleware_chain=chain_b)
    """

    def _compose_filter(
        base: Optional[EventFilter],
        anti_source: str,
    ) -> Optional[EventFilter]:
        """将用户过滤与反递归过滤组合。"""
        if not anti_recursion:

            async def _anti_pass(event: Event) -> bool:
                return True

            _anti: EventFilter = _anti_pass
        else:

            async def _anti_source_filter(event: Event) -> bool:
                return anti_source not in event.sources

            _anti = _anti_source_filter

        if base is None:
            return _anti

        async def _composed(event: Event) -> bool:
            base_result = base(event)
            if isawaitable(base_result):
                base_result = await base_result
            if not base_result:
                return False
            anti_result = _anti(event)
            if isawaitable(anti_result):
                anti_result = await anti_result
            return bool(anti_result)

        return _composed

    a_to_b = EventForwardMiddleware(
        target=bus_b,
        source_name=source_a_to_b,
        event_filter=_compose_filter(event_filter, source_b_to_a),
        forward_system_events=forward_system_events,
    )
    b_to_a = EventForwardMiddleware(
        target=bus_a,
        source_name=source_b_to_a,
        event_filter=_compose_filter(event_filter, source_a_to_b),
        forward_system_events=forward_system_events,
    )
    return a_to_b, b_to_a
