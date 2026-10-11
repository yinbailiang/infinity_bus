import asyncio
import logging
from contextlib import asynccontextmanager, contextmanager
from inspect import isawaitable
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Generator,
    List,
    Optional,
    Union,
)

from pydantic import BaseModel

from .. import (
    Event,
    EventBus,
    EventHandler,
    EventHandlerRegistry,
    Regex,
)

logger = logging.getLogger(__name__)


class OneShotEventHandler(EventHandler):
    """一次性事件处理器：等待匹配的事件并触发回调。"""

    def __init__(
        self,
        event_patterns: List[Union[str, Regex]],
        on_match: Callable[[Event], Any],
        filter_func: Optional[Callable[[Event], Union[Awaitable[bool], bool]]] = None,
        on_error: Optional[Callable[[Exception], Any]] = None,
        handle_timeout: Optional[float] = None,
    ) -> None:
        # 若需要监听关闭事件，自动添加内置事件名
        super().__init__(subscriptions=event_patterns.copy(), handle_timeout=handle_timeout)

        self._on_match: Callable[[Event], Any] = on_match
        self._filter: Optional[Callable[[Event], Union[Awaitable[bool], bool]]] = filter_func
        self._on_error: Optional[Callable[[Exception], Any]] = on_error

        # 用于逻辑停用：即使处理器尚未从注册表移除，也阻止其继续执行回调
        self._active = asyncio.Event()
        self._active.set()

    async def handle(self, payload: Optional[BaseModel], bus_proxy: EventBus.Proxy, raw_event: Event) -> None:
        """事件入口，内部进行状态检查与过滤"""
        if not self._active.is_set():
            logger.debug('OneShotEventHandler 已停用，跳过事件: %s (id=%s)', raw_event.name, raw_event.id)
            return

        logger.debug('OneShotEventHandler 收到事件: %s (id=%s)', raw_event.name, raw_event.id)

        # 过滤
        if self._filter is not None:
            try:
                result: Union[Awaitable[bool], bool] = self._filter(raw_event)
                if isawaitable(result):
                    result = await result
                if not result:
                    logger.debug('OneShotEventHandler 过滤器返回 False，跳过事件: %s', raw_event.name)
                    return
            except Exception as e:
                logger.debug('OneShotEventHandler 过滤器异常: %s: %s', type(e).__name__, e)
                if self._active.is_set():
                    self._active.clear()
                    if self._on_error:
                        self._on_error(e)
                    else:
                        raise  # 触发总线错误事件（兜底）

        if self._active.is_set():
            self._active.clear()
            logger.debug('OneShotEventHandler 匹配成功，触发回调: %s', raw_event.name)
            self._on_match(raw_event)


@contextmanager
def temporary_handler(
    handler_registry: EventHandlerRegistry,
    handler: EventHandler,
) -> Generator[None, None, None]:
    """临时注册一个事件处理器，离开上下文时自动注销。"""
    handler_id: str = handler_registry.register(handler)
    try:
        yield
    finally:
        handler_registry.unregister(handler_id)


@asynccontextmanager
async def expect(
    bus_proxy: EventBus.Proxy,
    event_patterns: Union[str, Regex, List[Union[str, Regex]]],
    filter_func: Optional[Callable[[Event], Union[Awaitable[bool], bool]]] = None,
) -> AsyncGenerator[asyncio.Future[Event], None]:
    """异步上下文管理器：注册一个一次性事件监听器，返回一个 Future 用于等待匹配的事件。"""
    if isinstance(event_patterns, (str, Regex)):
        patterns: List[Union[str, Regex]] = [event_patterns]
    else:
        patterns: List[Union[str, Regex]] = list(event_patterns)
    future: asyncio.Future[Event] = asyncio.Future()

    def on_error(exc: BaseException) -> None:
        """过滤器异常时设置 future 异常"""
        if future.done():
            return
        try:
            future.set_exception(exc)
        except asyncio.InvalidStateError:
            pass

    def on_match(raw_event: Event) -> None:
        """同步回调，安全地设置 future 结果"""
        if future.done():
            return

        try:
            future.set_result(raw_event)
        except asyncio.InvalidStateError:
            pass

    handler = OneShotEventHandler(
        event_patterns=patterns,
        filter_func=filter_func,
        on_error=on_error,
        on_match=on_match,
    )

    # 使用 temporary_handler 保证清理
    with temporary_handler(bus_proxy.handlers_registry, handler):
        try:
            yield future
        finally:
            if not future.done():
                future.cancel()
