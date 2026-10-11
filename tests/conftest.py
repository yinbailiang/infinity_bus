"""共享的 fixtures、测试用 Payload/Event/Handler 和工具函数。"""

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Type

import pytest
from pydantic import BaseModel, Field

from infinity_bus import (
    Event,
    EventBus,
    EventDeclaration,
    EventHandler,
    EventHandlerRegistry,
    EventQueue,
    EventRegistry,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
    MiddlewareChain,
    Regex,
    Router,
    TaskErrorEvent,
    TaskErrorPayload,
)

# 抑制日志噪音
logging.getLogger('infinity_bus').setLevel(logging.WARNING)

# ============================================================================
# 测试用 Payload
# ============================================================================


class BusTestPayload(BaseModel):
    value: int
    msg: str = Field(default='test')


class MiddlewareTestPayload(BaseModel):
    key: str
    count: int = Field(default=0)


# ============================================================================
# 事件声明
# ============================================================================


class TestEventDecl(EventDeclaration):
    name = 'test.event'
    payload_type = BusTestPayload


class SlowEventDecl(EventDeclaration):
    name = 'test.slow'


class BlockEventDecl(EventDeclaration):
    name = 'test.block'


class UserLoginEventDecl(EventDeclaration):
    name = 'user.login'


class UserLogoutEventDecl(EventDeclaration):
    name = 'user.logout'


class AdminLoginEventDecl(EventDeclaration):
    name = 'admin.login'


class MiddlewarePingEventDecl(EventDeclaration):
    name = 'mw.ping'
    payload_type = MiddlewareTestPayload


# 基础事件列表（所有需要预先注册的）
BASE_EVENT_DECLS: List[Type[EventDeclaration]] = [
    TestEventDecl,
    SlowEventDecl,
    BlockEventDecl,
    UserLoginEventDecl,
    UserLogoutEventDecl,
    AdminLoginEventDecl,
    MiddlewarePingEventDecl,
    TaskErrorEvent,
]

# ============================================================================
# 可复用的 Handler 基类
# ============================================================================


class BaseTestHandler(EventHandler):
    """提供常用辅助方法的测试 Handler 基类"""

    def __init__(self, subscriptions: List[str | Regex], handle_timeout: float = 1.0):
        super().__init__(subscriptions, handle_timeout)
        self._started = asyncio.Event()
        self._completed = asyncio.Event()
        self._error: Optional[Exception] = None

    async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
        self._started.set()
        try:
            await self._do_handle(payload, bus_proxy, raw_event)
        except Exception as e:
            self._error = e
            raise
        finally:
            self._completed.set()

    async def _do_handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
        raise NotImplementedError

    async def wait_started(self, timeout: float = 1.0) -> None:
        await asyncio.wait_for(self._started.wait(), timeout)

    async def wait_completed(self, timeout: float = 1.0) -> None:
        await asyncio.wait_for(self._completed.wait(), timeout)

    @property
    def error(self) -> Optional[Exception]:
        return self._error


class CountingHandler(BaseTestHandler):
    """统计 test.event 成功处理次数（value >= 0）"""

    def __init__(self):
        super().__init__(['test.event'])
        self.count = 0

    async def _do_handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event):
        if isinstance(payload, BusTestPayload):
            if payload.value >= 0:
                self.count += 1
            else:
                raise ValueError(f'Invalid value: {payload.value}')


class SlowHandler(BaseTestHandler):
    """模拟耗时处理，可配置延迟"""

    def __init__(
        self,
        delay: float = 0.5,
        subscriptions: Optional[List[str | Regex]] = None,
        handle_timeout: float = 0.1,
    ):
        super().__init__(subscriptions or ['test.slow'], handle_timeout=handle_timeout)
        self.delay = delay
        self.completed = 0

    async def _do_handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event):
        await asyncio.sleep(self.delay)
        self.completed += 1


class BlockingHandler(BaseTestHandler):
    """可控制阻塞的 Handler，用于测试背压/关闭"""

    def __init__(self, subscriptions: Optional[List[str | Regex]] = None):
        super().__init__(subscriptions or ['test.block'], handle_timeout=10.0)
        self._block = asyncio.Event()

    async def _do_handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event):
        await self._block.wait()

    def release(self):
        self._block.set()


class ConcurrentTrackingHandler(BaseTestHandler):
    """跟踪并发数，用于测试 Semaphore 限流"""

    def __init__(self, subscriptions: Optional[List[str | Regex]] = None):
        super().__init__(subscriptions or ['test.block'], handle_timeout=10.0)
        self.active_count = 0
        self.max_seen = 0
        self._lock = asyncio.Lock()
        self._done = asyncio.Event()

    async def _do_handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event):
        async with self._lock:
            self.active_count += 1
            self.max_seen = max(self.max_seen, self.active_count)

        await self._done.wait()

        async with self._lock:
            self.active_count -= 1

    def release_all(self):
        self._done.set()


class ErrorSpyHandler(BaseTestHandler):
    """捕获 __task_error__ 事件"""

    def __init__(self):
        super().__init__(['infinity_bus.__task_error__'])
        self.captured: List[Dict[str, str]] = []

    async def _do_handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event):
        if isinstance(payload, TaskErrorPayload):
            self.captured.append(
                {
                    'handler': payload.handler_name,
                    'type': payload.error_type,
                    'msg': payload.error_message,
                }
            )


class PatternSpyHandler(BaseTestHandler):
    """记录匹配正则的事件名"""

    def __init__(self, pattern: str = r'user\..*'):
        super().__init__([Regex(pattern)])
        self.triggered: List[str] = []

    async def _do_handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event):
        self.triggered.append(raw_event.name)


class SimplePingHandler(EventHandler):
    """简单 ping 处理器 —— 用于中间件测试"""

    def __init__(self):
        super().__init__(['mw.ping'])
        self.received: List[MiddlewareTestPayload] = []
        self._event = asyncio.Event()

    async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
        if isinstance(payload, MiddlewareTestPayload):
            self.received.append(payload)
            self._event.set()

    async def wait_received(self, timeout: float = 2.0) -> None:
        await asyncio.wait_for(self._event.wait(), timeout)


# ============================================================================
# Fixtures
# ============================================================================


def create_event_registry(
    event_classes: List[Type[EventDeclaration]],
) -> EventRegistry:
    reg = EventRegistry()
    for cls in event_classes:
        reg.register(cls)
    return reg


@pytest.fixture
def base_event_registry() -> EventRegistry:
    """包含所有基础测试事件的注册表"""
    return create_event_registry(BASE_EVENT_DECLS)


@pytest.fixture
def empty_event_registry() -> EventRegistry:
    """空注册表"""
    return EventRegistry()


@pytest.fixture
def handler_registry() -> EventHandlerRegistry:
    """空 Handler 注册表"""
    return EventHandlerRegistry()


@pytest.fixture
def router(base_event_registry: EventRegistry) -> Router:
    """基于事件注册表的路由器"""
    return Router(base_event_registry)


@pytest.fixture
def event_bus_factory(
    base_event_registry: EventRegistry, handler_registry: EventHandlerRegistry
) -> Callable[..., EventBus]:
    """可配置参数的 EventBus 工厂函数"""

    def _create(
        max_queue_size: int = 10,
        max_handler_semaphore: int = 20,
        registry: Optional[EventRegistry] = None,
        h_registry: Optional[EventHandlerRegistry] = None,
        middleware_chain: Optional[MiddlewareChain] = None,
        queue: Optional[EventQueue] = None,
    ) -> EventBus:
        # 注意：注册表实现 __len__，空注册表 bool() 为 False——须用显式 None 判断
        # 而非 `x or default`，否则传入空注册表会被静默回退到默认 fixture。
        return EventBus(
            registry if registry is not None else base_event_registry,
            h_registry if h_registry is not None else handler_registry,
            queue=queue if queue is not None else InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=max_queue_size)),
            max_handler_semaphore=max_handler_semaphore,
            middleware_chain=middleware_chain,
        )

    return _create


@pytest.fixture
async def event_bus(event_bus_factory: Callable[..., EventBus]):
    """默认的、已启动的 EventBus，测试结束后自动停止"""
    bus: EventBus = event_bus_factory()
    await bus.start()
    yield bus
    await bus.stop()


# ============================================================================
# 辅助工具函数
# ============================================================================


async def wait_for_condition(condition: Callable[[], bool], timeout: float = 2.0, interval: float = 0.01) -> None:
    """轮询等待条件成立"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        await asyncio.sleep(interval)
    raise TimeoutError(f'Condition not met within {timeout}s')


async def publish_many(
    bus: EventBus,
    event_name: str,
    payloads: List[Any],
    client_name: str = 'pub',
) -> List[Awaitable[Any]]:
    """批量发布事件"""
    proxy = bus.proxy(client_name)
    return [proxy.publish(event_name, p) for p in payloads]
