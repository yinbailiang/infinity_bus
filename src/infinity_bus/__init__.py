"""infinity_bus - 基于 asyncio 的轻量级事件总线"""

__version__ = '5.0.0'

from .bus import (
    BusError,
    BusShuttingDown,
    BusStartFailed,
    BusStartRollbackError,
    EventBus,
    ShutdownConfig,
    ShutdownEvent,
    TaskErrorEvent,
    TaskErrorPayload,
)
from .event import (
    Event,
    EventDeclaration,
    EventRegistry,
)
from .handler import (
    EventHandler,
    EventHandlerRegistry,
    Regex,
)
from .middleware import (
    BeforePublishNext,
    DispatchNext,
    Middleware,
    MiddlewareChain,
    OnPublishErrorNext,
    OnPublishNext,
    TaskOutcome,
    TaskRecord,
)
from .queue import (
    EventQueue,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
)
from .router import (
    Router,
    Subscriptions,
)

__all__: list[str] = [
    '__version__',
    'Event',
    'EventDeclaration',
    'EventRegistry',
    'EventHandler',
    'EventHandlerRegistry',
    'Regex',
    'Router',
    'Subscriptions',
    'Middleware',
    'MiddlewareChain',
    'BeforePublishNext',
    'OnPublishNext',
    'OnPublishErrorNext',
    'DispatchNext',
    'TaskOutcome',
    'TaskRecord',
    'EventQueue',
    'InMemoryEventQueue',
    'InMemoryEventQueueConfig',
    'EventBus',
    'BusError',
    'BusShuttingDown',
    'BusStartFailed',
    'BusStartRollbackError',
    'ShutdownEvent',
    'TaskErrorPayload',
    'TaskErrorEvent',
    'ShutdownConfig',
]
