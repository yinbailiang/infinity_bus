"""infinity_bus.templates — 可插拔的事件处理模板与中间件集合。

提供开箱即用的处理器装饰器、管道通信、请求-响应模式、事件注册器
以及日志、指标、限流、转换等常用中间件。
"""

from .expect import OneShotEventHandler, expect, temporary_handler
from .handlers.mailbox import MailboxConfig, MailboxHandler, StopMailbox
from .handlers.service import (
    AccessDeniedError,
    ConflictError,
    InvalidRequestError,
    NotFoundError,
    ProcessorMeta,
    ServiceError,
    ServiceHandler,
    process,
)
from .idempotency import (
    IdempotencyRecorder,
    IdempotentHandler,
    InMemoryIdempotencyRecorder,
    SqliteIdempotencyRecorder,
)
from .middlewares import (
    BlockPredicate,
    EventBlockMiddleware,
    EventFilter,
    EventForwardMiddleware,
    EventTransformMiddleware,
    JSONLLoggingMiddleware,
    LogFallback,
    MetricsMiddleware,
    MetricsSnapshot,
    RateLimitMiddleware,
    RecursionDetectedError,
    RecursionGuardMiddleware,
    SQLiteLoggingMiddleware,
    TargetBusProvider,
    TransformFunc,
    make_allowlist_predicate,
    make_bidirectional_forward,
    make_blocklist_predicate,
    make_event_name_filter,
    make_field_inject_transform,
    make_field_redact_transform,
    make_rename_transform,
    serialize_data,
)
from .pipe import (
    InProcessPipe,
    InProcessPipeAllocator,
    Pipe,
    PipeAllocator,
    PipeClosedError,
    PipeHandshakeError,
    PipeLinkedResponse,
    PipeOpenRequest,
    PipeTeardownError,
    expect_pipe,
    get_default_allocator,
    open_pipe,
)
from .queues import EventCodec, PayloadType, RabbitFanoutQueue
from .register import ModuleEventRegister, ModuleHandlerRegister
from .request import RequestProtocol, ResponseProtocol, build_response, request
from .simple_handler import handler

__all__ = [
    # handler
    'handler',
    # expect
    'expect',
    'OneShotEventHandler',
    'temporary_handler',
    # middlewares
    'SQLiteLoggingMiddleware',
    'JSONLLoggingMiddleware',
    'MetricsMiddleware',
    'MetricsSnapshot',
    'RateLimitMiddleware',
    'EventTransformMiddleware',
    'EventBlockMiddleware',
    'EventForwardMiddleware',
    'RecursionGuardMiddleware',
    'RecursionDetectedError',
    'make_rename_transform',
    'make_field_inject_transform',
    'make_field_redact_transform',
    'make_blocklist_predicate',
    'make_allowlist_predicate',
    'make_event_name_filter',
    'make_bidirectional_forward',
    # middlewares 类型别名 & 工具
    'BlockPredicate',
    'EventFilter',
    'LogFallback',
    'TargetBusProvider',
    'TransformFunc',
    'serialize_data',
    # pipe
    'PipeHandshakeError',
    'PipeClosedError',
    'PipeTeardownError',
    'PipeLinkedResponse',
    'PipeOpenRequest',
    'Pipe',
    'InProcessPipe',
    'InProcessPipeAllocator',
    'PipeAllocator',
    'open_pipe',
    'expect_pipe',
    'get_default_allocator',
    # register
    'ModuleEventRegister',
    'ModuleHandlerRegister',
    # request
    'request',
    'RequestProtocol',
    'ResponseProtocol',
    'build_response',
    # mailbox（邮箱模式处理器）
    'MailboxHandler',
    'MailboxConfig',
    'StopMailbox',
    # service（服务处理器）
    'ServiceHandler',
    'process',
    'ServiceError',
    'NotFoundError',
    'ConflictError',
    'InvalidRequestError',
    'AccessDeniedError',
    'ProcessorMeta',
    # idempotency（幂等 recorder 注入策略）
    'IdempotencyRecorder',
    'IdempotentHandler',
    'InMemoryIdempotencyRecorder',
    'SqliteIdempotencyRecorder',
    # queues（跨进程 EventQueue）
    'EventCodec',
    'PayloadType',
    'RabbitFanoutQueue',
]
