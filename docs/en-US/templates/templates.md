# Templates

Advanced patterns built on top of the core event bus. These are optional — import from
`infinity_bus.templates` to use them.

> Some templates require optional dependencies. Install with:
>
> ```bash
> pip install infinity_bus[templates]
> ```

---

## Quick Index

| Template | Pattern | Use Case | Docs |
| - | - | - | - |
| `handler` | Function→Handler | Quickly define event handlers, sync/async auto-adapt, signature validation | [simple_handler.md](simple_handler.md) |
| `expect` | One-shot Listener | Wait for specific events, test assertions, low-level wait logic | [expect.md](expect.md) |
| `request` | Request-Response (RPC) | Sync-style async calls, inter-service communication | [request.md](request.md) |
| `pipe` | Bidirectional Pipe | Streaming data exchange, long-connection simulation, persistent bidirectional flow | [pipe.md](pipe.md) |
| `register` | Bulk Registration + DI | Large project modular organization, deferred registration, avoid circular imports | [register.md](register.md) |
| [handlers/](handlers/handlers.md) | Handler Template Collection | Mailbox pattern (serial, backpressure) + service handler (RPC server side) | [Handlers Overview](handlers/handlers.md) |
| `idempotency` | Idempotency (injected dedup) | at-least-once dedup, cross-restart strong idempotency | [idempotency.md](idempotency.md) |
| [queues/](queues/queues.md) | Cross-Process Queues | Shared bus across processes, fanout, replay / no-replay, payload rebuild | [Queues Overview](queues/queues.md) |
| [middlewares/](middlewares/middlewares.md) | Middleware Collection | Logging, rate-limiting, transform, blocking, recursion guard | [Middlewares Overview](middlewares/middlewares.md) |

---

## Hierarchy

```text
                    ┌──────────────┐
                    │   handler    │
                    │  Fn→Handler  │
                    └──────┬───────┘
                           │ Generates EventHandler subclass
                           ▼
┌─────────────────────────────────────────────────┐
│                    register                     │
│  Module-level event/handler collection →        │
│  bulk registration at app startup              │
└────────────────────┬────────────────────────────┘
                     │ Registers into
                     ▼
┌─────────────────────────────────────────────────┐
│                   EventBus                      │
│      Publish/Subscribe · Middleware Chain       │
│               · Dispatch Loop                  │
└──────┬────────────────────────────┬─────────────┘
       │                            │
       ▼                            ▼
┌──────────────┐            ┌──────────────┐
│   request    │            │     pipe     │
│  RPC wrapper │◄── depends │  Bidirectional│
│ (uses expect)│            │  handshake    │
└──────┬───────┘            │ (uses request │
       │                    │   + expect)   │
       │ depends             └──────────────┘
       ▼
┌──────────────┐
│    expect    │
│  One-shot    │
│  listener    │
└──────────────┘
```

> `handler` converts plain functions into `EventHandler` subclasses. Both `request` and `pipe` internally depend on `expect` for wait logic. `pipe` also depends on `request` for the handshake protocol.

---

## All Exports

```python
from infinity_bus.templates import (
    # handler
    'handler',
    # expect
    'expect', 'OneShotEventHandler', 'temporary_handler',
    # pipe
    'Pipe', 'InProcessPipe', 'InProcessPipeAllocator', 'PipeAllocator',
    'open_pipe', 'expect_pipe', 'get_default_allocator',
    'PipeHandshakeError', 'PipeClosedError', 'PipeTeardownError',
    'PipeLinkedResponse', 'PipeOpenRequest',
    # register
    'ModuleEventRegister', 'ModuleHandlerRegister',
    # request
    'request', 'RequestProtocol', 'ResponseProtocol', 'build_response',
    # mailbox
    'MailboxHandler', 'MailboxConfig', 'StopMailbox',
    # service
    'ServiceHandler', 'process', 'ServiceError',
    'NotFoundError', 'ConflictError', 'InvalidRequestError', 'AccessDeniedError',
    'ProcessorMeta',
    # idempotency (see idempotency.md)
    'IdempotencyRecorder', 'IdempotentHandler',
    'InMemoryIdempotencyRecorder', 'SqliteIdempotencyRecorder',
    # queues (see queues/queues.md)
    'EventCodec', 'PayloadType', 'RabbitFanoutQueue',
    # middlewares (see middlewares overview)
    'EventBlockMiddleware', 'EventForwardMiddleware', 'EventTransformMiddleware',
    'JSONLLoggingMiddleware', 'SQLiteLoggingMiddleware',
    'MetricsMiddleware', 'MetricsSnapshot',
    'RateLimitMiddleware', 'RecursionGuardMiddleware',
    'RecursionDetectedError',
    'make_rename_transform', 'make_field_inject_transform',
    'make_field_redact_transform', 'make_blocklist_predicate',
    'make_allowlist_predicate', 'make_event_name_filter',
    'make_bidirectional_forward',
    # middlewares type aliases & utilities
    'BlockPredicate', 'EventFilter', 'LogFallback',
    'TargetBusProvider', 'TransformFunc', 'serialize_data',
)
```

---

## Selection Guide

| You want to… | Use this |
| - | - |
| Quickly turn a function into an event handler | `handler` |
| Send a request, wait for a response | `request` |
| Quickly implement the server side of an RPC | `service` |
| Establish a long connection, bidirectional send/receive | `pipe` |
| Wait for a specific event to occur once | `expect` |
| Organize events and handlers by module | `register` |
| Add cross-cutting logic to the publish flow | [middlewares/](middlewares/middlewares.md) |

---

## handler

```python
from infinity_bus.templates import handler

@handler(UserCreated)
async def send_welcome_email(payload: UserCreatedPayload) -> None:
    print(f"Welcome, {payload.email}!")

handler_registry.register(send_welcome_email())
```

See [simple_handler.md](simple_handler.md) for signature validation, sync/async support, and custom timeouts.

## expect

```python
from infinity_bus.templates import expect

async with expect(bus_proxy, "user.login") as future:
    await bus_proxy.publish("auth.request", {...})
    event = await future  # blocks until user.login received
```

See [expect.md](expect.md) for filtering, error handling, and advanced usage.

## request

```python
from infinity_bus.templates import request

response = await request(
    bus_proxy,
    req_event="order.create",
    req_data={"item": "widget"},
    resp_event="order.created",
    timeout=10.0,
)
```

See [request.md](request.md) for protocol definition and error handling.

## handlers

`infinity_bus.templates.handlers` ships two handler base classes:

```python
from infinity_bus.templates import MailboxHandler, ServiceHandler, process

# Service handler: RPC server side (@process decorator)
class OrderService(ServiceHandler):
    @process(OrderCreateReqEvent, OrderCreateRespEvent)
    async def create(self, payload, proxy, raw):
        return {'order_id': 'ord-1'}

# Mailbox pattern: serial consumption on a dedicated task
class SerialWorker(MailboxHandler):
    async def process(self) -> None:
        while True:
            event, proxy = await self.get()
            ...
```

See [Handlers Overview](handlers/handlers.md) for the comparison and selection guide.

## pipe

```python
from infinity_bus.templates import open_pipe

async with open_pipe(pipe_id="my_pipe") as pipe:
    await pipe.send(MyData(...))
    reply = await pipe.receive()
```

See [pipe.md](pipe.md) for allocators and multi-process pipes.

## register

```python
from infinity_bus.templates import ModuleEventRegister, ModuleHandlerRegister

events = ModuleEventRegister("orders")

@events.event
class OrderCreated(EventDeclaration):
    name = "order.created"
    payload_type = OrderPayload

events.register_all_events(event_registry)
```

See [register.md](register.md) for atomic registration and error handling.

## Built-in Middlewares

See [middlewares/middlewares.md](middlewares/middlewares.md) for full documentation.

```python
from infinity_bus.templates.middlewares import (
    JSONLLoggingMiddleware,
    RateLimitMiddleware,
    MetricsMiddleware,
)

chain = MiddlewareChain()
chain.add(JSONLLoggingMiddleware("events.jsonl"))
chain.add(RateLimitMiddleware(max_requests=100, window_seconds=1.0))
chain.add(MetricsMiddleware())
```
