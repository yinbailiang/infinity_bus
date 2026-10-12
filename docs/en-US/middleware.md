# Middleware / MiddlewareChain

## Overview

The middleware system provides pluggable hooks into the publish flow. By inserting custom logic
before and after publishes, you can implement logging, validation, tracing, monitoring,
message transformation, and other cross-cutting concerns **without modifying core business handlers**.

Middlewares use the **onion model (chain of responsibility)** design: multiple middlewares
wrap around each other in registration order, with each middleware able to run logic both
before and after calling `next()`.

---

## Architecture

```text
publish request
  │
  ▼
┌─────────────────────────────────────┐
│  Middleware 1 (outer)               │
│  ┌─────────────────────────────────┐ │
│  │ Middleware 2 (inner)            │ │
│  │ ┌─────────────────────────────┐ │ │
│  │ │ Core publish logic          │ │ │
│  │ │ ├─ validate → build → queue │ │ │
│  │ │ └─ on_publish chain (onion) │ │ │
│  │ └─────────────────────────────┘ │ │
│  └─────────────────────────────────┘ │
└─────────────────────────────────────┘
```

The `on_publish` chain is nested **inside** the core publish logic,
which is the innermost layer of the `before_publish` chain.
Code after `await next(...)` in `before_publish` executes **after**
the entire `on_publish` chain completes.

### Two Hook Phases

| Phase | Timing | Available Info |
| - | - | - |
| `before_publish` | **Before** validation, Event construction, and enqueue | Event name, source, raw data, predecessor event |
| `on_publish` | **After** Event is successfully enqueued | Complete Event object (id, sources, timestamps) |

---

## Middleware

Abstract base class. Custom middlewares must implement `before_publish` and `on_publish`.

```python
class Middleware(ABC):
    # Lifecycle
    async def on_setup(self, bus: EventBus) -> None: ...
    async def on_teardown(self, bus: EventBus) -> None: ...

    # Publish hooks (required)
    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None: ...

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None: ...

    # Error hook (optional)
    async def on_publish_error(
        self,
        error: Exception,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> None: ...

    # Task pipeline hooks (optional)
    async def before_dispatch(
        self,
        event: Event,
        handler_ids: list[str],
        next: DispatchNext,
    ) -> None: ...

    async def on_task_start(self, record: TaskRecord) -> None: ...

    async def on_task_done(self, record: TaskRecord) -> None: ...
```

### Hook Details

#### `on_setup(bus)` / `on_teardown(bus)`

Bus lifecycle hooks. `on_setup` is called during `bus.start()` (**before handler activation**, so messages published by handlers at activation go through a fully-ready middleware pipeline), and **immediately when hot-adding via `add()` / `insert()` on a running bus**. `on_teardown` is called in **reverse registration order** when `stop()` finishes, and immediately when `remove()` / `clear()` is called on a running bus. Use these for initializing connection pools, registering background tasks, etc.

> **Note**: Middleware lifecycle is **fail-fast and atomic**, mirroring handler activation: an `on_setup` failure fails the whole bus start (rolled back, retryable after fixing), and a hot-added middleware whose `on_setup` fails is rolled back and rejected. Failing middlewares are never silently dropped.
>
> **Warning**: Runtime `remove()` does not wait for in-flight hooks to complete. After `on_teardown` is called, a middleware instance that has already entered `before_publish` / `on_publish` may still execute. Middleware authors should ensure their own state cleanup is safe against these residual calls.

#### `before_publish(event_registry, name, source, data, old_event, next)`

Runs **before** any publish logic (including event declaration validation). **Must** call `await next(...)` for the event to proceed.

| Parameter | Description |
| - | - |
| `event_registry` | Access event declaration metadata. |
| `name` | Event type name. |
| `source` | Publisher identifier. |
| `data` | Raw payload (dict or BaseModel, may be `None`). |
| `old_event` | Predecessor event in a chain (may be `None`). |
| `next` | Call to invoke next middleware (or core publish). **Must be called** for event to proceed. |

#### `on_publish(event, next)`

Runs **after** the Event is successfully enqueued. Full runtime event info available via `event.name`, `event.data`, `event.id`, `event.sources`, etc.

| Parameter | Description |
| - | - |
| `event` | Complete Event object (name, data, id, sources, timestamps). |
| `next` | Call to invoke next middleware. |

#### `on_publish_error(error, name, source, data)`

Called when the publish flow raises an exception (optional). All middlewares are notified in registration order. One middleware's error doesn't block others from being notified.

| Parameter | Description |
| - | - |
| `error` | The exception that occurred. |
| `name` | Event type name. |
| `source` | Publisher identifier. |
| `data` | Raw payload data. |

#### `before_dispatch(event, handler_ids, next)`

Runs **before** task creation, right after route matching. Call `await next(event, handler_ids)` to proceed; **the chain end creates one task per id, in list order** (pass a filtered / reordered list if needed). Not calling `next` **blocks** this dispatch (the event is dropped — compensate in your middleware, and log it yourself if you need an audit trail).

> **Warning**: This hook runs on the dispatch-loop coroutine — a critical path. Return quickly, or offload heavy work via `asyncio.create_task`, otherwise you throttle the entire dispatch rate.

| Parameter | Description |
| - | - |
| `event` | Dequeued, route-matched Event (tasks not yet created; zero-match events are also observable). |
| `handler_ids` | Matched handler ids; the list passed to `next` determines task creation order. |
| `next` | Invoke next middleware (or the chain end that creates tasks for the final id list). |

#### `on_task_start(record)`

Task-**start** hook (notification, optional) — fires **after the task acquires the concurrency semaphore**, right before `handle()`. On entry, `record.outcome` / `duration_seconds` are `None`; semaphore wait time is derivable as `started_at - dispatched_at`. Strictly paired with `on_task_done`; hook errors are isolated per middleware and never affect task execution.

#### `on_task_done(record)`

Task-**reclaim** hook (notification, optional) — fires when the task ends (success / error / timeout / cancellation / shutdown skip) with the **same record instance** as `on_task_start`, now filled in:

| Field | Description |
| - | - |
| `outcome` | `success` / `error` / `cancelled` / `skipped` (publish rejected during shutdown). |
| `duration_seconds` | Execution duration in seconds. |
| `error_type` / `error_message` | Error info (when `outcome=error`). |

Tasks cancelled while waiting for the semaphore (never started) do not fire — strictly paired with `on_task_start`. Hook errors are isolated the same way and never change the task outcome.

---

## MiddlewareChain

Manages an ordered list of middlewares with lazy chain building and hot-reload support.

```python
class MiddlewareChain:
    def __init__(self) -> None

    # CRUD (async — triggers lifecycle immediately when bus is running)
    async def add(self, middleware: Middleware) -> 'MiddlewareChain'
    async def insert(self, index: int, middleware: Middleware) -> 'MiddlewareChain'
    async def remove(self, middleware: Middleware) -> None
    async def clear(self) -> None

    @property
    def middlewares(self) -> list[Middleware]

    # Chain builders
    def build_before_publish(
        self, final_handler: BeforePublishNext
    ) -> BeforePublishNext
    def build_on_publish(
        self, final_handler: OnPublishNext
    ) -> OnPublishNext
    def build_on_publish_error(
        self, final_handler: OnPublishErrorNext
    ) -> OnPublishErrorNext

    # Lifecycle
    async def setup(self, bus: EventBus) -> None
    async def teardown(self, bus: EventBus, strict: bool = False) -> list[tuple[Middleware, Exception]]
```

| Method | Description |
| - | - |
| `add(mw)` | **async**. Append to the chain. Calls `on_setup` immediately if bus is running. If that `on_setup` fails, its `on_teardown` is fired first (side-effect rollback), it is **not** added, and a `RuntimeError` is raised. Returns self. |
| `insert(i, mw)` | **async**. Insert at position. Calls `on_setup` immediately if bus is running (same failure rollback as `add`). |
| `remove(mw)` | **async**. Remove from chain. A missing middleware is an idempotent no-op returning `None` (use `in chain.middlewares` to test membership). Calls `on_teardown` immediately if bus is running; its failure is returned but the removal still happens. |
| `clear()` | **async**. Remove all middlewares. Calls `on_teardown` on each if bus is running; returns `[(middleware, exception), ...]` for failures. |
| `middlewares` | Returns a copy of the current middleware list. |
| `build_before_publish(f)` | Build the `before_publish` chain with `f` as the innermost handler. |
| `build_on_publish(f)` | Build the `on_publish` chain with `f` as the innermost handler. |
| `build_on_publish_error(f)` | Build the `on_publish_error` chain with `f` as the innermost handler. |
| `build_dispatch(f)` | Build the `before_dispatch` chain; the innermost handler creates tasks for the final id list. |
| `build_task_start(f)` | Build the `on_task_start` notification chain (per-middleware error isolation, always continues). |
| `build_task_done(f)` | Build the `on_task_done` notification chain (per-middleware error isolation, always continues). |
| `setup(bus)` | Call `on_setup` on all middlewares in registration order. **Atomic (fail-fast)**: if any `on_setup` raises, the chain unbinds and fires `on_teardown` on every middleware already attempted (including the failing one), then re-raises — middlewares stay in the chain so you can fix and retry. Consistent with handler `activate`. |
| `teardown(bus)` | Call `on_teardown` on all middlewares in **reverse** order. **Idempotent**. Returns `[(middleware, exception), ...]` so callers (e.g. `EventBus.stop()`) can observe incomplete cleanup; pass `strict=True` to raise the first failure after attempting all. |

### Hot Reload

Middlewares can be added or removed at runtime via `bus.proxy().middleware`.
Changes take effect on the **next** publish — the chain cache is invalidated
automatically.

```python
# Inside a handler or middleware
chain = bus_proxy.middleware

# Hot-add — on_setup is called immediately
await chain.add(AuditMiddleware())

# Hot-remove — on_teardown is called immediately
# Note: in-flight chains may still invoke the middleware after this
await chain.remove(some_mw)

# Clear all
await chain.clear()
```

> **Warning**: `remove()` returns immediately without waiting for in-flight hooks.
> Middleware authors should make `on_teardown` idempotent — see
> `Middleware.on_teardown` docs.

### Building the Chain

On each publish, `build_before_publish()`, `build_on_publish()`, and
`build_on_publish_error()` lazily construct the chain. The same applies to
dispatch and task lifecycle: `build_dispatch()` (per dispatch),
`build_task_start()` / `build_task_done()` (per task). Results are cached
and auto-invalidated when the middleware list changes.

---

## Built-in Middlewares

See [templates/middlewares/](templates/middlewares/middlewares.md) for detailed docs.

| Middleware | Hook | Description |
| - | - | - |
| `JSONLLoggingMiddleware` | `on_publish` | Append events to JSONL file (zero deps). |
| `SQLiteLoggingMiddleware` | `on_publish` | Log events to SQLite database (requires `aiosqlite`). |
| `RateLimitMiddleware` | `before_publish` | Sliding-window rate limiting (global or per-event). |
| `EventTransformMiddleware` | `before_publish` | Transform event name, data fields (rename, redact, inject). |
| `EventBlockMiddleware` | `before_publish` | Block events matching predicates (allowlist/blocklist). |
| `EventForwardMiddleware` | `before_publish` | Forward events to another EventBus instance. |
| `RecursionGuardMiddleware` | `before_publish` | Prevent infinite event loops (configurable depth). |
| `MetricsMiddleware` | `before_publish` | Lightweight Prometheus/OTel-style metrics. |

---

## Usage Examples

### Logging Middleware

Log every published event with timing:

```python
import time
import logging
from infinity_bus import Middleware, MiddlewareChain

logger = logging.getLogger(__name__)

class LoggingMiddleware(Middleware):
    async def on_setup(self, bus):
        logger.info("LoggingMiddleware initialized")

    async def on_teardown(self, bus):
        logger.info("LoggingMiddleware shutting down")

    async def before_publish(self, event_registry, name, source, data, old_event, next):
        t0 = time.monotonic()
        logger.debug(f"[Publish] {name} from {source}")
        try:
            await next(event_registry, name, source, data, old_event)
        finally:
            elapsed = time.monotonic() - t0
            logger.debug(f"[Publish] {name} completed in {elapsed:.3f}s")

    async def on_publish(self, event, next):
        logger.debug(f"[Enqueued] {event.name} (id={event.id})")
        await next(event)

    async def on_publish_error(self, error, name, source, data):
        logger.error(f"[Error] {name} from {source}: {error}")

# Register with the bus
chain = MiddlewareChain()
await chain.add(LoggingMiddleware())

bus = EventBus(reg, h_reg, middleware_chain=chain)
```

### Validation Middleware

Perform extra data validation before publishing:

```python
class ValidationMiddleware(Middleware):
    async def on_setup(self, bus): pass
    async def on_teardown(self, bus): pass

    async def before_publish(self, event_registry, name, source, data, old_event, next):
        if name == "order.create" and isinstance(data, dict):
            if data.get("amount", 0) <= 0:
                raise ValueError("Order amount must be positive")
        await next(event_registry, name, source, data, old_event)

    async def on_publish(self, event, next):
        await next(event)
```

### Short-Circuit Middleware

Block events under certain conditions by **not** calling `next`:

```python
class RateLimitMiddleware(Middleware):
    def __init__(self, max_per_second: int = 100):
        self._max = max_per_second
        self._count = 0
        self._window_start = time.monotonic()

    async def on_setup(self, bus): pass
    async def on_teardown(self, bus): pass

    async def before_publish(self, event_registry, name, source, data, old_event, next):
        now = time.monotonic()
        if now - self._window_start > 1.0:
            self._window_start = now
            self._count = 0
        self._count += 1
        if self._count > self._max:
            logger.warning(f"Rate limit exceeded, dropping {name}")
            return  # Don't call next — event is dropped
        await next(event_registry, name, source, data, old_event)

    async def on_publish(self, event, next):
        await next(event)
```

### Multi-Middleware Onion Order

```python
chain = MiddlewareChain()
await chain.add(LoggingMiddleware())       # outermost
await chain.add(ValidationMiddleware())    # middle
await chain.add(RateLimitMiddleware())     # innermost

# Execution order (on_publish chain nested inside before_publish chain):
#   Logging.before enter → Validation.before enter → RateLimit.before enter
#     → Core publish logic (validate → construct → enqueue)
#     → Logging.on enter → Validation.on enter → RateLimit.on enter
#         → no-op (on_publish chain endpoint)
#       ← RateLimit.on exit ← Validation.on exit ← Logging.on exit
#   ← RateLimit.before exit ← Validation.before exit ← Logging.before exit
```

### Runtime Hot Reload

The middleware chain supports dynamic add/remove at runtime. Changes take effect immediately and can be triggered from within handlers or middleware via the bus proxy:

```python
class HotReloadHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=["admin.toggle"])

    async def handle(self, payload, bus_proxy, raw_event):
        chain = bus_proxy.middleware

        # Hot-add — on_setup is called immediately
        await chain.add(AuditMiddleware())

        # Hot-remove — on_teardown is called immediately
        # Note: in-flight chains may still invoke the middleware after this
        await chain.remove(some_mw)

        # Hot-clear all
        await chain.clear()
```

> **Note**: Add/remove operations return immediately without waiting for in-flight hooks. After `on_teardown` is called on a removed middleware, in-flight `before_publish` / `on_publish` may still execute. Middleware authors should make `on_teardown` idempotent.

---

## Custom Middleware Example

```python
from infinity_bus import (
    BeforePublishNext, Event, EventBus, EventRegistry,
    Middleware, OnPublishNext,
)

class TimingMiddleware(Middleware):
    """Logs publish duration per event type."""

    async def on_setup(self, bus: EventBus) -> None:
        self._bus = bus

    async def on_teardown(self, bus: EventBus) -> None:
        pass

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        import time
        t0 = time.perf_counter()
        await next(event_registry, name, source, data, old_event)
        elapsed = time.perf_counter() - t0
        logger.info("Event %s took %.4fs", name, elapsed)

    async def on_publish(
        self, event: Event, next: OnPublishNext
    ) -> None:
        await next(event)

# Usage
chain = MiddlewareChain()
await chain.add(TimingMiddleware())
bus = EventBus(events, handlers, middleware_chain=chain)
```
