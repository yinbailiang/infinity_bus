# Built-in Middlewares Overview

EventBus provides 8 out-of-the-box middlewares covering common cross-cutting concerns:
logging, metrics, rate limiting, transform, blocking, recursion guard, and cross-bus forwarding.
All middlewares are registered via `MiddlewareChain`, forming an onion pipeline in
registration order.

---

## Quick Index

| Middleware | Phase | Purpose | Docs |
| - | - | - | - |
| `RateLimitMiddleware` | `before_publish` | Sliding-window rate limiting | [rate_limit.md](rate_limit.md) |
| `RecursionGuardMiddleware` | `before_publish` | Dual-layer recursion detection | [recursion_guard.md](recursion_guard.md) |
| `EventBlockMiddleware` | `before_publish` | Rule-based event blocking (discard) | [event_block.md](event_block.md) |
| `EventTransformMiddleware` | `before_publish` | Event name transform / field inject / redact | [event_transform.md](event_transform.md) |
| `MetricsMiddleware` | `before_publish` + `on_publish` + `on_task_done` | Prometheus / OTel style metrics collection | [metrics.md](metrics.md) |
| `EventForwardMiddleware` | `on_publish` | Unidirectional cross-bus event forwarding | [event_forward.md](event_forward.md) |
| `JSONLLoggingMiddleware` | `on_publish` | JSONL file persistent logging | [logging.md](logging.md) |
| `SQLiteLoggingMiddleware` | `on_publish` | SQLite database persistent logging | [logging.md](logging.md) |

---

## Execution Order

```text
Publish Request
  │
  ▼
┌─ before_publish chain ─────────────────────────────────────┐
│  RateLimitMiddleware       ← Limit first, discard if exceeded│
│  RecursionGuardMiddleware  ← Detect recursion, prevent loops │
│  EventBlockMiddleware      ← Block by rules                  │
│  EventTransformMiddleware  ← Rename / inject / redact         │
│  MetricsMiddleware         ← Metrics collection (timer start)  │
│  Core publish logic        ← Validate → Build Event → Enqueue│
└──────────────────────────────────────────────────────────────┘
  │
  ▼
┌─ on_publish chain ─────────────────────────────────────────┐
│  EventForwardMiddleware    ← Forward to other buses          │
│  MetricsMiddleware         ← Record full publish latency (timer end) │
│  JSONLLoggingMiddleware    ← Write to JSONL file              │
│  SQLiteLoggingMiddleware   ← Write to SQLite database         │
└──────────────────────────────────────────────────────────────┘
```

> **Recommended registration order**: Rate Limit → Recursion Guard → Block → Transform →
> Metrics → Forward → Logging.
> The metrics middleware spans `before_publish` (timer start), `on_publish` (timer end), and
> `on_task_done` (task outcome / execution latency / queue latency);
> placing it after Transform ensures transformed event names are correctly recorded in metrics.
> Placing forwarding before logging in the `on_publish` phase ensures log middlewares only
> record events from the local bus.

---

## Full Exports

```python
from infinity_bus.templates.middlewares import (
    # Logging
    JSONLLoggingMiddleware,
    SQLiteLoggingMiddleware,
    LogFallback,
    # Metrics
    MetricsMiddleware,
    MetricsSnapshot,
    # Rate Limiting
    RateLimitMiddleware,
    # Transform
    EventTransformMiddleware,
    TransformFunc,
    make_rename_transform,
    make_field_inject_transform,
    make_field_redact_transform,
    # Blocking
    EventBlockMiddleware,
    BlockPredicate,
    make_blocklist_predicate,
    make_allowlist_predicate,
    # Recursion Guard
    RecursionGuardMiddleware,
    RecursionDetectedError,
    # Forwarding
    EventForwardMiddleware,
    EventFilter,
    TargetBusProvider,
    make_event_name_filter,
    make_static_target_provider,
    # Utilities
    serialize_data,
)
```

---

## Custom Middleware

All middlewares inherit from the `Middleware` abstract base class. Custom middlewares
only need to implement the required hooks:

```python
from infinity_bus import Middleware

class CustomMiddleware(Middleware):
    async def on_setup(self, bus): ...
    async def on_teardown(self, bus): ...
    async def before_publish(self, event_registry, name, source, data, old_event, next): ...
    async def on_publish(self, event, next): ...
    async def on_publish_error(self, error, name, source, data): ...
```

See the [Middleware docs](../../middleware.md) for details.
