# EventHandler / EventHandlerRegistry

## Overview

`EventHandler` is the abstract base class for event handlers. It matches events via
subscription patterns — exact `str` or `Regex` regular expressions.
`EventHandlerRegistry` manages handler instance CRUD.

---

## EventHandler

Abstract base class. All business handlers must subclass and implement `handle()`.

```python
class EventHandler(ABC):
    def __init__(
        self,
        subscriptions: Optional[List[Regex | str]] = None,
        handle_timeout: Optional[float] = 32.0
    ) -> None

    async def __call__(self, bus: EventBus, event: Event) -> None

    @abstractmethod
    async def handle(
        self,
        payload: Optional[BaseModel],
        bus_proxy: EventBus.Proxy,
        raw_event: Event
    ) -> None: ...

    @property
    def handler_id(self) -> Optional[str]: ...

    def on_registered(self, handler_id: str) -> None: ...
    def on_unregistered(self) -> None: ...
    def on_activate(self, bus: EventBus) -> None: ...
    def on_deactivate(self, bus: EventBus) -> None: ...
```

| Parameter/Method | Description |
| - | - |
| `subscriptions` | Event patterns this handler listens to. `str` for exact match (`event_type == subscription`), `Regex` for full regex match. E.g., `["user.login"]` only matches `"user.login"`, `[Regex(r"user\..*")]` matches all `user.*` events. |
| `handle_timeout` | Per-invocation timeout (seconds). `None` = no limit. Default `32.0`. |
| `__call__` | Internal entry point. Receives the raw `EventBus`, creates a proxy via `bus.proxy(handler_name, event)`, then delegates to `handle()`. |
| `handle(payload, bus_proxy, raw_event)` | **Subclass must implement.** `payload` is the unpacked payload (may be `None`). `bus_proxy` provides limited bus access. `raw_event` is the full event object. |
| `handler_id` | Read-only. The registry-assigned id once registered; `None` before registration / after removal. |
| `on_registered(handler_id)` | **Lifecycle hook.** Called by the registry after `register()` succeeds, backfilling the handler with its assigned id. Subclasses may override — call `super()` to keep backfill. |
| `on_unregistered()` | **Lifecycle hook.** Called by the registry after `unregister()` / `clear()` removes the handler, clearing its id. Subclasses may override — call `super()` to keep clearing. |
| `on_activate(bus)` | **Bus-activation hook.** Called when the owning registry is bound to a bus (`registry.activate(bus)`) or when a new handler is registered into an already-bound registry. `handler_id` is already available. **Base default: registers this handler's `Subscriptions` bundle into `bus.router` (activation is routing)** — even an empty bundle is registered, so *active ⟺ routed* always holds and a runtime `add()` takes effect immediately. Overriders must call `super().on_activate(bus)` to keep that behavior. |
| `on_deactivate(bus)` | **Bus-deactivation hook.** Symmetric to `on_activate`: called on `registry.deactivate()` or when a single handler is unregistered from a bound registry. `handler_id` is still available. **Base default: unregisters the bundle from `bus.router`.** Overriders must call `super().on_deactivate(bus)` to keep that behavior. |

### Lifecycle Hooks (self-aware id)

The moment a handler is registered, the registry delivers its assigned `handler_id` to it through a hook;
removal (`unregister` / `clear`) symmetrically clears it. The handler thus knows its identity inside the
registry (e.g., building subscription records, self-identification, error reporting).

```python
class MyHandler(EventHandler):
    def on_registered(self, handler_id: str) -> None:
        super().on_registered(handler_id)   # base class backfills self.handler_id
        print(f'online, my id is {handler_id}')

    def on_unregistered(self) -> None:
        print(f'offline, my id was {self.handler_id}')
        super().on_unregistered()
```

### `handle()` Signature

```python
async def handle(
    self,
    payload: Optional[BaseModel],      # Typed payload (None if event has no payload)
    bus_proxy: EventBus.Proxy,          # Proxy for publishing follow-up events
    raw_event: Event                    # Full event metadata (id, sources, timestamps)
) -> None:
```

### Usage

```python
from infinity_bus import EventHandler, Regex

class LoginHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=[Regex(r"user\..*")])  # match all user.* events

    async def handle(self, payload, bus_proxy, raw_event):
        if isinstance(payload, UserLoginPayload):
            print(f"User {payload.user_id} logged in at {payload.timestamp}")
            # Chain-publish via bus_proxy.publish
```

### Subscription Patterns

`subscriptions` accepts two matching modes (see table below). At runtime the handler holds them
as a managed `Subscriptions` bundle (`self.subscriptions`): mutate it with `add` / `remove` /
`replace` — while the bus is running (active) the change auto-syncs to the live `Router`. See
[Router](router.md).

| Type | Example | Matches |
| - | - | - |
| Exact `str` | `"order.created"` | Exact match only |
| `Regex` | `Regex(r"order\..*")` | `order.created`, `order.paid`, etc. |
| `Regex` | `Regex(r"(?!system\.).*")` | All events except `system.*` |

```python
from infinity_bus import EventHandler, Regex

class AuditHandler(EventHandler):
    def __init__(self):
        # Exact match for single event
        # Regex match for all order.* and payment.* events
        super().__init__(subscriptions=["user.login", Regex(r"order\..*"), Regex(r"payment\..*")])

    async def handle(self, payload, bus_proxy, raw_event):
        print(f"Audit: {raw_event.name}")
```

---

## EventHandlerRegistry

Manages handler instance registration, lookup, and removal. **Active routing is owned by [Router](router.md)**.

```python
class EventHandlerRegistry:
    def __init__(self) -> None
    def register(self, handler: EventHandler) -> str
    def unregister(self, handler_id: str) -> Exception | None
    def activate(self, bus: EventBus) -> None
    def deactivate(self, strict: bool = False) -> list[tuple[str, Exception]]
    def get(self, handler_id: str) -> Optional[EventHandler]
    def clear(self) -> list[tuple[str, Exception]]

    def __len__(self) -> int
    def __contains__(self, handler_id: str) -> bool
    def __iter__(self) -> Iterator[tuple[str, EventHandler]]

    @property
    def version(self) -> int
    @property
    def handlers_count(self) -> int
    @property
    def all_handlers(self) -> Dict[str, EventHandler]
```

| Method/Property | Description |
| - | - |
| `register(handler)` | Register a handler instance, returns a unique handler ID (UUID hex). **Atomic, including bus-side side-effect rollback**: fires `handler.on_registered(id)` after success (plus `on_activate(bus)` if the registry is already bound to a bus); re-registering the same instance raises `ValueError`. If `on_registered` fails the registry rolls back with `on_unregistered()` (clears the id); if `on_activate` fails it first fires `on_deactivate(bus)` to revoke any subscriptions/tasks it already created (no ghost routes), then `on_unregistered()`. Version unchanged on failure and the error re-raises; version increments on success. |
| `unregister(handler_id)` | Remove by ID. **Guaranteed removal with explicit failure reporting**: the handler is always removed; a missing id is an idempotent no-op returning `None` (use `handler_id in registry` to test existence). If bound to a bus it first fires `handler.on_deactivate(bus)` (id still available), then `handler.on_unregistered()`. Hook failures do not block removal, but the **first failure is returned** (others are logged). Version increments. |
| `activate(bus)` | Bind the registry to one bus and activate every registered handler (fires `on_activate` on each). **Atomic, including bus-side side-effect rollback**: a registry **cannot be shared across buses** — calling again while bound raises `RuntimeError`; the bus is bound *before* activation so handlers nested-`register`ed during `on_activate` get `on_activate` immediately; if any `on_activate` raises the whole activation rolls back (unbind, fire `on_deactivate` on **every handler already attempted, including the failing one** — revoking its partial subscriptions/tasks) and re-raises — handlers stay registered. |
| `deactivate()` | Unbind the bus and fire `on_deactivate` on every registered handler (id still available). **Keeps failing handlers**: those whose `on_deactivate` raises stay registered (logged) rather than being silently removed. Returns `[(handler_id, exception), ...]` so callers (e.g. `EventBus.stop()`) can observe incomplete cleanup; pass `strict=True` to raise the first failure after attempting all. Returns `[]` when not bound (idempotent). |
| `get(handler_id)` | Lookup by ID, returns `None` if not found. |
| `clear()` | Remove all handlers. **Guaranteed removal with explicit failure reporting**: first `deactivate()` if bound, then fires `on_unregistered()` on each remaining handler; a single hook failure does not interrupt the rest. Returns `[(handler_id, exception), ...]` covering both the `deactivate` and `on_unregistered` phases. Version increments. |
| `__len__()` | Supports `len(registry)`. |
| `__contains__()` | Supports `handler_id in registry`. |
| `__iter__()` | Supports `for hid, h in registry` iteration. |

> **Registry-bus binding**: `EventBus.start()` calls `registry.activate(bus)` and `stop()` calls `registry.deactivate()` automatically.
> So "registered (has an id)" and "active on a bus (can receive events)" are two distinct lifecycle layers,
> and one registry cannot be bound to two buses at the same time.
| `version` | Monotonic version, incremented on every change (add/remove/clear). Used by [Router](router.md) for invalidation. |
| `handlers_count` | Total number of registered handlers. |
| `all_handlers` | Snapshot copy of all registered handlers as `Dict[str, EventHandler]`. |

### Usage

```python
from infinity_bus import EventHandlerRegistry, EventHandler

handler_registry = EventHandlerRegistry()
handler_id = handler_registry.register(my_handler)
assert handler_id in handler_registry
assert handler_registry.version == 1  # increments on every register

handler_registry.unregister(handler_id)
assert handler_registry.version == 2  # unregister also increments
```

> **Note**: `get_handlers(event_type)` has been removed. Matching logic moved to [Router](router.md); the bus uses it internally.
