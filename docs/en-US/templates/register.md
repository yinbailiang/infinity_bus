# Register — Module Event & Handler Registration

## Overview

The `register` module provides two registrar classes — `ModuleEventRegister` and
`ModuleHandlerRegister` — for **collecting** event declarations and handler definitions
at the module level, then **bulk-registering** them into the global `EventRegistry` and
`EventHandlerRegistry` at application startup.

Unlike directly calling `event_registry.register()` or awaiting `handler_registry.register()`,
module registrars separate "definition" from "registration": they only collect during
module import (via decorators or manual addition), and perform the actual registration
in one batch at startup. This allows large projects to organize events and handlers by
module, avoid circular imports, and control registration timing.

---

## Core Classes

### `ModuleEventRegister`

Collects `EventDeclaration` subclasses and registers them in batch.

```python
class ModuleEventRegister:
    def __init__(self, name: str) -> None
    def add_event(self, event_decl: Type[EventDeclaration]) -> None
    def event(self, event_cls: EventDeclT) -> EventDeclT
    def register_all_events(self, event_registry: EventRegistry) -> None
    def get_all_event_names(self) -> List[str]
```

| Member | Description |
| - | - |
| `__init__(name)` | Constructor. `name` is the module name for identification and debugging. |
| `add_event(event_decl)` | Manually add an event declaration class. Duplicates are auto-deduplicated. |
| `event` | **Decorator**. Automatically adds the event declaration class to the registrar and returns it unchanged (preserving type). |
| `register_all_events(event_registry)` | Registers all collected event declarations into the given `EventRegistry` in one batch. |
| `get_all_event_names()` | Returns a list of all collected event names. |

---

### `ModuleHandlerRegister`

Collects `EventHandler` subclasses with their dependency factories, instantiates them,
and registers in batch.

```python
class ModuleHandlerRegister:
    def __init__(self, name: str) -> None
    def add_handler(
        self,
        handler_type: Type[EventHandler],
        depends: Callable[[], Dict[str, Any]]
    ) -> None
    def handler(
        self,
        depends: Callable[[], Dict[str, Any]] = lambda: {}
    ) -> Callable[[HandlerT], HandlerT]
    async def register_all_handlers(
        self, handler_registry: EventHandlerRegistry, *, atomic: bool = False
    ) -> None
```

| Member | Description |
| - | - |
| `__init__(name)` | Constructor. `name` is the module name for identification and debugging. |
| `add_handler(handler_type, depends)` | Manually add a handler class and its dependency factory. Duplicates are auto-deduplicated. |
| `handler(depends)` | **Decorator factory**. Returns a class decorator that adds the handler class to the registrar. `depends` is a callable returning a dependency dict; keys correspond to `__init__` parameter names. Defaults to `lambda: {}`. |
| `register_all_handlers(handler_registry, *, atomic=False)` | **async**. **Instantiates** all collected handlers (injecting dependencies via `depends` factories) and registers them in batch. `atomic=True` enables transactional registration: any failure rolls back already-registered handlers and raises; default `False` logs and continues on failure. |

---

## Workflow

```text
Module Import Phase                        App Startup Phase
     |                                         |
     |  module_events = ModuleEventRegister()   |
     |  module_handlers = ModuleHandlerRegister()|
     |                                         |
     |  @module_events.event                   |
     |  class MyEvent(EventDeclaration): ...   |
     |                                         |
     |  @module_handlers.handler(depends=...)  |
     |  class MyHandler(EventHandler): ...     |
     |                                         |
     |  # —— Collection only, no registration —— |
     |                                         |
     |                                         |  module_events.register_all_events(event_registry)
     |                                         |  await module_handlers.register_all_handlers(handler_registry)
     |                                         |
     |                                         |  # —— Now registered globally ——
```

---

## Usage Examples

### Basic: Event Registration

```python
from infinity_bus import EventDeclaration, EventRegistry
from infinity_bus.templates.register import ModuleEventRegister

# Create module-level registrar
module_events = ModuleEventRegister("user_module")

# Method 1: Decorator collection
@module_events.event
class UserCreatedEvent(EventDeclaration):
    name = "user.created"
    payload_type = UserCreatedPayload

@module_events.event
class UserDeletedEvent(EventDeclaration):
    name = "user.deleted"
    payload_type = None

# Method 2: Manual addition
module_events.add_event(UserLoginEvent)

# Batch register at startup
event_registry = EventRegistry()
module_events.register_all_events(event_registry)

print(module_events.get_all_event_names())
# ['user.created', 'user.deleted', 'user.login']
```

### Basic: Handler Registration

```python
from infinity_bus import EventHandler, EventHandlerRegistry
from infinity_bus.templates.register import ModuleHandlerRegister

module_handlers = ModuleHandlerRegister("user_module")

# Collect handlers via decorator with dependency declaration
@module_handlers.handler(depends=lambda: {"db": get_db_connection()})
class UserCreatedHandler(EventHandler):
    def __init__(self, db):
        super().__init__(subscriptions=["user.created"])
        self.db = db

    async def handle(self, payload, bus_proxy, raw_event):
        await self.db.insert_user(payload)

# Handler without extra dependencies (uses default depends)
@module_handlers.handler()
class AuditLogHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=[Regex(r"user\..*")])

    async def handle(self, payload, bus_proxy, raw_event):
        print(f"Audit: {raw_event.name}")

# Instantiate and register at startup
handler_registry = EventHandlerRegistry()
await module_handlers.register_all_handlers(handler_registry)
```

### Full Module Organization Example

```python
# user_module/events.py
from infinity_bus import EventDeclaration
from infinity_bus.templates.register import ModuleEventRegister

user_events = ModuleEventRegister("user")

@user_events.event
class UserCreated(EventDeclaration):
    name = "user.created"
    payload_type = UserPayload

@user_events.event
class UserUpdated(EventDeclaration):
    name = "user.updated"
    payload_type = UserPayload


# user_module/handlers.py
from infinity_bus import EventHandler
from infinity_bus.templates.register import ModuleHandlerRegister

user_handlers = ModuleHandlerRegister("user")

@user_handlers.handler(depends=lambda: {"user_service": get_user_service()})
class UserEventHandler(EventHandler):
    def __init__(self, user_service):
        super().__init__(subscriptions=["user.created", "user.updated"])
        self.user_service = user_service

    async def handle(self, payload, bus_proxy, raw_event):
        if raw_event.name == "user.created":
            await self.user_service.on_user_created(payload)
        else:
            await self.user_service.on_user_updated(payload)


# app.py (at startup)
from user_module.events import user_events
from user_module.handlers import user_handlers
from infinity_bus import EventRegistry, EventHandlerRegistry, EventBus

event_registry = EventRegistry()
handler_registry = EventHandlerRegistry()

# Batch register by module
user_events.register_all_events(event_registry)
await user_handlers.register_all_handlers(handler_registry)

bus = EventBus(event_registry, handler_registry)
```

---

## Design Intent

| Feature | Description |
| - | - |
| **Declaration–Registration Separation** | Collect declarations during module import, register in batch at startup — no import-time side effects. |
| **Deduplication** | Adding the same event declaration or the same `(handler class, dependency factory)` pair is silently ignored. |
| **Lazy Instantiation** | Handlers are only instantiated via their dependency factories at `register_all_handlers` call time, ensuring dependencies are available. |
| **Transactional Registration** | `register_all_handlers` supports `atomic=True` mode: if any handler fails, already-registered handlers are rolled back, guaranteeing all-or-nothing semantics. |
| **Decorator-Friendly** | `@register.event` and `@register.handler(depends=...)` decorator styles keep code clean. |
| **Module Isolation** | Each module has its own independent registrar instance; events/handlers can be registered partially or fully as needed. |

---

## Notes

1. **Dependency factory call timing**: The `depends` factory is called during
   `register_all_handlers()`, not at module import time. Ensure that resources required
   by the factory (e.g. database connections) are already initialized at registration time.
2. **Handler parameter names must match dependency dict keys**: The keys returned by
   `depends` must exactly match the `__init__` parameter names of the handler; otherwise
   a `TypeError` will be raised at instantiation.
3. **Registration order**: It is recommended to register all module events first, then
   handlers. While the registries themselves do not enforce ordering, handlers may depend
   on event declarations being ready.
4. **Modules without both event and handler**: If a module only has events or only has
   handlers, use only the corresponding registrar — there is no need to create the other.
5. **Deduplication basis**: `ModuleEventRegister` deduplicates by class object itself;
   `ModuleHandlerRegister` deduplicates by `(handler_type, depends)` tuple. Note that
   different `depends` factories (even if behaviorally identical) are treated as
   different entries.
6. **Transactional registration (`atomic=True`)**: When any handler registration fails,
   all already-registered handlers are automatically removed (rolled back) and the
   original exception propagates. Suitable for scenarios requiring all-or-nothing
   semantics. The default `atomic=False` is relaxed: failures are only logged and
   remaining handlers continue to register.

---

## Complete Example

See `tests/templates/register_test.py` for complete test cases covering event
registration, handler registration, deduplication, decorator behavior, and batch
registration scenarios.
