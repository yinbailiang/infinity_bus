# Simple Handler Decorator

## Overview

`handler` is a **function decorator** that converts a plain async (or sync) function into an
`EventHandler` subclass. It automatically handles event subscription configuration, parameter
signature validation, and payload unpacking — letting you focus on business logic without
writing `EventHandler` boilerplate.

Compared to manually subclassing `EventHandler` and implementing `handle()`, the `handler`
decorator is more concise, type-safe, and suitable for most simple event handling scenarios.

---

## Use Cases

- Quickly define event handlers without writing full `EventHandler` subclasses.
- Leverage function signatures to validate payload types at **definition time** (not runtime).
- Combine with `ModuleHandlerRegister` for module-level batch handler registration.
- Support both sync and async handler functions — the decorator adapts automatically.

---

## Signature

```python
def handler(
    event_decl: Type[EventDeclaration],
    *,
    handle_timeout: Optional[float] = 32.0,
) -> Callable[[HandlerFunc], Type[GenericEventHandler]]
```

| Parameter | Type | Description |
| - | - | - |
| `event_decl` | `Type[EventDeclaration]` | Event declaration class with `name` and optional `payload_type` defined. |
| `handle_timeout` | `Optional[float]` | Handler timeout in seconds. `None` means no timeout. Default `32.0`. |

**Returns**: A decorator that accepts a plain function and returns an `EventHandler` subclass.

---

## How It Works

1. **Declare the event**: Define an `EventDeclaration` with an event name and optional payload type.
2. **Write the function**: Write a plain function whose signature matches the event declaration:
   - If the event **has** a payload (`payload_type` is not `None`), the function's first parameter should be of that payload type.
   - If the event **has no** payload (`payload_type` is `None`), the function should have no parameters.
3. **Apply the decorator**: Decorate the function with `@handler(YourEvent)` to auto-generate an `EventHandler` subclass.
4. **Instantiate and register**: Call the generated class to create an instance, then register it with `EventHandlerRegistry`.
5. **At runtime**: When the event fires, the bus unpacks the payload and calls your original function.

The decorator performs signature validation at **definition time**:

- Payload event + parameterless function → immediate `TypeError`
- No-payload event + function with parameters → immediate `TypeError`
- Payload event + mismatched parameter type → immediate `TypeError` (only when parameter has a type annotation)

---

## Usage Examples

### Basic: No-Payload Event

```python
from infinity_bus import EventDeclaration
from infinity_bus.templates import handler

class SystemReady(EventDeclaration):
    name = "system.ready"

@handler(SystemReady)
async def on_system_ready() -> None:
    print("System is ready!")

# Register with the bus
await handler_registry.register(on_system_ready())
```

### Basic: Payload Event

```python
from pydantic import BaseModel, Field
from infinity_bus import EventDeclaration
from infinity_bus.templates import handler

class UserCreatedPayload(BaseModel):
    user_id: str = Field(description="User ID")
    email: str = Field(description="User email")

class UserCreated(EventDeclaration):
    name = "user.created"
    payload_type = UserCreatedPayload

@handler(UserCreated)
async def send_welcome_email(payload: UserCreatedPayload) -> None:
    print(f"Sending welcome email to {payload.email}")

await handler_registry.register(send_welcome_email())
```

### Sync Handlers

```python
@handler(UserCreated)
def log_user_creation(payload: UserCreatedPayload) -> None:
    # Sync functions work too — the decorator adapts automatically
    print(f"[LOG] User created: {payload.user_id}")

await handler_registry.register(log_user_creation())
```

### Custom Timeout

```python
@handler(UserCreated, handle_timeout=5.0)
async def quick_validation(payload: UserCreatedPayload) -> None:
    # 5 second timeout
    await validate_user(payload)

@handler(UserCreated, handle_timeout=None)
async def long_running_task(payload: UserCreatedPayload) -> None:
    # No timeout limit
    await heavy_computation(payload)
```

### Signature Validation: Errors Caught at Definition Time

```python
class OrderCreated(EventDeclaration):
    name = "order.created"
    payload_type = OrderPayload

# ❌ Caught immediately at definition time (TypeError raised)
@handler(OrderCreated)
def bad_handler() -> None:  # Missing payload parameter
    pass
# TypeError: Event order.created requires a payload parameter,
# but handler bad_handler() has no parameters defined.

# ❌ Type mismatch
@handler(OrderCreated)
def another_bad(payload: OtherPayload) -> None:
    pass
# TypeError: Handler another_bad parameter type should be OrderPayload,
# not 'OtherPayload'.
```

### With ModuleHandlerRegister

```python
from infinity_bus.templates import ModuleHandlerRegister, handler

module_handlers = ModuleHandlerRegister("user_module")

@handler(UserCreated)
async def handle_user_created(payload: UserCreatedPayload) -> None:
    ...

# Manually add to the module registrar
module_handlers.add_handler(
    handle_user_created,  # This is already an EventHandler subclass
    depends=lambda: {},
)
```

---

## Generated Class Members

The generated `EventHandler` subclass:

| Member | Description |
| - | - |
| `__init__()` | Constructor; timeout is determined by the decorator parameter `handle_timeout`. |
| `handle(payload, bus_proxy, raw_event)` | Event handling entry point; unpacks payload and calls the original function. |
| `subscriptions` | Inherited from `EventHandler`; contains `event_decl.name`. |
| `handle_timeout` | Inherited from `EventHandler`; matches the decorator parameter. |

---

## Notes

- The generated class is **dynamically created** — each `@handler(...)` call produces a new `EventHandler` subclass.
- If the function parameter has no type annotation, the decorator **skips** type validation (runtime validation by the bus still applies).
- The original function's `__name__`, `__qualname__`, `__module__`, and `__doc__` are copied to the generated class.
- The decorator does not support `*args` or `**kwargs` — only zero or one parameter is accepted.
