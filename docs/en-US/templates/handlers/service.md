# Service Handler

## Overview

`ServiceHandler` is a one-stop base class for request-response **services**: subclass it and decorate plain methods (sync or async) with `@process(...)` to get:

- **Automatic subscriptions** — subscribes to every registered request event;
- **Table-driven dispatch** — `handle()` looks up the matching method by event name;
- **Automatic responses** — returning a `dict` constructs the response via `build_response` (with `session_id` / `request_id` echoed back);
- **Automatic error translation** — raising `ServiceError` becomes a `success=False` response.

It is the **server-side counterpart** of the `request` template: `request()` initiates the RPC call, `ServiceHandler` answers it.

---

## Use Cases

- Exposing domain services in RPC style (together with the `request` template).
- Unified success / failure response protocol and error semantics.
- Organizing multiple request handlers in one class without hand-written dispatch.

---

## Class Signature

```python
class ServiceHandler(EventHandler):
    @property
    def processors(self) -> MappingProxyType[str, ProcessorMeta]: ...

    async def handle(self, payload: BaseModel | None, bus_proxy: EventBus.Proxy, raw_event: Event) -> None: ...
```

| Member | Type | Description |
| - | - | - |
| `processors` | `MappingProxyType[str, ProcessorMeta]` | Registered processor table (keyed by request event name, read-only view). |
| `handle()` | `async` | Dispatch entry: lookup, invoke, construct and publish the response. Empty payloads / unregistered / mismatched events are ignored. |
| `ProcessorMeta` | model | One registration entry: `func_name` plus the `request` / `response` event declarations. |

### @process Decorator

```python
@process(request: type[EventDeclaration], response: type[EventDeclaration] | None = None)
```

| Parameter | Description |
| - | - |
| `request` | Request event declaration; its `payload_type` must extend `RequestProtocol`. |
| `response` | Response event declaration (optional); its `payload_type` must extend `ResponseProtocol`. Omit it for a **single-direction event**. |

Return value conventions for the decorated method:

| Return value | Behavior |
| - | - |
| `dict[str, Any]` | Constructs and publishes the success response: `build_response(payload, response_cls, **ret)`. |
| `None` | **No response** (side effect only). |
| Raises `ServiceError` | Publishes a `success=False` response (`error_msg=str(e)`, `e.body` merged into response fields). |
| Raises anything else | Propagates upward; handled by bus error isolation (`TaskErrorEvent`). |

> `@process` validates the event protocols at **class-definition time**; misconfiguration raises `TypeError` immediately.

### ServiceError Hierarchy

| Exception | Semantics |
| - | - |
| `ServiceError` | Base class for business failures: `str(e)` → `error_msg`; `e.body` → merged into the remaining response fields. |
| `NotFoundError` | Target resource does not exist. |
| `ConflictError` | State conflict (duplicate registration, concurrent modification, etc.). |
| `InvalidRequestError` | Invalid request parameters. |
| `AccessDeniedError` | Insufficient permissions. |

---

## How It Works

```text
Class definition time
  @process(ReqEvent, RespEvent) marks the method
        │
        ▼
  __init_subclass__ collects the processor table → builds the subscription list
        │
Handler registered on the bus → routes registered per subscriptions
        │
Request event arrives
        ▼
handle() → lookup by event name → validate payload type
        │
        ▼
Invoke the processor (sync / async)
        ├─ returns dict    → build_response(success) → publish response event
        ├─ returns None    → no response
        ├─ ServiceError    → build_response(success=False, …) → publish
        └─ other exception → propagate (bus error isolation)
```

1. **Collection**: when a subclass is created, the MRO is scanned for `@process`-marked methods; re-registering the same request event makes the **subclass entry win**.
2. **Subscription**: the keys of the `processors` table are the subscription list.
3. **Dispatch**: `handle()` looks up the table by event name and validates the payload type.
4. **Response**: `build_response` echoes `session_id` / `request_id` back, matching the `request()` filter directly.

---

## Examples

### Basic Service + RPC Call

```python
from pydantic import Field
from infinity_bus import EventBus, EventDeclaration, EventHandlerRegistry, EventRegistry
from infinity_bus.templates import ServiceHandler, process
from infinity_bus.templates.request import RequestProtocol, ResponseProtocol, request

# 1. Request / response payloads and events
class GetUserReq(RequestProtocol):
    user_id: int

class GetUserResp(ResponseProtocol):
    user_name: str = Field(default='')
    email: str = Field(default='')

class GetUserReqEvent(EventDeclaration):
    name = 'user.get.request'
    payload_type = GetUserReq

class GetUserRespEvent(EventDeclaration):
    name = 'user.get.response'
    payload_type = GetUserResp

# 2. The service
class UserService(ServiceHandler):
    @process(GetUserReqEvent, GetUserRespEvent)
    async def get_user(self, payload: GetUserReq, proxy, raw):
        return {'user_name': 'alice', 'email': 'alice@example.com'}

# 3. Wiring and invocation
event_registry = EventRegistry()
event_registry.register(GetUserReqEvent)
event_registry.register(GetUserRespEvent)
handler_registry = EventHandlerRegistry()
handler_registry.register(UserService())

async with EventBus(event_registry, handler_registry) as bus:
    resp = await request(bus.proxy('cli'), 'user.get.request', {'user_id': 1}, 'user.get.response')
    print(resp.user_name)  # alice
```

### Business Error → Failed Response

```python
class UserService(ServiceHandler):
    @process(GetUserReqEvent, GetUserRespEvent)
    async def get_user(self, payload: GetUserReq, proxy, raw):
        if payload.user_id not in self._db:
            raise NotFoundError('user not found', user_id=payload.user_id)
        return {'user_name': 'alice', 'email': 'alice@example.com'}
```

The caller receives `success=False` with `error_msg='user not found'`; fields in `e.body` are merged into the response (subject to the response model's declared fields).

### Single-Direction Event (No Response)

```python
class AuditService(ServiceHandler):
    def __init__(self):
        super().__init__()
        self.entries: list[str] = []

    @process(AuditEvent)  # no response → never replies
    def audit(self, payload: AuditReq, proxy, raw):
        self.entries.append(payload.action)
```

### Subclass Override

```python
class BaseUserService(ServiceHandler):
    @process(GetUserReqEvent, GetUserRespEvent)
    def get_user(self, payload, proxy, raw):
        return {'user_name': 'guest'}

class CachedUserService(BaseUserService):
    @process(GetUserReqEvent, GetUserRespEvent)
    def get_user_cached(self, payload, proxy, raw):
        return {'user_name': 'alice'}  # subclass entry replaces the parent's
```

---

## Important Notes

- **Method signature**: processors receive `(payload, bus_proxy, raw_event)`; both plain and `async` methods are supported (awaitables are awaited automatically).
- **Ignore rules**: empty payloads, unregistered event names, and payload type mismatches are silently ignored (debug log only).
- **Errors on single-direction events**: with `response` omitted there is no failed response to translate into, so `ServiceError` propagates upward (eventually published as `TaskErrorEvent` by bus error isolation).
- **Protocol requirements**: request payloads must extend `RequestProtocol` (carrying `session_id` / `request_id`) and response payloads must extend `ResponseProtocol` — this is what makes the pairing with the `request` template work.
- **Response event reuse**: multiple request events may share the same response event declaration.
- **Override semantics**: when a subclass re-registers the same request event (the method name may differ), the subclass entry replaces the parent's — no explicit removal needed.
