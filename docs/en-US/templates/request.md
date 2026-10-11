# Request Template

## Overview

`request` is a **sync-style async RPC utility** built on top of the EventBus. It
encapsulates the common pattern of "publish a request event → wait for a matching
response event", allowing business code to perform remote calls with the intuitive
`await request(...)` syntax — no need to manually manage temporary handlers, session
matching, or timeout control.

---

## Core Protocols

Before using `request`, request and response payload models must follow the protocol
base classes.

| Base Class | Purpose |
| - | - |
| `RequestProtocol` | Base class for all request payloads. Mandates `session_id` and `request_id` fields, auto-injected by the framework. |
| `ResponseProtocol` | Base class for all response payloads. Mandates `session_id`, `request_id`, `success`, and `error_msg` fields. Provides `raise_if_failed()` for quick failure checks. |

### Protocol Fields

#### RequestProtocol

- `session_id: str` — Session identifier for correlating multiple requests within a session.
- `request_id: str` — Unique request identifier for precise request-response matching.

#### ResponseProtocol

- `session_id: str` — Must match the corresponding request's `session_id`.
- `request_id: str` — Must match the corresponding request's `request_id`.
- `success: bool` — Whether the business operation succeeded. Defaults to `True`.
- `error_msg: Optional[str]` — Error message on failure.

---

## Workflow

### 1. Define Request and Response Events

Create Pydantic models inheriting from `RequestProtocol` and `ResponseProtocol`, then
declare corresponding events.

```python
from pydantic import BaseModel, Field
from infinity_bus import EventDeclaration
from infinity_bus.templates import RequestProtocol, ResponseProtocol, build_response

# Define payloads
class GetUserRequest(RequestProtocol):
    user_id: int = Field(description="ID of the user to query")

class GetUserResponse(ResponseProtocol):
    user_name: str = Field(description="Username")
    email: str = Field(description="Email address")

# Declare events
class GetUserRequestEvent(EventDeclaration):
    name = "user.get.request"
    payload_type = GetUserRequest

class GetUserResponseEvent(EventDeclaration):
    name = "user.get.response"
    payload_type = GetUserResponse
```

### 2. Register Events

Register event declarations with `EventRegistry` (typically at app startup).

```python
registry = EventRegistry()
registry.register(GetUserRequestEvent)
registry.register(GetUserResponseEvent)
```

### 3. Implement Server-Side Handler

Implement an `EventHandler` on the server side to listen for request events, process
them, and publish response events.

```python
class GetUserHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=["user.get.request"])

    async def handle(self, payload, bus_proxy, raw_event):
        if not isinstance(payload, GetUserRequest):
            return

        # Business logic: query user
        user = await db.get_user(payload.user_id)

        # Build response payload: build_response echoes session_id / request_id back
        response = build_response(
            payload,
            GetUserResponse,
            success=user is not None,
            error_msg=None if user else "User not found",
            user_name=user.name if user else "",
            email=user.email if user else "",
        )
        await bus_proxy.publish("user.get.response", response)
```

### 4. Client Initiates Request

The client calls the `request` function to make a call and wait for the response.

```python
from infinity_bus.templates.request import request

# Assuming an EventBus.Proxy instance is available (injected or created by the bus)
proxy = bus.proxy(source="UserServiceClient")

try:
    resp = await request(
        bus_proxy=proxy,
        req_event="user.get.request",
        req_data={"user_id": 123},
        resp_event="user.get.response",
        session_id=None,          # Auto-generate new session ID if omitted
        timeout=10.0,             # Timeout in seconds; None = wait indefinitely
    )
    resp.raise_if_failed()        # Checks success field; raises RuntimeError on failure
    print(f"User: {resp.user_name}, Email: {resp.email}")
except asyncio.TimeoutError:
    print("Request timed out")
except RuntimeError as e:
    print(f"Business failure: {e}")
```

---

## Function Signature

```python
async def request(
    bus_proxy: EventBus.Proxy,
    req_event: str,
    req_data: Dict[str, Any],
    resp_event: str,
    session_id: Optional[str] = None,
    timeout: Optional[float] = 60.0,
) -> ResponseProtocol
```

| Parameter | Type | Description |
| - | - | - |
| `bus_proxy` | `EventBus.Proxy` | Event bus proxy for publishing the request event. |
| `req_event` | `str` | Request event name (must be registered and payload must extend `RequestProtocol`). |
| `req_data` | `Dict[str, Any]` | Request payload data. `session_id` and `request_id` are auto-injected. |
| `resp_event` | `str` | Expected response event name (must be registered and payload must extend `ResponseProtocol`). |
| `session_id` | `Optional[str]` | Session ID. Auto-generates a UUID if `None`. |
| `timeout` | `Optional[float]` | Timeout in seconds for waiting. Raises `asyncio.TimeoutError` on expiry. `None` means indefinite wait. |

**Returns**: A `ResponseProtocol` instance (concrete type determined by the response
event's `payload_type`).

---

## build_response Helper

Helper for building response payloads on the server side: echoes the request's
`session_id` / `request_id` back automatically, while the remaining fields come from `kargs`.

```python
def build_response(
    request: RequestProtocol,
    response_type: type[ResponseProtocol],
    **kargs: Any,
) -> ResponseProtocol
```

| Parameter | Type | Description |
| - | - | - |
| `request` | `RequestProtocol` | The corresponding request payload (source of truth for `session_id` / `request_id`). |
| `response_type` | `type[ResponseProtocol]` | The response payload class. |
| `kargs` | `Any` | Remaining business fields, passed to the response model's `model_validate`. |

**Returns**: An instance of `response_type`.

- Conflicting `session_id` / `request_id` in `kargs` are **overridden** by the request values (the request is the single source of truth) — no manual echo-back needed.
- `ServiceHandler`'s automatic response construction uses this same function (see [handlers/service.md](handlers/service.md)).

---

## Exceptions

| Exception Type | Trigger Condition |
| - | - |
| `ValueError` | Request/response event not registered. |
| `TypeError` | Event payload does not meet `RequestProtocol` or `ResponseProtocol` requirements. |
| `BusShuttingDown` | Event bus is shutting down, cannot publish new events. |
| `asyncio.TimeoutError` | No matching response received within `timeout`. |
| `asyncio.CancelledError` | External cancellation of the request task (temporary handler auto-cleaned). |
| `RuntimeError` | `resp.raise_if_failed()` called with `success=False`. |

---

## Internals

- **Temporary Handler**: Each `request` call dynamically registers a `OneShotHandler`
  listening for `resp_event` in the current context. It auto-unregisters on return
  (success, failure, or cancellation), preventing resource leaks.
- **Session Isolation**: Dual matching via `session_id` + `request_id` ensures concurrent
  requests never interfere with each other.
- **Type Safety**: Validates event declaration payload types before publishing; validates
  response payload inherits `ResponseProtocol` — mismatches fail immediately.

---

## Notes

1. **Response handler must echo back `session_id` and `request_id`** — otherwise the
   client cannot match, causing timeout.
2. **Do not manually provide `session_id` or `request_id` in `req_data`** — the framework
   overwrites them to guarantee uniqueness.
3. **Server should publish responses promptly** to avoid long client waits. For
   long-running operations, consider returning an "accepted" status first, then push
   results via a separate event.
4. **Set timeout slightly above the max expected business logic duration**, but not
   excessively long to avoid resource hogging.
5. **Ensure the bus is started** and all relevant event declarations are registered
   before use with `EventBus`.

---

## Full Example

See `tests/templates/request_test.py`

Contains test cases for normal requests, timeouts, cancellations, type errors, and more.
