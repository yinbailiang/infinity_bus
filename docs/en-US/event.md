# Event / EventDeclaration / EventRegistry

## Overview

The event system is the type foundation of EventBus. `EventDeclaration` declares event metadata,
`Event` is the runtime instance, and `EventRegistry` manages all valid event types centrally.

---

## Event

Runtime event instance, auto-created by the bus on publish.

```python
class Event(BaseModel):
    name: str                                      # Event type name
    data: SerializeAsAny[BaseModel] | None = None  # Payload (keeps concrete fields when serialized)
    id: str                                        # UUID (auto-generated)
    sources: list[str]                  # Processing chain sources
    timestamps: list[datetime]          # Per-node timestamps
    event_ids: list[str]                # Causal event chain IDs
```

| Field | Type | Description |
| - | - | - |
| `name` | `str` | Event type, matches `EventDeclaration.name`. |
| `data` | `SerializeAsAny[BaseModel] \| None` | Payload. `None` for no-payload events; concrete payload fields are preserved when serialized. |
| `id` | `str` | Unique identifier (UUID hex), auto-generated. |
| `sources` | `list[str]` | Names of nodes this event passed through. |
| `timestamps` | `list[datetime]` | Timestamps per node, 1:1 with `sources`. |
| `event_ids` | `list[str]` | All event IDs in the chain (including self), used for causality tracing. |

> `Event.model_dump()` / `model_dump_json()` preserves the concrete `BaseModel` subclass fields; use `EventCodec` for the cross-process wire format.

---

## EventDeclaration

Abstract base class for event declarations. All custom events must subclass.

```python
class EventDeclaration(ABC):
    name: ClassVar[str]                              # Event name (must be non-empty)
    payload_type: ClassVar[type[BaseModel] | None] = None     # Optional payload model
```

| ClassVar | Type | Description |
| - | - | - |
| `name` | `ClassVar[str]` | **Required.** Unique event identifier. Cannot be empty or whitespace. Automatically validated at subclass definition time — raises `TypeError` if invalid. |
| `payload_type` | `ClassVar[type[BaseModel] \| None]` | Pydantic model for payload. `None` = no payload. |

### Usage

```python
from pydantic import BaseModel
from datetime import datetime
from infinity_bus import EventDeclaration

class UserLoginPayload(BaseModel):
    user_id: str
    timestamp: datetime

class UserLoginEvent(EventDeclaration):
    name = "user.login"
    payload_type = UserLoginPayload

# No-payload event
class HeartbeatEvent(EventDeclaration):
    name = "system.heartbeat"
    # payload_type defaults to None
```

---

## EventRegistry

Manages all valid event declarations. Used by the bus to validate event types and payloads.

```python
class EventRegistry:
    def __init__(self) -> None
    def register(self, event_decl: type[EventDeclaration]) -> None
    def unregister(self, event_name: str) -> None
    def get(self, name: str) -> type[EventDeclaration] | None
    def list_names(self) -> list[str]

    @property
    def version(self) -> int
```

| Method/Property | Description |
| - | - |
| `register(event_decl)` | Register an event declaration. Raises `ValueError` on duplicates. |
| `unregister(event_name)` | Unregister by name (silently ignores if not found). |
| `get(name)` | Lookup by name, returns `None` if not found. |
| `list_names()` | List all registered event names. |
| `version` | Monotonic version — incremented on every add/remove. Used by [Router](router.md) for invalidation. |

### Usage

```python
registry = EventRegistry()
registry.register(UserLoginEvent)
registry.register(HeartbeatEvent)

assert registry.get("user.login") is UserLoginEvent
assert registry.get("unknown.event") is None
print(registry.list_names())  # ["user.login", "system.heartbeat"]
```
