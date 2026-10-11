# Event / EventDeclaration / EventRegistry

## Overview

The event system is the type foundation of EventBus. `EventDeclaration` declares event metadata,
`Event` is the runtime instance, and `EventRegistry` manages all valid event types centrally.

---

## Event

Runtime event instance, auto-created by the bus on publish.

```python
class Event(BaseModel):
    name: str                           # Event type name
    data: Optional[BaseModel] = None    # Payload (Pydantic model instance)
    id: str                             # UUID (auto-generated)
    sources: List[str]                  # Processing chain sources
    timestamps: List[datetime]          # Per-node timestamps
    event_ids: List[str]                # Causal event chain IDs
```

| Field | Type | Description |
| - | - | - |
| `name` | `str` | Event type, matches `EventDeclaration.name`. |
| `data` | `Optional[BaseModel]` | Payload. `None` for no-payload events. |
| `id` | `str` | Unique identifier (UUID hex), auto-generated. |
| `sources` | `List[str]` | Names of nodes this event passed through. |
| `timestamps` | `List[datetime]` | Timestamps per node, 1:1 with `sources`. |
| `event_ids` | `List[str]` | All event IDs in the chain (including self), used for causality tracing. |

---

## EventDeclaration

Abstract base class for event declarations. All custom events must subclass.

```python
class EventDeclaration(ABC):
    name: ClassVar[str]                              # Event name (must be non-empty)
    payload_type: ClassVar[Optional[Type[BaseModel]]] = None  # Optional payload model
```

| ClassVar | Type | Description |
| - | - | - |
| `name` | `ClassVar[str]` | **Required.** Unique event identifier. Cannot be empty or whitespace. Automatically validated at subclass definition time — raises `TypeError` if invalid. |
| `payload_type` | `ClassVar[Optional[Type[BaseModel]]]` | Pydantic model for payload. `None` = no payload. |

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
    def register(self, event_decl: Type[EventDeclaration]) -> None
    def unregister(self, event_name: str) -> None
    def get(self, name: str) -> Optional[Type[EventDeclaration]]
    def list_names(self) -> List[str]

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
