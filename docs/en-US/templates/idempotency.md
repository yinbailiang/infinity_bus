# Idempotency (idempotency)

`infinity_bus.templates.idempotency` provides an **injectable idempotency mechanism**:
events carry a unique `Event.id` (uuid4 hex) as the dedup key, and a "processed" marker
is recorded so duplicate deliveries under at-least-once semantics are handled once.

> No third-party dependency. `IdempotencyRecorder` is the injected policy:
> in-process memory or SQLite-persistent.

---

## Why it is needed

Event delivery can duplicate:

- a consumer processes but disconnects before ack → the broker redelivers the same id;
- the `queues/rabbit.md` restart strategy replays the backlog accumulated while away.

Re-running handlers produces duplicate side effects. Idempotency lifts "check + mark on
success" out of business code into a replaceable recorder.

---

## IdempotencyRecorder (Protocol)

```python
class IdempotencyRecorder(Protocol):
    async def is_processed(self, consumer: str, event_id: str) -> bool: ...
    async def mark_processed(self, consumer: str, event_id: str) -> None: ...
```

| Method | Semantics |
| - | - |
| `is_processed(consumer, event_id)` | Whether (consumer, event_id) was already processed |
| `mark_processed(consumer, event_id)` | Mark as processed (repeated marks must be idempotent) |

`consumer` distinguishes consumers: under fanout every member processes the same events
and records its own markers, never interfering with each other.

---

## IdempotentHandler (base class)

Subclass it and implement `handle`; the idempotency semantics apply automatically:

```python
from infinity_bus.templates.idempotency import IdempotentHandler

class MyHandler(IdempotentHandler):
    def __init__(self, recorder, consumer):
        super().__init__(['my.event'], recorder, consumer)

    async def handle(self, payload, bus_proxy, raw_event):
        ...  # business logic; idempotency handled for you
```

Flow (overriding `__call__`):

```text
is_processed? ── processed → drop (return)
     │ not processed
     ▼
run handle
     │ success          │ raises
     ▼                  ▼
mark_processed   not marked (left for at-least-once redelivery)
```

> **Mark only on success**: a raising handler is not marked, so redelivery retries it —
> consistent with at-least-once semantics.

---

## Injected Policies

| Policy | Description |
| - | - |
| `InMemoryIdempotencyRecorder` | In-process `set` dedup (session scope) |
| `SqliteIdempotencyRecorder` | SQLite-persistent (stdlib `sqlite3` + `asyncio.to_thread`, zero third-party deps); "processed log = dedup table", dedups across processes / restarts |

### SQLite persistent

```python
from infinity_bus.templates.idempotency import SqliteIdempotencyRecorder

recorder = SqliteIdempotencyRecorder('processed.db')  # or ':memory:'
await recorder.start()
try:
    ...
finally:
    await recorder.close()
```

Table `processed_log(consumer, event_id, ts)` with primary key `(consumer, event_id)`;
writes use `INSERT OR IGNORE`, so concurrent duplicate marks are made safe by the
primary-key constraint.

---

## Example: inject into a consumer

```python
from infinity_bus.templates.idempotency import (
    IdempotentHandler,
    InMemoryIdempotencyRecorder,
)

recorder = InMemoryIdempotencyRecorder()
handler = MyHandler(recorder, 'consumer-A')   # consumer distinguishes members
```

> Pair with `queues/`: give every member a recorder to dedup end-to-end under a
> cross-process queue (at-least-once + replay); `SqliteIdempotencyRecorder` can be
> shared by multiple processes via the same database file.

---

## Notes

- The dedup key is `Event.id` (uuid4 hex); duplicate publishes of the same business
  event must reuse the same id to be deduplicated.
- The memory policy works only within one process; for cross-restart strong idempotency
  use the SQLite policy or natural business idempotency.
- Marking and side effects are not atomic: in the narrow "side effect done, crash before
  mark" window a retry may still happen — prefer idempotent handlers (or narrow the
  window with the persistent `SqliteIdempotencyRecorder`).
