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
    async def claim(self, consumer: str, event_id: str) -> bool: ...
    async def release(self, consumer: str, event_id: str) -> None: ...
```

| Method | Semantics |
| - | - |
| `is_processed(consumer, event_id)` | Whether (consumer, event_id) was already processed |
| `mark_processed(consumer, event_id)` | Mark as processed (repeated marks must be idempotent) |
| `claim(consumer, event_id)` | Atomic claim: mark and return `True` if unprocessed, `False` otherwise (`INSERT OR IGNORE` + rowcount in one step) |
| `release(consumer, event_id)` | Withdraw a claim: delete the marker so a redelivered event can be dispatched again (no-op if absent) |

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

## Dispatch-boundary claiming (claim / release)

`IdempotentHandler` marks **on success**; when dedup lives at the dispatch boundary
(check before dispatching in `before_dispatch`), use the atomic `claim`:

```python
class DedupMiddleware(Middleware):
    def __init__(self, recorder: IdempotencyRecorder, consumer: str) -> None:
        self._recorder = recorder
        self._consumer = consumer

    async def before_dispatch(self, event, handler_ids, next):
        if not await self._recorder.claim(self._consumer, event.id):
            return                      # already claimed: drop this delivery
        await next(event, handler_ids)

    async def on_dispatch_abandoned(self, event):
        # shutdown abandoned the dispatch (some task never started):
        # withdraw the claim so the redelivery can be dispatched again
        await self._recorder.release(self._consumer, event.id)
```

> `claim` records **before** handling (a failure is not auto-reverted); `release` pairs
> with the `on_dispatch_abandoned` middleware hook so that "claimed ⇔ actually handed
> to the handlers".

---

## Injected Policies

| Policy | Description |
| - | - |
| `InMemoryIdempotencyRecorder` | In-process `set` dedup (session scope) |
| `SqliteIdempotencyRecorder` | SQLite-persistent (stdlib `sqlite3` + `asyncio.to_thread`, zero third-party deps); "processed log = dedup table", dedups across processes / restarts |

### SQLite persistent

```python
from infinity_bus.templates.idempotency import SqliteIdempotencyRecorder

recorder = SqliteIdempotencyRecorder(
    'processed.db',                     # or ':memory:'
    retention_seconds=7 * 24 * 3600.0,  # 0 = keep forever
    sweep_interval_seconds=600.0,       # prune check interval
)
await recorder.start()
try:
    ...
finally:
    await recorder.close()
```

Table `processed_log(consumer, event_id, ts)` with primary key `(consumer, event_id)`;
writes use `INSERT OR IGNORE`, so concurrent duplicate marks are made safe by the
primary-key constraint.

The connection runs `journal_mode=WAL` + `synchronous=NORMAL`: with the defaults every
mark fsyncs (measured ~13ms per check+mark — dedup runs once per event per handler, i.e.
an fsync on the dispatch path). WAL may lose the last few transactions on power loss;
acceptable for a dedup table: worst case the event runs one extra time.

Rows older than `retention_seconds` (default 7 days) are pruned on `start()`, then every
`sweep_interval_seconds` on the write path; `await recorder.prune()` is available for
manual maintenance and returns the deleted row count. Without pruning the table only
grows (measured 50k rows/day for a single consumer).

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
- The `claim` pattern records at the dispatch boundary; if an event is never fully
  dispatched, `release` it in `on_dispatch_abandoned` — otherwise redelivery will be
  blocked by your own claim.
- The memory policy works only within one process; for cross-restart strong idempotency
  use the SQLite policy or natural business idempotency.
- Marking and side effects are not atomic: in the narrow "side effect done, crash before
  mark" window a retry may still happen — prefer idempotent handlers (or narrow the
  window with the persistent `SqliteIdempotencyRecorder`).
