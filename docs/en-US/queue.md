# Event Queue (EventQueue)

## Overview

`EventQueue` is a replaceable abstraction for the bus's **internal dispatch queue**. It
decouples the "queue" from `EventBus`: the **bus only depends on the `EventQueue`
interface** for enqueue-on-publish, dequeue-on-dispatch, and drain-on-shutdown — it never
knows about any concrete implementation or its configuration. Bounded/unbounded,
persistent, priority, or cross-process semantics are entirely up to the injected queue.

| Symbol | Description |
| - | - |
| `EventQueue` | Queue abstract base class (ABC); the minimal queue protocol the bus needs |
| `InMemoryEventQueue` | Default in-process implementation backed by `asyncio.Queue`, supports bounded backpressure |
| `InMemoryEventQueueConfig` | The in-memory queue's own config model (independent from the bus) |

---

## EventQueue (ABC)

```python
class EventQueue(ABC):
    async def put(self, event: Event) -> None   # enqueue; blocks when full (backpressure)
    async def get(self) -> Event                # pop head; blocks when empty
    def task_done(self, event: Event) -> None   # mark one event as dispatched
    def task_abandoned(self, event: Event) -> None  # not fully dispatched: stop waiting
    def qsize(self) -> int                      # current number of pending events
    async def join(self) -> None                # wait until every event is dispatched
```

| Method | Description |
| - | - |
| `put(event)` | Enqueues on the publishing side. Blocks until a slot frees when full — backpressure. |
| `get()` | Dequeues on the dispatch side. Blocks until an event arrives when empty. |
| `task_done(event)` | Called once **after all handler tasks of that event have started**; used by `join()` to detect drain. `event` lets cross-process implementations locate the exact message (concurrent starts may differ from fetch order). |
| `task_abandoned(event)` | Called on shutdown when an event was **not fully dispatched** (some handler tasks never started): tells the queue to stop waiting. The default implementation equals `task_done(event)` (in-process queues release the count); redelivering implementations should override it to drop local tracking **without acking**, so the broker redelivers when the channel closes. The same cleanup also fires the `on_dispatch_abandoned(event)` middleware hook so dedup / claiming middlewares can withdraw their claims. |
| `qsize()` | Backlog depth; used for observability and dispatch-wait timeout estimation on shutdown. |
| `join()` | Blocks until every enqueued event has been fetched and `task_done(event)`-ed (the event has been fully dispatched; key to no-loss graceful shutdown). |

Implementers only need to satisfy this minimal protocol to be usable by the bus; capacity
policy, storage medium, and queue semantics are the implementation's own responsibility.

---

## InMemoryEventQueueConfig

The in-memory queue's **own configuration** — not owned by the bus.

```python
class InMemoryEventQueueConfig(BaseModel):
    maxsize: int = 1024   # max capacity; 0 means unbounded (asyncio.Queue semantics)
```

| Field | Type | Default | Description |
| - | - | - | - |
| `maxsize` | `int` | `1024` | Max queue capacity; `put` blocks when full; `0` means unbounded. |

---

## InMemoryEventQueue

Default implementation backed by `asyncio.Queue`; the fallback when the bus is built
without an injected queue.

```python
queue = InMemoryEventQueue()                                       # default bounded 1024
queue = InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=0))     # unbounded
queue = InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=100))   # bounded 100
```

---

## Injection into EventBus

`EventBus` receives an `EventQueue` instance through the `queue` constructor parameter;
when omitted, it creates an `InMemoryEventQueue()` internally (capacity from the queue's
own default config, 1024).

```python
class EventBus:
    def __init__(
        self,
        event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
        queue: Optional[EventQueue] = None,   # injected queue; default InMemoryEventQueue()
        router: Optional[Router] = None,      # injected router; default Router(event_registry)
        max_handler_semaphore: int = 256,
        shutdown: ShutdownConfig = ShutdownConfig(),
        middleware_chain: Optional[MiddlewareChain] = None,
    ) -> None
```

| Parameter | Type | Default | Description |
| - | - | - | - |
| `queue` | `Optional[EventQueue]` | `None` | The internal dispatch queue. Defaults to the in-process implementation with its own default capacity. |

> **Decoupling**: queue capacity and other settings live on the queue itself
> (`InMemoryEventQueueConfig`). `EventBus` neither knows nor forwards any queue config.

---

## Use Cases

### Inject a bounded queue (replaces old `max_queue_size`)

```python
from infinity_bus import EventBus, InMemoryEventQueue, InMemoryEventQueueConfig

# Old: EventBus(reg, hreg, max_queue_size=100)
queue = InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=100))
bus = EventBus(reg, hreg, queue=queue)
```

### Inject a custom queue (persistent / priority / cross-process)

Just implement the five `EventQueue` methods to swap the internal queue:

```python
from infinity_bus import EventQueue, Event

class PriorityEventQueue(EventQueue):
    """Example: priority-ordered custom queue (sketch; implement the full protocol)."""

    async def put(self, event: Event) -> None: ...
    async def get(self) -> Event: ...
    def task_done(self, event: Event) -> None: ...
    def qsize(self) -> int: ...
    async def join(self) -> None: ...

bus = EventBus(reg, hreg, queue=PriorityEventQueue())
```

---

## Workflow

1. **Construction**: `EventBus` stores the injected `EventQueue` as its dispatch queue.
2. **Publish**: `_core_publish` calls `await queue.put(event)`; publishers block when the
   queue is full (backpressure).
3. **Dispatch**: the loop does `event = await queue.get()` → match handlers → create
   tasks and attach a **start fence**; once every handler task of that event has started,
   the fence calls `queue.task_done(event)` exactly once. Handler failure or cancellation
   does not affect the ack — the bus only promises dispatch; business outcomes belong to
   `TaskErrorEvent` and the consumer's own policy.
4. **Shutdown**: `stop()` estimates a dynamic timeout from `queue.qsize()` and awaits
   `queue.join()` until every queued event has been dispatched (all handler tasks
   started). Tasks that never start are cancelled; their events stay unacknowledged and
   are redelivered by a persistent queue when the channel closes.

---

## Notes

- The `EventQueue` protocol is **intentionally minimal** — only the 5 methods the bus
  needs. Non-bus helpers (`empty`/`full`/`put_nowait`) stay out of the abstraction;
  implementations may add them freely.
- Custom implementations must honor the `task_done(event)` / `task_abandoned(event)` /
  `join` accounting semantics (exactly one settlement per `get`), otherwise shutdown drain
  may block forever. `task_done(event)` is the only ack protocol: with concurrent starts the
  order differs from fetch order, so implementations must locate the message by `event.id`;
  an implementation that does not accept the event identity does not satisfy the contract.
- On shutdown, events that were never fully dispatched are reported through
  `task_abandoned(event)`: the default releases the `join` count (no redelivery), while
  persistent implementations keep them unacked for broker redelivery.
- To get capacity control equivalent to the old `max_queue_size`, inject
  `InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=N))`.
