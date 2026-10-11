# Cross-Process Queues (queues)

`infinity_bus.templates.queues` provides **cross-process `EventQueue`** implementations:
multiple `EventBus` instances join into **one logical bus** (any publish is received by
every online member, including the publisher itself), while honoring the `EventQueue`
**"perfect Event queue" contract** — `Event.data` on both sides of the queue is always a
validated `BaseModel` instance, so cross-process semantics equal in-process semantics.

> Requires optional dependencies: `pip install infinity_bus[templates]`.
> The RabbitMQ backend additionally needs `aio-pika` (included in that extra, lazy
> import).

---

## Directory Layout (mirrors code)

```text
infinity_bus.templates.queues/
├── queues.md     # this overview
├── codec.md      # EventCodec — wire codec (perfect Event contract)
└── rabbit.md     # rabbit/ — RabbitMQ fanout transport backend (two strategies)
```

| Doc | Module | Description | Deps |
| - | - | - | - |
| [codec.md](codec.md) | `queues/codec.py` | `EventCodec`: Event ↔ bytes + payload rebuild | none |
| [rabbit.md](rabbit.md) | `queues/rabbit/queue.py` | `RabbitFanoutQueue`: RabbitMQ fanout cross-process queue | `aio-pika` (lazy) |

> Idempotency (`IdempotencyRecorder` / `IdempotentHandler`) lives in the top-level
> template [`templates/idempotency.md`](../idempotency.md), used for at-least-once dedup.

---

## The "Perfect Event Queue" Contract

The `EventQueue` abstraction promises that the Event passed to `put` and returned by
`get` both satisfy the invariant: `Event.data` is a **validated `BaseModel` instance**
(or None).

- In-memory queues satisfy this naturally;
- Cross-process implementations (e.g. `RabbitFanoutQueue`) perform "encode on put /
  decode + rebuild on get" at the boundary — encoding/decoding goes through
  `EventCodec`, and rebuild uses the injected registry (`name → payload_type`).

```text
    EventBus  ──put(perfect Event)──▶  EventQueue  ──get(perfect Event)──▶  EventBus
                                      │  implementation boundary
                     cross-process:  encode ─┘        └─ decode + rebuild
```

Thus the bus / middlewares / handlers only ever see perfect Events —
**cross-process semantics = in-process semantics**.

---

## Quick Start

```python
from infinity_bus import EventBus, EventDeclaration, EventHandlerRegistry, EventRegistry
from infinity_bus.templates.queues import EventCodec, RabbitFanoutQueue

class PingEvent(EventDeclaration):
    name = 'demo.ping'
    payload_type = None

async def member(member_id: str) -> None:
    reg = EventRegistry()
    reg.register(PingEvent)
    q = await RabbitFanoutQueue.create(member_id, registry=reg)   # restart default
    bus = EventBus(reg, EventHandlerRegistry(), queue=q)
    async with bus:
        await bus.proxy(member_id).publish('demo.ping', None)
```

See [codec.md](codec.md) and [rabbit.md](rabbit.md) for details.

---

## Notes

- The RabbitMQ backend depends on `aio-pika` (lazy import; `create` raises an
  `ImportError` hint when missing).
- Durable queues carry replay backlogs: delete them explicitly when unused, otherwise
  leftovers affect the next run.
- Payload rebuild depends on the injected registry / `EventCodec`; unregistered events
  pass data through as the raw JSON value.
- Cross-process runs need a running broker (e.g. `docker run -p 5672:5672 rabbitmq:3`).
