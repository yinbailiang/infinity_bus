# RabbitMQ fanout Queue (queues/rabbit)

`RabbitFanoutQueue` (`infinity_bus.templates.queues.rabbit.queue`) is a cross-process
`EventQueue` built on a **RabbitMQ fanout exchange**: multiple `EventBus` instances
joined on one fanout behave as **one logical bus** (any publish is received by every
online member, including the publisher).

- Honors the "perfect Event queue" contract: encode via `EventCodec` before put,
  decode + rebuild before get;
- Member leave semantics are set by `strategy`: `restart` (replay) / `offline`
  (no replay).

> Depends on `aio-pika` (included in `infinity_bus[templates]`; lazy import).

---

## Topology

```text
fanout exchange 'infinity_bus.fanout' (durable)
each member = one durable named queue 'infinity_bus.fanout.<member_id>'
           (not exclusive / auto-delete), bound to the exchange
           → broker keeps an independent backlog per member (replay carrier)
```

- `task_done` → `basic_ack` (per-message confirmation, broker-authoritative);
- `join` detaches input and drains "its own queue"; `resume` restores.

---

## create Arguments

```python
q = await RabbitFanoutQueue.create(
    member_id,                 # member id (queue name suffix; must be unique)
    url='amqp://guest:guest@127.0.0.1:5672/',  # defaults to RABBITMQ_URL env var
    codec=None,                # EventCodec; defaults to EventCodec(registry)
    registry=None,             # event registry (basis for payload rebuild)
    strategy='restart',        # 'restart' | 'offline'
)
```

| Argument | Type | Description |
| - | - | - |
| `member_id` | `str` | Member id (durable queue name suffix; must be unique) |
| `url` | `str` | AMQP URL; defaults to `RABBITMQ_URL` or `amqp://guest:guest@127.0.0.1:5672/` |
| `codec` | `EventCodec \| None` | Codec; defaults to `EventCodec(registry)` |
| `registry` | `EventRegistry \| None` | Event registry |
| `strategy` | `'restart' \| 'offline'` | Member leave strategy |

---

## Two Strategies (strategy)

| | restart (default) | offline |
| - | - | - |
| `join` | Pause **consumption** (`basic.cancel`), keep routing & backlog | Stop **routing** (`queue.unbind`) + consume all already-routed messages |
| events while away | Keep entering the queue (backlog) | Never enter the queue (no backlog) |
| `resume` | Re-`consume` → **replays** backlog | Re-`bind` → **only new** events (fire-and-forget broadcast) |

- **restart**: the process/member is coming back — events must not be lost; the backlog
  is kept while away and resumed later;
- **offline**: the member is leaving — missed while away, equivalent to NATS/ZeroMQ
  fire-and-forget broadcast.

> RabbitMQ/AMQP is a push model with no first-class Kafka-style `pause/resume` — the
> "pause receiving" primitives are `basic.cancel` (pause consumption) and
> `queue.unbind` (pause routing).

---

## EventQueue Interface Mapping

| `EventQueue` | RabbitMQ |
| - | - |
| `put(event)` | `exchange.publish` (fanout to all bound queues, incl. self) |
| `get()` | From the local inbound (fed by the subscription callback; message un-acked) |
| `task_done()` | `basic_ack` per message (refills the prefetch quota) |
| `join()` | Detach input and drain per strategy (see two strategies) |
| `resume()` | restart: re-`consume` / offline: re-`bind` + `consume` |
| `qsize()` | Local inbound + un-acked |

---

## Example

```python
from infinity_bus import EventBus, EventDeclaration, EventHandlerRegistry, EventRegistry
from infinity_bus.templates.queues.rabbit import RabbitFanoutQueue

class PingEvent(EventDeclaration):
    name = 'demo.ping'
    payload_type = None

async def member(member_id: str, strategy: str = 'restart') -> None:
    reg = EventRegistry()
    reg.register(PingEvent)
    q = await RabbitFanoutQueue.create(member_id, registry=reg, strategy=strategy)
    bus = EventBus(reg, EventHandlerRegistry(), queue=q)
    async with bus:
        await bus.proxy(member_id).publish('demo.ping', None)
```

---

## Known Trade-offs / Notes

- Replay relies on durable named queues: delete them explicitly when unused
  (`channel.queue_delete`), otherwise leftovers affect the next run.
- In-flight crash (delivered but un-acked): the broker redelivers un-acked messages
  (at-least-once); pair with the recorder from
  [`templates/idempotency.md`](../idempotency.md) for dedup.
- Payload rebuild depends on the injected registry / `EventCodec`; unregistered events
  pass data through as the raw JSON value.
- `aio-pika` is lazily imported: `create` raises an `ImportError` hint when missing
  (install `infinity_bus[templates]`).
- Needs a running broker: `docker run -p 5672:5672 rabbitmq:3`.
