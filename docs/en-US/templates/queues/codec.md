# EventCodec (queues/codec)

`EventCodec` is the **wire codec for "perfect Events"**: `encode(event) -> bytes` /
`decode(bytes) -> Event`, used by cross-process EventQueues at the boundary (encode
before put, decode + rebuild before get).

> No third-party dependency.

---

## Why it is needed

`Event.data` is annotated as abstract `Optional[BaseModel]`; Pydantic 2 installs a
**mock serializer/validator** for abstract BaseModel fields:

1. `Event.model_dump_json()` dumps the real payload as an empty object `{}`;
2. `Event.model_validate_json()` yields an unusable mock instance.

`EventCodec` avoids this with a custom envelope (below), so **`decode` returns a
"perfect Event"** — `Event.data` is a validated `BaseModel` instance (or None).

---

## API

```python
from infinity_bus import EventRegistry
from infinity_bus.templates.queues import EventCodec

codec = EventCodec(registry)     # registry: name → EventDeclaration (with payload_type)
blob = codec.encode(event)       # perfect Event → bytes (UTF-8 JSON envelope)
event = codec.decode(blob)       # bytes → perfect Event (data rebuilt to validated instance)
```

| Method | Description |
| - | - |
| `encode(event) -> bytes` | Metadata via `model_dump(mode='json', exclude={'data'})`; if `data` is a `BaseModel` it is dumped separately by its concrete type (`mode='json'` handles datetime/uuid), otherwise passed through |
| `decode(bytes) -> Event` | Uses `model_construct` to keep `data` as the raw JSON value (bypassing the mock), then rebuilds it into the concrete payload via the registry |

| Argument | Type | Description |
| - | - | - |
| `registry` | `EventRegistry \| None` | When provided, rebuilds via `name → payload_type`; when `None`, or the event is unregistered / `payload_type` is `None` (no payload), `data` passes through as the raw JSON value without raising |

> Pass-through compatibility: during cross-version evolution, events not yet registered
> by the consumer are not corrupted by decode — the raw JSON value is preserved for an
> upper-layer policy (ignore / dead-letter / process after upgrade).

---

## Use Cases

- `RabbitFanoutQueue` (see [rabbit.md](rabbit.md)) calls `encode` / `decode` at the
  `put` / `_on_message` boundary to honor the "perfect Event queue" contract;
- when building a custom cross-process EventQueue, reuse this codec instead of
  re-implementing the mock workarounds.

```python
# Standalone usage (payload rebuild example)
class MyPayload(BaseModel):
    value: int

class MyEventDecl(EventDeclaration):
    name = 'my.event'
    payload_type = MyPayload

reg = EventRegistry()
reg.register(MyEventDecl)
codec = EventCodec(reg)

ev = Event(name='my.event', data=MyPayload(value=1))
assert isinstance(codec.decode(codec.encode(ev)).data, MyPayload)
```

---

## Notes

- Encoding/decoding only guarantees "perfect Events"; **payload rebuild needs the
  injected registry** — the consumer must register the same `EventDeclaration` as the
  publisher (or rely on a schema-version policy, cf. idempotency/evolution).
- Metadata such as timestamps round-trips through ISO strings; unparsable timestamp
  entries are skipped (the rest stays faithful).
- In-memory queues do not need `EventCodec` (Event objects pass through directly and are
  naturally perfect).
