# Frame Payload Codec (pipe/codec)

`infinity_bus.templates.pipe.codec` provides JSON encoding/decoding for cross-process
frame payloads. Back to the [overview](pipe.md).

> The in-process implementation passes Pydantic objects directly between endpoints and does
> not use this module.

---

## API

```python
from pydantic import BaseModel

from infinity_bus.templates.pipe.codec import decode_payload, encode_payload

raw = encode_payload(my_model)                 # BaseModel -> UTF-8 JSON bytes
restored = decode_payload(raw, MyPayload)      # bytes -> MyPayload
```

| Function | Description |
| ---- | ---- |
| `encode_payload(data) -> bytes` | `data.model_dump_json().encode('utf-8')`, preserving concrete fields. |
| `decode_payload[PayloadType](raw, payload_type) -> PayloadType` | Validates and rebuilds with `payload_type.model_validate_json()`. |

- The wire format carries no type name; the receiver always decodes with the
  `payload_type` given to the allocator.
- Invalid JSON or incompatible fields raise Pydantic `ValidationError`.
- `ProcessPipe` and `WsPipe` use this module; both ends must agree on a compatible payload
  model.
