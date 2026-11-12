# 帧载荷编解码（pipe/codec）

`infinity_bus.templates.pipe.codec` 为跨进程传输提供帧载荷的 JSON 编解码。返回总览
见 [pipe.md](pipe.md)。

> 进程内实现直接在端点之间传递 Pydantic 对象，不经过此模块。

---

## API

```python
from pydantic import BaseModel

from infinity_bus.templates.pipe.codec import decode_payload, encode_payload

raw = encode_payload(my_model)                 # BaseModel -> UTF-8 JSON bytes
restored = decode_payload(raw, MyPayload)      # bytes -> MyPayload
```

| 函数 | 说明 |
| ---- | ---- |
| `encode_payload(data) -> bytes` | `data.model_dump_json().encode('utf-8')`，保留具体类型的字段。 |
| `decode_payload[PayloadType](raw, payload_type) -> PayloadType` | 按 `payload_type.model_validate_json()` 校验并重建实例。 |

- 编码不写类型名；接收端始终按构造分配器时传入的 `payload_type` 解码。
- 字段不兼容或 JSON 非法时抛出 Pydantic `ValidationError`。
- `ProcessPipe` 与 `WsPipe` 使用该模块；两端应使用兼容的载荷模型。
