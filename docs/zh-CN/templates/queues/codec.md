# EventCodec（queues/codec）

`EventCodec` 是「完美 Event」的**线格式编解码器**：`encode(event) -> bytes` /
`decode(bytes) -> Event`，供跨进程 EventQueue 在边界使用（put 前编码、get 前解码
+ 重建）。

> 无第三方依赖。

---

## 为什么需要它

`Event.data` 注解为抽象 `Optional[BaseModel]`，Pydantic 2 会给抽象 BaseModel 字段装
**mock serializer / validator**：

1. `Event.model_dump_json()`：真实负载被 dump 成空对象 `{}`；
2. `Event.model_validate_json()`：data 变成不可用的 mock 实例。

`EventCodec` 用自定义信封规避（见下），使 **`decode` 返回「完美 Event」** ——
`Event.data` 为已校验的 `BaseModel` 实例（或 None）。

---

## API

```python
from infinity_bus import EventRegistry
from infinity_bus.templates.queues import EventCodec

codec = EventCodec(registry)     # registry: name → EventDeclaration（含 payload_type）
blob = codec.encode(event)       # 完美 Event → bytes（UTF-8 JSON 信封）
event = codec.decode(blob)       # bytes → 完美 Event（data 重建为校验实例）
```

| 方法 | 说明 |
| - | - |
| `encode(event) -> bytes` | 元数据经 `model_dump(mode='json', exclude={'data'})`；data 若为 `BaseModel` 按具体类型单独 dump（datetime/uuid 等经 mode='json' 处理），否则原样 |
| `decode(bytes) -> Event` | 用 `model_construct` 保留 data 为原始 JSON 值（绕开 mock），再按注册表重建为具体负载 |

| 构造参数 | 类型 | 说明 |
| - | - | - |
| `registry` | `EventRegistry \| None` | 提供时按 `name → payload_type` 重建；`None` 或事件未注册 / `payload_type` 为 `None`（无负载）时 data 以原始 JSON 值透传，不抛错 |

> 兼容透传：跨版本演进中，消费方尚未注册的新事件不会被 decode 破坏 —— 保持原始
> JSON 值，交由上层策略（忽略 / 死信 / 升级后补处理）。

---

## 使用场景

- `RabbitFanoutQueue`（见 [rabbit.md](rabbit.md)）在 `put` / `_on_message` 边界调用
  `encode` / `decode`，实现「完美 Event 队列」契约；
- 需要自定义跨进程 EventQueue 时，直接复用本编解码器，不必重写 mock 规避逻辑。

```python
# 单独使用（负载重建示例）
from pydantic import BaseModel

from infinity_bus import Event, EventDeclaration, EventRegistry
from infinity_bus.templates.queues import EventCodec

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

## 注意事项

- 编解码只保证「完美 Event」；**负载重建需要注入的 registry** —— 消费方须注册与
  发布方一致的 `EventDeclaration`（或依赖 schema 版本策略，见 idempotency/演进）。
- 时间戳等元数据经 ISO 字符串往返；无法解析的时间戳条目会被跳过（其余保真）。
- 内存队列不需要 `EventCodec`（Event 对象直传，天然完美）。
