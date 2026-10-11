# 跨进程队列（queues）

`infinity_bus.templates.queues` 提供**跨进程 EventQueue**：把多个 `EventBus` 连成
**一个逻辑总线**（任一发布 → 所有在线成员收到，含自收），并满足 `EventQueue` 的
**「完美 Event 队列」契约** —— 队列两侧的 `Event.data` 始终是已校验的 `BaseModel`
实例，跨进程语义与单进程一致。

> 需要可选依赖：`pip install infinity_bus[templates]`。
> RabbitMQ 后端额外依赖 `aio-pika`（已包含在该 extra 中，惰性导入）。

---

## 目录结构（镜像代码）

```text
infinity_bus.templates.queues/
├── queues.md     # 本总览
├── codec.md      # EventCodec —— 线格式编解码（完美 Event 契约）
└── rabbit.md     # rabbit/ —— RabbitMQ fanout 传输后端（双策略）
```

| 文档 | 对应模块 | 说明 | 依赖 |
| - | - | - | - |
| [codec.md](codec.md) | `queues/codec.py` | `EventCodec`：Event ↔ bytes + 负载重建 | 无 |
| [rabbit.md](rabbit.md) | `queues/rabbit/queue.py` | `RabbitFanoutQueue`：RabbitMQ fanout 跨进程队列 | `aio-pika`（惰性） |

> 幂等（`IdempotencyRecorder` / `IdempotentHandler`）已提取为顶层独立模板
> [`templates/idempotency.md`](../idempotency.md)，配合 at-least-once 去重使用。

---

## 「完美 Event 队列」契约

`EventQueue` 抽象承诺：`put` 的 Event 与 `get` 返回的 Event 均满足 `Event.data` 为
**已校验 `BaseModel` 实例**（或 None）的不变量。

- 内存队列天然满足；
- 跨进程实现（如 `RabbitFanoutQueue`）在边界完成「put 编码 / get 解码 + 重建」——
  编解码统一走 `EventCodec`，重建依据注入的注册表（`name → payload_type`）。

```text
    EventBus  ──put(完美 Event)──▶  EventQueue  ──get(完美 Event)──▶  EventBus
                                    │ 实现边界
                    跨进程:  encode ─┘        └─ decode + rebuild
```

由此：总线 / 中间件 / 处理器永远只见完美 Event —— **跨进程语义 = 单进程语义**。

---

## 快速使用

```python
from infinity_bus import EventBus, EventDeclaration, EventHandlerRegistry, EventRegistry
from infinity_bus.templates.queues import EventCodec, RabbitFanoutQueue

class PingEvent(EventDeclaration):
    name = 'demo.ping'
    payload_type = None

async def member(member_id: str) -> None:
    reg = EventRegistry()
    reg.register(PingEvent)
    q = await RabbitFanoutQueue.create(member_id, registry=reg)   # restart 默认
    bus = EventBus(reg, EventHandlerRegistry(), queue=q)
    async with bus:
        await bus.proxy(member_id).publish('demo.ping', None)
```

更多见 [codec.md](codec.md) 与 [rabbit.md](rabbit.md)。

---

## 注意事项

- RabbitMQ 后端依赖 `aio-pika`（惰性导入；缺失时 `create` 抛 `ImportError` 提示）。
- durable 队列承载补投积压：不用时须显式删队列，否则残留影响下次运行。
- 负载重建依赖注入的 registry / `EventCodec`；未注册事件保持原始 JSON 值透传。
- 跨进程运行需要真实 broker（如 `docker run -p 5672:5672 rabbitmq:3`）。
