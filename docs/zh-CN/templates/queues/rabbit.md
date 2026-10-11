# RabbitMQ fanout 队列（queues/rabbit）

`RabbitFanoutQueue`（`infinity_bus.templates.queues.rabbit.queue`）是基于 **RabbitMQ
fanout exchange** 的跨进程 `EventQueue`：多个 `EventBus` 注入同一 fanout 后表现为
**一个逻辑总线**（任一发布 → 所有在线成员收到，含自收）。

- 满足「完美 Event 队列」契约：put 前经 `EventCodec` 编码、get 前解码 + 重建；
- 成员离线语义由 `strategy` 决定：`restart`（补投）/ `offline`（不补投）。

> 依赖 `aio-pika`（`infinity_bus[templates]` 已含，模块内惰性导入）。

---

## 拓扑

```text
fanout exchange 'infinity_bus.fanout'（durable）
每个成员 = 一条 durable 命名队列 'infinity_bus.fanout.<member_id>'（非 exclusive/auto-delete）
          绑定到 exchange → broker 为每成员维护独立 backlog（补投的载体）
```

- `task_done` → `basic_ack`（逐条确认，broker 权威）；
- `join` 分离输入并排空「自己的队列」；`resume` 恢复。

---

## create 参数

```python
q = await RabbitFanoutQueue.create(
    member_id,                 # 成员标识（队列名后缀，须唯一）
    url='amqp://guest:guest@127.0.0.1:5672/',  # 默认读 RABBITMQ_URL 环境变量
    codec=None,                # EventCodec；缺省用 EventCodec(registry)
    registry=None,             # 事件注册表（负载重建依据）
    strategy='restart',        # 'restart' | 'offline'
)
```

| 参数 | 类型 | 说明 |
| - | - | - |
| `member_id` | `str` | 成员标识（持久队列名后缀，需唯一） |
| `url` | `str` | AMQP URL，默认 `RABBITMQ_URL` 或 `amqp://guest:guest@127.0.0.1:5672/` |
| `codec` | `EventCodec \| None` | 编解码器；缺省用 `EventCodec(registry)` |
| `registry` | `EventRegistry \| None` | 事件注册表 |
| `strategy` | `'restart' \| 'offline'` | 成员离开策略 |

---

## 双策略（strategy）

| | restart（重启，默认） | offline（下线） |
| - | - | - |
| `join` | 停**消费**（`basic.cancel`），保留路由与积压 | 停**路由**（`queue.unbind`）+ 消费掉所有已路由 |
| 离线期事件 | 继续进队列（积压） | 不进队列（无积压） |
| `resume` | 重新 `consume` → **补投**积压 | 重新 `bind` → **只收新**（尽力而为广播） |

- **restart**：进程/成员要重启还会回来 —— 事件不能丢，离线期积压保留，恢复后续上；
- **offline**：成员下线离开 —— 离线即错过，等价 NATS/ZeroMQ 尽力而为广播。

> RabbitMQ/AMQP 是 push 模型，无 Kafka 式 `pause/resume` 一等 API：「暂停接收」的
> 原语即 `basic.cancel`（停消费）与 `queue.unbind`（停路由）。

---

## EventQueue 接口映射

| `EventQueue` | RabbitMQ |
| - | - |
| `put(event)` | `exchange.publish`（broker 泛洪，含自己队列） |
| `get()` | 从本地 inbound 取（订阅回调灌入，消息未 ack） |
| `task_done()` | `basic_ack` 逐条确认（据此补 prefetch 配额） |
| `join()` | 按策略分离输入并排空（见双策略） |
| `resume()` | restart 重 `consume` / offline 重 `bind` + `consume` |
| `qsize()` | 本地 inbound + 未 ack |

---

## 示例

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

## 已知取舍 / 注意事项

- 补投靠 durable 命名队列：不用时须显式删队列（`channel.queue_delete`），否则残留
  影响下次运行。
- 消费端在途崩溃（已投递未 ack）：broker 会重投未 ack 消息（at-least-once）；
  重复投递配合 [`templates/idempotency.md`](../idempotency.md) 的 recorder 去重。
- 负载重建依赖注入的 registry / `EventCodec`；未注册事件保持原始 JSON 值透传。
- `aio-pika` 惰性导入：缺失时 `create` 抛 `ImportError` 提示安装 `infinity_bus[templates]`。
- 需要运行中的 broker：`docker run -p 5672:5672 rabbitmq:3`。
