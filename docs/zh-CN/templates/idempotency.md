# 幂等（idempotency）

`infinity_bus.templates.idempotency` 提供**可注入的幂等机制**：以事件自带唯一
`Event.id`（uuid4 hex）为去重键，记录「已处理标记」，让 at-least-once 语义下的重复
投递只被处理一次。

> 无第三方依赖。`IdempotencyRecorder` 为注入策略：进程内内存版或 SQLite 持久版。

---

## 为什么需要

事件投递可能重复：

- 消费端处理了但未 ack 即断连 → broker 重投同 id 消息；
- `queues/rabbit.md` 的 restart 补投会重放离线期积压。

重复执行 handler 会产生重复副作用。幂等把「查重 + 成功记」从业务里抽出，交给
可替换的记录器。

---

## IdempotencyRecorder（协议）

```python
class IdempotencyRecorder(Protocol):
    async def is_processed(self, consumer: str, event_id: str) -> bool: ...
    async def mark_processed(self, consumer: str, event_id: str) -> None: ...
```

| 方法 | 语义 |
| - | - |
| `is_processed(consumer, event_id)` | 该 (consumer, event_id) 是否已处理 |
| `mark_processed(consumer, event_id)` | 标记已处理（重复标记应幂等安全） |

`consumer` 区分消费方标识：泛洪下各成员处理同一事件，各自记录自己的标记，互不串扰。

---

## IdempotentHandler（基类）

继承它并实现 `handle`，幂等语义自动生效：

```python
from infinity_bus.templates.idempotency import IdempotentHandler

class MyHandler(IdempotentHandler):
    def __init__(self, recorder, consumer):
        super().__init__(['my.event'], recorder, consumer)

    async def handle(self, payload, bus_proxy, raw_event):
        ...  # 业务逻辑，无需自管幂等
```

处理流程（覆写 `__call__`）：

```text
is_processed? ── 已处理 → 丢弃（直接返回）
     │ 未处理
     ▼
执行 handle
     │ 成功            │ 抛错
     ▼                ▼
mark_processed   不标记（留给 at-least-once 重投重试）
```

> **成功才标记**：handler 抛错不写「已处理」，重投会再次执行 —— 与 at-least-once
> 语义自洽。

---

## 注入策略

| 策略 | 说明 |
| - | - |
| `InMemoryIdempotencyRecorder` | 进程内 `set` 去重（会话内） |
| `SqliteIdempotencyRecorder` | SQLite 持久（stdlib `sqlite3` + `asyncio.to_thread`，零第三方依赖）；「处理完成日志 = 幂等表」，跨进程 / 跨重启去重 |

### SQLite 持久版

```python
from infinity_bus.templates.idempotency import SqliteIdempotencyRecorder

recorder = SqliteIdempotencyRecorder('processed.db')  # 或 ':memory:'
await recorder.start()
try:
    ...
finally:
    await recorder.close()
```

表 `processed_log(consumer, event_id, ts)`，主键 `(consumer, event_id)`；写入用
`INSERT OR IGNORE`，并发重复的 mark 由主键约束兜底，天然幂等安全。

---

## 示例：注入到消费方

```python
from infinity_bus.templates.idempotency import (
    IdempotentHandler,
    InMemoryIdempotencyRecorder,
)

recorder = InMemoryIdempotencyRecorder()
handler = MyHandler(recorder, 'consumer-A')   # consumer 区分成员
```

> 与 `queues/` 搭配：跨进程队列（at-least-once + 补投）下给每个成员注入 recorder
> 即可端到端去重；`SqliteIdempotencyRecorder` 可多个进程共享同一数据库文件。

---

## 注意事项

- 去重键是 `Event.id`（uuid4 hex）；同一业务事件的重复发布应复用同一 id 才有效。
- 内存版仅进程内有效；跨重启强幂等请用 SQLite 版或业务层天然幂等。
- 标记与业务副作用并非原子：极端的「副作用完成、mark 前崩溃」窗口仍可能重试一次，
  业务 handler 应尽量幂等（或依赖 `SqliteIdempotencyRecorder` 持久化缩小窗口）。
