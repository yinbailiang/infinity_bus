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
    async def claim(self, consumer: str, event_id: str) -> bool: ...
    async def release(self, consumer: str, event_id: str) -> None: ...
```

| 方法 | 语义 |
| - | - |
| `is_processed(consumer, event_id)` | 该 (consumer, event_id) 是否已处理 |
| `mark_processed(consumer, event_id)` | 标记已处理（重复标记应幂等安全） |
| `claim(consumer, event_id)` | 原子认领：未处理则标记并返回 `True`，已处理返回 `False`（`INSERT OR IGNORE` + rowcount 一步完成） |
| `release(consumer, event_id)` | 撤回认领：删除已处理标记，让重投的事件可以重新派发（不存在时无副作用） |

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

## 派发边界认领（claim / release）

`IdempotentHandler` 是「成功后标记」；若去重落在派发边界（`before_dispatch` 里先查
后派发），用原子的 `claim`：

```python
class DedupMiddleware(Middleware):
    def __init__(self, recorder: IdempotencyRecorder, consumer: str) -> None:
        self._recorder = recorder
        self._consumer = consumer

    async def before_dispatch(self, event, handler_ids, next):
        if not await self._recorder.claim(self._consumer, event.id):
            return                      # 已认领：丢弃本次投递
        await next(event, handler_ids)

    async def on_dispatch_abandoned(self, event):
        # 停机放弃派发（有任务从未启动）：撤回认领，重投才能重新派发
        await self._recorder.release(self._consumer, event.id)
```

> `claim` 是**处理前**记账（失败不会自动撤回）；`release` 与中间件
> `on_dispatch_abandoned` 配合，保证「认领 ⇔ 真的交给处理器」。

> 进程内记录器是**有界**的：`max_entries`（默认 100000，0 = 无界）超出后按插入顺序
> 淘汰最旧记录；`ttl_seconds`（默认 0）可让记录过期。被淘汰 / 过期的记录会失去去重
> 保护，重复投递可能再次执行——需要强幂等请用 SQLite 版。

---

## 注入策略

| 策略 | 说明 |
| - | - |
| `InMemoryIdempotencyRecorder` | 进程内**有界**去重（默认最多 100k 条，按插入顺序淘汰最旧；可选 TTL） |
| `SqliteIdempotencyRecorder` | SQLite 持久（stdlib `sqlite3` + `asyncio.to_thread`，零第三方依赖）；「处理完成日志 = 幂等表」，跨进程 / 跨重启去重 |

### SQLite 持久版

```python
from infinity_bus.templates.idempotency import SqliteIdempotencyRecorder

recorder = SqliteIdempotencyRecorder(
    'processed.db',                     # 或 ':memory:'
    retention_seconds=7 * 24 * 3600.0,  # 0 = 永久保留
    sweep_interval_seconds=600.0,       # 清理巡检间隔
)
await recorder.start()
try:
    ...
finally:
    await recorder.close()
```

表 `processed_log(consumer, event_id, ts)`，主键 `(consumer, event_id)`；写入用
`INSERT OR IGNORE`，并发重复的 mark 由主键约束兜底，天然幂等安全。

连接开 `journal_mode=WAL` + `synchronous=NORMAL`：默认参数下每次 mark 都要 fsync，
实测一条「查重 + 标记」约 13ms（去重是「每事件 × 每处理器」各一次，等于在派发路径上
插了个 fsync）；WAL 下掉电可能丢最后几个事务——对去重表可接受，最坏等于「该事件被
重复执行一次」。

只保留 `retention_seconds` 内的记录（默认 7 天）：`start()` 时清一次，之后每
`sweep_interval_seconds` 在写路径上顺带清一次；也可手工 `await recorder.prune()`
（返回删除条数）。不清的话这张表只增不减（实测单个消费方一天 5 万行）。`ts` 建有
索引（`idx_processed_log_ts`），保留期清理不会全表扫描。

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
- `claim` 模式在派发边界记账；若事件最终未完整派发，记得在 `on_dispatch_abandoned`
  里 `release`，否则重投会被自己的认领挡住。
- 内存版仅进程内有效；跨重启强幂等请用 SQLite 版或业务层天然幂等。
- 标记与业务副作用并非原子：极端的「副作用完成、mark 前崩溃」窗口仍可能重试一次，
  业务 handler 应尽量幂等（或依赖 `SqliteIdempotencyRecorder` 持久化缩小窗口）。
