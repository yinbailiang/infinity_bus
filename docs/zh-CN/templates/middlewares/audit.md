# 审计日志

## 概述

统一审计日志系统由以下部分组成：

- `AuditMiddleware`：`on_publish` 门面。只负责把事件 / 错误包装成 `PendingEvent` /
  `PendingError` 并交给 writer。
- `AuditWriter`：持有有界队列、攒批、写入 / 维护任务、重试退避、状态机、计数器与
  fallback 处理。
- `AuditRecordRenderer`：按 `AuditPayloadConfig` 把 `Pending` 渲染成 `AuditRecord`。
- `AuditSink`：被动存储接口。内置 `JSONLSink` 与 `SQLiteSink`，也可以自行实现。

组装由用户显式完成：

```python
sink = SQLiteSink('audit.db')
renderer = AuditRecordRenderer(AuditPayloadConfig(payload_mode='full'))
writer = AuditWriter(sink, renderer, AuditWriterConfig(batch_size=200))
middleware = AuditMiddleware(writer)
```

发布路径只执行一次 `put_nowait`；记录渲染、攒批、写库与保留策略全部在 writer 的后台任务中完成，
不会阻塞业务发布。

```text
publish ──on_publish──▶ AuditMiddleware ──enqueue──▶ AuditWriter 队列
                                                          │
                                          writer task（渲染 + 攒批）
                                                          │
                                            AuditSink.write(list[AuditRecord])
                                                       ╱          ╲
                                                JSONLSink      SQLiteSink
                                                                  │
                                                        retention / prune / vacuum
```

> **挂载位置**：必须挂在 `on_publish`（发射侧），成功发布产生一条事件记录；
> 发布失败会额外产生一条 `ERROR` 记录。挂到 `before_dispatch` 会按 fanout 成员数重复记录
> （25 个服务就是 25 倍）。

## 快速开始

### SQLite

```python
from infinity_bus import EventBus, MiddlewareChain
from infinity_bus.templates.middlewares import (
    AuditMiddleware,
    AuditPayloadConfig,
    AuditRecordRenderer,
    AuditWriter,
    AuditWriterConfig,
    RetentionPolicy,
    SQLiteSink,
)

sink = SQLiteSink(
    'audit.db',
    retention=RetentionPolicy(
        retention_seconds=7 * 24 * 3600,
        max_rows=1_000_000,
        max_bytes=64 * 1024 * 1024,
    ),
)
renderer = AuditRecordRenderer(
    AuditPayloadConfig(
        payload_mode='full',
        payload_max_bytes=256 * 1024,
    )
)
writer = AuditWriter(sink, renderer, AuditWriterConfig(batch_size=200))
middleware = AuditMiddleware(writer)

chain = MiddlewareChain()
await chain.add(middleware)
bus = EventBus(events, handlers, middleware_chain=chain)
```

### JSONL

```python
from infinity_bus.templates.middlewares import (
    AuditMiddleware,
    AuditRecordRenderer,
    AuditWriter,
    JSONLSink,
)

sink = JSONLSink('events.jsonl')
writer = AuditWriter(sink, AuditRecordRenderer())
middleware = AuditMiddleware(writer)

await chain.add(middleware)
```

## AuditMiddleware

### 构造

```python
AuditMiddleware(writer: AuditWriter)
```

`AuditMiddleware` 没有自己的配置。队列 / 重试 / 生命周期配置属于
`AuditWriterConfig`；负载渲染配置属于 `AuditPayloadConfig`。两个配置均为不可变
Pydantic 模型，构造时一次校验。

### AuditWriterConfig 参数

| 参数 | 默认 | 说明 |
| - | - | - |
| `batch_size` | `100` | 单批最大记录数 |
| `flush_interval` | `0.5` | 单批最长等待（秒），与 `batch_size` 先到先写 |
| `queue_maxsize` | `10_000` | 有界队列；满时丢弃最新记录并计数，绝不阻塞发布 |
| `fallback` | `None` | 降级回调，签名 `(line: str) -> None`；默认 `logging.warning` |
| `reconnect_backoff` | `1.0` | 首次重连退避（秒），之后指数增长 |
| `reconnect_backoff_max` | `30.0` | 退避上限（秒），不得小于 `reconnect_backoff` |
| `close_timeout` | `5.0` | 停机排空队列的超时（秒） |
| `maintenance_interval` | `60.0` | 维护间隔（秒）；`None` 关闭维护 |

> `maintenance_interval=None` 同时关闭独立维护任务与每批写入后的 `sink.maintain()`。

### AuditPayloadConfig 参数

| 参数 | 默认 | 说明 |
| - | - | - |
| `payload_mode` | `'digest'` | `'digest'` 只存大小与 SHA-256；`'full'` 额外存正文 |
| `payload_allowlist` | `()` | 事件名白名单，命中时保留正文（仍受 `payload_max_bytes` 限制） |
| `payload_max_bytes` | `64 * 1024` | 正文超过此值时只保留摘要；`None` 表示不限制 |
| `extra_fields` | `{}` | 每条记录追加的静态字段，例如 `{"env": "prod"}` |

### 观测

| 属性 / 方法 | 说明 |
| - | - |
| `flush()` | 等待当前队列中的记录全部落库或降级 |
| `state` | 当前状态对象；计数器与降级信息都在其中 |
| `writer` | 底层 `AuditWriter` |
| `sink` | 底层 `AuditSink` |

计数直接读取 `middleware.state`（`written_count`、`dropped_count`、
`write_error_count`、`fallback_count`）；`dropped_count` 包含队列溢出、停机后入队、
停机残留与渲染失败。队列长度读取 `middleware.writer.queue_size`。
配置通过 `middleware.writer.config` 与 `middleware.writer.renderer.config` 访问。

### 状态机

真正的状态迁移由 `AuditStateMachine`（`state_machine.py`）负责；`AuditWriter`
持有队列、任务、状态机与 Sink I/O，`AuditMiddleware` 只是发布侧门面。状态机只持有一个状态对象：

| 状态 | 含义 | 专属字段 |
| - | - | - |
| `RunningState` | Sink 可用，正常写入 | - |
| `DegradedState` | Sink 不可用，记录走 fallback | `next_retry_at`、`backoff` |
| `ClosingState` | 停机中，新记录被丢弃 | - |
| `ClosedState` | 停机完成 | - |

共享计数器定义在 `AuditState` 上；状态切换通过 `inherit()` 保留计数，行为分支由
`match` 语句驱动。时钟可注入，退避行为可用同步测试验证。

### 失败与恢复

1. `AuditWriter.start()` 打开 Sink 失败（依赖缺失除外）不会阻断总线启动：进入降级模式，
   队列中的记录走 `fallback`，并按指数退避重连。
2. 写入失败：先调用 `sink.reclaim()` 强制回收空间（忽略保留策略的内部限频），
   再关闭 Sink、进入退避；到点后重新 `open()`，成功即自动恢复写入。
3. 保留策略由独立维护任务按 `maintenance_interval` 周期执行，不依赖事件写入；
   服务空闲或持续写失败时仍会清理。Sink 因写失败关闭时，维护任务会尝试
   重新打开它并继续清理。
4. Sink 的 `maintain()` 失败只记录日志，不影响后续写入。
5. `AuditWriter.stop()` 先停止维护任务，再排空队列（`close_timeout`）并关闭 Sink；
   `AuditMiddleware.on_teardown` 委托给它，可重复调用。
6. `aiosqlite` / `aiofiles` 未安装时，对应的 Sink 在 `open()` 阶段抛出 `ImportError`
   并由总线启动流程上报（不会被静默降级）。

## 审计记录

`AuditRecord` 与存储后端无关，字段如下：

| 字段 | 说明 |
| - | - |
| `ts` | 记录构建时刻（Unix 秒） |
| `emitted_at` | 事件最后一个时间戳（Unix 秒） |
| `service` | 发射者（`event.sources[-1]`） |
| `name` | 事件名称 |
| `event_id` | 事件 ID；发布失败记录为 `ERROR` |
| `event_ids` | 因果事件 ID 链 |
| `sources` | 事件来源链 |
| `timestamps` | 逐跳时间戳（ISO 8601） |
| `payload_size` | 负载 UTF-8 字节数 |
| `payload_sha256` | 负载规范化 JSON 的 SHA-256 |
| `data` | 负载正文；按模式 / 白名单 / 上限决定是否填充 |
| `error` | 发布失败描述 |
| `extra` | 调用方追加的静态字段 |

负载使用键序稳定的紧凑 JSON 序列化；保留正文时满足
`sha256(data.encode('utf-8')) == payload_sha256`。

## 存储后端

### JSONLSink

```python
class JSONLSink(AuditSink):
    def __init__(
        self,
        file_path: str = 'events.jsonl',
        *,
        encoding: str = 'utf-8',
        max_bytes: int | None = None,
        backup_count: int = 3,
    ) -> None
```

- 追加写，每条记录一行紧凑 JSON，`extra_fields` 平铺到顶层，可直接 `tail -f | jq`。
- 父目录不存在时自动创建。
- 设置 `max_bytes` 后，当前文件会轮转为 `.1`（随后 `.2`...），最多保留
  `backup_count` 个历史文件。`maintain()` 在当前文件达到上限时轮转，
  `reclaim()` 在写入失败时强制轮转；`max_bytes=None` 时两者均为空操作。

### SQLiteSink

```python
class SQLiteSink(AuditSink):
    def __init__(
        self,
        db_path: str = ':memory:',
        *,
        table_name: str = 'event_log',
        retention: RetentionPolicy | None = None,
        prune_interval: float = 60.0,
        prune_batch: int = 1000,
        extra_columns: Sequence[str] | None = None,
        busy_timeout: float = 5.0,
        vacuum: bool = True,
    ) -> None
```

| 参数 | 说明 |
| - | - |
| `db_path` | SQLite 路径；`":memory:"` 表示内存库 |
| `table_name` | 表名，默认 `event_log` |
| `retention` | 保留策略；`None` 表示不清理 |
| `prune_interval` | 两次清理的最小间隔（秒） |
| `prune_batch` | 单次清理最多删除的行 / 页 |
| `extra_columns` | 额外列 DDL 片段，值从 `record.extra` 按列名取 |
| `busy_timeout` | 锁等待超时（秒） |
| `vacuum` | 旧库启用 `max_bytes` 时是否执行一次 `VACUUM` 以启用 `auto_vacuum` |

初始化连接时设置：

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
PRAGMA auto_vacuum = INCREMENTAL;
```

默认表结构：

```sql
CREATE TABLE IF NOT EXISTS event_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    ts             REAL    NOT NULL DEFAULT 0,
    emitted_at     REAL,
    service        TEXT    NOT NULL DEFAULT '',
    name           TEXT    NOT NULL,
    event_id       TEXT    NOT NULL,
    event_ids      TEXT    NOT NULL DEFAULT '[]',
    sources        TEXT    NOT NULL DEFAULT '[]',
    timestamps     TEXT    NOT NULL DEFAULT '[]',
    payload_size   INTEGER NOT NULL DEFAULT 0,
    payload_sha256 TEXT,
    data           TEXT,
    error          TEXT
);
CREATE INDEX IF NOT EXISTS idx_event_log_ts ON event_log(ts);
```

旧版 `event_log` 表会在 `open()` 时通过 `ALTER TABLE ADD COLUMN` 增量补齐缺失列，
历史行保留；`SQLiteLoggingMiddleware` 时代写入的 `data` 列继续有效。

### 保留策略

`RetentionPolicy` 的三个上限相互正交，任一超出都从最老的记录开始删除：

| 字段 | 说明 |
| - | - |
| `retention_seconds` | 删除早于 `now - retention_seconds` 的记录 |
| `max_rows` | 目标最大行数；超出部分按 `prune_batch` 分批删除 |
| `max_bytes` | 把主数据库逻辑大小压到目标值以下 |

`max_rows` 使用 `ORDER BY id ... OFFSET`，避免 `MAX(id) - n` 在 AUTOINCREMENT
空洞下越删越多。删除后调用 `PRAGMA incremental_vacuum` 回收空间；旧库首次启用
`max_bytes` 时会执行一次 `VACUUM`（可用 `vacuum=False` 关闭）。

清理是增量的：每次 sweep 每个上限最多删除 `prune_batch` 行，超出较多时需要多个维护
周期才能收敛。迁移时 `ts=0` 的历史行会立即满足 `retention_seconds` 条件。
`incremental_vacuum` 只在 `auto_vacuum` 已启用时真正回收空间；旧库启用 `max_bytes`
时会执行上述一次性 `VACUUM`。

清理不依赖事件写入：`AuditWriter` 的维护任务按 `maintenance_interval` 周期调用
`sink.maintain()`；写入失败时还会调用 `sink.reclaim()` 跳过 `prune_interval` 立即
回收，避免磁盘已满导致持续失败。

## 自定义 Sink

```python
from collections.abc import Sequence

from infinity_bus.templates.middlewares import (
    AuditMiddleware,
    AuditRecord,
    AuditRecordRenderer,
    AuditSink,
    AuditWriter,
)


class PrintSink(AuditSink):
    @property
    def is_open(self) -> bool:
        return True

    async def open(self) -> None:
        pass

    async def write(self, records: Sequence[AuditRecord]) -> None:
        for record in records:
            print(record.to_json_dict())

    async def close(self) -> None:
        pass


writer = AuditWriter(PrintSink(), AuditRecordRenderer())
middleware = AuditMiddleware(writer)
```

自定义 Sink 只需实现 `open / write / close` 与 `is_open`；重连、退避、队列与降级由
`AuditWriter` 统一处理。可选实现 `maintain()` 做周期性维护，以及 `reclaim()`
在写入失败后执行一次跳过限频的强制空间回收。Writer 会串行调用 Sink 方法
（写入与维护不会并发），实现无需额外加锁。

## 从旧版迁移

旧版两个中间件已被移除（breaking）：

| v6 | 新版 |
| - | - |
| `JSONLLoggingMiddleware(path)` | `AuditMiddleware(AuditWriter(JSONLSink(path), AuditRecordRenderer()))` |
| `SQLiteLoggingMiddleware(path)` | `AuditMiddleware(AuditWriter(SQLiteSink(path), AuditRecordRenderer()))` |
| `serialize_data(data)` | 记录内部使用规范化 JSON；需要时用 `json.dumps` |
| `LogFallback` | 仍从 `infinity_bus.templates` 导出 |

行为差异：

- 配置拆分：`AuditWriterConfig` 负责队列 / 攒批 / 重试 / 生命周期，
  `AuditPayloadConfig` 负责负载记录方式；分别交给 writer 与 renderer，两个配置均为
  不可变模型、构造时一次校验。
- 新 API 默认 `payload_mode='digest'`，不保存正文；需要旧行为时给 renderer 传
  `AuditPayloadConfig(payload_mode='full')`。
- 写入改为后台攒批，不再在发布路径上同步落库；停机与测试可用
  `await middleware.flush()`（或 `await writer.flush()`）等待落库。
- `SQLiteLoggingMiddleware` 的 `is_connect` 永久降级 bug 不再存在；`SQLiteSink.is_open`
  在关闭后返回 `False`。

## 完整示例

参见 `tests/templates/middlewares/audit/` 与 `tests/templates/middlewares/audit/sinks/`：
记录模型、JSONL 后端、SQLite 保留策略与磁盘回收，
writer / middleware 攒批、降级、重连、停机排空等场景均有覆盖。
