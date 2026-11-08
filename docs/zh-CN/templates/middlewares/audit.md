# 审计日志

## 概述

统一审计日志系统由两部分组成：

- `AuditMiddleware`：挂在 `on_publish` 的管线层，负责有界队列、攒批、失败退避重连、
  停机排空与降级计数。
- `AuditSink`：存储后端接口。内置 `JSONLSink` 与 `SQLiteSink`，也可以自行实现。

发布路径只执行一次 `put_nowait`；记录构建、序列化、写库与保留策略全部在后台任务中完成，
不会阻塞业务发布。

```text
publish ──on_publish──▶ AuditMiddleware ──put_nowait──▶ 有界队列
                                                          │
                                          writer task（攒批 + flush_interval）
                                                          │
                                            AuditSink.write(list[AuditRecord])
                                                       ╱          ╲
                                                JSONLSink      SQLiteSink
                                                                  │
                                                        retention / prune / vacuum
```

> **挂载位置**：必须挂在 `on_publish`（发射侧），每次发布恰好记录一次。
> 挂到 `before_dispatch` 会按 fanout 成员数重复记录（25 个服务就是 25 倍）。

## 快速开始

### SQLite

```python
from infinity_bus import EventBus, MiddlewareChain
from infinity_bus.templates.middlewares import AuditMiddleware, RetentionPolicy, SQLiteSink

chain = MiddlewareChain()
await chain.add(
    AuditMiddleware(
        SQLiteSink(
            'audit.db',
            retention=RetentionPolicy(
                retention_seconds=7 * 24 * 3600,
                max_rows=1_000_000,
                max_bytes=64 * 1024 * 1024,
            ),
        )
    )
)

bus = EventBus(events, handlers, middleware_chain=chain)
```

### JSONL

```python
from infinity_bus.templates.middlewares import AuditMiddleware, JSONLSink

await chain.add(AuditMiddleware(JSONLSink('events.jsonl')))
```

## AuditMiddleware

### 参数

| 参数 | 默认 | 说明 |
| - | - | - |
| `sink` | 必填 | 存储后端 |
| `batch_size` | `100` | 单批最大记录数 |
| `flush_interval` | `0.5` | 单批最长等待（秒），与 `batch_size` 先到先写 |
| `queue_maxsize` | `10_000` | 有界队列；满时丢弃最新记录并计数，绝不阻塞发布 |
| `payload_mode` | `'digest'` | `'digest'` 只存大小与 SHA-256；`'full'` 额外存正文 |
| `payload_allowlist` | `()` | 事件名白名单，命中时始终存正文 |
| `payload_max_bytes` | `64 * 1024` | 正文超过此值时只保留摘要 |
| `fallback` | `None` | 降级回调，签名 `(line: str) -> None`；默认 `logging.warning` |
| `extra_fields` | `None` | 每条记录追加的静态字段，例如 `{"env": "prod"}` |
| `reconnect_backoff` | `1.0` | 首次重连退避（秒），之后指数增长 |
| `reconnect_backoff_max` | `30.0` | 退避上限（秒） |
| `close_timeout` | `5.0` | 停机排空队列的超时（秒） |
| `maintenance_interval` | `60.0` | 独立周期维护间隔（秒）；`None` 关闭。即使没有事件也会触发保留策略 |

### 观测

| 属性 / 方法 | 说明 |
| - | - |
| `flush()` | 等待当前队列中的记录全部落库或降级 |
| `written_count` | 成功写入 Sink 的记录数 |
| `dropped_count` | 因队列满 / 关闭后调用而丢弃的记录数 |
| `write_error_count` | 写入失败批次数 |
| `fallback_count` | 转交 fallback 的记录数 |
| `degraded` | 当前是否处于「存储不可用、只走 fallback」状态 |
| `queue_size` | 当前待写入队列长度 |

### 失败与恢复

1. `on_setup` 打开 Sink 失败（依赖缺失除外）不会阻断总线启动：进入降级模式，
   队列中的记录走 `fallback`，并按指数退避重连。
2. 写入失败：先调用 `sink.reclaim()` 强制回收空间（忽略保留策略的内部限频），
   再关闭 Sink、进入退避；到点后重新 `open()`，成功即自动恢复写入。
3. 保留策略由独立维护任务按 `maintenance_interval` 周期执行，不依赖事件写入；
   服务空闲或持续写失败时仍会清理。
4. Sink 的 `maintain()` 失败只记录日志，不影响后续写入。
5. 停机时先停止维护任务，再排空队列（`close_timeout`）并关闭 Sink；`on_teardown`
   可重复调用。
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
    def __init__(self, file_path: str = 'events.jsonl', *, encoding: str = 'utf-8') -> None
```

- 追加写，每条记录一行紧凑 JSON，`extra_fields` 平铺到顶层，可直接 `tail -f | jq`。
- 父目录不存在时自动创建。
- 当前不做文件轮转；长期运行建议使用 `SQLiteSink` 的保留策略。

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
| `max_rows` | 只保留最新 `max_rows` 行 |
| `max_bytes` | 把主数据库逻辑大小压到目标值以下 |

`max_rows` 使用 `ORDER BY id ... OFFSET`，避免 `MAX(id) - n` 在 AUTOINCREMENT
空洞下越删越多。删除后调用 `PRAGMA incremental_vacuum` 回收空间；旧库首次启用
`max_bytes` 时会执行一次 `VACUUM`（可用 `vacuum=False` 关闭）。

清理不依赖事件写入：`AuditMiddleware` 的维护任务按 `maintenance_interval` 周期调用
`sink.maintain()`；写入失败时还会调用 `sink.reclaim()` 跳过 `prune_interval` 立即
回收，避免磁盘已满导致持续失败。

## 自定义 Sink

```python
from collections.abc import Sequence

from infinity_bus.templates.middlewares import AuditRecord, AuditSink


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
```

自定义 Sink 只需实现 `open / write / close` 与 `is_open`；重连、退避、队列与降级由
`AuditMiddleware` 统一处理。可选实现 `maintain()` 做周期性维护，以及 `reclaim()`
在写入失败后执行一次跳过限频的强制空间回收。中间件会串行调用 Sink 方法
（写入与维护不会并发），实现无需额外加锁。

## 从旧版迁移

旧版两个中间件已被移除（breaking）：

| v6 | 新版 |
| - | - |
| `JSONLLoggingMiddleware(path)` | `AuditMiddleware(JSONLSink(path))` |
| `SQLiteLoggingMiddleware(path)` | `AuditMiddleware(SQLiteSink(path))` |
| `serialize_data(data)` | 记录内部使用规范化 JSON；需要时用 `json.dumps` |
| `LogFallback` | 仍从 `infinity_bus.templates` 导出 |

行为差异：

- 新 API 默认 `payload_mode='digest'`，不保存正文；需要旧行为时显式传 `payload_mode='full'`。
- 写入改为后台攒批，不再在发布路径上同步落库；停机与测试可用 `await middleware.flush()` 等待落库。
- `SQLiteLoggingMiddleware` 的 `is_connect` 永久降级 bug 不再存在；`SQLiteSink.is_open`
  在关闭后返回 `False`。

## 完整示例

参见 `tests/templates/middlewares/audit/`：记录模型、JSONL 后端、SQLite 保留策略与磁盘回收、
中间件攒批 / 降级 / 重连 / 停机排空等场景均有覆盖。
