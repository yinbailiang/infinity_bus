# 日志中间件文档

## 概述

日志中间件提供了两种开箱即用的事件发布记录方案：**JSONL 文件日志**（`JSONLLoggingMiddleware`）和 **SQLite 数据库日志**（`SQLiteLoggingMiddleware`）。两者均通过 `on_publish` 钩子在事件成功入队后异步记录，不影响事件发布的正常流程。

两种中间件都内置了**降级机制**：当目标存储不可用时，自动 fallback 到 `logging.warning` 或用户提供的自定义回调，确保日志记录的可靠性不会阻塞业务。

---

## 使用场景

- **审计追踪**：记录所有事件的完整链路信息（ID、来源链、时间戳）。
- **故障排查**：回溯事件流以定位问题根因。
- **数据分析**：将事件日志导入数据仓库进行离线分析。
- **合规要求**：满足监管对操作记录的持久化要求。

---

## JSONLLoggingMiddleware

### 函数签名

```python
class JSONLLoggingMiddleware(Middleware):
    def __init__(
        self,
        file_path: str = 'events.jsonl',
        *,
        fallback: Optional[LogFallback] = None,
        extra_fields: Optional[Dict[str, Any]] = None,
    ) -> None
```

| 参数 | 类型 | 说明 |
| - | - | - |
| `file_path` | `str` | JSONL 文件路径，默认 `"events.jsonl"`。父目录不存在时自动创建。 |
| `fallback` | `Optional[LogFallback]` | 降级回调，签名 `(line: str) -> None`。为 `None` 时使用 `logging.warning`。 |
| `extra_fields` | `Optional[Dict[str, Any]]` | 除默认字段外追加的静态字段，例如 `{"service": "api-gateway"}`。 |

### 记录字段

每条 JSONL 行包含以下字段：

| 字段 | 说明 |
| - | - |
| `name` | 事件名称 |
| `source` | 最近一个发布者 |
| `data` | 负载数据（JSON 序列化） |
| `event_id` | 事件唯一 ID |
| `event_ids` | 事件链 ID 列表 |
| `sources` | 来源链列表 |
| `timestamp` | 记录时间（UTC ISO 8601） |
| `...(extra_fields)` | 用户自定义静态字段 |

### 使用示例

```python
from infinity_bus.templates.middlewares import JSONLLoggingMiddleware
from infinity_bus import MiddlewareChain

# 基础用法
mw = JSONLLoggingMiddleware("events.jsonl")
chain = MiddlewareChain()
chain.add(mw)

# 带静态字段
mw = JSONLLoggingMiddleware(
    "events.jsonl",
    extra_fields={"service": "order-service", "env": "production"},
)

# 带降级回调
def fallback_handler(line: str) -> None:
    # 发送到远程日志服务
    send_to_remote(line)

mw = JSONLLoggingMiddleware(
    "events.jsonl",
    fallback=fallback_handler,
)
```

### 注意事项

1. 文件路径的父目录会在 `on_setup` 时自动创建。
2. 一旦写入失败，该中间件会永久降级（`_ready = False`），后续所有事件仅触发 fallback。
3. 文件写入通过 `asyncio.to_thread` 在后台线程执行，不阻塞事件循环。
4. 适合人类可读场景，可直接使用 `tail -f events.jsonl | jq` 实时查看。

---

## SQLiteLoggingMiddleware

### 函数签名

```python
class SQLiteLoggingMiddleware(Middleware):
    def __init__(
        self,
        db_path: str = ':memory:',
        *,
        table_name: str = 'event_log',
        extra_columns: Optional[List[str]] = None,
        fallback: Optional[LogFallback] = None,
    ) -> None
```

| 参数 | 类型 | 说明 |
| - | - | - |
| `db_path` | `str` | SQLite 数据库路径。`":memory:"` 表示内存数据库。 |
| `table_name` | `str` | 表名，默认 `"event_log"`。 |
| `extra_columns` | `Optional[List[str]]` | 额外列定义，例如 `["user_agent TEXT", "trace_id TEXT"]`。 |
| `fallback` | `Optional[LogFallback]` | 降级回调，签名 `(line: str) -> None`。为 `None` 时使用 `logging.warning`。 |

### 默认表结构

```sql
CREATE TABLE IF NOT EXISTS event_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL,
    sources     TEXT    NOT NULL DEFAULT '[]',
    data        TEXT,
    event_id    TEXT    NOT NULL,
    event_ids   TEXT    NOT NULL DEFAULT '[]',
    timestamps  TEXT    NOT NULL DEFAULT '[]'
)
```

### 使用示例

```python
from infinity_bus.templates.middlewares import SQLiteLoggingMiddleware
from infinity_bus import MiddlewareChain

# 内存数据库（适合测试）
mw = SQLiteLoggingMiddleware(":memory:")

# 文件数据库
mw = SQLiteLoggingMiddleware("events.db")

# 自定义表名和额外列
mw = SQLiteLoggingMiddleware(
    "events.db",
    table_name="audit_log",
    extra_columns=["user_agent TEXT", "trace_id TEXT"],
)

chain = MiddlewareChain()
chain.add(mw)
```

### 注意事项

1. 依赖 `aiosqlite` 包。若未安装，初始化阶段会降级运行。
2. 默认启用 WAL 模式和 `synchronous=NORMAL`，兼顾写入性能与安全性。
3. 写入失败后会永久降级，与 JSONL 中间件行为一致。
4. 适合需要结构化查询的场景（如按时间范围、事件名过滤）。

---

## 完整示例

参见 `tests/templates/middlewares/logging_test.py`

其中包含了内存数据库、文件数据库、JSONL 文件写入、降级处理等场景的测试用例。
