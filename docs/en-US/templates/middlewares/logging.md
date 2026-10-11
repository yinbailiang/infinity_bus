# Logging Middlewares

## Overview

The logging middlewares provide two out-of-the-box event publishing record solutions:
**JSONL file logging** (`JSONLLoggingMiddleware`) and **SQLite database logging**
(`SQLiteLoggingMiddleware`). Both record events asynchronously via the `on_publish` hook
after successful enqueue, without affecting the normal event publishing flow.

Both middlewares include built-in **degradation mechanisms**: when the target storage
becomes unavailable, they automatically fall back to `logging.warning` or a user-provided
callback, ensuring logging reliability never blocks business operations.

---

## Use Cases

- **Audit trails**: Record complete event chain information (IDs, source chain, timestamps).
- **Troubleshooting**: Trace event flows to identify root causes.
- **Data analysis**: Import event logs into data warehouses for offline analysis.
- **Compliance**: Meet regulatory requirements for persistent operation records.

---

## JSONLLoggingMiddleware

### Function Signature

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

| Parameter | Type | Description |
| - | - | - |
| `file_path` | `str` | JSONL file path, default `"events.jsonl"`. Parent directories are auto-created if they don't exist. |
| `fallback` | `Optional[LogFallback]` | Degradation callback with signature `(line: str) -> None`. Falls back to `logging.warning` if `None`. |
| `extra_fields` | `Optional[Dict[str, Any]]` | Additional static fields appended to each record, e.g., `{"service": "api-gateway"}`. |

### Recorded Fields

Each JSONL line contains the following fields:

| Field | Description |
| - | - |
| `name` | Event name |
| `source` | The most recent publisher |
| `data` | Payload data (JSON serialized) |
| `event_id` | Unique event ID |
| `event_ids` | Event chain ID list |
| `sources` | Source chain list |
| `timestamp` | Record time (UTC ISO 8601) |
| `...(extra_fields)` | User-defined static fields |

### Usage Examples

```python
from infinity_bus.templates.middlewares import JSONLLoggingMiddleware
from infinity_bus import MiddlewareChain

# Basic usage
mw = JSONLLoggingMiddleware("events.jsonl")
chain = MiddlewareChain()
chain.add(mw)

# With static fields
mw = JSONLLoggingMiddleware(
    "events.jsonl",
    extra_fields={"service": "order-service", "env": "production"},
)

# With degradation callback
def fallback_handler(line: str) -> None:
    # Send to remote logging service
    send_to_remote(line)

mw = JSONLLoggingMiddleware(
    "events.jsonl",
    fallback=fallback_handler,
)
```

### Notes

1. The parent directory of `file_path` is auto-created during `on_setup`.
2. Once a write fails, the middleware permanently degrades (`_ready = False`); all
   subsequent events only trigger the fallback.
3. File writes execute on a background thread via `asyncio.to_thread`, not blocking the
   event loop.
4. Suitable for human-readable scenarios; use `tail -f events.jsonl | jq` for real-time
   viewing.

---

## SQLiteLoggingMiddleware

### Function Signature

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

| Parameter | Type | Description |
| - | - | - |
| `db_path` | `str` | SQLite database path. `":memory:"` for in-memory database. |
| `table_name` | `str` | Table name, default `"event_log"`. |
| `extra_columns` | `Optional[List[str]]` | Additional column definitions, e.g., `["user_agent TEXT", "trace_id TEXT"]`. |
| `fallback` | `Optional[LogFallback]` | Degradation callback with signature `(line: str) -> None`. Falls back to `logging.warning` if `None`. |

### Default Table Schema

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

### Usage Examples

```python
from infinity_bus.templates.middlewares import SQLiteLoggingMiddleware
from infinity_bus import MiddlewareChain

# In-memory database (suitable for testing)
mw = SQLiteLoggingMiddleware(":memory:")

# File database
mw = SQLiteLoggingMiddleware("events.db")

# Custom table name and extra columns
mw = SQLiteLoggingMiddleware(
    "events.db",
    table_name="audit_log",
    extra_columns=["user_agent TEXT", "trace_id TEXT"],
)

chain = MiddlewareChain()
chain.add(mw)
```

### Notes

1. Requires the `aiosqlite` package. Falls back to degraded mode if not installed.
2. WAL mode and `synchronous=NORMAL` are enabled by default, balancing write performance
   and safety.
3. On write failure, permanently degrades — consistent with JSONL middleware behavior.
4. Suitable for structured query scenarios (e.g., filtering by time range or event name).

---

## Full Example

See `tests/templates/middlewares/logging_test.py`

Contains test cases for in-memory database, file database, JSONL file writing, and
degradation handling scenarios.
