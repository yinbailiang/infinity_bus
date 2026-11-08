# Audit Logging

## Overview

The audit logging system has two layers:

- `AuditMiddleware`: an `on_publish` middleware that owns the bounded queue, batching,
  reconnect backoff, shutdown draining, and drop/error counters.
- `AuditSink`: the storage interface. `JSONLSink` and `SQLiteSink` are built in;
  custom backends only need to implement the interface.

The publish path performs a single `put_nowait`; record building, serialization,
writes, and retention all run in a background task, so publishing is never blocked.

```text
publish ──on_publish──▶ AuditMiddleware ──put_nowait──▶ bounded queue
                                                          │
                                          writer task (batching + flush_interval)
                                                          │
                                            AuditSink.write(list[AuditRecord])
                                                       ╱          ╲
                                                JSONLSink      SQLiteSink
                                                                  │
                                                        retention / prune / vacuum
```

> **Mount point**: always mount on `on_publish` (emission side) for exactly one
> record per publish. Mounting on `before_dispatch` multiplies records by the
> fanout member count.

## Quick Start

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

### Parameters

| Parameter | Default | Description |
| - | - | - |
| `sink` | required | Storage backend |
| `batch_size` | `100` | Maximum records per batch |
| `flush_interval` | `0.5` | Maximum wait (seconds) before flushing a batch |
| `queue_maxsize` | `10_000` | Bounded queue; drops newest records and counts them when full |
| `payload_mode` | `'digest'` | `'digest'`: size + SHA-256 only; `'full'`: also store the body |
| `payload_allowlist` | `()` | Event names that always keep the body |
| `payload_max_bytes` | `64 * 1024` | Bodies above this size fall back to digest |
| `fallback` | `None` | Callback `(line: str) -> None`; defaults to `logging.warning` |
| `extra_fields` | `None` | Static fields added to every record, e.g. `{"env": "prod"}` |
| `reconnect_backoff` | `1.0` | Initial reconnect backoff (seconds), exponential afterwards |
| `reconnect_backoff_max` | `30.0` | Backoff ceiling (seconds) |
| `close_timeout` | `5.0` | Queue drain timeout during shutdown (seconds) |
| `maintenance_interval` | `60.0` | Independent maintenance interval (seconds); `None` disables it. Retention runs even without events |

### Observability

| Member | Description |
| - | - |
| `flush()` | Wait until queued records are written or downgraded |
| `written_count` | Records successfully written to the sink |
| `dropped_count` | Records dropped due to overflow or post-shutdown calls |
| `write_error_count` | Failed write batches |
| `fallback_count` | Records handed to the fallback callback |
| `degraded` | Whether the sink is unavailable and only fallback is active |
| `queue_size` | Current queue length |

### Failure and recovery

1. If `on_setup` cannot open the sink (other than a missing dependency), the bus still
   starts; records go to `fallback` and the sink is retried with exponential backoff.
2. On a write failure, the middleware calls `sink.reclaim()` first to force space
   reclamation (bypassing the retention rate limit), then closes the sink and backs off;
   a successful `open()` restores normal writes.
3. Retention runs on an independent maintenance task every `maintenance_interval`, not
   only after writes; idle services and persistent write failures still get cleaned up.
   If a write failure closed the sink, the maintenance task reopens it and keeps pruning.
4. A `maintain()` failure is logged and never affects subsequent writes.
5. Shutdown stops the maintenance task, drains the queue (`close_timeout`), then closes
   the sink; `on_teardown` is idempotent.
6. Missing `aiosqlite` / `aiofiles` raises `ImportError` from `open()` and fails bus
   startup rather than silently degrading.

## Audit Record

`AuditRecord` is storage-agnostic:

| Field | Description |
| - | - |
| `ts` | Record build time (Unix seconds) |
| `emitted_at` | Last event timestamp (Unix seconds) |
| `service` | Emitter (`event.sources[-1]`) |
| `name` | Event name |
| `event_id` | Event id; publish failures use `ERROR` |
| `event_ids` | Causal event id chain |
| `sources` | Event source chain |
| `timestamps` | Per-hop timestamps (ISO 8601) |
| `payload_size` | UTF-8 byte length of the payload |
| `payload_sha256` | SHA-256 of the canonical JSON payload |
| `data` | Payload body, depending on mode / allowlist / size limit |
| `error` | Publish failure description |
| `extra` | Static fields supplied by the caller |

Payloads use canonical, key-sorted compact JSON. When the body is stored,
`sha256(data.encode('utf-8')) == payload_sha256`.

## Sinks

### JSONLSink

```python
class JSONLSink(AuditSink):
    def __init__(self, file_path: str = 'events.jsonl', *, encoding: str = 'utf-8') -> None
```

- Append-only; one compact JSON object per line, with `extra_fields` flattened.
  Works directly with `tail -f | jq`.
- Creates the parent directory when needed.
- File rotation is not implemented; use `SQLiteSink` retention for long-running services.

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

| Parameter | Description |
| - | - |
| `db_path` | SQLite path; `":memory:"` for an in-memory database |
| `table_name` | Table name, default `event_log` |
| `retention` | Retention policy; `None` disables pruning |
| `prune_interval` | Minimum seconds between pruning sweeps |
| `prune_batch` | Maximum rows / pages deleted per sweep |
| `extra_columns` | Extra column DDL fragments; values come from `record.extra` by name |
| `busy_timeout` | Lock wait timeout (seconds) |
| `vacuum` | Run a one-time `VACUUM` on legacy databases when `max_bytes` is enabled |

Connection setup:

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
PRAGMA auto_vacuum = INCREMENTAL;
```

Default schema:

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

Legacy `event_log` tables are migrated in place with `ALTER TABLE ADD COLUMN`;
existing rows are preserved and the old `data` column keeps working.

### Retention

`RetentionPolicy` has three orthogonal caps; any excess deletes the oldest records first:

| Field | Description |
| - | - |
| `retention_seconds` | Delete records older than `now - retention_seconds` |
| `max_rows` | Keep only the newest `max_rows` rows |
| `max_bytes` | Shrink the main database below the target logical size |

`max_rows` uses `ORDER BY id ... OFFSET` instead of `MAX(id) - n`, which would delete
too much once AUTOINCREMENT leaves gaps. Deletions are followed by
`PRAGMA incremental_vacuum`; legacy databases run a one-time `VACUUM` when
`max_bytes` is first enabled (`vacuum=False` opts out).

Cleanup does not depend on event writes: `AuditMiddleware` calls `sink.maintain()` every
`maintenance_interval`, and on write failure it calls `sink.reclaim()` to bypass
`prune_interval` and reclaim space immediately, avoiding a permanent disk-full loop.

## Custom Sink

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

A custom sink implements `open / write / close` and `is_open`; retries, backoff,
queueing, and fallback stay in `AuditMiddleware`. Override `maintain()` for periodic
housekeeping and `reclaim()` for a forced, rate-limit-free cleanup after write failures.
The middleware serializes sink calls, so implementations do not need their own lock.

## Migration from v6

The two old middlewares are removed (breaking):

| v6 | New API |
| - | - |
| `JSONLLoggingMiddleware(path)` | `AuditMiddleware(JSONLSink(path))` |
| `SQLiteLoggingMiddleware(path)` | `AuditMiddleware(SQLiteSink(path))` |
| `serialize_data(data)` | Records use canonical JSON internally; use `json.dumps` when needed |
| `LogFallback` | Still exported from `infinity_bus.templates` |

Behavior changes:

- The new API defaults to `payload_mode='digest'`; pass `payload_mode='full'` to keep bodies.
- Writes are batched in the background; use `await middleware.flush()` in tests and at checkpoints.
- The old `is_connect` permanent-failure bug is gone: `SQLiteSink.is_open` returns `False`
  after `close()`.

## Examples

See `tests/templates/middlewares/audit/`: record building, the JSONL backend, SQLite retention and
disk reclamation, plus batching / fallback / reconnect / shutdown draining in the middleware.
