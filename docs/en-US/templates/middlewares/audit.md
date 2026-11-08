# Audit Logging

## Overview

The audit logging system has four parts:

- `AuditMiddleware`: the `on_publish` facade. It only wraps events / errors into
  `PendingEvent` / `PendingError` and delegates to a writer.
- `AuditWriter`: owns the bounded queue, batching, writer / maintenance tasks,
  reconnect backoff, state machine, counters, and fallback handling.
- `AuditRecordRenderer`: converts `Pending` items into `AuditRecord` using
  `AuditPayloadConfig`.
- `AuditSink`: the passive storage interface. `JSONLSink` and `SQLiteSink` are built in;
  custom backends only need to implement the interface.

Assembly is explicit:

```python
sink = SQLiteSink('audit.db')
renderer = AuditRecordRenderer(AuditPayloadConfig(payload_mode='full'))
writer = AuditWriter(sink, renderer, AuditWriterConfig(batch_size=200))
middleware = AuditMiddleware(writer)
```

The publish path performs a single `put_nowait`; rendering, batching, writes, and
retention all run in the writer's background task, so publishing is never blocked.

```text
publish ──on_publish──▶ AuditMiddleware ──enqueue──▶ AuditWriter queue
                                                          │
                                          writer task (renderer + batching)
                                                          │
                                            AuditSink.write(list[AuditRecord])
                                                       ╱          ╲
                                                JSONLSink      SQLiteSink
                                                                  │
                                                        retention / prune / vacuum
```

> **Mount point**: always mount the middleware on `on_publish` (emission side) so each
> successful publish produces one event record. A failed publish may additionally
> produce an `ERROR` record. Mounting on `before_dispatch` multiplies records by the
> fanout member count.

## Quick Start

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

### Construction

```python
AuditMiddleware(writer: AuditWriter)
```

`AuditMiddleware` has no configuration of its own. Queue / retry / lifecycle settings
belong to `AuditWriterConfig`; payload rendering settings belong to
`AuditPayloadConfig`. Both configs are frozen and validated once by Pydantic.

### AuditWriterConfig Parameters

| Parameter | Default | Description |
| - | - | - |
| `batch_size` | `100` | Maximum records per batch |
| `flush_interval` | `0.5` | Maximum wait (seconds) before flushing a batch |
| `queue_maxsize` | `10_000` | Bounded queue; drops newest records and counts them when full |
| `fallback` | `None` | Callback `(line: str) -> None`; defaults to `logging.warning` |
| `reconnect_backoff` | `1.0` | Initial reconnect backoff (seconds), exponential afterwards |
| `reconnect_backoff_max` | `30.0` | Backoff ceiling (seconds); must be >= `reconnect_backoff` |
| `close_timeout` | `5.0` | Queue drain timeout during shutdown (seconds) |
| `maintenance_interval` | `60.0` | Maintenance interval (seconds); `None` disables maintenance |

> `maintenance_interval=None` disables both the periodic task and the per-write
> `sink.maintain()` call.

### AuditPayloadConfig Parameters

| Parameter | Default | Description |
| - | - | - |
| `payload_mode` | `'digest'` | `'digest'`: size + SHA-256 only; `'full'`: also store the body |
| `payload_allowlist` | `()` | Event names that keep the body (still subject to `payload_max_bytes`) |
| `payload_max_bytes` | `64 * 1024` | Bodies above this size fall back to digest; `None` disables the limit |
| `extra_fields` | `{}` | Static fields added to every record, e.g. `{"env": "prod"}` |

### Observability

| Member | Description |
| - | - |
| `flush()` | Wait until queued records are written or downgraded |
| `state` | Current state object; all counters and degradation info live on it |
| `writer` | The underlying `AuditWriter` |
| `sink` | The underlying `AuditSink` |

Read counters directly from `middleware.state` (`written_count`, `dropped_count`,
`write_error_count`, `fallback_count`); `dropped_count` includes queue overflow,
post-shutdown enqueues, shutdown leftovers, and render failures. Read queue depth from
`middleware.writer.queue_size`. Configuration is accessed through
`middleware.writer.config` and `middleware.writer.renderer.config`.

### State machine

The real transition logic lives in `AuditStateMachine` (`state_machine.py`);
`AuditWriter` owns the queue, tasks, state machine, and sink I/O, while
`AuditMiddleware` is only the publish-side facade. The state machine holds a single
state object:

| State | Meaning | State-specific fields |
| - | - | - |
| `RunningState` | Sink is available; normal writes | - |
| `DegradedState` | Sink is unavailable; records go to fallback | `next_retry_at`, `backoff` |
| `ClosingState` | Shutdown in progress; new records are dropped | - |
| `ClosedState` | Shutdown finished | - |

Shared counters live on `AuditState`; `inherit()` carries them across state transitions,
and `match` statements drive the state-specific behavior. The clock is injectable, so
backoff behavior can be tested synchronously.

### Failure and recovery

1. If `AuditWriter.start()` cannot open the sink (other than a missing dependency), the
   writer still starts; records go to `fallback` and the sink is retried with exponential
   backoff.
2. On a write failure, the writer calls `sink.reclaim()` first to force space
   reclamation (bypassing the retention rate limit), then closes the sink and backs off;
   a successful `open()` restores normal writes.
3. Retention runs on an independent maintenance task every `maintenance_interval`, not
   only after writes; idle services and persistent write failures still get cleaned up.
   If a write failure closed the sink, the maintenance task reopens it and keeps pruning.
4. A `maintain()` failure is logged and never affects subsequent writes.
5. `AuditWriter.stop()` stops the maintenance task, drains the queue (`close_timeout`),
   then closes the sink; `AuditMiddleware.on_teardown` delegates to it and is idempotent.
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
    def __init__(
        self,
        file_path: str = 'events.jsonl',
        *,
        encoding: str = 'utf-8',
        max_bytes: int | None = None,
        backup_count: int = 3,
    ) -> None
```

- Append-only; one compact JSON object per line, with `extra_fields` flattened.
  Works directly with `tail -f | jq`.
- Creates the parent directory when needed.
- When `max_bytes` is set, the active file rotates to `.1` (then `.2`, ...); at most
  `backup_count` historical files are kept. `maintain()` rotates a full active file,
  while `reclaim()` forces rotation on write failure. Both are no-ops when
  `max_bytes=None`.

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
| `max_rows` | Target maximum rows; excess is pruned in `prune_batch` increments |
| `max_bytes` | Shrink the main database below the target logical size |

`max_rows` uses `ORDER BY id ... OFFSET` instead of `MAX(id) - n`, which would delete
too much once AUTOINCREMENT leaves gaps. Deletions are followed by
`PRAGMA incremental_vacuum`; legacy databases run a one-time `VACUUM` when
`max_bytes` is first enabled (`vacuum=False` opts out).

Pruning is incremental: each sweep removes at most `prune_batch` rows per cap, so a
large excess may take several maintenance intervals to converge. Legacy rows migrated
with `ts=0` are immediately eligible for `retention_seconds`. `incremental_vacuum`
only reclaims space when `auto_vacuum` is enabled; legacy databases enabling
`max_bytes` perform the one-time `VACUUM` described above.

Cleanup does not depend on event writes: `AuditWriter` calls `sink.maintain()` every
`maintenance_interval`, and on write failure it calls `sink.reclaim()` to bypass
`prune_interval` and reclaim space immediately, avoiding a permanent disk-full loop.

## Custom Sink

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

A custom sink implements `open / write / close` and `is_open`; retries, backoff,
queueing, and fallback stay in `AuditWriter`. Override `maintain()` for periodic
housekeeping and `reclaim()` for a forced, rate-limit-free cleanup after write failures.
The writer serializes sink calls, so implementations do not need their own lock.

## Migration from v6

The two old middlewares are removed (breaking):

| v6 | New API |
| - | - |
| `JSONLLoggingMiddleware(path)` | `AuditMiddleware(AuditWriter(JSONLSink(path), AuditRecordRenderer()))` |
| `SQLiteLoggingMiddleware(path)` | `AuditMiddleware(AuditWriter(SQLiteSink(path), AuditRecordRenderer()))` |
| `serialize_data(data)` | Records use canonical JSON internally; use `json.dumps` when needed |
| `LogFallback` | Still exported from `infinity_bus.templates` |

Behavior changes:

- Settings are split: `AuditWriterConfig` covers queue / batching / retry / lifecycle,
  while `AuditPayloadConfig` covers payload recording. Pass each config to the module
  that consumes it: writer and renderer respectively. Both are frozen and validated once.
- The new API defaults to `payload_mode='digest'`; pass
  `AuditPayloadConfig(payload_mode='full')` to the renderer to keep bodies.
- Writes are batched in the background; use `await middleware.flush()` (or
  `await writer.flush()`) in tests and at checkpoints.
- The old `is_connect` permanent-failure bug is gone: `SQLiteSink.is_open` returns `False`
  after `close()`.

## Examples

See `tests/templates/middlewares/audit/` and `tests/templates/middlewares/audit/sinks/`:
record building, the JSONL backend, SQLite retention and disk reclamation, plus
writer / middleware batching, fallback, reconnect, and shutdown draining.
