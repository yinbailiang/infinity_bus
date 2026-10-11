# Mailbox Handler

## Overview

`MailboxHandler` is a **mailbox-pattern** `EventHandler` abstract base class. It enqueues incoming events into an internal mailbox queue, and subclasses implement the `process()` method to dequeue and handle events one at a time. Ideal for scenarios requiring **serial consumption**, **backpressure control**, or a **custom task loop**.

Unlike directly subclassing `EventHandler` and implementing `handle()`, `MailboxHandler` decouples "receiving events" and "processing events" into two separate coroutines:

- `handle()` — runs in the bus dispatch context, only responsible for enqueuing events into the mailbox.
- `process()` — runs in its own `asyncio.Task`, consuming events from the queue at its own pace.

---

## Use Cases

- Scenarios requiring **strictly serial** event processing (avoid concurrent execution within the same handler).
- **Backpressure control** for burst traffic (via bounded queue capacity).
- **Custom event loops** (e.g., periodic cleanup, timed batch processing, state-machine-driven consumption).
- Emulating the **mailbox** semantics of the Actor model.
- Aggregating events from multiple sources into a single, ordered consumer.

---

## Class Signature

```python
class MailboxHandler(EventHandler, ABC):
    def __init__(
        self,
        subscriptions: list[str | Regex],
        config: MailboxConfig | None = None,
    ) -> None: ...

    async def process(self) -> None: ...
    async def get(self) -> tuple[Event, EventBus.Proxy]: ...
```

| Member | Type | Description |
| - | - | - |
| `subscriptions` | `list[str \| Regex]` | Event patterns to subscribe to (kept as-is; `ShutdownEvent` is no longer auto-appended). |
| `config` | `MailboxConfig \| None` | Mailbox configuration. Uses defaults when `None`. |
| `process()` | `@abstractmethod async` | Custom task loop that subclasses must implement. Use `await self.get()` to fetch the next event. |
| `get()` | `async → (Event, Proxy)` | Dequeues the next `(event, bus proxy)` from the mailbox. Blocks when the queue is empty. |
| `on_activate(bus)` | hook | Starts the `process()` task when the registry is bound to a bus (`activate`). |
| `on_deactivate(bus)` | hook | Cancels the task and clears the backlog on bus unbind / unregister. Late in-flight `put()`s after shutdown are dropped (rolled back) rather than left as orphaned backlog. |
| `bus` | `EventBus \| None` | The bound `EventBus` instance, available after activation (`on_activate`). |
| `is_running` | `bool` | Whether the `process()` background task is currently running. |

### Exceptions

| Exception | Raised in | Description |
| - | - | - |
| `StopMailbox` | `process()` | Voluntary stop signal: the consumption loop exits normally **without restarting**. |

---

## MailboxConfig

```python
class MailboxConfig(BaseModel):
    queue_put_timeout: float | None = None   # Enqueue timeout (seconds); None = wait indefinitely
    restart_delay: float = 0.5               # Base restart delay after exception (seconds)
    restart_jitter: float = 0.2              # Random jitter added to restart delay, prevents thundering herd
    max_queue_size: int = 0                  # Max mailbox queue capacity; 0 = unbounded
```

| Parameter | Default | Description |
| - | - | - |
| `queue_put_timeout` | `None` | Enqueue timeout. `None` waits forever; a value raises `RuntimeError` on timeout. |
| `restart_delay` | `0.5` | Base seconds to wait before restarting after `process()` exits with an exception. |
| `restart_jitter` | `0.2` | Random offset in `[0, jitter]` added to `restart_delay`, avoiding thundering herd with multiple instances. |
| `max_queue_size` | `0` | Maximum queue capacity. `0` means unbounded. |

> Actual restart wait time = `restart_delay + random(0, restart_jitter)`

---

## How It Works

```text
Event arrives
  │
  ▼
handle() → put(event, proxy) → Enqueue
  │
  ▼
process() coroutine (independent Task)
  │
  ▼
await get() → Dequeue (event, proxy)
  │
  ▼
Business logic (publish new events via proxy.publish())
  │
  ▼
Loop back to get()
```

1. **Hook-driven start**: `on_activate` creates the `process()` background `asyncio.Task` when the handler is registered to an active bus, or when the bus starts (`registry.activate`).
2. **Enqueue**: `handle()` places `(Event, EventBus.Proxy)` tuples into the internal `asyncio.Queue`.
3. **Serial consumption**: `process()` loops on `await self.get()` to handle events one by one.
4. **Exception restart**: If `process()` exits due to an exception other than `CancelledError` / `StopMailbox`, `_process_loop` waits `restart_delay + jitter` seconds, then re-invokes `process()`.
5. **Voluntary stop**: Raising `StopMailbox` inside `process()` exits the loop immediately with no restart.
6. **Hook-driven shutdown**: On bus stop (`registry.deactivate`), `on_deactivate` cancels the `process()` task and clears the backlog.

---

## Examples

### Basic: Serial Event Collection

```python
from infinity_bus import Event, EventBus
from infinity_bus.templates import MailboxHandler

class MyHandler(MailboxHandler):
    def __init__(self):
        super().__init__(subscriptions=['my.event'])
        self.received: list[Event] = []

    async def process(self) -> None:
        while True:
            event, proxy = await self.get()
            self.received.append(event)
            # Publish new events via proxy.publish()
```

### Custom Loop: Timed Batch Processing

```python
import asyncio

class BatchHandler(MailboxHandler):
    def __init__(self):
        super().__init__(subscriptions=['data.input'])

    async def process(self) -> None:
        while True:
            batch: list[Event] = []
            try:
                event, _ = await asyncio.wait_for(self.get(), timeout=1.0)
                batch.append(event)
                while True:
                    try:
                        event, _ = await asyncio.wait_for(self.get(), timeout=0.1)
                        batch.append(event)
                    except asyncio.TimeoutError:
                        break
            except asyncio.TimeoutError:
                pass
            if batch:
                await self._process_batch(batch)

    async def _process_batch(self, batch: list[Event]) -> None:
        print(f'Processed {len(batch)} events')
```

### Exception Restart: Auto-Recovery After Crash

```python
class RobustHandler(MailboxHandler):
    def __init__(self):
        super().__init__(
            subscriptions=['task.request'],
            config=MailboxConfig(restart_delay=0.5, restart_jitter=0.2),
        )

    async def process(self) -> None:
        while True:
            event, proxy = await self.get()
            try:
                await self._handle_task(event)
            except Exception:
                # Failure of a single event continues to the next.
                # Only a crash of process() as a whole triggers a restart.
                pass
```

### Voluntary Stop: Business-Driven Exit

```python
from infinity_bus.templates import MailboxHandler, StopMailbox

class DrainThenStopHandler(MailboxHandler):
    """ends the consumption loop after a stop command"""

    def __init__(self):
        super().__init__(subscriptions=['task.command'])

    async def process(self) -> None:
        while True:
            event, proxy = await self.get()
            if event.data is not None and getattr(event.data, 'action', None) == 'stop':
                raise StopMailbox  # exits the loop, no restart
            await self._handle(event)
```

### Accessing the Bus

```python
class BusAwareHandler(MailboxHandler):
    def __init__(self):
        super().__init__(subscriptions=['status.check'])

    async def process(self) -> None:
        while True:
            event, proxy = await self.get()
            # Access the raw bus via self.bus
            if self.bus and self.bus.is_running:
                await proxy.publish('status.reply', {'ok': True})
```

---

## Important Notes

- **`process()` must be an infinite loop**: `_process_loop` re-invokes `process()` after it returns normally. If `process()` runs once and returns, it will be called again immediately, creating a busy loop. Always wrap your logic in `while True`.
- **Event loss on crash**: If `process()` crashes after `get()` returns but before processing completes, that event is lost. The restart loop calls `get()` again for the next event — it does not retry the lost one.
- **No implicit `ShutdownEvent` subscription**: Shutdown is driven by the `on_deactivate` hook; if you subscribe to `ShutdownEvent` yourself it is just enqueued as an ordinary event.
- **Bus bound on activation**: `self.bus` is set in `on_activate` (bus activation) and cleared in `on_deactivate`. The single-bus registry constraint prevents cross-bus confusion.
- **Restart vs. stop**: `CancelledError` (external cancellation) and `StopMailbox` (voluntary stop) both exit the loop without restart; any other `Exception` triggers a restart after the delay. `KeyboardInterrupt` and `SystemExit` (subclasses of `BaseException`) are not caught and will propagate upward.
- **Mailbox after stop**: raising `StopMailbox` only ends the consumption task (`is_running` becomes `False`); events arriving later are still enqueued but never consumed. To stop receiving entirely, unregister the handler or stop the bus.

---

## Comparison with Raw EventHandler

| Feature | Raw EventHandler | MailboxHandler |
| - | - | - |
| Concurrency model | Events may execute concurrently | Enforced serial consumption |
| Backpressure | Relies on bus Semaphore | Additional queue capacity limit |
| Task lifecycle | Managed by the bus | Independent `asyncio.Task`, started/stopped by `activate`/`deactivate` |
| Custom loop | Not supported | Batch, timer, and other flexible patterns |
| Error recovery | Relies on `TaskErrorEvent` | Built-in restart mechanism |
| Shutdown behavior | Relies on `ShutdownEvent` subscription | `on_deactivate` hook auto-cancels the task |
