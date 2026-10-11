# Handler Templates (handlers)

`infinity_bus.templates.handlers` ships **two ready-to-use handler base classes** covering the two typical event-consumption patterns:

- **`MailboxHandler`** — mailbox pattern: events are enqueued first and consumed serially by a dedicated task (backpressure, custom loops, automatic restart on crash, voluntary stop).
- **`ServiceHandler`** — service handler: collapses the request-response (RPC) server-side boilerplate into a `@process` decorator.

Both extend `EventHandler` and can be registered directly with `EventHandlerRegistry`;
their extra capabilities build on the core handler protocol (lifecycle hooks / subscriptions / bus dispatch).

---

## Directory Layout (mirrors the code)

```text
infinity_bus.templates.handlers/
├── handlers.md     # this overview
├── mailbox.md      # MailboxHandler — mailbox-pattern handler
└── service.md      # ServiceHandler — service handler (RPC server side)
```

| Doc | Module | Pattern | Use Cases |
| - | - | - | - |
| [mailbox.md](mailbox.md) | `handlers/mailbox.py` | Mailbox (serial consumption) | Strict serial processing, backpressure, custom task loops |
| [service.md](service.md) | `handlers/service.py` | Service handler (request-response) | RPC server side, unified success/failure responses, automatic error translation |

---

## Which One to Choose

| You need… | Use |
| - | - |
| **Strictly serial** consumption (no concurrency per handler) | `MailboxHandler` |
| **Backpressure control** (bounded backlog queue) | `MailboxHandler` |
| A **custom consumption loop** (batching, timers, state machines) | `MailboxHandler` |
| Automatic **restart after a crash** | `MailboxHandler` |
| To expose an **RPC service** (request → response) | `ServiceHandler` |
| Multiple request methods in one handler | `ServiceHandler` |
| **Unified failure responses** (`success` / `error_msg`) | `ServiceHandler` |
| Just plain pub/sub handling | Subclass [`EventHandler`](../../handler.md) directly |

---

## Pattern Comparison

| Aspect | `MailboxHandler` | `ServiceHandler` |
| - | - | - |
| Consumption model | Queue + dedicated task, strictly serial | Handled in the bus dispatch context (may run concurrently) |
| Entry point | Override the `process()` loop | Decorate methods with `@process(...)` |
| Response construction | Manual (usually `proxy.publish()`) | Automatic (`build_response` echoes `session_id` / `request_id`) |
| Error handling | Crashes restart the loop; `StopMailbox` stops it | `ServiceError` becomes a `success=False` response |
| Typical companion | Internal workflows, event aggregation, state machines | The [`request()`](../request.md) RPC client |

---

## Quick Examples

### Mailbox Pattern (Serial Consumption)

```python
from infinity_bus.templates import MailboxHandler

class SerialCollector(MailboxHandler):
    def __init__(self):
        super().__init__(subscriptions=['data.input'])
        self.buffer: list[str] = []

    async def process(self) -> None:
        while True:
            event, proxy = await self.get()
            await self._handle(event)  # strictly serial
```

### Service Handler (RPC Server Side)

```python
from infinity_bus.templates import ServiceHandler, process

class UserService(ServiceHandler):
    @process(GetUserReqEvent, GetUserRespEvent)
    async def get_user(self, payload, proxy, raw):
        return {'user_name': 'alice'}
```

See [mailbox.md](mailbox.md) and [service.md](service.md) for full usage.

---

## All Exports

```python
from infinity_bus.templates import (
    # mailbox
    MailboxHandler, MailboxConfig, StopMailbox,
    # service
    ServiceHandler, process, ProcessorMeta,
    ServiceError,
    NotFoundError, ConflictError, InvalidRequestError, AccessDeniedError,
)
```

---

## Notes

- Both templates ship with `infinity_bus[templates]`: `pip install infinity_bus[templates]`.
- Both follow the core handler protocol: once registered, their lifecycle is driven by the bus (`on_activate` / `on_deactivate`) — no manual subscription management.
- They complement the [handler decorator](../simple_handler.md) (function → handler) and [register](../register.md) (bulk registration) — they can be combined.
