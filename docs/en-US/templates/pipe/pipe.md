# Bidirectional Async Pipe (pipe)

`infinity_bus.templates.pipe` provides **bidirectional async pipes** on top of the event
bus: the control plane (handshake) goes through `EventBus`, while the data plane is handled
directly by a `Pipe` implementation and never touches the bus. The transport is pluggable;
in-process, `multiprocessing.connection`, and WebSocket implementations ship with the
subpackage.

- The handshake reuses the [`request`](../request.md) / [`expect`](../expect.md) templates.
- Each side holds its own endpoint, rendezvousing on `pipe_id`.
- An allocator is always required; there is no module-level default.

> WebSocket transport needs the optional dependency: `pip install infinity_bus[ws]` (aiohttp).
> The other transports have no extra dependencies.

---

## Directory Structure (mirrors the code)

```text
infinity_bus.templates.pipe/
├── pipe.md          # this overview
├── base.md          # Pipe ABC and exceptions
├── protocol.md      # handshake payload models
├── codec.md         # frame payload codec
├── handshake.md     # open_pipe / expect_pipe
└── allocator/
    ├── allocator.md # allocator overview and contract
    ├── base.md      # PipeAllocator / SingleRoleAllocator
    ├── inprocess.md # in-process implementation
    ├── process.md   # multiprocessing.connection transport
    └── ws.md        # aiohttp WebSocket transport
```

| Doc | Module | Description |
| - | - | - |
| [base.md](base.md) | `pipe/base.py` | `Pipe` ABC, exceptions, close semantics |
| [protocol.md](protocol.md) | `pipe/protocol.py` | `PipeOpenRequest` / `PipeLinkedResponse` |
| [codec.md](codec.md) | `pipe/codec.py` | JSON codec for cross-process frames |
| [handshake.md](handshake.md) | `pipe/handshake.py` | `open_pipe` / `expect_pipe` |
| [allocator/allocator.md](allocator/allocator.md) | `pipe/allocator/__init__.py` | Allocator contract and cross-process rules |
| [allocator/base.md](allocator/base.md) | `pipe/allocator/base.py` | `PipeAllocator` / `SingleRoleAllocator` |
| [allocator/inprocess.md](allocator/inprocess.md) | `pipe/allocator/inprocess.py` | In-process endpoints and allocator |
| [allocator/process.md](allocator/process.md) | `pipe/allocator/process.py` | `multiprocessing.connection` transport |
| [allocator/ws.md](allocator/ws.md) | `pipe/allocator/ws.py` | aiohttp WebSocket transport |

---

## Core Concepts

### Handshake

The client calls [`open_pipe`](handshake.md); the server calls
[`expect_pipe`](handshake.md), which claims its endpoint with
`allocator.checkout(pipe_id)`. After a successful handshake both sides use `send` /
`receive` directly.

**Dial timing**: cross-process clients must establish the data-plane connection during
`allocate()`. `expect_pipe` calls `checkout()` *before* publishing the success response;
otherwise both sides wait for each other until `Handshake timeout`.

### Allocator

[`PipeAllocator`](allocator/base.md) owns four operations:

- `allocate()` / `release()`: the owner (client) manages the resource lifecycle;
- `checkout()` / `checkin()`: the borrower (server) claims/returns endpoints;
- `start()` / `stop()`: allocator-level resources (listeners, sessions); no-ops by default.

See [allocator/allocator.md](allocator/allocator.md) for the full contract, single-borrow
semantics, and cross-process rules.

---

## Built-in Transports

| Endpoint | Client allocator | Server allocator | Dependency |
| ---- | ---- | ---- | ---- |
| [`InProcessPipe`](allocator/inprocess.md) | [`InProcessPipeAllocator`](allocator/inprocess.md) | same allocator shared in-process | none |
| [`ProcessPipe`](allocator/process.md) | `ProcessPipeClientAllocator` | `ProcessPipeServerAllocator` | stdlib |
| [`WsPipe`](allocator/ws.md) | `WsPipeClientAllocator` | `WsPipeServerAllocator` | `aiohttp` (`infinity_bus[ws]`) |

```python
from infinity_bus.templates.pipe import expect_pipe, open_pipe
from infinity_bus.templates.pipe.allocator import InProcessPipeAllocator

allocator = InProcessPipeAllocator[MyPayload]()

async with expect_pipe(bus_server, 'pipe.connect', 'pipe.linked', allocator) as server_pipe:
    ...

async with open_pipe(bus_client, 'pipe.connect', 'pipe.linked', allocator) as client_pipe:
    ...
```

> In-process peers must share the same allocator instance; cross-process peers create a
> client allocator and a server allocator separately.

---

## Notes

1. **Pair the templates**: `open_pipe` and `expect_pipe` must use the same `req_event` /
   `resp_event`, and both sides must be running.
2. **Lifecycle**: the context managers close the pipe and call `release()` / `checkin()`
   respectively; cross-process allocators must also pair `start()` / `stop()` in
   `try/finally`.
3. **Backpressure**: in-process `create_pair(maxsize=...)` and WebSocket allocator
   `maxsize` bound the inbound buffer; `send()` blocks when it is full.
4. **Close is terminal**: after `close()`, buffered data is drained before
   `PipeClosedError`; reconnecting requires a new handshake and a new `pipe_id`.
5. **Single asyncio thread**: pipes are used from the event loop; the process transport
   uses a background thread internally but its public API stays on the loop.
6. **Trust boundary**: `ProcessPipe` with `authkey=None` is unauthenticated; `WsPipe` has
   no built-in auth, so bind it to loopback/private networks or add a token to the hello
   frame.

## Full Example

See `tests/templates/pipe/` for handshake success, timeouts, close, cancellation,
backpressure, and cross-process end-to-end coverage.
