# WebSocket Transport (pipe/allocator/ws)

`infinity_bus.templates.pipe.allocator.ws` implements WebSocket pipes with aiohttp. Back to
the [overview](allocator.md).

> Requires the optional dependency: `pip install infinity_bus[ws]` (aiohttp).

---

## `WsPipe`

```python
class WsPipe[PayloadType: BaseModel](Pipe[PayloadType]):
    def __init__(self, ws: Any, payload_type: type[PayloadType], maxsize: int | None = None)
```

- A background pump reads WebSocket frames, decodes them with the [codec](../codec.md), and
  puts them into the inbound `asyncio.Queue`; close frames and decode errors are logged and
  mark the pipe closed.
- `maxsize` bounds the inbound queue (`None` = unlimited). Frames enter the queue first, so
  backpressure is governed by the queue capacity rather than directly by WebSocket flow
  control.
- `receive()` drains the queue after close and then raises `PipeClosedError`; cancellation
  leaves no consuming task behind.
- `close()` cancels the pump and closes the WebSocket without re-raising pump errors.

---

## `WsPipeServerAllocator`

```python
class WsPipeServerAllocator[PayloadType: BaseModel](SingleRoleAllocator[PayloadType]):
    def __init__(
        self,
        host: str,
        port: int,
        payload_type: type[PayloadType],
        *,
        path: str = '/pipe',
        accept_timeout: float = 5.0,
        hello_timeout: float = 2.0,
        max_pending: int = 64,
        maxsize: int | None = None,
        shutdown_timeout: float = 5.0,
    )
```

| Parameter | Description |
| ---- | ---- |
| `host` / `port` | aiohttp listen address; `port=0` lets the OS choose, readable from the `port` property. |
| `path` | WebSocket path, `/pipe` by default. |
| `accept_timeout` | How long `checkout()` waits for a connection. |
| `hello_timeout` | Timeout for the first `{"pipe_id": ...}` frame; timeout/invalid frames close only that connection. |
| `max_pending` | Cap on unclaimed connections; the oldest is closed when exceeded. |
| `maxsize` | Inbound queue capacity passed to `WsPipe`. |
| `shutdown_timeout` | Shutdown wait bound passed to `AppRunner`. |

- `start()` creates the `AppRunner` / `TCPSite`; `stop()` fails pending `checkout()` calls,
  closes pre-handshake connections and all endpoints, then stops the site and cleans up the
  runner.
- Before the hello frame the handler only holds a WebSocket; these connections are tracked
  and closed by `stop()`.

---

## `WsPipeClientAllocator`

```python
class WsPipeClientAllocator[PayloadType: BaseModel](SingleRoleAllocator[PayloadType]):
    def __init__(
        self,
        url: str,
        payload_type: type[PayloadType],
        *,
        connect_timeout: float = 5.0,
        maxsize: int | None = None,
    )
```

`allocate()` connects and sends the hello; `release()` closes the local pipe; `stop()`
closes the internal `ClientSession`. `allocate(**kwargs)` uses no extra parameters; set
`maxsize` at construction time.

---

## Security Boundary

`WsPipe` has **no built-in authentication**: any client that can reach the port can register
a `pipe_id`. In production, bind to loopback/private networks or add a token to the hello
frame and validate it; use a reverse proxy for TLS termination.

## Example

```python
from infinity_bus.templates.pipe import open_pipe
from infinity_bus.templates.pipe.allocator import (
    WsPipeClientAllocator,
    WsPipeServerAllocator,
)

server_alloc = WsPipeServerAllocator('0.0.0.0', 8765, MyPayload, maxsize=64)
await server_alloc.start()

client_alloc = WsPipeClientAllocator('ws://server-host:8765/pipe', MyPayload, maxsize=64)
try:
    async with open_pipe(bus, 'pipe.connect', 'pipe.linked', client_alloc) as pipe:
        ...
finally:
    await client_alloc.stop()
    await server_alloc.stop()
```
