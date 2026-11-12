# Inter-Process Transport (pipe/allocator/process)

`infinity_bus.templates.pipe.allocator.process` implements cross-process pipes on the
stdlib `multiprocessing.connection` `Listener` / `Client`. Back to the
[overview](allocator.md).

- The client `allocate()` dials the listener and sends a first `pipe_id` hello frame.
- The server accept thread reads the hello and parks the connection by `pipe_id`.
- `expect_pipe` claims it with `checkout(pipe_id)`; `checkin()` returns it.
- Frames are encoded/decoded with the [codec](../codec.md).

---

## `ProcessPipe`

```python
class ProcessPipe[PayloadType: BaseModel](Pipe[PayloadType]):
    def __init__(self, conn: Connection, payload_type: type[PayloadType])
```

- `send()` calls `Connection.send_bytes()` through `asyncio.to_thread`.
- `receive()` polls with `poll(0)` plus `POLL_INTERVAL` (10 ms) on the event loop, avoiding
  the problem that `close()` cannot wake a blocking reader thread.
- After the peer closes (EOF), `receive()` raises `PipeClosedError`.
- Each waiting pipe wakes every 10 ms; this suits a small number of strict P2P
  connections.

---

## `ProcessPipeServerAllocator`

```python
class ProcessPipeServerAllocator[PayloadType: BaseModel](SingleRoleAllocator[PayloadType]):
    def __init__(
        self,
        address: str | tuple[str, int],
        payload_type: type[PayloadType],
        *,
        authkey: bytes | None = None,
        accept_timeout: float = 5.0,
        hello_timeout: float = 2.0,
        max_pending: int = 64,
    )
```

| Parameter | Description |
| ---- | ---- |
| `address` | `Listener` address; `port=0` lets the OS choose. |
| `payload_type` | Frame payload type used for decoding. |
| `authkey` | `Connection` auth key; `None` means unauthenticated. |
| `accept_timeout` | How long `checkout()` waits for a connection. |
| `hello_timeout` | How long the accept thread waits for the hello; timeout closes only that connection. |
| `max_pending` | Cap on unclaimed connections; the oldest is closed when exceeded. |

- `start()` creates the `Listener` and accept thread; `stop()` sends a sentinel, fails
  pending `checkout()` calls with `PipeHandshakeError`, and closes the listener and all
  endpoints.
- Authentication failures, hello timeouts, invalid UTF-8, and per-connection errors never
  kill the accept thread.
- Unclaimed connections are closed by `stop()` or when `max_pending` is exceeded.

---

## `ProcessPipeClientAllocator`

```python
class ProcessPipeClientAllocator[PayloadType: BaseModel](SingleRoleAllocator[PayloadType]):
    def __init__(
        self,
        address: str | tuple[str, int],
        payload_type: type[PayloadType],
        *,
        authkey: bytes | None = None,
        connect_timeout: float = 5.0,
    )
```

`allocate()` connects within `connect_timeout` and sends the hello; on failure it closes the
connection explicitly and raises `PipeHandshakeError`. `release(pipe_id)` closes the local
endpoint; `stop()` closes all local endpoints.

> `allocate(**kwargs)` uses no extra parameters; the process transport has no configurable
> inbound queue.

---

## Example

```python
from infinity_bus.templates.pipe import open_pipe
from infinity_bus.templates.pipe.allocator import (
    ProcessPipeClientAllocator,
    ProcessPipeServerAllocator,
)

server_alloc = ProcessPipeServerAllocator(
    ('0.0.0.0', 8765), MyPayload, authkey=b'secret'
)
await server_alloc.start()

client_alloc = ProcessPipeClientAllocator(
    ('server-host', 8765), MyPayload, authkey=b'secret'
)
try:
    async with open_pipe(bus, 'pipe.connect', 'pipe.linked', client_alloc) as pipe:
        ...
finally:
    await client_alloc.stop()
    await server_alloc.stop()
```
