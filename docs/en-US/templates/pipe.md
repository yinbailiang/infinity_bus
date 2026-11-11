# Pipe — Async Bidirectional Channel

## Overview

The `pipe` module provides a **bidirectional async pipe** abstraction built on the event
bus, enabling type-safe communication channels between two async contexts. It dynamically
establishes connections via an event handshake protocol and encapsulates backpressure
control and lifecycle management.

Typical use cases include:

- Streaming data exchange between microservice components.
- Long-connection simulation (e.g., WebSocket-style communication).
- Simulating bidirectional RPC channels in test frameworks.
- Extending synchronous request-response models into persistent bidirectional streams.

This module depends on [EventBus](../infinity_bus.md) for underlying message routing, and
reuses the `request` and `expect` templates for the handshake protocol.

Allocator implementations live in the `infinity_bus.templates.pipe.allocator` subpackage
and **must be passed explicitly** to `open_pipe` / `expect_pipe` — the templates provide
no default instance:

```python
from infinity_bus.templates.pipe import expect_pipe, open_pipe
from infinity_bus.templates.pipe.allocator import InProcessPipeAllocator

allocator = InProcessPipeAllocator()
```

---

## Core Concepts

### Pipe (`Pipe`)

Abstract base class defining the basic `send` / `receive` operations. All concrete pipe
implementations must follow this interface.

### Handshake Protocol

A connection is established through a pair of **request/response events**:

- The client calls `open_pipe` to initiate a request, allocating a pipe instance via the
  allocator passed in explicitly.
- The server calls `expect_pipe` to await the request, claims its endpoint via
  `allocator.checkout(pipe_id)`, and completes the binding.

After a successful handshake, both ends communicate directly via `send` / `receive`
without going through the event bus. **How each side holds the pipe is decided by the
allocator implementation**:

- **Same process** (built-in `InProcessPipeAllocator`): each end holds **its own endpoint
  object**; both share one duplex channel but each direction has its own buffer, so a
  side never reads back messages it sent itself.
- **Cross-process** (custom `PipeAllocator`): each end holds *its own* `Pipe` object
  (local buffer plus its own transport handle), keyed by `pipe_id`. The `Pipe` object
  itself is never transmitted.

#### Cross-Process Implementation Contract

The `pipe` package makes no assumptions about the transport layer; cross-process
semantics are entirely an agreement between the two allocators. Implementations must
satisfy:

1. **Dial timing**: the client must open the data-plane connection during `allocate()`.
   `expect_pipe` calls `allocator.checkout(pipe_id)` *before* publishing the handshake
   response; if the server waits inside that call for a dial that only happens after the
   response, both sides wait on each other until `Handshake timeout`.
2. **Rendezvous key**: `pipe_id` is an opaque string that the server uses to claim an
   already-accepted connection. How addresses, tokens, and parameters are encoded into
   it is a private protocol between the two allocators; the templates never parse it.
3. **Endpoint ownership**: the server may own the data-plane endpoint (for example a
   WebSocket listener), start it once for the allocator's lifetime, and deliver
   connections from `checkout()` by `pipe_id`.
4. **Strictly P2P**: one `pipe_id` maps to exactly one pair of endpoints; pipes do not
   multiplex. `expect_pipe` claims a single pipe, so concurrent `expect_pipe` calls must
   not compete for the same handshake request.
5. **Close is terminal**: `PipeClosedError` means the pipe is closed and its buffer is
   drained. Reconnecting requires a new handshake and a new `pipe_id`.

### In-Process Pipe (`InProcessPipe`)

The built-in **full-duplex** implementation based on `asyncio.Queue`: `create_pair()`
returns two endpoints sharing one duplex channel with an independent queue per direction,
so concurrent bidirectional traffic never crosses over. `maxsize` is the per-direction
capacity used for backpressure.

### Pipe Allocator (`PipeAllocator` / `InProcessPipeAllocator`)

The abstract base class `PipeAllocator` defines the pipe lifecycle management interface
and lives in the `infinity_bus.templates.pipe.allocator` subpackage.
`InProcessPipeAllocator` is the built-in in-process implementation: `allocate()` creates
a pair of endpoints and returns the owner endpoint, `checkout()` hands out the peer
endpoint, `checkin()` returns it, and `release()` destroys the whole pipe.

Callers must pass an allocator explicitly; there is no module-level default instance.
Cross-process transports are supported by implementing a custom `PipeAllocator` (see the
cross-process contract above); the templates themselves require no changes.

### Built-in Cross-Process Transports

The `infinity_bus.templates.pipe.allocator` subpackage ships two transports:

| Transport | Endpoint | Client allocator | Server allocator | Dependency |
| ---- | ---- | ---- | ---- | ---- |
| Inter-process (`multiprocessing.connection`) | `ProcessPipe` | `ProcessPipeClientAllocator` | `ProcessPipeServerAllocator` | stdlib |
| WebSocket (aiohttp) | `WsPipe` | `WsPipeClientAllocator` | `WsPipeServerAllocator` | `aiohttp` (`infinity_bus[ws]`) |

Both follow the same contract: `allocate()` dials the data plane and sends the first
`pipe_id` frame; the server accepts and parks the connection by `pipe_id`, and
`checkout()` claims it. Frames are encoded/decoded with the `payload_type` given at
construction time.

```python
from infinity_bus.templates.pipe.allocator import (
    ProcessPipeClientAllocator,
    ProcessPipeServerAllocator,
)

# Server: owns the listener, started once
server_alloc = ProcessPipeServerAllocator(('0.0.0.0', 8765), MyPayload, authkey=b'secret')
await server_alloc.start()

# Client: dials during allocate()
client_alloc = ProcessPipeClientAllocator(('server-host', 8765), MyPayload, authkey=b'secret')
async with open_pipe(bus, 'pipe.connect', 'pipe.linked', client_alloc) as pipe:
    ...
```

`ProcessPipe` wraps the blocking `Connection` API. Since `close()` does not wake a read
blocked in another thread, the receive side polls with `poll(0)` at a short interval
inside the event loop, while writes go through `asyncio.to_thread`; `authkey` provides
data-plane authentication. `WsPipe` uses an aiohttp inbound pump, with backpressure from
WebSocket flow control and close frames handled by the pump.

---

## Class & Function Reference

### `Pipe` (Abstract Base Class)

```python
class Pipe(ABC):
    async def __aenter__(self) -> "Pipe"
    async def __aexit__(self, ...)
    async def open(self) -> None
    async def close(self) -> None
    async def send(self, data: BaseModel) -> None
    async def receive(self) -> BaseModel
```

| Method | Description |
| ---- | ---- |
| `open()` | Opens the pipe, ready for sending/receiving data. |
| `close()` | Closes the pipe, releasing resources. |
| `send(data)` | Writes a Pydantic model instance to the pipe. Raises an exception if the pipe is closed or capacity is full. |
| `receive()` | Reads the next Pydantic model instance from the pipe. Raises `PipeClosedError` if the pipe is closed with no data. |
| Async context manager | Auto-calls `open()` on enter and `close()` on exit. |

### `PipeAllocator` (Abstract Base Class)

```python
class PipeAllocator(ABC):
    async def allocate(self, **kwargs: Any) -> tuple[str, Pipe]
    async def release(self, pipe_id: str) -> None
    async def checkout(self, pipe_id: str) -> Pipe | None
    async def checkin(self, pipe_id: str) -> None
```

| Method | Description |
| ---- | ---- |
| `allocate(**kwargs)` | Allocates a new pipe and returns `(pipe_id, owner endpoint)`; keyword arguments are forwarded to the concrete pipe constructor. |
| `release(pipe_id)` | Releases the whole pipe: closes both endpoints and unregisters it. Idempotent; called by the owner on exit. |
| `checkout(pipe_id)` | The borrower (server) claims its endpoint; returns `None` if absent. |
| `checkin(pipe_id)` | The borrower returns its endpoint and unregisters it without destroying the owner's resource. Idempotent. |

### `InProcessPipeAllocator`

In-process implementation of `PipeAllocator`, managing creation, lookup, and release of
pipe instances.

```python
class InProcessPipeAllocator(PipeAllocator):
    def __init__(self, pipe_type: type[InProcessPipe] = InProcessPipe)
```

- `pipe_type` must be `InProcessPipe` (or a subclass) and support `create_pair()`.
- `allocate(**kwargs)` creates a pair of endpoints and returns `(pipe_id, owner endpoint)`;
  `kwargs` go to `create_pair()` (e.g., `maxsize=10`).
- `checkout(pipe_id)` returns the borrower endpoint; `checkin(pipe_id)` returns and closes it.
- `release(pipe_id)` closes both endpoints and unregisters the whole pipe.
- `open_pipe` / `expect_pipe` require the same allocator instance to be passed
  explicitly; there is no module-level default.

### `InProcessPipe`

In-process `Pipe` implementation based on `asyncio.Queue`.

```python
class InProcessPipe(Pipe):
    def __init__(self, maxsize: int | None = None)

    @classmethod
    def create_pair(cls, maxsize: int | None = None) -> tuple[InProcessPipe, InProcessPipe]
```

- `create_pair()` returns two endpoints sharing one duplex channel, with an independent
  queue per direction; `maxsize` is the **per-direction** capacity (`None` = unlimited).
- Constructing `InProcessPipe(maxsize=...)` directly yields a loopback endpoint (data
  written is read back by itself); it is only meant for single-ended tests and does not
  model two-party communication.
- `receive()` waits for inbound data or the **peer close** signal; after the peer closes
  it drains the buffer and then raises `PipeClosedError`.

### `open_pipe` (Client Context Manager)

```python
@asynccontextmanager
async def open_pipe(
    bus_proxy: EventBus.Proxy,
    req_event: str,
    resp_event: str,
    allocator: PipeAllocator,
    handshake_timeout: float = 5.0,
    session_id: str | None = None,
    pipe_kargs: dict[str, Any] | None = None,
) -> AsyncIterator[Pipe]
```

| Parameter | Type | Description |
| - | - | - |
| `bus_proxy` | `EventBus.Proxy` | Event bus proxy for publishing the handshake request. |
| `req_event` | `str` | Handshake request event name. |
| `resp_event` | `str` | Handshake response event name. |
| `handshake_timeout` | `float` | Handshake timeout in seconds. Raises `PipeHandshakeError` on expiry. |
| `session_id` | `str \| None` | Session ID for correlating request-response. Auto-generated if omitted. |
| `allocator` | `PipeAllocator` | Pipe allocator instance, **required**. In-process, both ends must pass the same instance. |
| `pipe_kargs` | `dict[str, Any] \| None` | Keyword arguments forwarded to the pipe constructor (e.g., `{"maxsize": 10}`), passed as `allocator.allocate(**pipe_kargs)`. |

**Yields**: A successfully handshaked and opened `Pipe` instance. Auto-closes the pipe
and calls `allocator.release(pipe_id)` on context exit.

**Exceptions**:

- `PipeHandshakeError`: Handshake timeout, response failure, or rejection.
- Other exceptions from the `request` template (e.g., bus shutdown).

### `expect_pipe` (Server Context Manager)

```python
@asynccontextmanager
async def expect_pipe(
    bus_proxy: EventBus.Proxy,
    req_event: str,
    resp_event: str,
    allocator: PipeAllocator,
    session_id: str | None = None,
    timeout: float = 5.0,
) -> AsyncIterator[Pipe]
```

| Parameter | Type | Description |
| - | - | - |
| `bus_proxy` | `EventBus.Proxy` | Event bus proxy for listening for requests and sending responses. |
| `req_event` | `str` | Expected handshake request event name. |
| `resp_event` | `str` | Event name for replying with handshake results. |
| `session_id` | `str \| None` | Session ID filter — only responds to requests with matching session ID. |
| `timeout` | `float` | Timeout in seconds for waiting on the handshake request. Raises `PipeHandshakeError` on expiry. |
| `allocator` | `PipeAllocator` | Pipe allocator instance, **required**. In-process, both ends must pass the same instance. |

**Yields**: After a successful handshake, returns the `Pipe` instance obtained from
`allocator.checkout(pipe_id)`. Auto-closes the pipe and calls `allocator.checkin(pipe_id)`
on context exit.

**Handshake Flow**:

1. Uses `expect` to listen for `req_event`, awaiting a `PipeOpenRequest` event, filtered
   by `session_id` if provided.
2. On receiving the request, calls `allocator.checkout(pipe_id)` to claim the borrower endpoint.
3. If the pipe doesn't exist, publishes a `PipeLinkedResponse` with `success=False` and
   raises `PipeHandshakeError`.
4. If it exists, publishes a `PipeLinkedResponse` with `success=True`, then `yield`s the
   pipe.
5. On context exit, closes the pipe and calls `allocator.checkin(pipe_id)` to return the
   endpoint (the owner's `release()` performs the final teardown).

---

## Built-in Data Models

### `PipeOpenRequest`

```python
class PipeOpenRequest(RequestProtocol):
    pipe_id: str
```

Handshake request payload containing the pipe's unique identifier.

### `PipeLinkedResponse`

```python
class PipeLinkedResponse(ResponseProtocol):
    pass
```

Handshake response payload, extending `ResponseProtocol` with `success`, `error_msg`, etc.

---

## Workflow Diagram

```text
Client (open_pipe)                          Server (expect_pipe)
      |                                             |
      |  1. Create Pipe via allocator                 |
      |-------------------------------------------->|
      |  2. Publish PipeOpenRequest (req_event)     |
      |                                             |
      |                                             |  3. Listen req_event, receive request
      |                                             |  4. checkout borrower endpoint
      |                                             |  5. Publish PipeLinkedResponse (resp_event)
      |  6. Receive success response                |
      |<--------------------------------------------|
      |                                             |
      |  7. Enter async with pipe block             |  7. Enter async with pipe block
      |     Each end holds its own endpoint   |
      |                                             |
      |  8. Exit context, close pipe & cleanup      |  8. Exit context, close pipe
```

---

## Usage Examples

The examples below assume a shared `allocator = InProcessPipeAllocator()` has already been
created (both ends must use the same allocator instance in-process, but each holds a
distinct endpoint).

### Basic Bidirectional Communication

**Server** (waiting for connection)

```python
async def server_task(bus_proxy):
    async with expect_pipe(bus_proxy, "pipe.connect", "pipe.linked", allocator) as pipe:
        print("Pipe connected")
        while True:
            data = await pipe.receive()
            if data is None:
                break
            print(f"Received: {data}")
            # Reply with data
            await pipe.send(SomeResponseModel(result="ok"))
```

**Client** (initiating connection)

```python
async def client_task(bus_proxy):
    async with open_pipe(bus_proxy, "pipe.connect", "pipe.linked", allocator) as pipe:
        await pipe.send(SomeRequestModel(command="hello"))
        reply = await pipe.receive()
        print(f"Reply: {reply}")
```

### Custom Pipe Implementation

Extend `Pipe` to implement network pipes, file pipes, etc.

```python
class TcpPipe(Pipe):
    def __init__(self, host: str, port: int):
        super().__init__()
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None

    async def open(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    async def send(self, data: BaseModel):
        self.writer.write(data.json().encode() + b"\n")
        await self.writer.drain()

    async def receive(self) -> BaseModel:
        line = await self.reader.readline()
        return SomeModel.parse_raw(line)
```

A custom pipe needs a matching `PipeAllocator` (see the cross-process contract above);
`InProcessPipeAllocator` only accepts `InProcessPipe` and its subclasses:

```python
class TcpPipeAllocator(PipeAllocator):
    ...  # creates local/peer handles and implements allocate() / release() / checkout() / checkin()

async with open_pipe(bus, "tcp.connect", "tcp.linked", allocator=TcpPipeAllocator(...)) as pipe:
    ...
```

Or pass constructor args via `pipe_kargs`:

```python
async with open_pipe(bus, "pipe.req", "pipe.resp", allocator, pipe_kargs={"maxsize": 10}) as pipe:
    ...
```

### Timeout & Error Handling

```python
try:
    async with open_pipe(bus, "pipe.req", "pipe.resp", allocator, handshake_timeout=2.0) as pipe:
        await asyncio.wait_for(pipe.receive(), timeout=5.0)
except PipeHandshakeError:
    print("Handshake failed")
except PipeClosedError:
    print("Pipe closed prematurely")
except asyncio.TimeoutError:
    print("Receive timeout")
```

---

## Exception Types

| Exception | Trigger |
| ---- | -------- |
| `PipeHandshakeError` | Handshake timeout, response failure, pipe not found. |
| `PipeTeardownError` | (Reserved) Error during pipe close. |
| `PipeClosedError` | Attempt to send on a closed pipe, or receive after close with empty queue. |

---

## Notes

1. **Must be used in pairs**: `open_pipe` and `expect_pipe` must use the same
   `req_event` / `resp_event` names, and both sides must be running.
2. **Pipe lifecycle**: Both `open_pipe` and `expect_pipe` use `async with` to manage the
   pipe. `pipe.close()` is called automatically on context exit. Do not hold pipe
   references outside the context.
3. **Backpressure control**: `InProcessPipe`'s `maxsize` parameter limits unprocessed
   messages. When the queue is full, `send()` blocks until the receiver consumes, providing
   natural backpressure.
4. **Thread safety**: This module is designed for `asyncio` single-threaded environments.
   Do not use across threads.
5. **Event bus dependency**: Ensure `EventBus` is started and `bus_proxy` is valid before use.
6. **Registry cleanup**: `open_pipe` ensures registered pipes are removed in a `finally`
   block. If an exception prevents handshake completion, leftover registry entries are
   also cleaned up.

---

## Internal Details

- `expect_pipe` internally uses the `expect` template for the handshake, leveraging
  `OneShotEventHandler` for one-shot waiting.
- Responses use the `PipeLinkedResponse` model, following the `ResponseProtocol` convention
  with `session_id` and `request_id` for correlation.
- `InProcessPipe.receive()` uses `asyncio.wait` to simultaneously await queue `get()` and
  the `_closed` event, preventing indefinite blocking after close.

---

## Full Example

See `tests/templates/pipe_test.py` for complete tests covering successful handshake,
timeout, pipe close, concurrent send/receive, and more.
