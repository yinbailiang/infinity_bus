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

---

## Core Concepts

### Pipe (`Pipe`)

Abstract base class defining the basic `send` / `receive` operations. All concrete pipe
implementations must follow this interface.

### Handshake Protocol

A connection is established through a pair of **request/response events**:

- The client calls `open_pipe` to initiate a request, allocating a pipe instance via
  `InProcessPipeAllocator`.
- The server calls `expect_pipe` to await the request, retrieves the corresponding pipe
  instance from the allocator, and completes the binding.

After a successful handshake, both ends hold a reference to the same `Pipe` instance and
can communicate directly via `send` / `receive` without going through the event bus.

### In-Process Pipe (`InProcessPipe`)

The default implementation based on `asyncio.Queue`, supporting configurable queue
capacity for backpressure control.

### Pipe Allocator (`PipeAllocator` / `InProcessPipeAllocator`)

The abstract base class `PipeAllocator` defines the pipe lifecycle management interface.
`InProcessPipeAllocator` is the default in-process implementation, temporarily storing
pipe instances during the handshake so both client and server can locate the same pipe
object.

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
    async def allocate(self, **kwargs: Dict[str, Any]) -> str
    async def release(self, pipe_id: str) -> None
    async def get(self, pipe_id: str) -> Optional[Pipe]
```

| Method | Description |
| ---- | ---- |
| `allocate(**kwargs)` | Creates a pipe instance and returns its unique identifier. Keyword arguments are forwarded to the concrete pipe constructor. |
| `release(pipe_id)` | Releases the specified pipe, removing it from the allocator. |
| `get(pipe_id)` | Retrieves a pipe instance; returns `None` if not found. |

### `InProcessPipeAllocator`

In-process implementation of `PipeAllocator`, managing creation, lookup, and release of
pipe instances.

```python
class InProcessPipeAllocator(PipeAllocator):
    def __init__(self, pipe_type: type[Pipe] = InProcessPipe)
```

- Constructor parameter `pipe_type` specifies the default pipe type used by `allocate()`.
- `allocate(**kwargs)` accepts keyword arguments and passes them through to the `pipe_type`
  constructor (e.g., `InProcessPipe(maxsize=10)`).
- `open_pipe` and `expect_pipe` share a module-level `InProcessPipeAllocator` instance by
  default (via `get_default_allocator()`); a custom instance can be passed via the
  `allocator` parameter.

### `InProcessPipe`

In-process `Pipe` implementation based on `asyncio.Queue`.

```python
class InProcessPipe(Pipe):
    def __init__(self, maxsize: Optional[int] = None)
```

- `maxsize` is passed directly to the internal `asyncio.Queue`; `None` means unlimited.
- `receive()` internally uses `asyncio.wait` to simultaneously await new data or the pipe
  close signal, ensuring `PipeClosedError` is raised promptly on close.

### `open_pipe` (Client Context Manager)

```python
@asynccontextmanager
async def open_pipe(
    bus_proxy: EventBus.Proxy,
    req_event: str,
    resp_event: str,
    handshake_timeout: float = 5.0,
    session_id: Optional[str] = None,
    allocator: Optional[InProcessPipeAllocator] = None,
    pipe_kargs: Optional[Dict[str, Any]] = None,
) -> AsyncIterator[Pipe]
```

| Parameter | Type | Description |
| - | - | - |
| `bus_proxy` | `EventBus.Proxy` | Event bus proxy for publishing the handshake request. |
| `req_event` | `str` | Handshake request event name. |
| `resp_event` | `str` | Handshake response event name. |
| `handshake_timeout` | `float` | Handshake timeout in seconds. Raises `PipeHandshakeError` on expiry. |
| `session_id` | `Optional[str]` | Session ID for correlating request-response. Auto-generated if omitted. |
| `allocator` | `Optional[InProcessPipeAllocator]` | Pipe allocator instance. Uses module-level default (`get_default_allocator()`) if omitted. |
| `pipe_kargs` | `Optional[Dict[str, Any]]` | Keyword arguments forwarded to the pipe constructor (e.g., `{"maxsize": 10}`), passed as `allocator.allocate(**pipe_kargs)`. |

**Yields**: A successfully handshaked and opened `Pipe` instance. Auto-closes the pipe
and releases it from the allocator on context exit.

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
    session_id: Optional[str] = None,
    timeout: float = 5.0,
    allocator: Optional[InProcessPipeAllocator] = None,
) -> AsyncIterator[Pipe]
```

| Parameter | Type | Description |
| - | - | - |
| `bus_proxy` | `EventBus.Proxy` | Event bus proxy for listening for requests and sending responses. |
| `req_event` | `str` | Expected handshake request event name. |
| `resp_event` | `str` | Event name for replying with handshake results. |
| `session_id` | `Optional[str]` | Session ID filter — only responds to requests with matching session ID. |
| `timeout` | `float` | Timeout in seconds for waiting on the handshake request. Raises `PipeHandshakeError` on expiry. |
| `allocator` | `Optional[InProcessPipeAllocator]` | Pipe allocator instance. Uses module-level default (`get_default_allocator()`) if omitted. |

**Yields**: After a successful handshake, returns the `Pipe` instance retrieved from the
allocator. Auto-closes the pipe on context exit.

**Handshake Flow**:

1. Uses `expect` to listen for `req_event`, awaiting a `PipeOpenRequest` event, filtered
   by `session_id` if provided.
2. On receiving the request, retrieves the corresponding pipe from `InProcessPipeAllocator`
   by `pipe_id`.
3. If the pipe doesn't exist, publishes a `PipeLinkedResponse` with `success=False` and
   raises `PipeHandshakeError`.
4. If it exists, publishes a `PipeLinkedResponse` with `success=True`, then `yield`s the
   pipe (pipe release is handled by the client `open_pipe` on exit).

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
      |  1. Create Pipe via InProcessPipeAllocator  |
      |-------------------------------------------->|
      |  2. Publish PipeOpenRequest (req_event)     |
      |                                             |
      |                                             |  3. Listen req_event, receive request
      |                                             |  4. Get Pipe from allocator, release
      |                                             |  5. Publish PipeLinkedResponse (resp_event)
      |  6. Receive success response                |
      |<--------------------------------------------|
      |                                             |
      |  7. Enter async with pipe block             |  7. Enter async with pipe block
      |     Both ends hold same Pipe, send/receive   |
      |                                             |
      |  8. Exit context, close pipe & cleanup      |  8. Exit context, close pipe
```

---

## Usage Examples

### Basic Bidirectional Communication

**Server** (waiting for connection)

```python
async def server_task(bus_proxy):
    async with expect_pipe(bus_proxy, "pipe.connect", "pipe.linked") as pipe:
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
    async with open_pipe(bus_proxy, "pipe.connect", "pipe.linked") as pipe:
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

Use with a custom `allocator` to specify the pipe type:

```python
allocator = InProcessPipeAllocator(pipe_type=TcpPipe)
async with open_pipe(bus, "tcp.connect", "tcp.linked", allocator=allocator) as pipe:
    ...
```

Or pass constructor args via `pipe_kargs`:

```python
async with open_pipe(bus, "pipe.req", "pipe.resp", pipe_kargs={"maxsize": 10}) as pipe:
    ...
```

### Timeout & Error Handling

```python
try:
    async with open_pipe(bus, "pipe.req", "pipe.resp", handshake_timeout=2.0) as pipe:
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
