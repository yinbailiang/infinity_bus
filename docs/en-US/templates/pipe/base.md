# Pipe ABC (pipe/base)

`infinity_bus.templates.pipe.base` defines the exception types and the `Pipe` abstract base
class. Back to the [overview](pipe.md).

---

## `Pipe` (Abstract Base Class)

```python
class Pipe[PayloadType: BaseModel](ABC):
    async def __aenter__(self) -> Pipe[PayloadType]
    async def __aexit__(self, ...)
    async def open(self) -> None
    async def close(self) -> None
    async def send(self, data: PayloadType) -> None
    async def receive(self) -> PayloadType
```

| Method | Description |
| ---- | ---- |
| `open()` | Opens the pipe, ready for send/receive. |
| `close()` | Closes the pipe; terminal and idempotent. |
| `send(data)` | Writes a Pydantic model instance. Raises `PipeClosedError` when closed; blocks on a full bounded buffer until the receiver consumes. |
| `receive()` | Reads the next instance. After either side closes it drains the buffer, then raises `PipeClosedError`. |
| Async context manager | Calls `open()` on enter; calls `close()` on exit and logs/swallows close errors. |

- `PayloadType` is a `BaseModel` subtype, parameterized by the implementation or allocator.
- Subclasses must implement `open`, `close`, `send`, and `receive`.
- Pipes do not multiplex: one `pipe_id` maps to exactly one pair of endpoints.

---

## Exceptions

| Exception | When |
| ---- | ---- |
| `PipeHandshakeError` | Handshake timeout, failed/rejected response, or allocator shutdown. |
| `PipeClosedError` | The pipe is closed and drained, or data is written after close. |
| `PipeTeardownError` | Reserved: an error occurred during teardown. |

> `open_pipe` / `expect_pipe` wrap handshake-phase exceptions in `PipeHandshakeError`;
> exceptions raised inside the user's context body propagate unchanged.
