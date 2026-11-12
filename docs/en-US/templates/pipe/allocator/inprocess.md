# In-Process Implementation (pipe/allocator/inprocess)

`infinity_bus.templates.pipe.allocator.inprocess` provides the in-process endpoint pair and
allocator. Back to the [overview](allocator.md).

---

## `InProcessPipeAllocator`

```python
class InProcessPipeAllocator[PayloadType: BaseModel = BaseModel](PipeAllocator[PayloadType]):
    def __init__(self, pipe_type: type[InProcessPipe[PayloadType]] = InProcessPipe)
```

- The type parameter drives inference: `InProcessPipeAllocator[MyPayload]()` makes
  `open_pipe` / `expect_pipe` yield `Pipe[MyPayload]`.
- `allocate(**kwargs)` calls `create_pair(pipe_type, **kwargs)` and returns
  `(pipe_id, owner endpoint)`; `maxsize` is forwarded to the queues.
- `checkout(pipe_id)` claims the borrower endpoint; a second call for the same id returns
  `None` (single borrow).
- `checkin(pipe_id)` returns and closes the borrower endpoint; `release(pipe_id)` closes
  both endpoints and unregisters.
- In-process peers must share the same allocator instance.

---

## `create_pair`

```python
def create_pair[PayloadType: BaseModel](
    pipe_type: type[InProcessPipe[PayloadType]],
    maxsize: int | None = None,
) -> tuple[InProcessPipe[PayloadType], InProcessPipe[PayloadType]]
```

Creates two endpoints sharing one duplex channel: each direction has its own queue, and
`maxsize` is the **per-direction** capacity (`None` = unlimited). The endpoints cross-share
their close events so either side can signal terminal closure.

```python
from infinity_bus.templates.pipe.allocator import InProcessPipe, create_pair

first, second = create_pair(InProcessPipe[MyPayload], maxsize=10)
await first.send(MyPayload(...))
received = await second.receive()
```

---

## `InProcessPipe`

```python
class InProcessPipe[PayloadType: BaseModel](Pipe[PayloadType]):
    def __init__(
        self,
        in_queue: asyncio.Queue[PayloadType],
        out_queue: asyncio.Queue[PayloadType],
        in_closed: asyncio.Event,
        out_closed: asyncio.Event,
    )
```

- `in_queue` / `out_queue`: local inbound/outbound queues; `in_closed` is triggered by the
  peer's `close()`, while `out_closed` is triggered by the local `close()`.
- `send()`: raises `PipeClosedError` when either side is closed; blocks on a full queue and
  is released when the peer closes.
- `receive()`: waits for inbound data or either side closing; after close it drains the
  buffer and raises `PipeClosedError`. Cancelling `receive()` leaves no consuming task
  behind.
- `open()`: resets the local close flag and clears leftover inbound data; `close()` is
  terminal.
- Prefer `create_pair(InProcessPipe, ...)`; direct construction requires the caller to wire
  queues and events.

> Endpoints pass Pydantic objects directly; only cross-process implementations use the
> [codec](../codec.md).
