# Pipe Allocators (pipe/allocator)

`infinity_bus.templates.pipe.allocator` creates, claims, and releases pipe endpoints. An
allocator must be passed explicitly to `open_pipe` / `expect_pipe`; there is no module-level
default. Back to the [overview](../pipe.md).

---

## Directory Structure (mirrors the code)

```text
infinity_bus.templates.pipe.allocator/
├── allocator.md # this overview
├── base.md      # PipeAllocator / SingleRoleAllocator
├── inprocess.md # in-process implementation
├── process.md   # multiprocessing.connection transport
└── ws.md        # aiohttp WebSocket transport
```

| Doc | Module | Description |
| - | - | - |
| [base.md](base.md) | `allocator/base.py` | Allocator abstractions and single-role base |
| [inprocess.md](inprocess.md) | `allocator/inprocess.py` | In-process endpoints, `create_pair`, allocator |
| [process.md](process.md) | `allocator/process.py` | `multiprocessing.connection` transport |
| [ws.md](ws.md) | `allocator/ws.py` | aiohttp WebSocket transport |

---

## Contract

[`PipeAllocator`](base.md) separates responsibilities into three pairs:

| Methods | Owner | Description |
| ---- | ---- | ---- |
| `start()` / `stop()` | Allocator user | Allocator-level resources (listeners, HTTP sessions); no-ops by default. |
| `allocate()` / `release()` | Owner (client) | Resource lifecycle; `release()` must be idempotent. |
| `checkout()` / `checkin()` | Borrower (server) | Claim/return endpoints; `checkin()` must not destroy the owner's resources and must be idempotent. |

- **Single borrow**: one `pipe_id` can be checked out once; a second call returns `None`.
- **kwargs**: `allocate(**kwargs)` is implementation-defined; in-process forwards them to
  `create_pair()` (e.g. `maxsize`), cross-process implementations may ignore them.
- **No default allocator**: in-process peers share one allocator instance; cross-process
  peers create client and server allocators separately.

---

## Cross-Process Implementation Contract

The `pipe` package makes no transport assumptions; cross-process semantics are an agreement
between the two allocators:

1. **Dial timing**: the client opens the data-plane connection during `allocate()`.
   `expect_pipe` calls `checkout()` before publishing the success response; if the server
   waits in `checkout()` for a dial that only happens after the response, both sides wait
   until `Handshake timeout`.
2. **Rendezvous key**: `pipe_id` is opaque; how addresses, tokens, and parameters are
   encoded is a private protocol between the allocators and is never parsed by the
   templates.
3. **Endpoint ownership**: the server may own the data-plane endpoint (e.g. a WebSocket
   listener), start it once for the allocator's lifetime, and deliver connections from
   `checkout()` by `pipe_id`.
4. **Strictly P2P**: one `pipe_id` maps to exactly one pair of endpoints; pipes do not
   multiplex, and `expect_pipe` is a one-shot takeover.
5. **Close is terminal**: `PipeClosedError` means the pipe is closed and drained;
   reconnecting requires a new handshake and a new `pipe_id`.

The built-in cross-process implementations share the same first-frame `pipe_id` rendezvous
protocol; see [process.md](process.md) and [ws.md](ws.md).

---

## Custom Pipes and Allocators

Subclass [`Pipe`](../base.md) for the transport, then implement
`PipeAllocator[PayloadType]`:

```python
class TcpPipe(Pipe[SomeModel]):
    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    async def open(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(self._host, self._port)

    async def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            await self._writer.wait_closed()

    async def send(self, data: SomeModel) -> None:
        assert self._writer is not None
        self._writer.write(data.model_dump_json().encode() + b'\n')
        await self._writer.drain()

    async def receive(self) -> SomeModel:
        assert self._reader is not None
        return SomeModel.model_validate_json(await self._reader.readline())


class TcpPipeAllocator(PipeAllocator[SomeModel]):
    ...  # implement start/stop and allocate/release/checkout/checkin

async with open_pipe(bus, 'tcp.connect', 'tcp.linked', allocator=TcpPipeAllocator(...)) as pipe:
    ...
```
