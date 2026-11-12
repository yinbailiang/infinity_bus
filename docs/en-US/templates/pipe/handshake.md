# Handshake Templates (pipe/handshake)

`infinity_bus.templates.pipe.handshake` provides `open_pipe` / `expect_pipe`: the control
plane performs the handshake over the event bus, while the data plane is delegated to the
`Pipe` created by the allocator. Back to the [overview](pipe.md).

---

## `open_pipe` (Client Context Manager)

```python
@asynccontextmanager
async def open_pipe[PayloadType: BaseModel](
    bus_proxy: EventBus.Proxy,
    req_event: str,
    resp_event: str,
    allocator: PipeAllocator[PayloadType],
    *,
    handshake_timeout: float = 5.0,
    session_id: str | None = None,
    pipe_kwargs: dict[str, Any] | None = None,
) -> AsyncGenerator[Pipe[PayloadType], None]
```

| Parameter | Type | Description |
| - | - | - |
| `bus_proxy` | `EventBus.Proxy` | Publishes the handshake request. |
| `req_event` | `str` | Handshake request event name. |
| `resp_event` | `str` | Handshake response event name. |
| `allocator` | `PipeAllocator[PayloadType]` | **Required**; in-process peers must share the same instance. |
| `handshake_timeout` | `float` | Handshake timeout in seconds. |
| `session_id` | `str \| None` | Correlates request/response; auto-generated when omitted. |
| `pipe_kwargs` | `dict[str, Any] \| None` | Forwarded to `allocator.allocate(**pipe_kwargs)`; only some implementations use it (e.g. in-process `maxsize`). |

**Yields**: an opened `Pipe[PayloadType]`. On exit it closes the pipe and calls
`allocator.release(pipe_id)`.

**Exceptions**: handshake-phase exceptions are wrapped in `PipeHandshakeError`; exceptions
from the context body propagate unchanged.

---

## `expect_pipe` (Server Context Manager)

```python
@asynccontextmanager
async def expect_pipe[PayloadType: BaseModel](
    bus_proxy: EventBus.Proxy,
    req_event: str,
    resp_event: str,
    allocator: PipeAllocator[PayloadType],
    session_id: str | None = None,
    timeout: float = 5.0,
) -> AsyncGenerator[Pipe[PayloadType], None]
```

| Parameter | Type | Description |
| - | - | - |
| `bus_proxy` | `EventBus.Proxy` | Listens for requests and publishes responses. |
| `req_event` | `str` | Handshake request event name. |
| `resp_event` | `str` | Handshake response event name. |
| `allocator` | `PipeAllocator[PayloadType]` | **Required**; in-process peers must share the same instance. |
| `session_id` | `str \| None` | Only respond to this session. |
| `timeout` | `float` | Handshake request timeout in seconds. |

**Yields**: the `Pipe[PayloadType]` obtained from `allocator.checkout(pipe_id)`. On exit it
closes the pipe and calls `allocator.checkin(pipe_id)`.

**Handshake flow**:

1. `expect` listens on `req_event`, filtering `PipeOpenRequest` by `session_id`;
2. `allocator.checkout(pipe_id)` claims the borrower endpoint;
3. if absent, publishes `success=False` and raises `PipeHandshakeError`;
4. otherwise publishes `success=True` and yields;
5. on exit, `checkin` runs and the owner's `release()` finishes teardown.

> `checkout()` happens *before* publishing the success response, so cross-process clients
> must dial during `allocate()`. See
> [allocator/allocator.md](allocator/allocator.md) for the full contract.

---

## Workflow

```text
Client (open_pipe)                            Server (expect_pipe)
      |                                             |
      | 1. allocator.allocate()                     |
      | 2. publish PipeOpenRequest (req_event) ---->|
      |                                             | 3. expect receives request
      |                                             | 4. allocator.checkout(pipe_id)
      |                                             | 5. publish PipeLinkedResponse
      | 6. receive success response <---------------|
      |                                             |
      | 7. both sides send / receive (data plane)   |
      |                                             |
      | 8. exit: close + release                    | 8. exit: close + checkin
```

---

## Timeout & Error Handling

```python
try:
    async with open_pipe(
        bus, 'pipe.req', 'pipe.resp', allocator, handshake_timeout=2.0
    ) as pipe:
        await asyncio.wait_for(pipe.receive(), timeout=5.0)
except PipeHandshakeError:
    print('Handshake failed')
except PipeClosedError:
    print('Pipe closed prematurely')
except asyncio.TimeoutError:
    print('Receive timeout')
```
