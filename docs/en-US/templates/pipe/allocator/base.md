# Allocator Abstractions (pipe/allocator/base)

`infinity_bus.templates.pipe.allocator.base` defines the allocator abstractions. Back to
the [overview](allocator.md).

---

## `PipeAllocator`

```python
class PipeAllocator[PayloadType: BaseModel](ABC):
    async def start(self) -> None
    async def stop(self) -> None
    async def allocate(self, **kwargs: Any) -> tuple[str, Pipe[PayloadType]]
    async def release(self, pipe_id: str) -> None
    async def checkout(self, pipe_id: str) -> Pipe[PayloadType] | None
    async def checkin(self, pipe_id: str) -> None
```

| Method | Description |
| ---- | ---- |
| `start()` | Starts allocator-level resources such as listeners/sessions; no-op by default. |
| `stop()` | Releases allocator-level resources; no-op by default. |
| `allocate(**kwargs)` | Allocates a pipe and returns `(pipe_id, owner endpoint)`. kwargs are implementation-defined. |
| `release(pipe_id)` | Releases local resources and unregisters; cross-process implementations only close the local connection, and the peer finishes via EOF / `checkin()`. Must be idempotent. |
| `checkout(pipe_id)` | The borrower (server) claims its endpoint; returns `None` if absent or already claimed. |
| `checkin(pipe_id)` | The borrower returns and closes its endpoint without destroying the owner's resources. Must be idempotent. |

`start` / `stop` are not abstract: in-process implementations inherit the no-op versions;
cross-process implementations override them and callers pair them in `try/finally`.

---

## `SingleRoleAllocator`

```python
class SingleRoleAllocator[PayloadType: BaseModel](PipeAllocator[PayloadType]):
    ...
```

Base class for allocators that implement only the client or server role. The unimplemented
side raises `NotImplementedError`, preventing a client allocator from being used as a
server. The built-in [`ProcessPipeClientAllocator`](process.md) /
`ProcessPipeServerAllocator` and `WsPipeClientAllocator` / `WsPipeServerAllocator` inherit
from it.
