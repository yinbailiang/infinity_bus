# 分配器抽象（pipe/allocator/base）

`infinity_bus.templates.pipe.allocator.base` 定义分配器抽象。返回总览见
[allocator.md](allocator.md)。

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

| 方法 | 说明 |
| ---- | ---- |
| `start()` | 启动监听器/会话等分配器级资源；默认无操作。 |
| `stop()` | 释放分配器级资源；默认无操作。 |
| `allocate(**kwargs)` | 分配一条新管道，返回 `(pipe_id, 分配方本端端点)`。kwargs 由实现解释。 |
| `release(pipe_id)` | 释放本端资源并注销；跨进程实现只关闭本端连接，对端通过 EOF / `checkin()` 收尾。必须幂等。 |
| `checkout(pipe_id)` | 借用方（服务端）领取端点；不存在或已被领取时返回 `None`。 |
| `checkin(pipe_id)` | 借用方归还并关闭端点；不得销毁分配方资源。必须幂等。 |

`start` / `stop` 不是抽象的：进程内实现直接继承无操作版本；跨进程实现覆盖它们，并
由调用方在 `try/finally` 中成对调用。

---

## `SingleRoleAllocator`

```python
class SingleRoleAllocator[PayloadType: BaseModel](PipeAllocator[PayloadType]):
    ...
```

用于只实现客户端或服务端单一角色的分配器。未实现的另一侧方法抛出
`NotImplementedError`，避免把客户端分配器当服务端使用。内置
[`ProcessPipeClientAllocator`](process.md) / `ProcessPipeServerAllocator` 与
`WsPipeClientAllocator` / `WsPipeServerAllocator` 均继承它。
