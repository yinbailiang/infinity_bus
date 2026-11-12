# 进程内实现（pipe/allocator/inprocess）

`infinity_bus.templates.pipe.allocator.inprocess` 提供进程内端点对与分配器。返回
总览见 [allocator.md](allocator.md)。

---

## `InProcessPipeAllocator`

```python
class InProcessPipeAllocator[PayloadType: BaseModel = BaseModel](PipeAllocator[PayloadType]):
    def __init__(self, pipe_type: type[InProcessPipe[PayloadType]] = InProcessPipe)
```

- 泛型参数用于类型推导：`InProcessPipeAllocator[MyPayload]()` 让
  `open_pipe` / `expect_pipe` 返回 `Pipe[MyPayload]`。
- `allocate(**kwargs)` 调用 `create_pair(pipe_type, **kwargs)`，返回
  `(pipe_id, 分配方端点)`；`maxsize` 透传给队列。
- `checkout(pipe_id)` 领取借用方端点；同一个 `pipe_id` 只能领取一次，重复返回
  `None`。
- `checkin(pipe_id)` 归还并关闭借用方端点；`release(pipe_id)` 关闭两端并注销。
- 同进程两端必须共享同一个分配器实例。

---

## `create_pair`

```python
def create_pair[PayloadType: BaseModel](
    pipe_type: type[InProcessPipe[PayloadType]],
    maxsize: int | None = None,
) -> tuple[InProcessPipe[PayloadType], InProcessPipe[PayloadType]]
```

创建共享同一双向通道的一对端点：每个方向独立队列，`maxsize` 是**单方向**容量
（`None` = 无限制）。两个端点的关闭事件交叉共享，实现双向关闭通知。

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

- `in_queue` / `out_queue`：本端入站/出站队列；`in_closed` 由对端 `close()` 触发，
  `out_closed` 由本端 `close()` 触发。
- `send()`：本端或对端关闭时抛 `PipeClosedError`；队列满时阻塞等待消费，并在对端
  关闭时解除阻塞。
- `receive()`：等待入站数据或任一端关闭；关闭后先排空缓冲，再抛
  `PipeClosedError`。取消 `receive()` 不会遗留消费任务。
- `open()`：重置本端关闭标记并清空残留入站数据；`close()` 是终态。
- 一般应通过 `create_pair(InProcessPipe, ...)` 构造；直接构造需要调用方自行接线队列与事件。

> 端点之间直接传递 Pydantic 对象，不做序列化；跨进程实现才经过
> [codec](../codec.md)。
