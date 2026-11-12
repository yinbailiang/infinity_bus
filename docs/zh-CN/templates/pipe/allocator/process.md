# 进程间传输（pipe/allocator/process）

`infinity_bus.templates.pipe.allocator.process` 基于标准库
`multiprocessing.connection` 的 `Listener` / `Client` 实现跨进程管道。返回总览见
[allocator.md](allocator.md)。

- 客户端 `allocate()` 直连监听地址，并先发送一帧 `pipe_id` 作为 hello；
- 服务端 accept 线程读取 hello，按 `pipe_id` 寄存连接；
- `expect_pipe` 通过 `checkout(pipe_id)` 领取，`checkin()` 归还；
- 帧载荷经 [codec](../codec.md) 做 JSON 编解码。

---

## `ProcessPipe`

```python
class ProcessPipe[PayloadType: BaseModel](Pipe[PayloadType]):
    def __init__(self, conn: Connection, payload_type: type[PayloadType])
```

- `send()` 在 `asyncio.to_thread` 中调用 `Connection.send_bytes()`。
- `receive()` 在事件循环中用 `poll(0)` + `POLL_INTERVAL`（10ms）轮询，避免阻塞读
  线程无法被 `close()` 唤醒的问题。
- 对端关闭（EOF）后 `receive()` 抛 `PipeClosedError`。
- 每条等待中的管道每 10ms 唤醒一次，适合少量严格 P2P 连接。

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

| 参数 | 说明 |
| ---- | ---- |
| `address` | `Listener` 地址；端口传 `0` 时由系统分配。 |
| `payload_type` | 帧载荷模型类型，用于解码。 |
| `authkey` | `Connection` 鉴权密钥；`None` 表示不鉴权。 |
| `accept_timeout` | `checkout()` 等待连接到达的超时。 |
| `hello_timeout` | accept 后等待首帧 hello 的超时；超时只关闭该连接。 |
| `max_pending` | 未领取连接上限；超出时关闭最旧连接。 |

- `start()` 创建 `Listener` 并启动 accept 线程；`stop()` 发送哨兵、取消等待中的
  `checkout()`（收到 `PipeHandshakeError`）、关闭监听器与所有端点。
- 单条连接鉴权失败、hello 超时、非法 UTF-8 或处理异常都不会终结 accept 线程。
- `_pending` 中无人领取的连接会在 `stop()` 或达到 `max_pending` 时关闭。

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

`allocate()` 在 `connect_timeout` 内建立连接并发送 hello；失败时显式关闭连接并抛
`PipeHandshakeError`。`release(pipe_id)` 关闭本端端点，`stop()` 关闭全部本端端点。

> `allocate(**kwargs)` 不使用额外参数；进程间传输没有可配置的入站队列。

---

## 示例

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
