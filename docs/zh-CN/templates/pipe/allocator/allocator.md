# 管道分配器（pipe/allocator）

`infinity_bus.templates.pipe.allocator` 负责创建、领取与释放管道端点。分配器必须由
调用方显式传入 `open_pipe` / `expect_pipe`，没有模块级默认实例。返回总览见
[pipe.md](../pipe.md)。

---

## 目录结构（镜像代码）

```text
infinity_bus.templates.pipe.allocator/
├── allocator.md # 本总览
├── base.md      # PipeAllocator / SingleRoleAllocator
├── inprocess.md # 进程内实现
├── process.md   # multiprocessing.connection 传输
└── ws.md        # aiohttp WebSocket 传输
```

| 文档 | 对应模块 | 说明 |
| - | - | - |
| [base.md](base.md) | `allocator/base.py` | 分配器抽象与单角色基类 |
| [inprocess.md](inprocess.md) | `allocator/inprocess.py` | 进程内端点、`create_pair`、进程内分配器 |
| [process.md](process.md) | `allocator/process.py` | 基于 `multiprocessing.connection` 的传输 |
| [ws.md](ws.md) | `allocator/ws.py` | 基于 aiohttp WebSocket 的传输 |

---

## 契约

[`PipeAllocator`](base.md) 将职责拆成三对方法：

| 方法 | 负责方 | 说明 |
| ---- | ---- | ---- |
| `start()` / `stop()` | 分配器使用者 | 监听器、HTTP 会话等分配器级资源；默认无操作。 |
| `allocate()` / `release()` | 分配方（客户端） | 资源生命周期；`release()` 必须幂等。 |
| `checkout()` / `checkin()` | 借用方（服务端） | 端点领取/归还；`checkin()` 不得销毁分配方资源，必须幂等。 |

- **单次借用**：同一个 `pipe_id` 只能 `checkout()` 一次，重复调用返回 `None`。
- **kwargs**：`allocate(**kwargs)` 由实现解释；进程内实现透传给 `create_pair()`
  （如 `maxsize`），跨进程实现可忽略。
- **无默认分配器**：同进程两端共享同一个分配器实例；跨进程两端分别创建客户端 /
  服务端分配器。

---

## 跨进程实现的约定

`pipe` 子包不对传输层做任何假设，跨进程语义完全由两端分配器约定：

1. **拨号时机**：客户端在 `allocate()` 阶段建立数据面连接。`expect_pipe` 在发布
   成功响应之前调用 `checkout()`；若服务端在 `checkout()` 中等待一个「响应之后才
   发生」的拨号，双方会互相等待至 `Handshake timeout`。
2. **会合键**：`pipe_id` 是不透明字符串；地址、令牌、参数如何编码属于两端分配器的
   私有协议，模板不解析。
3. **端点归属**：服务端可自持数据面端点（如 WebSocket 监听器），在分配器生命周期
   内启动一次，并在 `checkout()` 中按 `pipe_id` 交付连接。
4. **严格 P2P**：一个 `pipe_id` 只对应一对端点，不承担多路复用；`expect_pipe` 为
   一次性接管。
5. **关闭是终态**：`PipeClosedError` 表示管道已关闭且缓冲耗尽；重连需要重新握手并
   分配新的 `pipe_id`。

内置跨进程实现遵循相同的首帧 `pipe_id` 会合协议，见 [process.md](process.md) 与
[ws.md](ws.md)。

---

## 自定义管道与分配器

继承 [`Pipe`](../base.md) 实现传输，再配套实现 `PipeAllocator[PayloadType]`：

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
    ...  # 实现 start/stop 与 allocate/release/checkout/checkin

async with open_pipe(bus, 'tcp.connect', 'tcp.linked', allocator=TcpPipeAllocator(...)) as pipe:
    ...
```
