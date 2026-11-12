# 双向异步管道（pipe）

`infinity_bus.templates.pipe` 提供基于事件总线的**双向异步管道**：控制面（握手）走
`EventBus`，数据面由 `Pipe` 实现直接收发，不再经过总线。模板本身不绑定传输层；
进程内、`multiprocessing.connection` 与 WebSocket 实现均随子包提供。

- 握手复用 [`request`](../request.md) / [`expect`](../expect.md) 模板；
- 两端各持一个端点，以 `pipe_id` 会合；
- 分配器必须显式传入，没有模块级默认实例。

> WebSocket 传输需要可选依赖：`pip install infinity_bus[ws]`（aiohttp）。其余传输无额外依赖。

---

## 目录结构（镜像代码）

```text
infinity_bus.templates.pipe/
├── pipe.md          # 本总览
├── base.md          # Pipe 抽象基类与异常
├── protocol.md      # 握手负载模型
├── codec.md         # 帧载荷编解码
├── handshake.md     # open_pipe / expect_pipe
└── allocator/
    ├── allocator.md # 分配器总览与契约
    ├── base.md      # PipeAllocator / SingleRoleAllocator
    ├── inprocess.md # 进程内实现
    ├── process.md   # multiprocessing.connection 传输
    └── ws.md        # aiohttp WebSocket 传输
```

| 文档 | 对应模块 | 说明 |
| - | - | - |
| [base.md](base.md) | `pipe/base.py` | `Pipe` 抽象、异常与关闭语义 |
| [protocol.md](protocol.md) | `pipe/protocol.py` | `PipeOpenRequest` / `PipeLinkedResponse` |
| [codec.md](codec.md) | `pipe/codec.py` | 跨进程帧的 JSON 编解码 |
| [handshake.md](handshake.md) | `pipe/handshake.py` | `open_pipe` / `expect_pipe` |
| [allocator/allocator.md](allocator/allocator.md) | `pipe/allocator/__init__.py` | 分配器契约与跨进程约定 |
| [allocator/base.md](allocator/base.md) | `pipe/allocator/base.py` | `PipeAllocator` / `SingleRoleAllocator` |
| [allocator/inprocess.md](allocator/inprocess.md) | `pipe/allocator/inprocess.py` | 进程内端点与分配器 |
| [allocator/process.md](allocator/process.md) | `pipe/allocator/process.py` | 基于 `multiprocessing.connection` |
| [allocator/ws.md](allocator/ws.md) | `pipe/allocator/ws.py` | 基于 aiohttp WebSocket |

---

## 核心概念

### 握手协议

客户端调用 [`open_pipe`](handshake.md) 发起请求，服务端调用
[`expect_pipe`](handshake.md) 等待，并通过 `allocator.checkout(pipe_id)` 领取本端
端点。握手成功后两端直接 `send` / `receive`，不再经过总线。

**拨号时机**：跨进程客户端必须在 `allocate()` 阶段建立数据面连接；`expect_pipe`
在发布成功响应**之前**调用 `checkout()`。否则双方会互相等待直至 `Handshake timeout`。

### 分配器

[`PipeAllocator`](allocator/base.md) 负责四个动作：

- `allocate()` / `release()`：分配方（客户端）管理资源生命周期；
- `checkout()` / `checkin()`：借用方（服务端）领取/归还端点；
- `start()` / `stop()`：分配器级资源（监听器、会话），默认无操作。

完整契约、单次借用语义与跨进程约定见 [allocator/allocator.md](allocator/allocator.md)。

---

## 内置传输

| 端点 | 客户端分配器 | 服务端分配器 | 依赖 |
| ---- | ---- | ---- | ---- |
| [`InProcessPipe`](allocator/inprocess.md) | [`InProcessPipeAllocator`](allocator/inprocess.md) | 同进程共享同一分配器 | 无 |
| [`ProcessPipe`](allocator/process.md) | `ProcessPipeClientAllocator` | `ProcessPipeServerAllocator` | 标准库 |
| [`WsPipe`](allocator/ws.md) | `WsPipeClientAllocator` | `WsPipeServerAllocator` | `aiohttp`（`infinity_bus[ws]`） |

```python
from infinity_bus.templates.pipe import expect_pipe, open_pipe
from infinity_bus.templates.pipe.allocator import InProcessPipeAllocator

allocator = InProcessPipeAllocator[MyPayload]()

async with expect_pipe(bus_server, 'pipe.connect', 'pipe.linked', allocator) as server_pipe:
    ...

async with open_pipe(bus_client, 'pipe.connect', 'pipe.linked', allocator) as client_pipe:
    ...
```

> 同进程两端必须使用同一个分配器实例；跨进程两端各自创建客户端 / 服务端分配器。

---

## 注意事项

1. **成对使用**：`open_pipe` 与 `expect_pipe` 必须使用相同的 `req_event` / `resp_event`，并确保双方都在运行。
2. **生命周期**：两个上下文管理器退出时会关闭管道并分别调用 `release()` / `checkin()`；跨进程分配器还需在 `try/finally` 中成对调用 `start()` / `stop()`。
3. **背压**：进程内 `create_pair(maxsize=...)` 与 WebSocket 分配器的 `maxsize` 控制入站缓冲；容量满时 `send()` 阻塞等待消费。
4. **关闭是终态**：`close()` 后先读完缓冲区，再抛 `PipeClosedError`；重连需要重新握手并分配新的 `pipe_id`。
5. **单线程 asyncio**：`Pipe` 实现面向事件循环；进程间传输内部使用后台线程，对外 API 仍在事件循环中调用。
6. **信任边界**：`ProcessPipe` 的 `authkey=None` 表示不鉴权；`WsPipe` 没有内建鉴权，应仅监听 loopback / 内网，或在 hello 帧中自行加入令牌。

## 完整示例

参考 `tests/templates/pipe/`，覆盖握手成功、超时、关闭、取消、背压与跨进程端到端场景。
