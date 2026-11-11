# Pipe 异步管道文档

## 概述

`pipe` 模块提供了一种基于事件总线的**双向异步管道**抽象，用于在两个异步上下文之间建立类型安全的通信通道。它通过事件握手协议动态建立连接，并封装了底层的背压控制与生命周期管理。

典型应用场景包括：

- 微服务组件间的流式数据交换。
- 长连接模拟（如 WebSocket 风格的通信）。
- 测试框架中模拟双向 RPC 通道。
- 将同步式请求-响应模型扩展为持久化双向流。

本模块依赖 [EventBus](../infinity_bus.md) 作为底层消息路由，并复用了 `request` 和 `expect` 模板来实现握手协议。

分配器实现位于子包 `infinity_bus.templates.pipe.allocator`，**必须显式传入** `open_pipe` / `expect_pipe`——模板不提供默认实例：

```python
from infinity_bus.templates.pipe import expect_pipe, open_pipe
from infinity_bus.templates.pipe.allocator import InProcessPipeAllocator

allocator = InProcessPipeAllocator()
```

---

## 核心概念

### 管道 (`Pipe`)

抽象基类，定义了 `send` / `receive` 基本操作。所有具体管道实现都必须遵循此接口。

### 握手协议

通过一对**请求/响应事件**建立连接：

- 客户端调用 `open_pipe` 发起请求，并通过显式传入的分配器分配管道实例。
- 服务端调用 `expect_pipe` 等待请求，通过 `allocator.checkout(pipe_id)` 领取本端端点，完成链路绑定。

握手成功后，两端通过 `send` / `receive` 直接通信，不再经过事件总线。**「两端如何持有管道」由分配器实现决定**：

- **同进程**（内置 `InProcessPipeAllocator`）：两端各持**一个端点对象**，共享同一双向通道但两个方向独立缓冲——任一端只会读到入站方向的数据，不会读到本端发出的消息。
- **跨进程**（自定义 `PipeAllocator`）：两端**各持一个** `Pipe` 对象（各自的本地缓冲 + 各自的传输句柄），以 `pipe_id` 作为会合键；`Pipe` 对象本身不会被传递。

#### 跨进程实现的约定

`pipe` 子包不对传输层做任何假设，跨进程语义完全由两端的分配器约定。实现时需满足：

1. **拨号时机**：客户端必须在 `allocate()` 阶段发起数据面连接。`expect_pipe` 在发布握手响应**之前**调用 `allocator.checkout(pipe_id)`；若服务端在该调用中等待一个「响应之后才会发生」的拨号，双方会互相等待直至 `Handshake timeout`。
2. **会合键**：`pipe_id` 是不透明字符串，服务端据此领取已接入的连接。地址、令牌、参数如何编码进 `pipe_id`，属于两端分配器的私有协议，模板不做解析。
3. **端点归属**：服务端可以自持数据面端点（例如 WebSocket 监听器），在分配器生命周期内启动一次，并在 `checkout()` 中按 `pipe_id` 交付连接。
4. **严格 P2P**：一个 `pipe_id` 只对应一对端点，管道不承担多路复用；`expect_pipe` 为一次性接管，不应并发调用去争抢同一个握手请求。
5. **关闭是终态**：`PipeClosedError` 表示管道已关闭且缓冲已耗尽；重连需要重新握手并分配新的 `pipe_id`。

### 进程内管道 (`InProcessPipe`)

基于 `asyncio.Queue` 的内置**全双工**实现：`create_pair()` 创建共享同一双向通道的一对端点，每个方向有独立队列，双向并发收发互不串台；`maxsize` 为单方向容量，提供背压。

### 管道分配器 (`PipeAllocator` / `InProcessPipeAllocator`)

抽象基类 `PipeAllocator` 定义了管道生命周期管理接口，位于子包 `infinity_bus.templates.pipe.allocator`：`allocate()` 创建一对端点并返回分配方端点，`checkout()` 领取借用方端点，`checkin()` 归还，`release()` 销毁整条管道。

分配器必须由调用方显式传入，模板不提供模块级默认实例。跨进程传输通过实现自定义 `PipeAllocator` 支持（见上文「跨进程实现的约定」），模板本身无需改动。

### 内置跨进程实现

子包 `infinity_bus.templates.pipe.allocator` 提供两套跨进程传输：

| 传输 | 端点 | 客户端分配器 | 服务端分配器 | 依赖 |
| ---- | ---- | ---- | ---- | ---- |
| 进程间（`multiprocessing.connection`） | `ProcessPipe` | `ProcessPipeClientAllocator` | `ProcessPipeServerAllocator` | 标准库 |
| WebSocket（aiohttp） | `WsPipe` | `WsPipeClientAllocator` | `WsPipeServerAllocator` | `aiohttp`（`infinity_bus[ws]`） |

两者遵循同一约定：`allocate()` 先建立数据面连接并发送首帧 `pipe_id`；服务端 accept 后按
`pipe_id` 寄存，`checkout()` 领取；帧载荷按构造时传入的 `payload_type` 编解码。

```python
from infinity_bus.templates.pipe.allocator import (
    ProcessPipeClientAllocator,
    ProcessPipeServerAllocator,
)

# 服务端：自持监听器，启动一次
server_alloc = ProcessPipeServerAllocator(('0.0.0.0', 8765), MyPayload, authkey=b'secret')
await server_alloc.start()

# 客户端：数据面直连（allocate() 阶段完成拨号）
client_alloc = ProcessPipeClientAllocator(('server-host', 8765), MyPayload, authkey=b'secret')
async with open_pipe(bus, 'pipe.connect', 'pipe.linked', client_alloc) as pipe:
    ...
```

`ProcessPipe` 底层 `Connection` 是阻塞 API 且 `close()` 不会唤醒其他线程中阻塞的读，因此接收侧在事件循环内用 `poll(0)` + 短间隔轮询，发送侧经 `asyncio.to_thread`；`authkey` 提供数据面鉴权。`WsPipe` 使用 aiohttp 入站泵，背压由 WebSocket flow control 提供，关闭帧由泵处理。

---

## 类与函数参考

### `Pipe` (抽象基类)

```python
class Pipe(ABC):
    async def __aenter__(self) -> "Pipe"
    async def __aexit__(self, ...)
    async def open(self) -> None
    async def close(self) -> None
    async def send(self, data: BaseModel) -> None
    async def receive(self) -> BaseModel
```

| 方法 | 说明 |
| ---- | ---- |
| `open()` | 打开管道，准备发送/接收数据。 |
| `close()` | 关闭管道，释放资源。 |
| `send(data)` | 向管道写入一个 Pydantic 模型实例。若管道已关闭或容量已满，则抛出相应异常。 |
| `receive()` | 从管道读取下一个 Pydantic 模型实例。若管道已关闭且无数据，则抛出 `PipeClosedError`。 |
| 异步上下文管理器 | 进入时自动调用 `open()`，退出时自动调用 `close()`。 |

### `PipeAllocator` (抽象基类)

```python
class PipeAllocator(ABC):
    async def allocate(self, **kwargs: Any) -> tuple[str, Pipe]
    async def release(self, pipe_id: str) -> None
    async def checkout(self, pipe_id: str) -> Pipe | None
    async def checkin(self, pipe_id: str) -> None
```

| 方法 | 说明 |
| ---- | ---- |
| `allocate(**kwargs)` | 分配一条新管道，返回 `(pipe_id, 分配方本端端点)`；关键字参数透传给具体管道构造函数。 |
| `release(pipe_id)` | 释放整条管道：关闭两端端点并注销。幂等，由分配方在退出时调用。 |
| `checkout(pipe_id)` | 借用方（服务端）领取自己的端点，不存在时返回 `None`。 |
| `checkin(pipe_id)` | 借用方归还自己的端点并注销，不销毁分配方资源。幂等。 |

### `InProcessPipeAllocator`

`PipeAllocator` 的进程内实现，管理管道实例的创建、查找与释放。

```python
class InProcessPipeAllocator(PipeAllocator):
    def __init__(self, pipe_type: type[InProcessPipe] = InProcessPipe)
```

- `pipe_type` 必须是 `InProcessPipe`（或其子类），并支持 `create_pair()`。
- `allocate(**kwargs)` 创建一对端点，返回 `(pipe_id, 分配方端点)`，`kwargs` 透传给 `create_pair()`（例如 `maxsize=10`）。
- `checkout(pipe_id)` 返回借用方端点；`checkin(pipe_id)` 归还并关闭它。
- `release(pipe_id)` 关闭两端端点并注销整条管道。
- `open_pipe` / `expect_pipe` 需要显式传入同一个分配器实例，不存在模块级默认实例。

### `InProcessPipe`

`Pipe` 的进程内实现，基于 `asyncio.Queue`。

```python
class InProcessPipe(Pipe):
    def __init__(self, maxsize: int | None = None)

    @classmethod
    def create_pair(cls, maxsize: int | None = None) -> tuple[InProcessPipe, InProcessPipe]
```

- `create_pair()` 创建一对共享双向通道的端点，每个方向独立队列；`maxsize` 是**单方向**容量，`None` 表示无限制。
- `InProcessPipe(maxsize=...)` 直接构造得到的是自环端点（写入由自身读回），仅用于单端测试，不表达两端通信语义。
- `receive()` 等待入站数据或**对端关闭**信号；对端关闭后先读完缓冲区，再抛 `PipeClosedError`。

### `open_pipe` (客户端上下文管理器)

```python
@asynccontextmanager
async def open_pipe(
    bus_proxy: EventBus.Proxy,
    req_event: str,
    resp_event: str,
    allocator: PipeAllocator,
    handshake_timeout: float = 5.0,
    session_id: str | None = None,
    pipe_kargs: dict[str, Any] | None = None,
) -> AsyncIterator[Pipe]
```

| 参数 | 类型 | 说明 |
| - | - | - |
| `bus_proxy` | `EventBus.Proxy` | 事件总线代理，用于发布握手请求。 |
| `req_event` | `str` | 握手请求事件名。 |
| `resp_event` | `str` | 握手响应事件名。 |
| `handshake_timeout` | `float` | 握手超时时间（秒）。超时抛出 `PipeHandshakeError`。 |
| `session_id` | `str \| None` | 用于关联请求-响应的会话 ID，若未提供则自动生成。 |
| `allocator` | `PipeAllocator` | 管道分配器实例，**必填**。同进程场景两端必须传入同一个实例。 |
| `pipe_kargs` | `dict[str, Any] \| None` | 传递给管道构造函数的关键字参数字典（如 `{"maxsize": 10}`），会透传给 `allocator.allocate(**pipe_kargs)`。 |

**Yields**
一个已握手成功并处于打开状态的 `Pipe` 实例。退出上下文时关闭管道并调用 `allocator.release(pipe_id)`。

**异常**

- `PipeHandshakeError`：握手超时、响应失败或响应表示拒绝。
- 其他由 `request` 模板抛出的异常（如总线关闭）。

### `expect_pipe` (服务端上下文管理器)

```python
@asynccontextmanager
async def expect_pipe(
    bus_proxy: EventBus.Proxy,
    req_event: str,
    resp_event: str,
    allocator: PipeAllocator,
    session_id: str | None = None,
    timeout: float = 5.0,
) -> AsyncIterator[Pipe]
```

| 参数 | 类型 | 说明 |
| - | - | - |
| `bus_proxy` | `EventBus.Proxy` | 事件总线代理，用于监听请求和发送响应。 |
| `req_event` | `str` | 期望监听的握手请求事件名。 |
| `resp_event` | `str` | 用于回复握手结果的事件名。 |
| `session_id` | `str \| None` | 用于筛选的会话 id，只响应指定会话 id 的请求。 |
| `timeout` | `float` | 等待握手请求的超时时间（秒）。超时抛出 `PipeHandshakeError`。 |
| `allocator` | `PipeAllocator` | 管道分配器实例，**必填**。同进程场景两端必须传入同一个实例。 |

**Yields**
握手成功后，返回 `allocator.checkout(pipe_id)` 领取到的 `Pipe` 实例。退出上下文时关闭管道并调用 `allocator.checkin(pipe_id)` 归还端点。

**握手流程**

1. 使用 `expect` 监听 `req_event`，等待 `PipeOpenRequest` 事件，并通过 `session_id` 筛选（若提供）。
2. 收到请求后，调用 `allocator.checkout(pipe_id)` 领取借用方端点。
3. 若管道不存在，发布一个 `success=False` 的 `PipeLinkedResponse` 并抛出 `PipeHandshakeError`。
4. 若存在，发布 `success=True` 的 `PipeLinkedResponse`，然后 `yield` 管道。
5. 退出上下文时关闭管道并调用 `allocator.checkin(pipe_id)` 归还端点（资源由分配方的 `release()` 收尾）。

---

## 内置数据模型

### `PipeOpenRequest`

```python
class PipeOpenRequest(RequestProtocol):
    pipe_id: str
```

握手请求的负载，包含管道唯一标识符。

### `PipeLinkedResponse`

```python
class PipeLinkedResponse(ResponseProtocol):
    pass
```

握手响应的负载，继承自 `ResponseProtocol`，包含 `success`、`error_msg` 等标准字段。

---

## 工作流程示意

```Text
客户端 (open_pipe)                          服务端 (expect_pipe)
      |                                             |
      |  1. 通过分配器创建 Pipe 实例                     |
      |-------------------------------------------->|
      |  2. 发布 PipeOpenRequest 事件 (req_event)    |
      |                                             |
      |                                             |  3. 监听 req_event，收到请求
      |                                             |  4. checkout 领取借用方端点
      |                                             |  5. 发布 PipeLinkedResponse (resp_event)
      |  6. 收到成功响应                             |
      |<--------------------------------------------|
      |                                             |
      |  7. 进入 async with pipe 块                  |  7. 进入 async with pipe 块
      |     两端各持端点，可同时双向收发             |
      |                                             |
      |  8. 退出上下文，关闭管道并清理注册表残留         |  8. 退出上下文，关闭管道
```

---

## 使用示例

以下示例假设已创建共享的 `allocator = InProcessPipeAllocator()`（同进程场景两端必须使用同一个分配器实例，但各自持有不同端点）。

### 基础双向通信

**服务端**（等待连接）

```python
async def server_task(bus_proxy):
    async with expect_pipe(bus_proxy, "pipe.connect", "pipe.linked", allocator) as pipe:
        print("Pipe connected")
        while True:
            data = await pipe.receive()
            if data is None:
                break
            print(f"Received: {data}")
            # 可回复数据
            await pipe.send(SomeResponseModel(result="ok"))
```

**客户端**（发起连接）

```python
async def client_task(bus_proxy):
    async with open_pipe(bus_proxy, "pipe.connect", "pipe.linked", allocator) as pipe:
        await pipe.send(SomeRequestModel(command="hello"))
        reply = await pipe.receive()
        print(f"Reply: {reply}")
```

### 自定义管道实现

继承 `Pipe` 可实现网络管道、文件管道等。

```python
class TcpPipe(Pipe):
    def __init__(self, host: str, port: int):
        super().__init__()
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None

    async def open(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    async def send(self, data: BaseModel):
        self.writer.write(data.json().encode() + b"\n")
        await self.writer.drain()

    async def receive(self) -> BaseModel:
        line = await self.reader.readline()
        return SomeModel.parse_raw(line)
```

自定义管道需要配套实现自己的 `PipeAllocator`（见上文「跨进程实现的约定」）；`InProcessPipeAllocator` 只接受 `InProcessPipe` 及其子类：

```python
class TcpPipeAllocator(PipeAllocator):
    ...  # 负责创建本端与对端凭据，并实现 allocate() / release() / checkout() / checkin()

async with open_pipe(bus, "tcp.connect", "tcp.linked", allocator=TcpPipeAllocator(...)) as pipe:
    ...
```

或者通过 `pipe_kargs` 传递构造参数：

```python
async with open_pipe(bus, "pipe.req", "pipe.resp", allocator, pipe_kargs={"maxsize": 10}) as pipe:
    ...
```

### 超时与错误处理

```python
try:
    async with open_pipe(bus, "pipe.req", "pipe.resp", allocator, handshake_timeout=2.0) as pipe:
        await asyncio.wait_for(pipe.receive(), timeout=5.0)
except PipeHandshakeError:
    print("Handshake failed")
except PipeClosedError:
    print("Pipe closed prematurely")
except asyncio.TimeoutError:
    print("Receive timeout")
```

---

## 异常类型

| 异常 | 触发场景 |
| ---- | -------- |
| `PipeHandshakeError` | 握手超时、响应失败、管道未找到。 |
| `PipeTeardownError` | （预留）关闭管道时发生错误。 |
| `PipeClosedError` | 尝试向已关闭的管道发送数据，或在关闭后等待接收且队列为空。 |

---

## 注意事项

1. **必须成对使用**：`open_pipe` 和 `expect_pipe` 必须使用相同的 `req_event` / `resp_event` 名称，并确保双方都处于运行状态。
2. **管道生命周期**：`open_pipe` 和 `expect_pipe` 都使用 `async with` 管理管道，退出上下文时会自动调用 `pipe.close()`。请勿在上下文外持有管道引用。
3. **背压控制**：`InProcessPipe` 的 `maxsize` 参数可限制未处理消息的数量。当队列满时，`send()` 会阻塞等待接收方消费，实现自然背压。
4. **线程安全**：本模块设计用于 `asyncio` 单线程环境，不可跨线程使用。
5. **事件总线依赖**：使用前确保 `EventBus` 已启动且 `bus_proxy` 有效。
6. **注册表清理**：`open_pipe` 在 `finally` 块中确保移除注册的管道。若发生异常导致握手未完成，残留的注册项也会被清理。

---

## 内部实现细节

- `expect_pipe` 内部使用 `expect` 模板监听握手请求，利用 `OneShotEventHandler` 实现一次性等待。
- 响应使用 `PipeLinkedResponse` 模型，遵循 `ResponseProtocol` 约定，包含 `session_id` 与 `request_id` 用于关联。
- `InProcessPipe.receive()` 使用 `asyncio.wait` 同时等待队列 `get()` 和 `_closed` 事件，避免在关闭后无限阻塞。

---

## 完整示例

参考项目中的 `tests/templates/pipe_test.py`，其中包含握手成功、超时、管道关闭、并发发送接收等场景的完整测试。
