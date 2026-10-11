# Pipe 异步管道文档

## 概述

`pipe` 模块提供了一种基于事件总线的**双向异步管道**抽象，用于在两个异步上下文之间建立类型安全的通信通道。它通过事件握手协议动态建立连接，并封装了底层的背压控制与生命周期管理。

典型应用场景包括：

- 微服务组件间的流式数据交换。
- 长连接模拟（如 WebSocket 风格的通信）。
- 测试框架中模拟双向 RPC 通道。
- 将同步式请求-响应模型扩展为持久化双向流。

本模块依赖 [EventBus](../infinity_bus.md) 作为底层消息路由，并复用了 `request` 和 `expect` 模板来实现握手协议。

---

## 核心概念

### 管道 (`Pipe`)

抽象基类，定义了 `send` / `receive` 基本操作。所有具体管道实现都必须遵循此接口。

### 握手协议

通过一对**请求/响应事件**建立连接：

- 客户端调用 `open_pipe` 发起请求，并通过 `InProcessPipeAllocator` 分配管道实例。
- 服务端调用 `expect_pipe` 等待请求，从分配器中取出对应的管道实例，完成链路绑定。

握手成功后，两端持有同一个 `Pipe` 实例的引用，可通过 `send` / `receive` 直接通信，无需再经过事件总线。

### 进程内管道 (`InProcessPipe`)

基于 `asyncio.Queue` 的默认实现，支持可配置的队列容量以提供背压控制。

### 管道分配器 (`PipeAllocator` / `InProcessPipeAllocator`)

抽象基类 `PipeAllocator` 定义了管道生命周期管理接口，`InProcessPipeAllocator` 是其默认进程内实现，用于在握手期间临时存储管道实例，确保客户端与服务端能找到同一个管道对象。

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
    async def allocate(self, **kwargs: Dict[str, Any]) -> str
    async def release(self, pipe_id: str) -> None
    async def get(self, pipe_id: str) -> Optional[Pipe]
```

| 方法 | 说明 |
| ---- | ---- |
| `allocate(**kwargs)` | 创建一个管道实例并返回其唯一标识符。关键字参数会传递给具体管道类型的构造函数。 |
| `release(pipe_id)` | 释放指定管道，从分配器中移除。 |
| `get(pipe_id)` | 获取管道实例，不存在时返回 `None`。 |

### `InProcessPipeAllocator`

`PipeAllocator` 的进程内实现，管理管道实例的创建、查找与释放。

```python
class InProcessPipeAllocator(PipeAllocator):
    def __init__(self, pipe_type: type[Pipe] = InProcessPipe)
```

- 构造参数 `pipe_type` 指定默认的管道类型，`allocate()` 时会用该类型创建实例。
- `allocate(**kwargs)` 接受关键字参数并透传给 `pipe_type` 的构造函数（例如 `InProcessPipe(maxsize=10)`）。
- `open_pipe` 和 `expect_pipe` 默认共享一个模块级 `InProcessPipeAllocator` 实例（通过 `get_default_allocator()` 获取），也可通过 `allocator` 参数传入自定义实例。

### `InProcessPipe`

`Pipe` 的进程内实现，基于 `asyncio.Queue`。

```python
class InProcessPipe(Pipe):
    def __init__(self, maxsize: Optional[int] = None)
```

- `maxsize` 直接传递给内部的 `asyncio.Queue`，`None` 表示无限容量。
- `receive()` 内部使用 `asyncio.wait` 同时等待新数据或管道关闭信号，确保关闭时能及时抛出 `PipeClosedError`。

### `open_pipe` (客户端上下文管理器)

```python
@asynccontextmanager
async def open_pipe(
    bus_proxy: EventBus.Proxy,
    req_event: str,
    resp_event: str,
    handshake_timeout: float = 5.0,
    session_id: Optional[str] = None,
    allocator: Optional[InProcessPipeAllocator] = None,
    pipe_kargs: Optional[Dict[str, Any]] = None,
) -> AsyncIterator[Pipe]
```

| 参数 | 类型 | 说明 |
| - | - | - |
| `bus_proxy` | `EventBus.Proxy` | 事件总线代理，用于发布握手请求。 |
| `req_event` | `str` | 握手请求事件名。 |
| `resp_event` | `str` | 握手响应事件名。 |
| `handshake_timeout` | `float` | 握手超时时间（秒）。超时抛出 `PipeHandshakeError`。 |
| `session_id` | `Optional[str]` | 用于关联请求-响应的会话 ID，若未提供则自动生成。 |
| `allocator` | `Optional[InProcessPipeAllocator]` | 管道分配器实例。若未提供则使用模块级默认实例（`get_default_allocator()`）。 |
| `pipe_kargs` | `Optional[Dict[str, Any]]` | 传递给管道构造函数的关键字参数字典（如 `{"maxsize": 10}`），会透传给 `allocator.allocate(**pipe_kargs)`。 |

**Yields**
一个已握手成功并处于打开状态的 `Pipe` 实例。退出上下文时自动关闭管道并从分配器中释放。

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
    session_id: Optional[str] = None,
    timeout: float = 5.0,
    allocator: Optional[InProcessPipeAllocator] = None,
) -> AsyncIterator[Pipe]
```

| 参数 | 类型 | 说明 |
| - | - | - |
| `bus_proxy` | `EventBus.Proxy` | 事件总线代理，用于监听请求和发送响应。 |
| `req_event` | `str` | 期望监听的握手请求事件名。 |
| `resp_event` | `str` | 用于回复握手结果的事件名。 |
| `session_id` | `Optional[str]` | 用于筛选的会话 id，只响应指定会话 id 的请求。 |
| `timeout` | `float` | 等待握手请求的超时时间（秒）。超时抛出 `PipeHandshakeError`。 |
| `allocator` | `Optional[InProcessPipeAllocator]` | 管道分配器实例。若未提供则使用模块级默认实例（`get_default_allocator()`）。 |

**Yields**
握手成功后，返回从分配器中取出的 `Pipe` 实例。退出上下文时自动关闭管道。

**握手流程**

1. 使用 `expect` 监听 `req_event`，等待 `PipeOpenRequest` 事件，并通过 `session_id` 筛选（若提供）。
2. 收到请求后，根据 `pipe_id` 从 `InProcessPipeAllocator` 中获取对应的管道。
3. 若管道不存在，发布一个 `success=False` 的 `PipeLinkedResponse` 并抛出 `PipeHandshakeError`。
4. 若存在，发布 `success=True` 的 `PipeLinkedResponse`，然后 `yield` 管道（管道的释放由客户端 `open_pipe` 在退出时负责）。

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
      |  1. 创建 Pipe 实例，通过 InProcessPipeAllocator 分配    |
      |-------------------------------------------->|
      |  2. 发布 PipeOpenRequest 事件 (req_event)    |
      |                                             |
      |                                             |  3. 监听 req_event，收到请求
      |                                             |  4. 从分配器获取 Pipe，并释放（转移所有权）
      |                                             |  5. 发布 PipeLinkedResponse (resp_event)
      |  6. 收到成功响应                             |
      |<--------------------------------------------|
      |                                             |
      |  7. 进入 async with pipe 块                  |  7. 进入 async with pipe 块
      |     两端持有同一 Pipe 实例，可直接 send/receive |
      |                                             |
      |  8. 退出上下文，关闭管道并清理注册表残留         |  8. 退出上下文，关闭管道
```

---

## 使用示例

### 基础双向通信

**服务端**（等待连接）

```python
async def server_task(bus_proxy):
    async with expect_pipe(bus_proxy, "pipe.connect", "pipe.linked") as pipe:
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
    async with open_pipe(bus_proxy, "pipe.connect", "pipe.linked") as pipe:
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

使用时通过自定义 `allocator` 指定管道类型：

```python
allocator = InProcessPipeAllocator(pipe_type=TcpPipe)
async with open_pipe(bus, "tcp.connect", "tcp.linked", allocator=allocator) as pipe:
    ...
```

或者通过 `pipe_kargs` 传递构造参数：

```python
async with open_pipe(bus, "pipe.req", "pipe.resp", pipe_kargs={"maxsize": 10}) as pipe:
    ...
```

### 超时与错误处理

```python
try:
    async with open_pipe(bus, "pipe.req", "pipe.resp", handshake_timeout=2.0) as pipe:
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
