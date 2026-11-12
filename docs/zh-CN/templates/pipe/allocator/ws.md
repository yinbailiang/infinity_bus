# WebSocket 传输（pipe/allocator/ws）

`infinity_bus.templates.pipe.allocator.ws` 基于 aiohttp 实现 WebSocket 管道。返回
总览见 [allocator.md](allocator.md)。

> 需要可选依赖：`pip install infinity_bus[ws]`（aiohttp）。

---

## `WsPipe`

```python
class WsPipe[PayloadType: BaseModel](Pipe[PayloadType]):
    def __init__(self, ws: Any, payload_type: type[PayloadType], maxsize: int | None = None)
```

- 后台泵从 WebSocket 读取帧，按 [codec](../codec.md) 解码后放入入站
  `asyncio.Queue`；关闭帧与解码异常由泵记录并标记关闭。
- `maxsize` 是入站队列容量（`None` = 无限制）。帧先进入队列，因此背压由队列容量
  决定，而不是直接依赖 WebSocket flow control。
- `receive()` 关闭后先排空队列，再抛 `PipeClosedError`；取消不会遗留消费任务。
- `close()` 取消泵并关闭 WebSocket，不向调用方抛出泵异常。

---

## `WsPipeServerAllocator`

```python
class WsPipeServerAllocator[PayloadType: BaseModel](SingleRoleAllocator[PayloadType]):
    def __init__(
        self,
        host: str,
        port: int,
        payload_type: type[PayloadType],
        *,
        path: str = '/pipe',
        accept_timeout: float = 5.0,
        hello_timeout: float = 2.0,
        max_pending: int = 64,
        maxsize: int | None = None,
        shutdown_timeout: float = 5.0,
    )
```

| 参数 | 说明 |
| ---- | ---- |
| `host` / `port` | aiohttp 监听地址；`port=0` 时由系统分配，可从 `port` 属性读取实际端口。 |
| `path` | WebSocket 路径，缺省 `/pipe`。 |
| `accept_timeout` | `checkout()` 等待连接到达的超时。 |
| `hello_timeout` | 等待首帧 `{"pipe_id": ...}` 的超时；超时或非法帧只关闭该连接。 |
| `max_pending` | 未领取连接上限；超出时关闭最旧连接。 |
| `maxsize` | 传给 `WsPipe` 的入站队列容量。 |
| `shutdown_timeout` | 传给 `AppRunner` 的关闭等待上限。 |

- `start()` 创建 `AppRunner` / `TCPSite`；`stop()` 取消等待中的 `checkout()`、关闭
  握手中的连接与所有端点，再停止站点并清理 runner。
- 服务端 handler 在握手前只持有 WebSocket；这些连接会被跟踪并在 `stop()` 中关闭。

---

## `WsPipeClientAllocator`

```python
class WsPipeClientAllocator[PayloadType: BaseModel](SingleRoleAllocator[PayloadType]):
    def __init__(
        self,
        url: str,
        payload_type: type[PayloadType],
        *,
        connect_timeout: float = 5.0,
        maxsize: int | None = None,
    )
```

`allocate()` 建立 WebSocket 连接并发送 hello；`release()` 关闭本端管道；`stop()`
关闭内部 `ClientSession`。`allocate(**kwargs)` 不使用额外参数，请通过构造参数设置
`maxsize`。

---

## 安全边界

`WsPipe` **没有内建鉴权**：任何能访问端口的客户端都可以注册 `pipe_id`。生产环境应
仅监听 loopback / 内网，或在 hello 帧中自行加入令牌并做校验；需要 TLS 时建议由反向
代理终止 TLS。

## 示例

```python
from infinity_bus.templates.pipe import open_pipe
from infinity_bus.templates.pipe.allocator import (
    WsPipeClientAllocator,
    WsPipeServerAllocator,
)

server_alloc = WsPipeServerAllocator('0.0.0.0', 8765, MyPayload, maxsize=64)
await server_alloc.start()

client_alloc = WsPipeClientAllocator('ws://server-host:8765/pipe', MyPayload, maxsize=64)
try:
    async with open_pipe(bus, 'pipe.connect', 'pipe.linked', client_alloc) as pipe:
        ...
finally:
    await client_alloc.stop()
    await server_alloc.stop()
```
