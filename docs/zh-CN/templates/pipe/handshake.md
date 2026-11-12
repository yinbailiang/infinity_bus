# 握手模板（pipe/handshake）

`infinity_bus.templates.pipe.handshake` 提供 `open_pipe` / `expect_pipe`：控制面通过
事件总线完成握手，数据面交给分配器创建的 `Pipe`。返回总览见 [pipe.md](pipe.md)。

---

## `open_pipe`（客户端上下文管理器）

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

| 参数 | 类型 | 说明 |
| - | - | - |
| `bus_proxy` | `EventBus.Proxy` | 用于发布握手请求。 |
| `req_event` | `str` | 握手请求事件名。 |
| `resp_event` | `str` | 握手响应事件名。 |
| `allocator` | `PipeAllocator[PayloadType]` | **必填**；同进程两端必须传入同一个实例。 |
| `handshake_timeout` | `float` | 握手超时（秒）。 |
| `session_id` | `str \| None` | 关联请求-响应；缺省自动生成。 |
| `pipe_kwargs` | `dict[str, Any] \| None` | 透传给 `allocator.allocate(**pipe_kwargs)`；仅部分实现使用（例如进程内 `maxsize`）。 |

**Yields**：已握手并打开的 `Pipe[PayloadType]`。退出时关闭管道并调用
`allocator.release(pipe_id)`。

**异常**：握手阶段的所有异常统一包装为 `PipeHandshakeError`；上下文体内的异常原样
传播。

---

## `expect_pipe`（服务端上下文管理器）

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

| 参数 | 类型 | 说明 |
| - | - | - |
| `bus_proxy` | `EventBus.Proxy` | 用于监听请求并发布响应。 |
| `req_event` | `str` | 期望监听的握手请求事件名。 |
| `resp_event` | `str` | 回复握手结果的事件名。 |
| `allocator` | `PipeAllocator[PayloadType]` | **必填**；同进程两端必须传入同一个实例。 |
| `session_id` | `str \| None` | 仅响应指定会话的请求。 |
| `timeout` | `float` | 等待握手请求的超时（秒）。 |

**Yields**：`allocator.checkout(pipe_id)` 领取到的 `Pipe[PayloadType]`。退出时关闭
管道并调用 `allocator.checkin(pipe_id)`。

**握手流程**：

1. `expect` 监听 `req_event`，按 `session_id` 过滤 `PipeOpenRequest`；
2. 调用 `allocator.checkout(pipe_id)` 领取借用方端点；
3. 端点不存在时发布 `success=False` 响应并抛出 `PipeHandshakeError`；
4. 端点存在时发布 `success=True` 响应，然后 `yield`；
5. 退出时 `checkin`，资源由分配方的 `release()` 收尾。

> `checkout()` 发生在发布成功响应**之前**，因此跨进程客户端必须在 `allocate()` 阶段
> 完成数据面拨号。完整约定见 [allocator/allocator.md](allocator/allocator.md)。

---

## 工作流程

```text
客户端 (open_pipe)                          服务端 (expect_pipe)
      |                                             |
      | 1. allocator.allocate()                     |
      | 2. 发布 PipeOpenRequest (req_event)  ------>|
      |                                             | 3. expect 收到请求
      |                                             | 4. allocator.checkout(pipe_id)
      |                                             | 5. 发布 PipeLinkedResponse
      | 6. 收到成功响应  <--------------------------|
      |                                             |
      | 7. 两端各自 send / receive（数据面直连）     |
      |                                             |
      | 8. 退出上下文：close + release               | 8. 退出上下文：close + checkin
```

---

## 超时与错误处理

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
