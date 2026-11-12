# Pipe 抽象基类（pipe/base）

`infinity_bus.templates.pipe.base` 定义异常类型与 `Pipe` 抽象基类。返回总览见
[pipe.md](pipe.md)。

---

## `Pipe`（抽象基类）

```python
class Pipe[PayloadType: BaseModel](ABC):
    async def __aenter__(self) -> Pipe[PayloadType]
    async def __aexit__(self, ...)
    async def open(self) -> None
    async def close(self) -> None
    async def send(self, data: PayloadType) -> None
    async def receive(self) -> PayloadType
```

| 方法 | 说明 |
| ---- | ---- |
| `open()` | 打开管道，准备收发。 |
| `close()` | 关闭管道；关闭是终态，重复调用无副作用。 |
| `send(data)` | 写入一个 Pydantic 模型实例。管道已关闭时抛 `PipeClosedError`；有界缓冲已满时阻塞等待接收方消费。 |
| `receive()` | 读取下一个实例。本端或对端关闭后先排空缓冲，耗尽后抛 `PipeClosedError`。 |
| 异步上下文管理器 | 进入时调用 `open()`；退出时调用 `close()`，并记录/吞掉 `close()` 异常。 |

- `PayloadType` 是 `BaseModel` 的子类型，由具体实现或分配器参数化。
- 子类必须实现 `open`、`close`、`send`、`receive` 四个抽象方法。
- 管道不承担多路复用：一个 `pipe_id` 只对应一对端点。

---

## 异常类型

| 异常 | 触发场景 |
| ---- | ---- |
| `PipeHandshakeError` | 握手超时、响应失败/拒绝，或分配器停止。 |
| `PipeClosedError` | 管道已关闭且缓冲耗尽，或在关闭后写入。 |
| `PipeTeardownError` | 预留：关闭过程中发生异常。 |

> `open_pipe` / `expect_pipe` 会把握手阶段的异常统一包装为 `PipeHandshakeError`；
> 用户上下文体内的异常保持原样传播。
