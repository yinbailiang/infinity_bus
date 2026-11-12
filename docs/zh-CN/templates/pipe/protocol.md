# 握手负载模型（pipe/protocol）

`infinity_bus.templates.pipe.protocol` 定义握手事件使用的两个 Pydantic 模型。返回
总览见 [pipe.md](pipe.md)。

---

## `PipeOpenRequest`

```python
class PipeOpenRequest(RequestProtocol):
    pipe_id: str
```

握手请求负载，继承 [`RequestProtocol`](../request.md)，额外携带管道唯一标识符
`pipe_id`。`session_id`、`request_id` 由 `request` 模板自动注入，服务端
`expect_pipe` 用 `session_id` 做可选筛选。

## `PipeLinkedResponse`

```python
class PipeLinkedResponse(ResponseProtocol):
    pass
```

握手响应负载，继承 [`ResponseProtocol`](../request.md)，使用标准字段：

| 字段 | 说明 |
| ---- | ---- |
| `session_id` | 与请求一致的会话 ID。 |
| `request_id` | 与请求一致的请求 ID。 |
| `success` | 握手是否成功。 |
| `error_msg` | 失败原因（`success=False` 时）。 |

`open_pipe` 只接受 `PipeLinkedResponse`；`success=False` 时抛出
`PipeHandshakeError`。
