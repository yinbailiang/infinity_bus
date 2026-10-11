# Request 模板文档

## 概述

`request` 模板是在 EventBus 事件总线之上构建的**同步风格异步 RPC 调用工具**。它封装了“发布请求事件 → 等待匹配的响应事件”这一常见模式，使业务代码能以直观的 `await request(...)` 方式发起远程调用，而无需手动管理临时处理器、会话匹配和超时控制。

---

## 核心协议

使用 `request` 前，请求与响应的负载模型必须遵循约定的协议基类。

| 基类 | 作用 |
| - | - |
| `RequestProtocol` | 所有请求负载的基类，强制包含 `session_id` 和 `request_id` 字段，由框架自动注入。 |
| `ResponseProtocol` | 所有响应负载的基类，强制包含 `session_id`、`request_id`、`success` 和 `error_msg` 字段，提供 `raise_if_failed()` 方法快速检查失败。 |

### 协议字段说明

#### RequestProtocol

- `session_id: str` — 会话标识，用于关联同一会话内的多次请求。
- `request_id: str` — 请求唯一标识，用于精准匹配请求与响应。

#### ResponseProtocol

- `session_id: str` — 必须与对应请求的 `session_id` 一致。
- `request_id: str` — 必须与对应请求的 `request_id` 一致。
- `success: bool` — 业务处理是否成功，默认为 `True`。
- `error_msg: Optional[str]` — 失败时的错误信息。

---

## 工作流程

### 1. 定义请求与响应事件

首先，创建继承自 `RequestProtocol` 和 `ResponseProtocol` 的 Pydantic 模型，并声明对应的事件。

```python
from pydantic import BaseModel, Field
from infinity_bus import EventDeclaration
from infinity_bus.templates import RequestProtocol, ResponseProtocol, build_response

# 定义负载
class GetUserRequest(RequestProtocol):
    user_id: int = Field(description="要查询的用户ID")

class GetUserResponse(ResponseProtocol):
    user_name: str = Field(description="用户名")
    email: str = Field(description="邮箱")

# 声明事件
class GetUserRequestEvent(EventDeclaration):
    name = "user.get.request"
    payload_type = GetUserRequest

class GetUserResponseEvent(EventDeclaration):
    name = "user.get.response"
    payload_type = GetUserResponse
```

### 2. 注册事件

将事件声明注册到 `EventRegistry`（通常在应用启动时完成）。

```python
registry = EventRegistry()
registry.register(GetUserRequestEvent)
registry.register(GetUserResponseEvent)
```

### 3. 实现服务端处理器

在服务端实现一个 `EventHandler` 监听请求事件，处理完成后发布响应事件。

```python
class GetUserHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=["user.get.request"])

    async def handle(self, payload, bus_proxy, raw_event):
        if not isinstance(payload, GetUserRequest):
            return

        # 业务逻辑：查询用户
        user = await db.get_user(payload.user_id)

        # 构造响应负载：build_response 自动回传 session_id / request_id
        response = build_response(
            payload,
            GetUserResponse,
            success=user is not None,
            error_msg=None if user else "User not found",
            user_name=user.name if user else "",
            email=user.email if user else "",
        )
        await bus_proxy.publish("user.get.response", response)
```

### 4. 客户端发起请求

客户端通过 `request` 函数发起调用，等待响应。

```python
from infinity_bus.templates.request import request

# 假设已获得 EventBus.Proxy 实例（通常由总线注入或创建）
proxy = bus.proxy(source="UserServiceClient")

try:
    resp = await request(
        bus_proxy=proxy,
        req_event="user.get.request",
        req_data={"user_id": 123},
        resp_event="user.get.response",
        session_id=None,          # 留空则自动生成新会话ID
        timeout=10.0,             # 超时时间（秒），None 表示无限等待
    )
    resp.raise_if_failed()        # 检查 success 字段，失败时抛出 RuntimeError
    print(f"User: {resp.user_name}, Email: {resp.email}")
except asyncio.TimeoutError:
    print("请求超时")
except RuntimeError as e:
    print(f"业务失败: {e}")
```

---

## 函数签名

```python
async def request(
    bus_proxy: EventBus.Proxy,
    req_event: str,
    req_data: Dict[str, Any],
    resp_event: str,
    session_id: Optional[str] = None,
    timeout: Optional[float] = 60.0,
) -> ResponseProtocol
```

| 参数 | 类型 | 说明 |
| - | - | - |
| `bus_proxy` | `EventBus.Proxy` | 事件总线代理，用于发布请求事件。 |
| `req_event` | `str` | 请求事件名称（必须在注册表中存在且负载继承 `RequestProtocol`）。 |
| `req_data` | `Dict[str, Any]` | 请求负载数据，会自动注入 `session_id` 和 `request_id`。 |
| `resp_event` | `str` | 期望的响应事件名称（必须在注册表中存在且负载继承 `ResponseProtocol`）。 |
| `session_id` | `Optional[str]` | 会话ID，若为 `None` 则自动生成 UUID。 |
| `timeout` | `Optional[float]` | 等待响应的超时时间（秒），超时抛出 `asyncio.TimeoutError`。`None` 表示无限等待。 |

**返回值**：`ResponseProtocol` 实例（具体类型由响应事件声明的 `payload_type` 决定）。

---

## build_response 辅助函数

服务端构造响应负载的辅助函数：自动回传请求的 `session_id` / `request_id`，其余字段由 `kargs` 提供。

```python
def build_response(
    request: RequestProtocol,
    response_type: type[ResponseProtocol],
    **kargs: Any,
) -> ResponseProtocol
```

| 参数 | 类型 | 说明 |
| - | - | - |
| `request` | `RequestProtocol` | 对应的请求负载（`session_id` / `request_id` 的事实来源）。 |
| `response_type` | `type[ResponseProtocol]` | 响应负载类。 |
| `kargs` | `Any` | 其余业务字段，直接交给响应模型的 `model_validate`。 |

**返回值**：`response_type` 的实例。

- `kargs` 中若携带冲突的 `session_id` / `request_id` 会被请求值**强制覆盖**（请求是唯一事实源），无需手动回传。
- `ServiceHandler` 的自动响应构造内部使用同一个函数（见 [handlers/service.md](handlers/service.md)）。

---

## 异常说明

| 异常类型 | 触发条件 |
| - | - |
| `ValueError` | 请求/响应事件未在注册表中注册。 |
| `TypeError` | 事件负载不符合 `RequestProtocol` 或 `ResponseProtocol` 要求。 |
| `BusShuttingDown` | 事件总线正在停止，无法发布新事件。 |
| `asyncio.TimeoutError` | 在指定 `timeout` 内未收到匹配的响应。 |
| `asyncio.CancelledError` | 外部取消了请求任务（此时临时处理器自动清理）。 |
| `RuntimeError` | 调用 `resp.raise_if_failed()` 且响应中 `success=False` 时抛出。 |

---

## 内部机制

- **临时处理器**：每次调用 `request` 会在当前上下文动态注册一个 `OneShotHandler`，监听 `resp_event`，并在函数返回（无论成功、失败、取消）后自动注销，杜绝资源泄漏。
- **会话隔离**：通过 `session_id` + `request_id` 双重匹配，确保并发请求互不干扰。
- **类型安全**：发布前校验事件声明的负载类型，响应时校验实际负载是否继承 `ResponseProtocol`，不一致则立即失败。

---

## 注意事项

1. **响应处理器必须原样回传 `session_id` 和 `request_id`**，否则客户端无法匹配，导致请求超时。
2. **不要在 `req_data` 中手动提供 `session_id` 或 `request_id`**，框架会强制覆盖以保证唯一性。
3. **服务端应尽快发布响应**，避免客户端长时间等待。对于耗时操作，可考虑先返回“已受理”状态，再通过独立事件推送结果。
4. **超时时间的设定**应略大于业务逻辑的最大预期耗时，但不宜过长以免资源占用。
5. **与 `EventBus` 配合使用时**，请确保总线已启动，且相关事件声明均已注册。

---

## 完整示例

参见 `tests/templates/request_test.py`

其中包含了正常请求、超时、取消、类型错误等场景的测试用例，可作为使用参考。
