# 处理器模板（handlers）

`infinity_bus.templates.handlers` 提供**两种开箱即用的处理器基类**，覆盖事件消费的两种典型模式：

- **`MailboxHandler`** —— 邮箱模式：事件先入队，由独立后台任务串行消费（背压控制、自定义循环、异常自动重启、主动停止）。
- **`ServiceHandler`** —— 服务处理器：把请求-响应（RPC）服务端的样板代码收敛为 `@process` 装饰器。

两者都继承 `EventHandler`，可直接注册到 `EventHandlerRegistry`；
额外能力建立在内核处理器协议之上（生命周期钩子 / 订阅束 / 总线分发）。

---

## 目录结构（镜像代码）

```text
infinity_bus.templates.handlers/
├── handlers.md     # 本总览
├── mailbox.md      # MailboxHandler —— 邮箱模式处理器
└── service.md      # ServiceHandler —— 服务处理器（RPC 服务端）
```

| 文档 | 对应模块 | 模式 | 适用场景 |
| - | - | - | - |
| [mailbox.md](mailbox.md) | `handlers/mailbox.py` | 邮箱模式（串行消费） | 严格串行处理、背压控制、自定义任务循环 |
| [service.md](service.md) | `handlers/service.py` | 服务处理器（请求-响应） | RPC 服务端、统一成功/失败响应、错误自动转换 |

---

## 如何选择

| 你的需求 | 用这个 |
| - | - |
| 事件需要**严格串行**消费，避免同一处理器并发 | `MailboxHandler` |
| 需要**背压控制**（限制积压队列容量） | `MailboxHandler` |
| 需要**自定义消费循环**（批处理、定时器、状态机） | `MailboxHandler` |
| 消费任务崩溃后需要**自动重启** | `MailboxHandler` |
| 对外提供 **RPC 服务**（请求 → 响应） | `ServiceHandler` |
| 在单个处理器中组织**多个请求处理方法** | `ServiceHandler` |
| 需要**统一的失败响应**（`success` / `error_msg`） | `ServiceHandler` |
| 只是简单的发布-订阅处理 | 直接继承 [`EventHandler`](../../handler.md) |

---

## 两种模式对比

| 维度 | `MailboxHandler` | `ServiceHandler` |
| - | - | - |
| 消费模型 | 队列 + 独立任务，严格串行 | 总线分发上下文内直接处理（可能并发） |
| 处理入口 | 重写 `process()` 循环 | 用 `@process(...)` 装饰方法 |
| 响应构造 | 手动（通常 `proxy.publish()`） | 自动（`build_response` 回传 `session_id` / `request_id`） |
| 错误处理 | `process()` 崩溃自动重启；`StopMailbox` 主动停止 | `ServiceError` 自动转 `success=False` 响应 |
| 典型搭配 | 内部工作流、事件聚合、状态机 | [`request()`](../request.md) RPC 客户端 |

---

## 快速示例

### 邮箱模式（串行消费）

```python
from infinity_bus.templates import MailboxHandler

class SerialCollector(MailboxHandler):
    def __init__(self):
        super().__init__(subscriptions=['data.input'])
        self.buffer: list[str] = []

    async def process(self) -> None:
        while True:
            event, proxy = await self.get()
            await self._handle(event)  # 严格串行
```

### 服务处理器（RPC 服务端）

```python
from infinity_bus.templates import ServiceHandler, process

class UserService(ServiceHandler):
    @process(GetUserReqEvent, GetUserRespEvent)
    async def get_user(self, payload, proxy, raw):
        return {'user_name': 'alice'}
```

完整用法见 [mailbox.md](mailbox.md) 与 [service.md](service.md)。

---

## 全部导出

```python
from infinity_bus.templates import (
    # 邮箱模式
    MailboxHandler, MailboxConfig, StopMailbox,
    # 服务处理器
    ServiceHandler, process, ProcessorMeta,
    ServiceError,
    NotFoundError, ConflictError, InvalidRequestError, AccessDeniedError,
)
```

---

## 注意事项

- 两个模板都随 `infinity_bus[templates]` 提供：`pip install infinity_bus[templates]`。
- 都遵循内核处理器协议：注册后由总线生命周期驱动（`on_activate` / `on_deactivate`），无需手动管理订阅。
- 与 [handler 装饰器](../simple_handler.md)（函数 → 处理器）和 [register](../register.md)（批量注册）互补，可组合使用。
