# Service Handler 服务处理器

## 概述

`ServiceHandler` 是"请求-响应"服务端的一站式基类：子类只需用 `@process(...)` 装饰普通方法（同步 / 异步均可），即可获得：

- **订阅自动生成**——按登记的请求事件自动订阅；
- **分发自动查表**——`handle()` 按事件名找到对应方法；
- **响应自动构造**——返回 `dict` 即通过 `build_response` 构造响应（自动回传 `session_id` / `request_id`）；
- **异常自动转换**——抛出 `ServiceError` 自动转为 `success=False` 的失败响应。

它是 `request` 模板的**服务端对应物**：`request()` 发起 RPC 调用，`ServiceHandler` 负责应答。

---

## 使用场景

- 以 RPC 风格对外暴露领域服务（与 `request` 模板配合）。
- 需要统一的成功 / 失败响应协议与错误语义。
- 在单个处理器中组织多个请求处理方法，避免手写分发逻辑。

---

## 类签名

```python
class ServiceHandler(EventHandler):
    @property
    def processors(self) -> MappingProxyType[str, ProcessorMeta]: ...

    async def handle(self, payload: BaseModel | None, bus_proxy: EventBus.Proxy, raw_event: Event) -> None: ...
```

| 成员 | 类型 | 说明 |
| - | - | - |
| `processors` | `MappingProxyType[str, ProcessorMeta]` | 已登记的 processor 表（键为请求事件名，只读视图）。 |
| `handle()` | `async` | 分发入口：查表、调用、构造并发布响应；空负载 / 未登记 / 类型不匹配的事件被忽略。 |
| `ProcessorMeta` | 模型 | 单条登记信息：`func_name` 与 `request` / `response` 事件声明。 |

### @process 装饰器

```python
@process(request: type[EventDeclaration], response: type[EventDeclaration] | None = None)
```

| 参数 | 说明 |
| - | - |
| `request` | 请求事件声明；`payload_type` 必须继承 `RequestProtocol`。 |
| `response` | 响应事件声明（可选）；`payload_type` 必须继承 `ResponseProtocol`。省略即**单向事件**。 |

被装饰方法的返回值约定：

| 返回值 | 行为 |
| - | - |
| `dict[str, Any]` | 构造成功响应并发布：`build_response(payload, response_cls, **ret)`。 |
| `None` | **不回应**（纯副作用）。 |
| 抛 `ServiceError` | 构造 `success=False` 响应（`error_msg=str(e)`，`e.body` 合并进响应字段）并发布。 |
| 抛其他异常 | 照常向上抛出，由总线错误隔离处理（发布 `TaskErrorEvent`）。 |

> `@process` 在**类定义期**即校验事件协议；配置错误立即抛 `TypeError`。

### ServiceError 体系

| 异常 | 语义 |
| - | - |
| `ServiceError` | 业务失败基类：`str(e)` → `error_msg`；`e.body` → 合并进响应其余字段。 |
| `NotFoundError` | 目标资源不存在。 |
| `ConflictError` | 状态冲突（重复注册、并发修改等）。 |
| `InvalidRequestError` | 请求参数不合法。 |
| `AccessDeniedError` | 权限不足。 |

---

## 工作流程

```text
类定义期
  @process(ReqEvent, RespEvent) 标记方法
        │
        ▼
  __init_subclass__ 收集 processor 表 → 生成订阅列表
        │
实例注册到总线 → 按订阅列表登记路由
        │
请求事件到达
        ▼
handle() → 按事件名查表 → 校验 payload 类型
        │
        ▼
调用 processor（sync / async 均可）
        ├─ 返回 dict    → build_response(成功) → 发布响应事件
        ├─ 返回 None    → 不回应
        ├─ ServiceError → build_response(success=False, …) → 发布
        └─ 其他异常     → 向上抛出（总线错误隔离）
```

1. **收集**：创建子类时遍历 MRO 收集带 `@process` 标记的方法；以同一请求事件重复登记时**子类版本胜出**。
2. **订阅**：`processors` 表的键即订阅列表。
3. **分发**：`handle()` 按事件名查表并校验负载类型。
4. **响应**：`build_response` 自动回传请求的 `session_id` / `request_id`，与 `request()` 的匹配逻辑直接对接。

---

## 使用示例

### 基础服务 + RPC 调用

```python
from pydantic import Field
from infinity_bus import EventBus, EventDeclaration, EventHandlerRegistry, EventRegistry
from infinity_bus.templates import ServiceHandler, process
from infinity_bus.templates.request import RequestProtocol, ResponseProtocol, request

# 1. 定义请求 / 响应负载与事件
class GetUserReq(RequestProtocol):
    user_id: int

class GetUserResp(ResponseProtocol):
    user_name: str = Field(default='')
    email: str = Field(default='')

class GetUserReqEvent(EventDeclaration):
    name = 'user.get.request'
    payload_type = GetUserReq

class GetUserRespEvent(EventDeclaration):
    name = 'user.get.response'
    payload_type = GetUserResp

# 2. 定义服务
class UserService(ServiceHandler):
    @process(GetUserReqEvent, GetUserRespEvent)
    async def get_user(self, payload: GetUserReq, proxy, raw):
        return {'user_name': 'alice', 'email': 'alice@example.com'}

# 3. 装配并调用
event_registry = EventRegistry()
event_registry.register(GetUserReqEvent)
event_registry.register(GetUserRespEvent)
handler_registry = EventHandlerRegistry()
handler_registry.register(UserService())

async with EventBus(event_registry, handler_registry) as bus:
    resp = await request(bus.proxy('cli'), 'user.get.request', {'user_id': 1}, 'user.get.response')
    print(resp.user_name)  # alice
```

### 业务错误 → 失败响应

```python
class UserService(ServiceHandler):
    @process(GetUserReqEvent, GetUserRespEvent)
    async def get_user(self, payload: GetUserReq, proxy, raw):
        if payload.user_id not in self._db:
            raise NotFoundError('user not found', user_id=payload.user_id)
        return {'user_name': 'alice', 'email': 'alice@example.com'}
```

调用方 `request()` 收到的响应为 `success=False`、`error_msg='user not found'`；`e.body` 中的字段会合并进响应（以响应模型声明的字段为准）。

### 单向事件（无响应）

```python
class AuditService(ServiceHandler):
    def __init__(self):
        super().__init__()
        self.entries: list[str] = []

    @process(AuditEvent)  # 省略 response → 不回应
    def audit(self, payload: AuditReq, proxy, raw):
        self.entries.append(payload.action)
```

### 子类覆写

```python
class BaseUserService(ServiceHandler):
    @process(GetUserReqEvent, GetUserRespEvent)
    def get_user(self, payload, proxy, raw):
        return {'user_name': 'guest'}

class CachedUserService(BaseUserService):
    @process(GetUserReqEvent, GetUserRespEvent)
    def get_user_cached(self, payload, proxy, raw):
        return {'user_name': 'alice'}  # 子类登记覆盖父类
```

---

## 注意事项

- **方法签名**：processor 方法接收 `(payload, bus_proxy, raw_event)`；支持普通方法与 `async` 方法（返回 awaitable 时自动 `await`）。
- **忽略规则**：空负载、未登记的事件名、payload 类型不匹配的事件会被静默忽略（仅 debug 日志）。
- **单向事件的错误**：`response` 省略时，`ServiceError` 没有失败响应可转，异常向上抛出（最终由总线错误隔离以 `TaskErrorEvent` 形式发布）。
- **协议要求**：请求负载必须继承 `RequestProtocol`（含 `session_id` / `request_id`），响应负载必须继承 `ResponseProtocol`——这是与 `request` 模板对接的前提。
- **响应事件复用**：多个请求事件可以共用同一个响应事件声明。
- **覆写语义**：子类重新登记同一请求事件时（方法名可以不同），子类登记覆盖父类登记，无需显式移除。
