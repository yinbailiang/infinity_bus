# Simple Handler 装饰器文档

## 概述

`handler` 是一个**函数装饰器**，将普通的异步（或同步）函数转换为 `EventHandler` 子类。它自动完成事件订阅配置、参数签名校验和负载解包，让开发者只需关注业务逻辑，无需手动编写 `EventHandler` 样板代码。

与直接继承 `EventHandler` 并实现 `handle()` 方法相比，`handler` 装饰器更简洁、类型安全，适合大多数简单的事件处理场景。

---

## 使用场景

- 快速定义事件处理器，无需编写完整的 `EventHandler` 子类。
- 利用函数签名自动校验负载类型，在**定义时**（而非运行时）发现类型错误。
- 与 `ModuleHandlerRegister` 配合使用，实现模块级处理器的批量注册。
- 需要同步处理器（非异步）的场景 —— 装饰器自动兼容两种函数类型。

---

## 函数签名

```python
def handler(
    event_decl: Type[EventDeclaration],
    *,
    handle_timeout: Optional[float] = 32.0,
) -> Callable[[HandlerFunc], Type[GenericEventHandler]]
```

| 参数 | 类型 | 说明 |
| - | - | - |
| `event_decl` | `Type[EventDeclaration]` | 事件声明类，必须已定义 `name` 和可选的 `payload_type`。 |
| `handle_timeout` | `Optional[float]` | 处理器超时时间（秒）。`None` 表示无超时。默认 `32.0`。 |

**返回值**：一个装饰器函数，接受普通函数并返回一个 `EventHandler` 子类。

---

## 工作流程

1. **声明事件**：先通过 `EventDeclaration` 定义事件名和可选的负载类型。
2. **编写函数**：编写一个普通函数，参数签名需与事件声明匹配：
   - 若事件**有**负载（`payload_type` 非 `None`），函数第一个参数应为该负载类型。
   - 若事件**无**负载（`payload_type` 为 `None`），函数不应有任何参数。
3. **应用装饰器**：用 `@handler(YourEvent)` 装饰该函数，自动生成 `EventHandler` 子类。
4. **实例化并注册**：调用生成的类创建实例，注册到 `EventHandlerRegistry` 中。
5. **运行时**：事件触发时，总线自动解包负载并调用原函数。

装饰器在**定义时**进行签名校验：

- 有负载事件 + 无参数函数 → 立即抛出 `TypeError`
- 无负载事件 + 有参数函数 → 立即抛出 `TypeError`
- 有负载事件 + 参数类型不匹配 → 立即抛出 `TypeError`（仅当参数有类型注解时）

---

## 使用示例

### 基础用法：无负载事件

```python
from infinity_bus import EventDeclaration
from infinity_bus.templates import handler

class SystemReady(EventDeclaration):
    name = "system.ready"

@handler(SystemReady)
async def on_system_ready() -> None:
    print("System is ready!")

# 注册到总线
await handler_registry.register(on_system_ready())
```

### 基础用法：有负载事件

```python
from pydantic import BaseModel, Field
from infinity_bus import EventDeclaration
from infinity_bus.templates import handler

class UserCreatedPayload(BaseModel):
    user_id: str = Field(description="用户ID")
    email: str = Field(description="用户邮箱")

class UserCreated(EventDeclaration):
    name = "user.created"
    payload_type = UserCreatedPayload

@handler(UserCreated)
async def send_welcome_email(payload: UserCreatedPayload) -> None:
    print(f"Sending welcome email to {payload.email}")

await handler_registry.register(send_welcome_email())
```

### 同步处理器

```python
@handler(UserCreated)
def log_user_creation(payload: UserCreatedPayload) -> None:
    # 同步函数同样支持 —— 装饰器自动适配
    print(f"[LOG] User created: {payload.user_id}")

await handler_registry.register(log_user_creation())
```

### 自定义超时

```python
@handler(UserCreated, handle_timeout=5.0)
async def quick_validation(payload: UserCreatedPayload) -> None:
    # 5 秒超时
    await validate_user(payload)

@handler(UserCreated, handle_timeout=None)
async def long_running_task(payload: UserCreatedPayload) -> None:
    # 无超时限制
    await heavy_computation(payload)
```

### 签名校验：类型错误在定义时发现

```python
class OrderCreated(EventDeclaration):
    name = "order.created"
    payload_type = OrderPayload

# ❌ 编译期即可发现错误（定义时抛出 TypeError）
@handler(OrderCreated)
def bad_handler() -> None:  # 缺少 payload 参数
    pass
# TypeError: 事件 order.created 要求负载参数，但处理器 bad_handler() 未定义参数。

# ❌ 类型不匹配
@handler(OrderCreated)
def another_bad(payload: OtherPayload) -> None:
    pass
# TypeError: 处理器 another_bad 参数类型应为 OrderPayload，而不是 'OtherPayload'。
```

### 与 ModuleHandlerRegister 配合

```python
from infinity_bus.templates import ModuleHandlerRegister

module_handlers = ModuleHandlerRegister("user_module")

@module_handlers.handler()
@handler(UserCreated)  # 叠加使用：内层生成 EventHandler，外层收集到模块注册器
class SendWelcomeEmail:
    pass  # 实际上 @handler 直接返回类，无需再手动定义

# 更常见的做法是用 @handler 单独装饰函数，然后手动添加到模块注册器
```

---

## 装饰器生成类的方法

生成的 `EventHandler` 子类：

| 方法/属性 | 说明 |
| - | - |
| `__init__()` | 构造器，超时由装饰器参数 `handle_timeout` 决定。 |
| `handle(payload, bus_proxy, raw_event)` | 事件处理入口，自动解包并调用原函数。 |
| `subscriptions` | 继承自 `EventHandler`，包含 `event_decl.name`。 |
| `handle_timeout` | 继承自 `EventHandler`，与装饰器参数一致。 |

---

## 注意事项

- 装饰器生成的类是**动态创建**的，每次调用 `@handler(...)` 都会生成一个新的 `EventHandler` 子类。
- 若函数参数没有类型注解，装饰器**不进行**类型校验，允许通过（运行时由总线校验）。
- 原函数的 `__name__`、`__qualname__`、`__module__`、`__doc__` 会被复制到生成的类上。
- 装饰器不支持 `*args`、`**kwargs` 等可变参数 —— 只接受零个或一个参数。
