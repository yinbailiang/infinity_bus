# Expect 异步事件监听器文档

## 概述

`expect` 是基于 EventBus 构建的一次性事件监听工具。它允许你在异步上下文中等待特定事件的发生，并获取完整的事件对象（包含负载数据及元数据）。与传统的处理器注册方式不同，`expect` 通过上下文管理器自动管理监听器的生命周期，特别适用于**触发-等待**模式（如等待确认、等待异步结果）。

---

## 使用场景

- 等待某个异步操作的完成通知。
- 在测试中验证特定事件是否被正确发布。
- 实现请求-响应模式的底层等待逻辑（已被 `request` 模板内部使用）。
- 监听一次性系统信号（如启动完成、关闭确认）。
- 需要访问事件元数据（ID、来源链、时间戳）的场景。

---

## 函数签名

```python
@asynccontextmanager
async def expect(
    bus_proxy: EventBus.Proxy,
    event_patterns: Union[str, Regex, List[Union[str, Regex]]],
    filter_func: Optional[Callable[[Event], Union[Awaitable[bool], bool]]] = None,
) -> AsyncGenerator[asyncio.Future[Event], None]
```

| 参数 | 类型 | 说明 |
| - | - | - |
| `bus_proxy` | `EventBus.Proxy` | 事件总线代理，用于访问处理器注册表。 |
| `event_patterns` | `str` \| `Regex` \| `List[str \| Regex]` | 监听的事件名模式。`str` 全字匹配，`Regex` 正则匹配。可传入单个或序列。 |
| `filter_func` | `Optional[Callable]` | 可选过滤器。接收 `Event` 对象，返回布尔值（或可等待的布尔值），`True` 表示匹配成功。若省略，任何匹配 `event_patterns` 的事件都会触发。 |

**Yields**：一个 `asyncio.Future[Event]` 对象，`await` 它将返回匹配的完整 `Event` 实例。

---

## 工作流程

1. **注册临时处理器**：进入 `async with expect(...) as future:` 时，`expect` 会向事件总线注册一个一次性监听器。
2. **发布事件**：你在上下文中执行业务逻辑（如发布一个请求事件），此时监听器已处于就绪状态。
3. **等待匹配**：监听器收到事件后，通过过滤器（若有）判断是否匹配。若匹配，则将整个 `Event` 对象设置到 `future` 中。
4. **获取结果**：通过 `await future` 获取完整事件，可从 `event.data` 访问负载，或从 `event.id`、`event.sources` 等字段读取元数据。
5. **自动清理**：退出上下文时，无论是否匹配成功，临时监听器都会被注销，`future` 若未完成则自动取消。

---

## 使用示例

### 基础用法：等待任意匹配事件

```python
async with expect(bus_proxy, "user.created") as future:
    await create_user(user_data)
    event = await asyncio.wait_for(future, timeout=5.0)
    payload = event.data
    print(f"User created with ID: {payload.user_id}")
```

### 访问事件元数据

```python
async with expect(bus_proxy, "order.shipped") as future:
    await ship_order(order_id)
    event = await asyncio.wait_for(future, timeout=3.0)
    print(f"Event ID: {event.id}")
    print(f"Processing chain: {' -> '.join(event.sources)}")
    print(f"Timestamps: {event.timestamps}")
```

### 带过滤器：仅匹配特定条件的事件

```python
def is_target_user(event: Event) -> bool:
    return event.data.user_id == "admin-123"

async with expect(bus_proxy, "user.updated", filter_func=is_target_user) as future:
    await update_user("admin-123", new_data)
    event = await asyncio.wait_for(future, timeout=3.0)
    print(f"Updated fields: {event.data.changed_fields}")
```

### 异步过滤器：需要异步判断

```python
async def check_permission(event: Event) -> bool:
    user = await db.get_user(event.data.user_id)
    return user.role == "admin"

async with expect(bus_proxy, "document.accessed", filter_func=check_permission) as future:
    await access_document(doc_id)
    try:
        event = await asyncio.wait_for(future, timeout=5.0)
        print(f"Admin {event.data.user_id} accessed document")
    except asyncio.TimeoutError:
        print("Non-admin access ignored")
```

### 监听多个事件模式

```python
async with expect(bus_proxy, ["order.paid", "order.cancelled"]) as future:
    await submit_order(order_data)
    event = await asyncio.wait_for(future, timeout=10.0)
    if event.name == "order.paid":
        print("Order payment confirmed")
    else:
        print("Order was cancelled")
```

### 正则模式匹配

```python
from infinity_bus import Regex

# 匹配所有以 "notify." 开头的事件
async with expect(bus_proxy, Regex(r"notify\..*")) as future:
    await trigger_notifications()
    event = await asyncio.wait_for(future, timeout=2.0)
    print(f"First notification: {event.name}")
```

---

## 异常处理

| 情况 | 行为 |
| - | - |
| 过滤器抛出异常 | 异常会通过 `future.set_exception()` 传递给等待方，`await future` 将抛出该异常。 |
| 上下文退出时 `future` 尚未完成 | `future` 被自动取消，等待它的协程将收到 `CancelledError`。 |
| 等待超时（使用 `asyncio.wait_for`） | 抛出 `asyncio.TimeoutError`，上下文退出时自动清理资源。 |
| 总线正在停止 | 注册处理器仍可进行，但若停止过程中发布事件可能无法被捕获（建议在总线启动后使用）。 |

---

## 注意事项

1. **必须配合 `async with` 使用**：`expect` 是异步上下文管理器，不可单独调用。
2. **`future` 仅完成一次**：一旦匹配成功，后续相同事件不会再影响 `future`，监听器自动停用。
3. **避免在上下文外持有 `future`**：退出上下文后，`future` 可能被取消，继续等待会引发错误。
4. **过滤器应尽量轻量**：过滤器在事件分发线程中执行，避免阻塞或耗时操作。如需复杂逻辑，可考虑异步过滤器但注意控制执行时间。
5. **与 `request` 的关系**：`expect` 是更底层的工具，`request` 模板内部使用它实现请求-响应模式。对于常规 RPC 调用，推荐直接使用 `request`。

---

## 内部机制

- 使用 `OneShotEventHandler` 实现单次触发。
- 通过 `temporary_handler` 上下文管理器自动注册/注销处理器。
- 过滤器异常通过 `future.set_exception` 直接传递给等待方，不会污染总线错误通道（不会触发 `__task_error__` 事件）。

---

## 完整示例

参考测试文件 `tests/templates/expect_test.py`，其中包含了正常匹配、过滤器、超时、多模式、正则、异常传递等场景的完整用例。
