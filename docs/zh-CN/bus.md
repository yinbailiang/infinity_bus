# EventBus 文档

## 概述

`EventBus` 是事件分发中枢，负责任务队列、并发控制、错误上报及生命周期管理。`Proxy` 是对外发布事件的唯一接口。系统内置 `ShutdownEvent` 和 `TaskErrorEvent` 两个特殊事件。

---

## EventBus

```python
class EventBus:
    def __init__(
        self,
        event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
        queue: Optional[EventQueue] = None,
        router: Optional[Router] = None,
        max_handler_semaphore: int = 256,
        shutdown: ShutdownConfig = ShutdownConfig(),
        middleware_chain: Optional[MiddlewareChain] = None
    ) -> None

    # 生命周期
    async def start(self) -> None
    async def stop(self) -> None
    async def __aenter__(self) -> "EventBus"
    async def __aexit__(self, ...) -> Optional[bool]

    # 创建代理
    def proxy(self, source: str, raw_event: Optional[Event] = None) -> Proxy

    # 路由器
    @property
    def router(self) -> Router

    # 可观测性
    @property
    def is_running(self) -> bool
    @property
    def is_publishing_enabled(self) -> bool
    @property
    def active_task_count(self) -> int
    @property
    def queue_size(self) -> int
```

### 构造参数

| 参数 | 类型 | 默认值 | 说明 |
| - | - | - | - |
| `event_registry` | `EventRegistry` | (必需) | 事件注册表实例。 |
| `handler_registry` | `EventHandlerRegistry` | (必需) | 处理器注册表实例。 |
| `queue` | `Optional[EventQueue]` | `None` | 内部派发队列，总线仅依赖 `EventQueue` 抽象。缺省为 `InMemoryEventQueue()`（容量取其自身默认配置 1024）。参见 [事件队列文档](queue.md)。 |
| `router` | `Optional[Router]` | `None` | 内部路由 / 分派表。缺省为 `Router(event_registry)`，经 `bus.router` 暴露。参见 [路由文档](router.md)。 |
| `max_handler_semaphore` | `int` | `256` | 最大并发处理器数量（信号量）。 |
| `shutdown` | `ShutdownConfig` | `ShutdownConfig()` | 停机行为配置，参见 [ShutdownConfig](#shutdownconfig)。 |
| `middleware_chain` | `Optional[MiddlewareChain]` | `None` | 中间件链，用于在发布流程中插入自定义逻辑。参见 [中间件文档](middleware.md)。 |

构造时自动注册 `ShutdownEvent` 和 `TaskErrorEvent`（若注册表中不存在），并内部创建 [Router](router.md)（经 `bus.router` 暴露）用于事件-处理器路由——处理器在激活（总线启动）时登记自己的订阅束。队列容量等配置由队列自身持有，总线不感知；需要自定义容量时注入 `InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=N))`。中间件链在每次分发时按需构建，运行时增删即时生效。

### 生命周期

| 方法 | 说明 |
| - | - |
| `start()` | 启动事件总线：先初始化中间件链（各中间件 `on_setup`），再激活处理器注册表（`registry.activate(bus)`——各处理器把订阅束登记进 `bus.router`），最后启动调度循环并开放发布——**顺序与 `stop()` 的清理顺序互为镜像**，处理器在激活阶段发布的消息也经过完整就绪的中间件管道。**失败安全可重试**：任一环节失败即整体回滚（撤销调度、下线处理器、回滚中间件）到干净的未启动态。重复调用安全；启动失败统一抛出 `BusError` 子类——回滚完整抛 `BusStartFailed`（携带 `original_error` 与 `__cause__` 链），回滚不完整抛 `BusStartRollbackError`（聚合原始异常与处理器 / 中间件失败清单）。 |
| `stop()` | 优雅停止：发布 `__shutdown__` → 拒绝新发布 → 等待队列排空 → 取消调度 → 等待活跃任务完成 → 下线处理器注册表（`registry.deactivate()`）；下线失败会记录并汇总告警。重复调用安全。 |
| `async with EventBus(...) as bus:` | 上下文管理器，自动启停。退出时 `stop()` 异常不会掩盖上下文体异常。 |

### 可观测性

| 属性 | 类型 | 说明 |
| - | - | - |
| `is_running` | `bool` | 总线是否在运行。详见 [is_running](#is_running)。 |
| `is_publishing_enabled` | `bool` | 是否允许发布新事件。详见 [is_publishing_enabled](#is_publishing_enabled)。 |
| `active_task_count` | `int` | 当前活跃的处理器任务数。 |
| `queue_size` | `int` | 事件队列中待处理的事件数。 |

#### `is_running`

指示事件总线的运行状态。其值在整个生命周期中的变化如下：

| 阶段 | `is_running` | 说明 |
| - | - | - |
| 构造后、`start()` 前 | `False` | 总线已创建但尚未启动。 |
| `start()` 执行中 | `False` | 初始化中间件、激活处理器与创建调度循环中，尚未标记运行。 |
| `start()` 完成后 | **`True`** | 调度循环已启动，正在分发事件。 |
| `stop()` 执行中 | **`True`** | 正在排空队列、等待活跃任务完成。 |
| `stop()` 完成后 | `False` | 所有资源已释放。 |

> **关键**：`is_running` 在 `stop()` 的**整个排空和等待期间保持 `True`**，
> 直到所有活跃任务完成、中间件拆卸完毕后才会变为 `False`。
> 这意味着在停机过程中，`is_running=True` 但 `is_publishing_enabled=False`
> （参见下文）。

```python
bus = EventBus(reg, h_reg)
print(bus.is_running)  # False

await bus.start()
print(bus.is_running)  # True

# stop() 内部：排空队列 + 等待任务期间，is_running 仍为 True
await bus.stop()
print(bus.is_running)  # False
```

#### `is_publishing_enabled`

指示是否允许向总线发布新事件。其值在整个生命周期中的变化如下：

| 阶段 | `is_publishing_enabled` | 说明 |
| - | - | - |
| 构造后、`start()` 前 | `False` | 发布将抛出 `RuntimeError`。 |
| `start()` 执行中 | `False` | 中间件与处理器初始化中，发布尚未开放。 |
| `start()` 完成后 | **`True`** | 可以正常发布事件。 |
| `stop()` 发布 `__shutdown__` 后 | **`False`** | 拒绝新事件入队，发布将抛出 `BusShuttingDown`。 |
| `stop()` 完成后 | `False` | — |

> **关键**：`is_publishing_enabled` 在 `stop()` 的**第一时间被清除**（紧随 `__shutdown__`
> 事件发布之后），早于队列排空和活跃任务等待。这确保了停机过程中不会有新事件
> 被加入队列，而已入队的事件仍会被正常分发处理。

##### 与 `is_running` 的区别

| 场景 | `is_running` | `is_publishing_enabled` |
| - | - | - |
| 正常运行 | `True` | `True` |
| 停机中（排空队列、等待任务） | `True` | **`False`** |
| 未启动 / 已停止 | `False` | `False` |

这两个属性在**停机过程中存在状态分离**：`is_running` 保持 `True` 以确保队列和
任务被完整处理，而 `is_publishing_enabled` 提前变为 `False` 以阻止新事件流入。

##### 使用场景

```python
# 场景 1：健康检查端点
@app.get("/health")
async def health():
    return {
        "status": "ok" if bus.is_running else "degraded",
        "can_publish": bus.is_publishing_enabled,
        "queue_depth": bus.queue_size,
        "active_handlers": bus.active_task_count,
    }

# 场景 2：优雅停机时等待总线完全停止
async def graceful_shutdown():
    signal.alarm("shutdown")
    await bus.stop()
    # 此时 is_running == False，可以安全退出进程
    assert not bus.is_running

# 场景 3：发布前检查（通常无需手动检查，publish 会抛出对应异常）
async def safe_publish(bus, name, data):
    if not bus.is_publishing_enabled:
        logger.warning("总线已停止接受新事件，跳过发布")
        return
    await bus.proxy("my_service").publish(name, data)
```

##### 状态转换时序图

```text
start()                                   stop()
  │                                         │
  │  dispatch_task 创建                      │  1. 发布 __shutdown__
  │  _running.set()           ◄────────── 仍在运行 ──────────►  _running.clear()
  │  _enable_publish.set()    ◄── 允许发布 ──►  _enable_publish.clear()
  │                                         │
  │  mw_chain.setup()                       │  2. 排空队列（queue.join()）
  │                                         │  3. 取消 dispatch_task
  ▼                                         ▼  4. 等待活跃任务
is_running=True                          │  5. mw_chain.teardown()
is_publishing_enabled=True               │
                                         ▼
                                       is_running=False
                                       is_publishing_enabled=False
```

---

## EventBus.Proxy

总线代理，**唯一允许对外发布事件的接口**。自动记录事件来源和处理链。

```python
class Proxy:
    async def publish(
        self,
        name: str,
        data: Optional[Union[Dict[str, Any], BaseModel]] = None
    ) -> None

    @property
    def handlers_registry(self) -> EventHandlerRegistry
    @property
    def events_registry(self) -> EventRegistry
    @property
    def middleware(self) -> MiddlewareChain
```

| 成员 | 说明 |
| - | - |
| `publish(name, data=None)` | 发布一个事件。`data` 可为字典或 Pydantic 模型实例。总线未运行时抛 `RuntimeError`，停止中抛 `BusShuttingDown`，未知事件抛 `ValueError`，负载类型不匹配抛 `TypeError`。 |
| `handlers_registry` | 只读访问处理器注册表。 |
| `events_registry` | 只读访问事件注册表。 |
| `middleware` | 访问中间件链，支持运行时动态增删。变更即时生效。参见 [中间件文档](middleware.md)。 |

### 使用示例

```python
# 基础发布
await bus.proxy("my_service").publish("user.login", {"user_id": "42"})

# 链式发布（事件转发）
class LoginHandler(EventHandler):
    async def handle(self, payload, bus_proxy, raw_event):
        await bus_proxy.publish("user.profile_loaded", profile_data)
```

---

## ShutdownConfig

总线停机行为配置，用于控制优雅停止过程中的超时参数。

```python
class ShutdownConfig(BaseModel):
    queue_timeout_min: float = 1.0
    queue_timeout_max: float = 15.0
    tasks_timeout: float = 15.0
    avg_wait_time: float = 0.05
```

| 字段 | 类型 | 默认值 | 说明 |
| - | - | - | - |
| `queue_timeout_min` | `float` | `1.0` | 队列排空最小等待时间（秒）。 |
| `queue_timeout_max` | `float` | `15.0` | 队列排空最大等待时间（秒）。 |
| `tasks_timeout` | `float` | `15.0` | 活跃处理器任务完成等待时间（秒）。 |
| `avg_wait_time` | `float` | `0.05` | 每个事件平均处理时间估算（秒），用于动态计算队列排空超时。 |

实际队列排空超时 = `max(queue_timeout_min, min(queue_timeout_max, queue_size × avg_wait_time))`。

---

## 异常

| 异常类 | 说明 |
| - | - |
| `BusShuttingDown` | 总线正在停止时尝试发布新事件抛出。继承自 `Exception`。调用方应捕获并执行清理逻辑。 |
| `RuntimeError` | 总线尚未启动时尝试发布事件抛出。 |
| `ValueError` | 未知事件名，或负载不匹配（声明要求负载但未传入、或声明无负载但传入了数据）。 |
| `TypeError` | 传入的负载类型与事件声明中定义的 `payload_type` 不匹配。 |

---

## 内置事件

### ShutdownEvent

```python
class ShutdownEvent(EventDeclaration):
    name = 'infinity_bus.__shutdown__'
    # payload_type = None (无负载)
```

在 `stop()` 调用时自动发布（队列排空前）。处理器可订阅此事件执行清理工作。

### TaskErrorEvent

```python
class TaskErrorPayload(BaseModel):
    error_event: Event
    handler_id: Optional[str]
    handler_name: str
    error_type: str
    error_message: str

class TaskErrorEvent(EventDeclaration):
    name = 'infinity_bus.__task_error__'
    payload_type = TaskErrorPayload
```

处理器抛出异常时自动发布。来源始终为 `"EventBusErrorReporter"`。可用于集中式错误监控。

```python
class ErrorMonitor(EventHandler):
    def __init__(self):
        super().__init__(["infinity_bus.__task_error__"])

    async def handle(self, payload, bus_proxy, raw_event):
        if isinstance(payload, TaskErrorPayload):
            logger.error(
                "处理器 %s 在处理 %s 时失败: %s",
                payload.handler_name,
                payload.error_event.name,
                payload.error_message,
            )
```

---

## 使用示例

### 基础组装与启动

```python
from infinity_bus import EventBus, EventRegistry, EventHandlerRegistry

# 1. 声明事件
class MyPayload(BaseModel):
    message: str

class MyEvent(EventDeclaration):
    name = "my.event"
    payload_type = MyPayload

# 2. 注册事件
reg = EventRegistry()
reg.register(MyEvent)

# 3. 实现处理器
class MyHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=["my.event"])

    async def handle(self, payload, bus_proxy, raw_event):
        print(f"Received: {payload.message}")

# 4. 注册处理器
h_reg = EventHandlerRegistry()
await h_reg.register(MyHandler())

# 5. 启动总线并发布
async with EventBus(reg, h_reg) as bus:
    await bus.proxy("cli").publish("my.event", {"message": "Hello"})
    await asyncio.sleep(0.1)  # 等待处理器输出
```

### 链式发布

处理器中可通过 `bus_proxy.publish` 发布新事件，形成处理链，总线会自动追踪来源：

```python
class OrderHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=["order.created"])

    async def handle(self, payload, bus_proxy, raw_event):
        # 处理订单...
        # 链式发布通知事件
        await bus_proxy.publish("notification.send", {
            "type": "order_confirmed",
            "order_id": payload.order_id
        })
```
