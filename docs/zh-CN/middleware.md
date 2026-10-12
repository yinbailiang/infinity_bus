# Middleware / MiddlewareChain 文档

## 概述

中间件系统为 EventBus 提供了可插拔的发布流程钩子。通过在发布前后插入自定义逻辑，可以实现日志记录、数据校验、链路追踪、性能监控、消息转换等横切关注点，而无需修改核心业务处理器。

中间件采用**洋葱模型（责任链）**设计：多个中间件按注册顺序层层包裹，每个中间件可以在调用 `next()` 前后执行自己的逻辑。

---

## 架构概念

```text
发布请求
  │
  ▼
┌─────────────────────────────────────┐
│  Middleware 1 (外层)                 │
│  ┌─────────────────────────────────┐ │
│  │ Middleware 2 (内层)             │ │
│  │ ┌─────────────────────────────┐ │ │
│  │ │ 核心发布逻辑                 │ │ │
│  │ │ ├─ 校验 → 构造 → 入队       │ │ │
│  │ │ └─ on_publish 链（洋葱模型） │ │ │
│  │ └─────────────────────────────┘ │ │
│  └─────────────────────────────────┘ │
└─────────────────────────────────────┘
```

> **注意**：`on_publish` 链嵌套在核心发布逻辑内部，而核心发布逻辑是 `before_publish` 链的最内层。因此 `before_publish` 钩子中 `await next(...)` 之后的代码会在 `on_publish` 链**完全结束之后**才执行。

### 两个钩子阶段

| 阶段 | 时机 | 可获取的信息 |
| - | - | - |
| `before_publish` | 事件声明校验、构造 Event 并入队**之前** | 事件名、来源、原始 data、前驱 event |
| `on_publish` | Event 成功入队**之后** | 完整的 Event 对象（含 id、sources、timestamps） |

---

## Middleware

中间件抽象基类。自定义中间件需继承此类并实现 `before_publish` 和 `on_publish` 两个抽象方法。

```python
class Middleware(ABC):
    # 生命周期
    async def on_setup(self, bus: EventBus) -> None: ...
    async def on_teardown(self, bus: EventBus) -> None: ...

    # 发布钩子（必须实现）
    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None: ...

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None: ...

    # 错误钩子（可选）
    async def on_publish_error(
        self,
        error: Exception,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> None: ...

    # 任务管道钩子（可选）
    async def before_dispatch(
        self,
        event: Event,
        handler_ids: list[str],
        next: DispatchNext,
    ) -> None: ...

    async def on_task_start(self, record: TaskRecord) -> None: ...

    async def on_task_done(self, record: TaskRecord) -> None: ...
```

### 钩子详解

#### `on_setup(bus)` / `on_teardown(bus)`

总线生命周期钩子。`on_setup` 在总线 `start()` 过程中调用（**先于处理器激活**——处理器在激活阶段发布的消息因此经过完整就绪的中间件管道），也会在**运行时通过 `add()` / `insert()` 热添加中间件时立即调用**。`on_teardown` 在 `stop()` 结束时**逆序**调用，也会在运行时 `remove()` / `clear()` 时立即调用。适用于初始化连接池、注册后台任务等场景。

> **注意**：中间件生命周期与处理器一致，**fail-fast 且原子**——`on_setup` 失败会使总线启动整体失败（回滚后可重试修复），运行时热添加的中间件 `on_setup` 失败会被回滚并拒绝加入；失败的中间件不会被静默丢弃。
>
> **警告**：运行时 `remove()` 不等待已在飞行的钩子完成。`on_teardown` 被调用后，已进入 `before_publish` / `on_publish` 的中间件实例仍可能继续执行。中间件作者应确保自身状态清理不影响这些残留调用。

#### `before_publish(event_registry, name, source, data, old_event, next)`

发布**前**钩子。在 publish 的任何逻辑（含事件声明校验）开始前执行。调用 ``next`` 后才会进入校验 → 构造 Event → 入队流程。

| 参数 | 说明 |
| - | - |
| `event_registry` | 事件注册表，可查询事件声明元数据。 |
| `name` | 事件名称。 |
| `source` | 发布者标识。 |
| `data` | 原始负载数据（字典或 BaseModel，可能为 `None`）。 |
| `old_event` | 前驱事件（链式发布时，可能为 `None`）。 |
| `next` | 调用下一个中间件（或核心发布逻辑）。**必须调用**，否则事件不会入队。 |

#### `on_publish(event, next)`

发布**后**钩子。在 Event 成功入队后执行。可通过 `event.name`、`event.data`、`event.id`、`event.sources` 等获取完整的运行时事件信息。

| 参数 | 说明 |
| - | - |
| `event` | 已入队的完整 Event 对象。 |
| `next` | 调用下一个中间件。 |

#### `on_publish_error(error, name, source, data)`

发布流程中发生异常时回调（可选实现）。按注册**顺序**通知所有中间件，某个中间件的异常不会影响其他中间件的通知。

| 参数 | 说明 |
| - | - |
| `error` | 发生的异常对象。 |
| `name` | 事件名称。 |
| `source` | 发布者标识。 |
| `data` | 原始负载数据。 |

#### `before_dispatch(event, handler_ids, next)`

分发**前**钩子 —— 路由匹配完成后、创建处理器任务前执行。调用 `await next(event, handler_ids)` 进入下一个中间件；**链末端按传入的 id 顺序逐个创建任务**（可传过滤 / 重排后的列表）；不调用 `next` 即**阻断**本次派发（事件被丢弃，由中间件自行补偿，建议自行记录以便审计）。

> **警告**：本钩子在分发循环协程内执行，处于关键路径——应快速返回，或自行 `asyncio.create_task` 卸载耗时工作，否则将拖慢整个分发节奏。

| 参数 | 说明 |
| - | - |
| `event` | 已出队、路由匹配后的 Event（此时任务尚未创建；零匹配事件也会触发）。 |
| `handler_ids` | 匹配到的处理器 id 列表；`next` 接收到的新列表顺序即任务创建顺序。 |
| `next` | 调用下一个中间件（或链末端：按最终 id 列表创建任务）。不调用即阻断。 |

#### `on_task_start(record)`

任务**开始**钩子（通知式，可选实现）—— 处理器任务**获取并发信号量后**、执行 `handle()` 前触发。触发时 `record.outcome` / `duration_seconds` 等结束字段为 `None`；信号量排队时长可由 `started_at - dispatched_at` 推导。与 `on_task_done` **严格配对**，钩子异常被逐中间件隔离记录，不影响任务执行。

#### `on_task_done(record)`

任务**回收**钩子（通知式，可选实现）—— 任务结束时（成功 / 异常 / 超时 / 取消 / 停机跳过）触发，收到与 `on_task_start` **同一记录实例**，已补齐：

| 字段 | 说明 |
| - | - |
| `outcome` | `success` / `error` / `cancelled` / `skipped`（停机期间发布被拒）。 |
| `duration_seconds` | 开始执行到结束的时长（秒）。 |
| `error_type` / `error_message` | 异常信息（`outcome=error` 时）。 |

等待信号量期间即被取消、从未开始的任务不会触发（与 `on_task_start` 严格配对）；钩子异常同样被隔离记录，不改变任务结果。

---

## MiddlewareChain

中间件链管理器，负责维护中间件的注册顺序和生命周期。

```python
class MiddlewareChain:
    def __init__(self) -> None

    # 增删（async —— 总线启动后立即触发生命周期）
    async def add(self, middleware: Middleware) -> "MiddlewareChain"
    async def insert(self, index: int, middleware: Middleware) -> "MiddlewareChain"
    async def remove(self, middleware: Middleware) -> None
    async def clear(self) -> None

    @property
    def middlewares(self) -> list[Middleware]

    # 责任链构建
    def build_before_publish(
        self,
        final_handler: BeforePublishNext,
    ) -> BeforePublishNext
    def build_on_publish(
        self,
        final_handler: OnPublishNext,
    ) -> OnPublishNext
    def build_on_publish_error(
        self,
        final_handler: OnPublishErrorNext,
    ) -> OnPublishErrorNext

    # 生命周期
    async def setup(self, bus: EventBus) -> None
    async def teardown(self, bus: EventBus, strict: bool = False) -> list[tuple[Middleware, Exception]]
```

| 方法 / 属性 | 说明 |
| - | - |
| `add(middleware)` | **async**。在链**末尾**追加中间件。总线启动后立即调用 `on_setup`；若 `on_setup` 失败，先补发 `on_teardown`（副作用回滚）、不加入链并抛 `RuntimeError`。返回自身。重复添加同一实例抛出 `ValueError`。 |
| `insert(index, middleware)` | **async**。在指定位置插入中间件。总线启动后立即调用 `on_setup`（失败回滚语义同 `add`）。重复添加同一实例抛出 `ValueError`。 |
| `remove(middleware)` | **async**。移除指定中间件实例。不存在的实例视为幂等空操作，返回 `None`（成员判断用 `in chain.middlewares`）；总线启动后立即调用 `on_teardown`，其失败会作为返回值返回，但移除仍完成。 |
| `clear()` | **async**。清空所有中间件。总线启动后立即逐一调用 `on_teardown`；返回 `[(middleware, 异常), ...]` 失败清单。 |
| `middlewares` | （属性）返回当前中间件列表的副本。 |
| `build_before_publish(final_handler)` | 构建 ``before_publish`` 责任链。传入核心发布逻辑作为末端处理器，返回包装后的可调用链。 |
| `build_on_publish(final_handler)` | 构建 ``on_publish`` 责任链。传入空操作作为末端处理器，返回包装后的可调用链。 |
| `build_on_publish_error(final_handler)` | 构建 ``on_publish_error`` 责任链。传入空操作作为末端处理器，返回包装后的可调用链。 |
| `build_dispatch(final_handler)` | 构建 ``before_dispatch`` 责任链（链末端为「按 id 列表创建任务」的总线逻辑）。 |
| `build_task_start(final_handler)` | 构建 ``on_task_start`` 通知链（逐中间件错误隔离后继续）。 |
| `build_task_done(final_handler)` | 构建 ``on_task_done`` 通知链（逐中间件错误隔离后继续）。 |
| `setup(bus)` | 按注册顺序调用所有中间件的 `on_setup`。**原子（fail-fast）**：任一 `on_setup` 抛错即解绑，并对**已尝试的中间件（含失败者）**补发 `on_teardown` 后向上抛出——中间件保留在链中（修复后可重试），与处理器 `activate` 语义一致。 |
| `teardown(bus)` | 按注册**逆序**调用所有中间件的 `on_teardown`。单个异常不影响其他。**幂等**。返回 `[(middleware, 异常), ...]` 失败清单，调用方（如 `EventBus.stop()`）可感知清理不完整；传 `strict=True` 在全部尝试后抛出首个失败。 |

### 责任链构建

每次发布时，`build_before_publish()`、`build_on_publish()` 和 `build_on_publish_error()` 会惰性构建责任链；分发与任务生命周期同理——`build_dispatch()`（每次分发）、`build_task_start()` / `build_task_done()`（每个任务）。构建结果被缓存，当中间件列表变更时自动失效。总线内部在发布 / 分发 / 任务三个时机通过对应方法将中间件列表包装为可调用链。

---

## 内置中间件

详见 [templates/middlewares/](templates/middlewares/middlewares.md) 完整文档。

| 中间件 | 阶段 | 用途 |
| - | - | - |
| `JSONLLoggingMiddleware` | `on_publish` | 零依赖 JSONL 文件日志 |
| `SQLiteLoggingMiddleware` | `on_publish` | SQLite 数据库日志（需 `aiosqlite`） |
| `RateLimitMiddleware` | `before_publish` | 滑动窗口速率限制（全局或按事件） |
| `EventTransformMiddleware` | `before_publish` | 事件名 / 字段转换（重命名、脱敏、注入） |
| `EventBlockMiddleware` | `before_publish` | 按规则屏蔽事件（白名单 / 黑名单） |
| `EventForwardMiddleware` | `before_publish` | 跨总线事件转发 |
| `RecursionGuardMiddleware` | `before_publish` | 防止无限事件循环（可配置深度） |
| `MetricsMiddleware` | `before_publish` | 轻量级 Prometheus / OTel 风格指标 |

---

## 使用示例

### 日志中间件

记录每次发布的事件信息和耗时：

```python
import time
import logging
from infinity_bus import Middleware, MiddlewareChain

logger = logging.getLogger(__name__)

class LoggingMiddleware(Middleware):
    async def on_setup(self, bus):
        logger.info("LoggingMiddleware initialized")

    async def on_teardown(self, bus):
        logger.info("LoggingMiddleware shutting down")

    async def before_publish(self, event_registry, name, source, data, old_event, next):
        t0 = time.monotonic()
        logger.debug(f"[Publish] {name} from {source}")
        try:
            await next(event_registry, name, source, data, old_event)
        finally:
            elapsed = time.monotonic() - t0
            logger.debug(f"[Publish] {name} completed in {elapsed:.3f}s")

    async def on_publish(self, event, next):
        logger.debug(f"[Enqueued] {event.name} (id={event.id})")
        await next(event)

    async def on_publish_error(self, error, name, source, data):
        logger.error(f"[Error] {name} from {source}: {error}")

# 注册到总线
chain = MiddlewareChain()
await chain.add(LoggingMiddleware())

bus = EventBus(reg, h_reg, middleware_chain=chain)
```

### 数据校验中间件

在发布前对数据进行额外校验：

```python
class ValidationMiddleware(Middleware):
    async def on_setup(self, bus): pass
    async def on_teardown(self, bus): pass

    async def before_publish(self, event_registry, name, source, data, old_event, next):
        if name == "order.create" and isinstance(data, dict):
            if data.get("amount", 0) <= 0:
                raise ValueError("Order amount must be positive")
        await next(event_registry, name, source, data, old_event)

    async def on_publish(self, event, next):
        await next(event)
```

### 短路中间件

在某些条件下阻止事件发布（不调用 `next`）：

```python
class RateLimitMiddleware(Middleware):
    def __init__(self, max_per_second: int = 100):
        self._max = max_per_second
        self._count = 0
        self._window_start = time.monotonic()

    async def on_setup(self, bus): pass
    async def on_teardown(self, bus): pass

    async def before_publish(self, event_registry, name, source, data, old_event, next):
        now = time.monotonic()
        if now - self._window_start > 1.0:
            self._window_start = now
            self._count = 0
        self._count += 1
        if self._count > self._max:
            logger.warning(f"Rate limit exceeded, dropping {name}")
            return  # 不调用 next，事件被丢弃
        await next(event_registry, name, source, data, old_event)

    async def on_publish(self, event, next):
        await next(event)
```

### 多中间件洋葱顺序

```python
chain = MiddlewareChain()
await chain.add(LoggingMiddleware())       # 最外层
await chain.add(ValidationMiddleware())    # 中层
await chain.add(RateLimitMiddleware())     # 最内层

# 执行顺序（on_publish 链嵌套在 before_publish 链内部）：
#   Logging.before 进入 → Validation.before 进入 → RateLimit.before 进入
#     → 核心发布逻辑（校验 → 构造 → 入队）
#     → Logging.on 进入 → Validation.on 进入 → RateLimit.on 进入
#         → 空操作（on_publish 链终点）
#       ← RateLimit.on 退出 ← Validation.on 退出 ← Logging.on 退出
#   ← RateLimit.before 退出 ← Validation.before 退出 ← Logging.before 退出
```

### 运行时热重载

中间件链支持运行时动态增删，变更即时生效。可通过总线代理在处理器或中间件内部操作：

```python
class HotReloadHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=["admin.toggle"])

    async def handle(self, payload, bus_proxy, raw_event):
        chain = bus_proxy.middleware

        # 热添加 —— on_setup 立即被调用
        await chain.add(AuditMiddleware())

        # 热移除 —— on_teardown 立即被调用
        # 注意：已在飞的链仍可能调用该中间件，见 Middleware.on_teardown 文档
        await chain.remove(some_mw)

        # 热清空
        await chain.clear()
```

> **注意**：增删操作直接返回，不等待已飞行的钩子完成。被移除的中间件的 `on_teardown` 调用后，已在执行的 `before_publish` / `on_publish` 仍可能继续运行。中间件作者应在 `on_teardown` 中做幂等清理。
