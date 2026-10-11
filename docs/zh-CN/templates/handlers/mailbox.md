# Mailbox Handler 邮箱模式处理器

## 概述

`MailboxHandler` 是一个**邮箱模式**的 `EventHandler` 抽象基类。它将事件入队到内部邮箱队列，由子类实现 `process()` 方法从队列中逐一取出事件并处理。适合需要**串行消费**、**背压控制**、**自定义任务循环**的场景。

与直接继承 `EventHandler` 实现 `handle()` 不同，`MailboxHandler` 将"接收事件"和"处理事件"解耦到两个不同的协程中：

- `handle()` — 运行在总线的调度上下文中，仅负责将事件放入邮箱队列。
- `process()` — 运行在独立的 `asyncio.Task` 中，按自己的节奏从队列取事件处理。

---

## 使用场景

- 需要**严格串行**处理事件的场景（避免同一处理器的并发执行）。
- 需要对突发流量提供**背压控制**（通过限制队列容量）。
- 需要**自定义事件循环**的场景（如定时清理、定期批处理、状态机驱动的消费逻辑）。
- 模拟 Actor 模型的**邮箱**语义。
- 多个事件源的数据需要在单一消费者中按序聚合处理。

---

## 类签名

```python
class MailboxHandler(EventHandler, ABC):
    def __init__(
        self,
        subscriptions: list[str | Regex],
        config: MailboxConfig | None = None,
    ) -> None: ...

    async def process(self) -> None: ...
    async def get(self) -> tuple[Event, EventBus.Proxy]: ...
```

| 成员 | 类型 | 说明 |
| - | - | - |
| `subscriptions` | `list[str \| Regex]` | 订阅的事件模式列表（原样保留，不隐式追加 `ShutdownEvent`）。 |
| `config` | `MailboxConfig \| None` | 邮箱配置，`None` 时使用默认值。 |
| `process()` | `@abstractmethod async` | 子类必须实现的自定义任务循环。通过 `await self.get()` 获取下一事件。 |
| `get()` | `async → (Event, Proxy)` | 从邮箱取出下一个 `(事件, 总线代理)`。队列空时阻塞等待。 |
| `on_activate(bus)` | 钩子 | 总线激活（注册表 `activate`）时启动 `process()` 任务。 |
| `on_deactivate(bus)` | 钩子 | 总线解绑 / 注销时取消任务并清空积压。停机后迟到的在途 `put()` 会被丢弃（自回滚），不留孤儿积压。 |
| `bus` | `EventBus \| None` | 当前绑定的 `EventBus` 实例，激活（`on_activate`）后可用。 |
| `is_running` | `bool` | `process()` 后台任务是否正在运行。 |

### 异常

| 异常 | 抛出位置 | 说明 |
| - | - | - |
| `StopMailbox` | `process()` 内 | 主动停止信号：消费循环正常退出且**不触发重启**。 |

---

## MailboxConfig 配置

```python
class MailboxConfig(BaseModel):
    queue_put_timeout: float | None = None   # 入队超时（秒），None 表示无限等待
    restart_delay: float = 0.5               # 异常后重启等待间隔（秒）
    restart_jitter: float = 0.2              # 重启等待随机偏移上限（秒），避免惊群
    max_queue_size: int = 0                  # 邮箱队列最大容量，0 表示无限制
```

| 参数 | 默认值 | 说明 |
| - | - | - |
| `queue_put_timeout` | `None` | 入队超时。`None` 无限等待；设值后超时将抛出 `RuntimeError`。 |
| `restart_delay` | `0.5` | `process()` 异常退出后，重启前等待的基础秒数。 |
| `restart_jitter` | `0.2` | 在 base delay 上叠加的随机偏移 `[0, jitter]`，用于多实例场景防惊群。 |
| `max_queue_size` | `0` | 队列容量上限。`0` 表示无界队列。 |

> 实际重启等待时间 = `restart_delay + random(0, restart_jitter)`

---

## 工作流程

```text
事件到达
  │
  ▼
handle() → put(event, proxy) → 入队
  │
  ▼
process() 协程（独立 Task）
  │
  ▼
await get() → 取出 (event, proxy)
  │
  ▼
处理业务逻辑（可通过 proxy.publish() 发布新事件）
  │
  ▼
循环回到 get()
```

1. **钩子启动**：注册到已激活总线或总线启动（`registry.activate`）时，`on_activate` 创建 `process()` 后台任务。
2. **事件入队**：`handle()` 将 `(Event, EventBus.Proxy)` 元组放入内部 `asyncio.Queue`。
3. **串行消费**：`process()` 循环调用 `await self.get()` 逐一取出事件处理。
4. **异常重启**：若 `process()` 因非 `CancelledError` / `StopMailbox` 异常退出，`_process_loop` 会在等待 `restart_delay + jitter` 后重新调用 `process()`。
5. **主动停止**：`process()` 内抛出 `StopMailbox` 时立即退出循环且不重启。
6. **钩子停机**：总线停止（`registry.deactivate`）时 `on_deactivate` 取消 `process()` 任务并清空积压。

---

## 使用示例

### 基础用法：串行事件收集

```python
from infinity_bus import Event, EventBus
from infinity_bus.templates import MailboxHandler

class MyHandler(MailboxHandler):
    def __init__(self):
        super().__init__(subscriptions=['my.event'])
        self.received: list[Event] = []

    async def process(self) -> None:
        while True:
            event, proxy = await self.get()
            self.received.append(event)
            # 可以通过 proxy.publish() 发布新事件
```

### 自定义任务循环：定时批处理

```python
import asyncio

class BatchHandler(MailboxHandler):
    def __init__(self):
        super().__init__(subscriptions=['data.input'])

    async def process(self) -> None:
        while True:
            batch: list[Event] = []
            # 收集一批事件，或等待超时
            try:
                event, _ = await asyncio.wait_for(self.get(), timeout=1.0)
                batch.append(event)
                # 尝试继续收集
                while True:
                    try:
                        event, _ = await asyncio.wait_for(self.get(), timeout=0.1)
                        batch.append(event)
                    except asyncio.TimeoutError:
                        break
            except asyncio.TimeoutError:
                pass
            if batch:
                await self._process_batch(batch)

    async def _process_batch(self, batch: list[Event]) -> None:
        print(f'处理了 {len(batch)} 个事件')
```

### 异常重启：崩溃后自动恢复

```python
class RobustHandler(MailboxHandler):
    def __init__(self):
        super().__init__(
            subscriptions=['task.request'],
            config=MailboxConfig(restart_delay=0.5, restart_jitter=0.2),
        )

    async def process(self) -> None:
        while True:
            event, proxy = await self.get()
            try:
                await self._handle_task(event)
            except Exception:
                # 单个事件失败继续处理下一个
                # 注意：process() 整体崩溃才会触发重启
                pass
```

### 主动停止：业务驱动的退出

```python
from infinity_bus.templates import MailboxHandler, StopMailbox

class DrainThenStopHandler(MailboxHandler):
    """处理到停机指令后主动结束消费循环"""

    def __init__(self):
        super().__init__(subscriptions=['task.command'])

    async def process(self) -> None:
        while True:
            event, proxy = await self.get()
            if event.data is not None and getattr(event.data, 'action', None) == 'stop':
                raise StopMailbox  # 退出循环且不触发重启
            await self._handle(event)
```

### 使用 bus 属性访问总线

```python
class BusAwareHandler(MailboxHandler):
    def __init__(self):
        super().__init__(subscriptions=['status.check'])

    async def process(self) -> None:
        while True:
            event, proxy = await self.get()
            # 通过 self.bus 访问原始总线
            if self.bus and self.bus.is_running:
                await proxy.publish('status.reply', {'ok': True})
```

---

## 注意事项

- **process() 必须是无限循环**：`_process_loop` 在 `process()` 正常返回后会重新调用它。如果 `process()` 只执行一次就返回，它将立即被再次调用，形成忙等循环。请始终使用 `while True` 包裹业务逻辑。
- **事件丢失**：若 `process()` 在 `get()` 返回后、处理完成前崩溃，该事件会丢失。`_process_loop` 重启后会从下一个 `get()` 开始，不会重试已取出的事件。
- **不再隐式订阅 ShutdownEvent**：停机由 `on_deactivate` 钩子驱动；若自行订阅了 `ShutdownEvent`，它只会作为普通事件入队。
- **bus 随激活绑定**：`self.bus` 在 `on_activate`（总线激活）时设置、`on_deactivate` 时清空。注册表单总线约束下不会出现多总线串扰。
- **重启与停止**：`CancelledError`（外部取消）与 `StopMailbox`（主动停止）都会退出循环且不重启；其余 `Exception` 触发重启等待后重新调用 `process()`。`KeyboardInterrupt` 和 `SystemExit` 属于 `BaseException`，不会被捕获，会向上传播。
- **停止后的邮箱**：抛出 `StopMailbox` 仅结束消费任务，`is_running` 变为 `False`；后续到达的事件仍会入队但无人消费。如需彻底停收，请从注册表注销该处理器或停止总线。

---

## 与直接继承 EventHandler 的对比

| 特性 | 直接继承 EventHandler | MailboxHandler |
| - | - | - |
| 并发模型 | 每个事件可能并发执行 | 强制串行消费 |
| 背压控制 | 依赖总线 Semaphore | 额外提供队列容量限制 |
| 任务生命周期 | 由总线管理 | 独立的 `asyncio.Task`，随 `activate`/`deactivate` 启停 |
| 自定义循环 | 不支持 | 支持批处理、定时器等灵活模式 |
| 异常恢复 | 依赖 `TaskErrorEvent` | 内建重启机制 |
| 关闭行为 | 依赖 `ShutdownEvent` 订阅 | `on_deactivate` 钩子自动取消任务 |
