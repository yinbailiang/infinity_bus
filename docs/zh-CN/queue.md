# 事件队列（EventQueue）

## 概述

`EventQueue` 是事件总线**内部派发队列**的可替换抽象。它把"队列"从 `EventBus`
中解耦出来：**总线只依赖 `EventQueue` 抽象接口**进行发布入队、分发取件与停机排空，
不感知任何具体实现及其配置；有界/无界、持久化、优先级、跨进程等语义完全由
调用方构造的具体队列自行决定。

| 符号 | 说明 |
| - | - |
| `EventQueue` | 队列抽象基类（ABC），定义总线派发所需的最小队列协议 |
| `InMemoryEventQueue` | 基于 `asyncio.Queue` 的默认进程内实现，支持有界背压 |
| `InMemoryEventQueueConfig` | 进程内队列自身的配置模型（与总线无关） |

---

## EventQueue（抽象基类）

```python
class EventQueue(ABC):
    async def put(self, event: Event) -> None   # 入队；满则阻塞（背压）
    async def get(self) -> Event                # 取出队首；空则阻塞
    def task_done(self) -> None                 # 标记一个事件处理完成
    def qsize(self) -> int                      # 当前待处理事件数
    async def join(self) -> None                # 等待队列排空
```

| 方法 | 说明 |
| - | - |
| `put(event)` | 发布端入队。队列已满时阻塞直至有空位，形成背压。 |
| `get()` | 分发端取件。队列为空时阻塞直至有事件到达。 |
| `task_done()` | 每个经 `get()` 取出的事件处理完毕后调用，供 `join()` 排空判定。 |
| `qsize()` | 积压深度，供可观测性与停机排空超时估算。 |
| `join()` | 阻塞直至所有已入队事件均被取出并完成 `task_done`（优雅停机不丢事件的关键）。 |

实现者只需满足上述最小协议即可被总线使用；具体的容量策略、存储介质、
队列语义由实现自身负责。

---

## InMemoryEventQueueConfig

进程内队列的**自身配置**，不属于总线。

```python
class InMemoryEventQueueConfig(BaseModel):
    maxsize: int = 1024   # 队列最大容量；0 表示无限制（沿用 asyncio.Queue 语义）
```

| 字段 | 类型 | 默认值 | 说明 |
| - | - | - | - |
| `maxsize` | `int` | `1024` | 队列最大容量，超出后 `put` 阻塞；`0` 表示无界。 |

---

## InMemoryEventQueue

基于 `asyncio.Queue` 的默认实现，`EventBus` 在未注入队列时的缺省选择。

```python
queue = InMemoryEventQueue()                                        # 默认有界 1024
queue = InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=0))     # 无界
queue = InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=100))   # 有界 100
```

---

## 与 EventBus 的注入关系

`EventBus` 构造时通过 `queue` 参数接收一个 `EventQueue` 实例；缺省时内部创建
`InMemoryEventQueue()`（容量取队列自身默认配置 1024）。

```python
class EventBus:
    def __init__(
        self,
        event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
        queue: Optional[EventQueue] = None,   # 注入的队列抽象，缺省为 InMemoryEventQueue()
        router: Optional[Router] = None,      # 注入的路由器，缺省为 Router(event_registry)
        max_handler_semaphore: int = 256,
        shutdown: ShutdownConfig = ShutdownConfig(),
        middleware_chain: Optional[MiddlewareChain] = None,
    ) -> None
```

| 参数 | 类型 | 默认值 | 说明 |
| - | - | - | - |
| `queue` | `Optional[EventQueue]` | `None` | 总线内部派发队列。不注入时使用默认进程内实现，容量取其自身默认配置。 |

> **解耦要点**：队列容量等配置由队列自身（`InMemoryEventQueueConfig`）持有，
> `EventBus` 不感知也不转发任何队列配置。

---

## 使用场景

### 注入有界队列（替代旧 `max_queue_size`）

```python
from infinity_bus import EventBus, InMemoryEventQueue, InMemoryEventQueueConfig

# 旧写法: EventBus(reg, hreg, max_queue_size=100)
queue = InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=100))
bus = EventBus(reg, hreg, queue=queue)
```

### 注入自定义队列（持久化 / 优先级 / 跨进程）

只需实现 `EventQueue` 五个方法，即可替换总线内部队列：

```python
from infinity_bus import EventQueue, Event

class PriorityEventQueue(EventQueue):
    """示例：按优先级取件的自定义队列（示意，需自行实现完整协议）"""

    async def put(self, event: Event) -> None: ...
    async def get(self) -> Event: ...
    def task_done(self) -> None: ...
    def qsize(self) -> int: ...
    async def join(self) -> None: ...

bus = EventBus(reg, hreg, queue=PriorityEventQueue())
```

---

## 工作流程

1. 构造阶段：`EventBus` 将注入的 `EventQueue` 实例保存为内部派发队列。
2. 发布阶段：`_core_publish` 调用 `await queue.put(event)`，队列满时发布端阻塞（背压）。
3. 分发阶段：调度循环 `event = await queue.get()` → 匹配处理器 → `queue.task_done()`。
4. 停机阶段：`stop()` 依据 `queue.qsize()` 估算动态超时，`await queue.join()` 排空队列，保证不丢事件。

---

## 注意事项

- `EventQueue` 的协议刻意**最小化**：仅覆盖总线派发所需的 5 个方法；`empty/full/put_nowait`
  等非总线所需方法不进入抽象，具体实现可按需自行扩展。
- 自定义队列实现者需保证 `task_done` 与 `join` 的计数语义（每取出一次须对应一次 `task_done`），
  否则停机排空可能永久阻塞。
- 若需与旧版 `max_queue_size` 等价的容量控制，请注入
  `InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=N))`。
