# EventHandler / EventHandlerRegistry 文档

## 概述

`EventHandler` 是事件处理器的抽象基类，通过订阅模式（`str` 全字匹配，`Regex` 正则匹配）监听感兴趣的事件。
`EventHandlerRegistry` 管理所有处理器实例的增删查。

---

## EventHandler

事件处理器抽象基类。所有业务处理器必须继承并实现 `handle` 方法。

```python
class EventHandler(ABC):
    def __init__(
        self,
        subscriptions: Optional[List[Regex | str]] = None,
        handle_timeout: Optional[float] = 32.0
    ) -> None

    async def __call__(self, bus: EventBus, event: Event) -> None

    @abstractmethod
    async def handle(
        self,
        payload: Optional[BaseModel],
        bus_proxy: EventBus.Proxy,
        raw_event: Event
    ) -> None: ...

    @property
    def handler_id(self) -> Optional[str]: ...

    def on_registered(self, handler_id: str) -> None: ...
    def on_unregistered(self) -> None: ...
    def on_activate(self, bus: EventBus) -> None: ...
    def on_deactivate(self, bus: EventBus) -> None: ...
```

| 参数 / 方法 | 说明 |
| - | - |
| `subscriptions` | 订阅的事件名模式列表。`str` 全字匹配（`event_type == subscription`），`Regex` 正则全匹配。如 `["user.login"]` 只匹配 `"user.login"`，`[Regex(r"user\..*")]` 匹配所有 `user.*` 事件。 |
| `handle_timeout` | 单次 `handle` 调用的超时时间（秒）。`None` 表示无限等待。默认 `32.0`。 |
| `__call__` | 总线内部入口，接收原始 ``EventBus`` 实例，内部调用 ``bus.proxy(handler_name, event)`` 创建代理后传递给 ``handle()``。 |
| `handle(payload, bus_proxy, raw_event)` | **子类必须实现**。`payload` 为已解包的负载（可能为 `None`）。`bus_proxy` 提供受限的总线访问。`raw_event` 为完整事件对象。 |
| `handler_id` | 只读属性。注册后为注册表分配的 id；未注册 / 已注销为 `None`。 |
| `on_registered(handler_id)` | **生命周期钩子**。注册表在 `register()` 成功后调用，把分配的 id 回填给处理器。子类可覆写，需调用 `super()` 保留回填。 |
| `on_unregistered()` | **生命周期钩子**。注册表在 `unregister()` / `clear()` 移除后调用，清空处理器的 id。子类可覆写，需调用 `super()` 保留清空。 |
| `on_activate(bus)` | **总线激活钩子**。所属注册表绑定总线（`registry.activate(bus)`）时，或在已绑定注册表内新注册时触发。调用时 `handler_id` 已可用。**基类默认把本处理器的 `Subscriptions` 订阅束登记进 `bus.router`（激活即路由）**——空订阅束也会登记，使「激活 ⟺ 已登记」恒成立，运行期 `add()` 立即生效。子类覆写需调用 `super().on_activate(bus)` 保留该行为。 |
| `on_deactivate(bus)` | **总线下线钩子**。与 `on_activate` 对称：注册表 `deactivate()` 或从已绑定注册表注销单个处理器时触发。调用时 `handler_id` 仍可用。**基类默认把订阅束从 `bus.router` 撤销**。子类覆写需调用 `super().on_deactivate(bus)` 保留该行为。 |

### 生命周期钩子（处理器可感知自己的 id）

处理器被注册进注册表的那一刻，注册表通过钩子把 `handler_id` 回填给处理器；注销 / 清空时对称清空。处理器据此感知自己在注册表中的身份（如构造订阅记录、自我标识、错误上报）。

```python
class MyHandler(EventHandler):
    def on_registered(self, handler_id: str) -> None:
        super().on_registered(handler_id)   # 基类回填 self.handler_id
        print(f'上线，我的 id 是 {handler_id}')

    def on_unregistered(self) -> None:
        print(f'下线，我曾经的 id 是 {self.handler_id}')
        super().on_unregistered()
```

### `handle()` 签名

```python
async def handle(
    self,
    payload: Optional[BaseModel],      # 已类型化的负载（事件无负载时为 None）
    bus_proxy: EventBus.Proxy,          # 用于发布后续事件的代理
    raw_event: Event                    # 完整事件元数据（id、sources、timestamps）
) -> None:
```

### 使用示例

```python
from infinity_bus import EventHandler, Regex

class LoginHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=[Regex(r"user\..*")])  # 匹配所有 user.* 事件

    async def handle(self, payload, bus_proxy, raw_event):
        if isinstance(payload, UserLoginPayload):
            print(f"User {payload.user_id} logged in at {payload.timestamp}")
            # 可通过 bus_proxy.publish 发布新事件，形成处理链
```

### 订阅模式

`subscriptions` 支持两种匹配方式（见下表）。运行期处理器以受管的 `Subscriptions` 订阅束持有它们
（`self.subscriptions`）：通过 `add` / `remove` / `replace` 修改——总线运行中（激活态）的改动
会自动同步到活跃 `Router`。参见 [路由文档](router.md)。

| 类型 | 示例 | 匹配规则 |
| - | - | - |
| 精确 `str` | `"order.created"` | 仅精确匹配 |
| `Regex` | `Regex(r"order\..*")` | 匹配 `order.created`、`order.paid` 等 |
| `Regex` | `Regex(r"(?!system\.).*")` | 匹配除 `system.*` 外的所有事件 |

```python
from infinity_bus import EventHandler, Regex

class AuditHandler(EventHandler):
    def __init__(self):
        # 全字匹配单个事件
        # 正则匹配所有 order.* 和 payment.* 事件
        super().__init__(subscriptions=["user.login", Regex(r"order\..*"), Regex(r"payment\..*")])

    async def handle(self, payload, bus_proxy, raw_event):
        print(f"Audit: {raw_event.name}")
```

---

## EventHandlerRegistry

处理器注册表，管理事件处理器实例的增删查。**激活路由由 [Router](router.md) 负责**。

```python
class EventHandlerRegistry:
    def __init__(self) -> None
    def register(self, handler: EventHandler) -> str
    def unregister(self, handler_id: str) -> Exception | None
    def activate(self, bus: EventBus) -> None
    def deactivate(self, strict: bool = False) -> list[tuple[str, Exception]]
    def get(self, handler_id: str) -> Optional[EventHandler]
    def clear(self) -> list[tuple[str, Exception]]
    def __len__(self) -> int
    def __contains__(self, handler_id: str) -> bool
    def __iter__(self) -> Iterator[tuple[str, EventHandler]]

    @property
    def version(self) -> int
    @property
    def handlers_count(self) -> int
    @property
    def all_handlers(self) -> Dict[str, EventHandler]
```

| 方法 / 属性 | 说明 |
| - | - |
| `register(handler)` | 注册处理器实例，返回唯一 handler ID（UUID hex）。**原子（含总线侧副作用回滚）**：注册成功后触发 `handler.on_registered(id)` 回填处理器 id；若注册表已绑定总线还触发 `handler.on_activate(bus)`；同一实例禁止重复注册（抛 `ValueError`）。`on_registered` 失败时补发 `on_unregistered()` 清空 id；`on_activate` 失败时先补发 `on_deactivate(bus)` 撤销其已产生的订阅/后台任务（避免幽灵路由），再补发 `on_unregistered()`。失败时版本不变并向上抛出；成功时版本号递增。 |
| `unregister(handler_id)` | 注销处理器。**兜底：保证移除 + 失败显式上报**——处理器必然出库；id 不存在视为幂等空操作，返回 `None`（存在性用 `handler_id in registry` 判断）；已绑定总线时先触发 `handler.on_deactivate(bus)`（id 仍可用）再触发 `handler.on_unregistered()`；钩子失败不阻断移除，但**返回首个失败异常**（其余记录日志）。版本号递增。 |
| `activate(bus)` | 把注册表绑定到一个总线并激活全部在册处理器（逐个触发 `handler.on_activate`，**原子，含总线侧副作用回滚**）。同一注册表**禁止跨总线共用**（已绑定再次调用抛 `RuntimeError`）；先绑定再激活——激活过程中嵌套注册的新处理器会立即收到 `on_activate`；任一 `on_activate` 抛错则**整体回退**：解绑，并对**全部已尝试的处理器（含当前失败者）**补发 `on_deactivate`（撤销其部分订阅/任务），再向上抛出，处理器保持注册。 |
| `deactivate()` | 解除总线绑定并逐个触发 `handler.on_deactivate`（id 仍可用）。**保留失败者**：`on_deactivate` 抛错的处理器保留在册（记录日志）而非静默移除。返回 `[(handler_id, 异常), ...]` 失败清单，调用方（如 `EventBus.stop()`）可感知清理不完整；传 `strict=True` 在全部尝试后抛出首个失败。未绑定时返回 `[]`（幂等）。 |
| `get(handler_id)` | 按 ID 获取处理器实例。 |
| `clear()` | 清除全部处理器。**兜底：保证全部移除 + 失败显式上报**——若已绑定总线先 `deactivate()`，再逐一向在册处理器触发 `on_unregistered()`，单个钩子失败不中断其余清理。返回 `[(handler_id, 异常), ...]` 失败清单（覆盖 deactivate 与 on_unregistered 两阶段）。版本号递增。 |
| `__len__()` | 支持 `len(registry)`。 |
| `__contains__()` | 支持 `handler_id in registry`。 |
| `__iter__()` | 支持 `for hid, h in registry` 迭代。 |
| `version` | 注册表版本号，每次变更（增/删/清空）递增，供 [Router](router.md) 感知变化。 |
| `handlers_count` | 当前注册的处理器总数。 |
| `all_handlers` | 返回所有注册处理器的副本 `Dict[str, EventHandler]`。 |

> **注册表-总线绑定**：`EventBus.start()` 会自动调用 `registry.activate(bus)`，`stop()` 自动调用 `registry.deactivate()`。
> 因此「注册（有 id）」与「在总线上激活（可收事件）」是两层生命周期：同一注册表不可同时被两个总线共用。

### 使用示例

```python
from infinity_bus import EventHandlerRegistry, EventHandler

handler_registry = EventHandlerRegistry()
handler_id = handler_registry.register(my_handler)
assert handler_id in handler_registry
assert handler_registry.version == 1  # 每次注册递增

handler_registry.unregister(handler_id)
assert handler_registry.version == 2  # 注销也递增
```

> **注意**：`get_handlers(event_type)` 已移除。匹配功能移至 [Router](router.md)，总线内部自动使用。
