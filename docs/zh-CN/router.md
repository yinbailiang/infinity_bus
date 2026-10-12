# Router / Subscriptions 路由文档

## 概述

`Router` 是事件路由的**唯一真相**：它持有**仅含已激活订阅**的路由表（对齐消息中间件
"broker 绑定随连接存续"的语义），`match()` 预计算分派表，把事件类型高效路由到已登记的
处理器。`Subscriptions` 订阅束是处理器持有的订阅托管容器（替代裸露列表），运行期可
`add/remove/replace` 动态修改订阅。

用户通常不直接构造 `Router`——`EventBus` 内部自动创建并通过 `bus.router` 暴露；
`EventHandler.on_activate` 默认把订阅束登记进 `bus.router`（激活即路由）。

---

## 架构定位

```text
EventHandler ──订阅束 Subscriptions──┐
                                     ├──> Router.route_table ──> {事件名: [handler_id, ...]}
        on_activate(bus) 登记 ────────┘            ↑
                                         事件注册表版本 + 路由版本 感知重建
```

- `Router` 取代旧 `Matcher`（被动扫描 handler.subscriptions）与旧
  `EventHandlerRegistry.get_handlers()` 的匹配职责；
- 订阅数据归属从"处理器裸列表 + 扫描"升级为"处理器持有订阅束、激活时登记到 Router"。

---

## Subscriptions（订阅束）

处理器构造时持有的订阅托管容器：

```python
class Subscriptions:
    def __init__(self, patterns: Iterable[str | Regex] | None = None) -> None
    def add(self, pattern: str | Regex) -> None              # 追加（去重）
    def remove(self, pattern: str | Regex) -> bool           # 删除
    def replace(self, patterns: Iterable[str | Regex]) -> None  # 全量替换（状态机换阶段）
    @property
    def patterns(self) -> tuple[str | Regex, ...]           # 只读快照
    @property
    def is_bound(self) -> bool                              # 是否已登记到 Router（激活中）
    def __iter__ / __len__ / __contains__ / __eq__(list|tuple) / __bool__
```

**激活中改动自动同步**：登记（`Router.subscribe`）时订阅束绑定该 Router，
之后任何 `add/remove/replace` 都会递增 Router 版本号 → 分派表惰性重建自动生效；
**未激活时改动**仅更新声明，下次 `subscribe`（激活）整体生效。

---

## Router

```python
class Router:
    def __init__(self, event_registry: EventRegistry) -> None

    def subscribe(self, handler_id: str, subs: Subscriptions) -> None  # 登记（幂等替换）
    def unsubscribe(self, handler_id: str) -> bool                     # 撤销
    def clear(self) -> None                                            # 撤销全部

    def match(self, event_name: str) -> list[str]                      # 命中的 handler_id
    @property
    def routes(self) -> tuple[tuple[str, Subscriptions], ...]          # 激活路由快照
    @property
    def dispatch_table(self) -> dict[str, list[str]]                   # 分派表只读副本
    version: int                                                       # 路由版本号
```

### 分派表

- **精确 `str` 订阅** → 反向索引，O(1) 查表
- **`Regex` 订阅** → 独立扫描列表，仅对正则执行 `fullmatch`
- 每个事件命中按订阅登记顺序排列、按 handler 去重；只存 handler id，
  handler 实例由总线从处理器注册表解析

### 版本感知

事件注册表版本或 Router 自身版本（subscribe/unsubscribe/clear/束变更）任一变化，
`match()`/`dispatch_table` 访问时自动重建，无需手动通知。

```text
match() 调用 → 检查版本 (O(1) int 比较) → stale? → _rebuild()
                                            ↓ fresh
                                         查分派表 (O(1) dict)
```

---

## 生命周期：激活即路由

`EventHandler` 基类把路由登记放进总线生命周期钩子（覆写者须调用 `super()`）：

```python
class EventHandler(ABC):
    async def on_activate(self, bus: EventBus) -> None:
        if self.handler_id is not None and self.subscriptions:
            bus.router.subscribe(self.handler_id, self.subscriptions)  # 激活即路由

    async def on_deactivate(self, bus: EventBus) -> None:
        if self.handler_id is not None:
            bus.router.unsubscribe(self.handler_id)                    # 下线即撤销
```

触发时机：注册表 `activate`（总线启动）对全部在册处理器触发；已绑定总线内新注册的
处理器在 `register` 时触发。**路由表只含已激活的订阅**——总线停止（`deactivate`）或
处理器注销后，Router 不再有其条目（broker 忘掉断开的消费者）。

---

## 动态自订阅

运行期修改自身订阅 = 操作自己的订阅束（激活中自动同步 live 路由）：

```python
class GameStateHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=['game.start'])

    async def handle(self, payload, bus_proxy, raw_event) -> None:
        if raw_event.name == 'game.start':
            # 状态机换阶段：只关心下一阶段事件
            self.subscriptions.replace(['game.tick', 'game.over'])

    # 或运行期增量：
    #   self.subscriptions.add('game.over')
    #   self.subscriptions.remove('game.start')
```

未激活时调用仅更新声明（下次激活生效），不抛错。

---

## 与总线的关系

`EventBus` 构造时内部创建 `Router`（可用 `router=` 注入自定义实例），调度循环从
Router 取命中 id、再向处理器注册表解析实例：

```python
class EventBus:
    def __init__(self, event_registry, handler_registry, router=None, ...):
        self._router = router or Router(event_registry)

    @property
    def router(self) -> Router: ...          # 公开访问（订阅管理/调试）

    async def _dispatch_loop(self):
        ...
        for handler_id in self._router.match(event.name):
            handler = self._handlers.get(handler_id)
            if handler is not None:
                ...
```

用户通常无需直接操作 Router；需要运行期动态订阅时用处理器的订阅束即可。

---

## 为什么需要预计算

不做预计算时，每次事件分发都要遍历全部激活订阅并逐一正则匹配。预计算之后：

- 精确 `str` 订阅 → O(1) 反向索引查表
- `Regex` 订阅 → 仅扫描正则列表
- 版本感知缓存 → 仅在事件注册表或路由变化时重建，而非每次分发都重建
