# EventForwardMiddleware 文档

## 概述

`EventForwardMiddleware` 是单向跨总线事件转发中间件，在 `on_publish` 阶段将源总线
的事件同步转发到另一个 EventBus 实例。适用于系统集成、事件镜像、多租户路由等场景。

---

## 架构

```text
源总线                                    目标总线
  │                                         │
  │  publish("order.created")               │
  ▼                                         │
┌─────────────────────────┐                 │
│  before_publish 链       │                 │
│  ┌─────────────────────┐ │                 │
│  │ 核心发布（校验→入队） │ │                 │
│  └─────────────────────┘ │                 │
│  on_publish 链           │                 │
│  ┌─────────────────────┐ │                 │
│  │ EventForwardMiddleware │─── publish ──► │
│  │ (错误隔离)            │ │                 │
│  └─────────────────────┘ │                 │
└─────────────────────────┘                 ▼
                                       ┌─────────────┐
                                       │ 目标 Handler │
                                       └─────────────┘
```

> **关键设计**：转发在 `on_publish` 阶段执行，此时源事件已完成校验并入队。
> 转发失败**不会**影响源总线的正常运行（错误隔离）。

---

## 快速开始

```python
from infinity_bus import EventBus, EventRegistry, EventHandlerRegistry, MiddlewareChain
from infinity_bus.templates.middlewares import EventForwardMiddleware

# 1. 创建两条总线
source_bus = EventBus(source_registry, source_handlers)
audit_bus = EventBus(audit_registry, audit_handlers)

# 2. 注册转发中间件
fw = EventForwardMiddleware(
    target=audit_bus,
    source_name="main→audit",  # 在目标总线上显示的发源地
)
chain = MiddlewareChain()
chain.add(fw)

# 3. 源总线使用转发中间件
source_bus = EventBus(
    source_registry,
    source_handlers,
    middleware_chain=chain,
)

# 4. 启动两条总线
await audit_bus.start()
await source_bus.start()

# 5. 发布事件 —— 自动转发到 audit_bus
await source_bus.proxy("svc").publish("order.created", {...})
```

---

## API 参考

### `EventForwardMiddleware`

```python
class EventForwardMiddleware(Middleware):
    def __init__(
        self,
        target: Union[EventBus, TargetBusProvider],
        source_name: str = "event_forward",
        event_filter: Optional[EventFilter] = None,
        forward_system_events: bool = False,
    ) -> None
```

| 参数 | 类型 | 默认值 | 说明 |
| - | - | - | - |
| `target` | `EventBus \| TargetBusProvider` | (必需) | 目标总线或返回目标总线的回调。 |
| `source_name` | `str` | `"event_forward"` | 在目标总线上发布时使用的来源标识。 |
| `event_filter` | `Optional[EventFilter]` | `None` | 事件过滤回调，`None` 时转发所有非系统事件。 |
| `forward_system_events` | `bool` | `False` | 是否转发 `infinity_bus.*` 系统事件。 |

## 使用模式

### 静态目标总线

```python
fw = EventForwardMiddleware(target=audit_bus, source_name="prod->audit")
```

### 动态目标总线

每次转发时动态获取最新总线实例

```python
def get_tenant_bus() -> EventBus:
    tenant_id = current_tenant.get()
    return tenant_bus_registry[tenant_id]

fw = EventForwardMiddleware(target=get_tenant_bus, source_name="router")
```

### 事件过滤

```python
# 仅转发订单相关事件
fw = EventForwardMiddleware(
    target=other_bus,
    event_filter=lambda e: e.name.startswith("order."),
)

# 使用工厂函数：白名单模式
from infinity_bus.templates import make_event_name_filter

fw = EventForwardMiddleware(
    target=other_bus,
    event_filter=make_event_name_filter("order.created", "order.paid", mode="white"),
)

# 异步过滤
async def async_filter(event: Event) -> bool:
    user = await get_user_from_event(event)
    return user.tier == "premium"

fw = EventForwardMiddleware(target=other_bus, event_filter=async_filter)
```

---

## 类型别名

### `EventFilter`

```python
EventFilter = Callable[[Event], Union[bool, Awaitable[bool]]]
```

事件过滤回调签名：接收 `Event` 对象，同步或异步返回 `bool`。

### `TargetBusProvider`

```python
TargetBusProvider = Callable[[], Union[EventBus, Awaitable[EventBus]]]
```

目标总线提供者签名：同步或异步返回 `EventBus` 实例。

---

## 预置工厂函数

### `make_event_name_filter`

```python
def make_event_name_filter(
    *event_names: str,
    mode: Literal['white', 'black'] = 'white',
) -> EventFilter:
```

创建一个基于事件名的过滤回调。

| 参数 | 说明 |
| - | - |
| `event_names` | 要匹配的事件名。 |
| `mode` | `"white"` 白名单模式，`"black"` 黑名单模式。 |

### `make_bidirectional_forward`

```python
def make_bidirectional_forward(
    bus_a: EventBus | TargetBusProvider,
    bus_b: EventBus | TargetBusProvider,
    *,
    source_a_to_b: str = 'a→b',
    source_b_to_a: str = 'b→a',
    event_filter: EventFilter | None = None,
    anti_recursion: bool = True,
    forward_system_events: bool = False,
) -> tuple[EventForwardMiddleware, EventForwardMiddleware]:
```

一键创建一对单向转发中间件，实现两条总线之间的双向事件同步。

| 参数 | 类型 | 默认值 | 说明 |
| - | - | - | - |
| `bus_a` | `EventBus \| TargetBusProvider` | (必需) | 总线 A 或其工厂回调。 |
| `bus_b` | `EventBus \| TargetBusProvider` | (必需) | 总线 B 或其工厂回调。 |
| `source_a_to_b` | `str` | `"a→b"` | A→B 方向在目标总线上使用的来源标识。 |
| `source_b_to_a` | `str` | `"b→a"` | B→A 方向在目标总线上使用的来源标识。 |
| `event_filter` | `EventFilter \| None` | `None` | 共享的事件过滤回调。 |
| `anti_recursion` | `bool` | `True` | 启用反递归过滤，防止 A→B→A 无限循环。 |
| `forward_system_events` | `bool` | `False` | 是否转发系统事件。 |

返回值 ``(a_to_b, b_to_a)``：

- ``a_to_b`` 挂载到总线 A，将 A 的事件转发到 B
- ``b_to_a`` 挂载到总线 B，将 B 的事件转发到 A

**反递归机制**：当 ``anti_recursion=True``（默认），每个方向的中间件会自动跳过
由对向转发过来的事件。具体而言：

- ``a_to_b`` 跳过 ``sources`` 中包含 ``"b→a"`` 的事件
- ``b_to_a`` 跳过 ``sources`` 中包含 ``"a→b"`` 的事件

```python
from infinity_bus import EventBus, MiddlewareChain
from infinity_bus.templates import make_bidirectional_forward

# 一键创建双向转发对
a_to_b, b_to_a = make_bidirectional_forward(
    bus_a,
    bus_b,
    source_a_to_b='main→audit',
    source_b_to_a='audit→main',
)

# 分别挂载到各自总线
bus_a.proxy('forward').middleware.add(a_to_b)
bus_b.proxy('forward').middleware.add(b_to_a)
```

> **注意**：``anti_recursion`` 防止的是 A→B→A 的直接回环。若业务处理器在收到转发
> 事件后**主动发布新事件**，该新事件仍会被正常转发（这是预期行为）。若需更严格的
> 递归控制，请配合使用 ``RecursionGuardMiddleware``。

---

## 注意事项

1. **目标总线生命周期**：`EventForwardMiddleware` 不负责目标总线的启动/停止。
   请在注册转发中间件之前确保目标总线已启动。
2. **系统事件**：默认跳过 `infinity_bus.*` 事件。如需转发（例如调试），设置
   `forward_system_events=True`。
3. **负载校验**：确保目标总线注册了与源事件**同名的 EventDeclaration**，且
   `payload_type` 兼容，否则转发将在目标总线抛出校验错误。
4. **错误隔离**：转发异常（目标不可达、校验失败等）会被捕获并记录，不会导致源总线
   的 `on_publish` 链中断。
5. **不保证顺序**：转发是异步非阻塞的，不保证事件在目标总线的处理顺序与源总线一致。
6. **避免递归转发**：若目标总线也配置了指向源总线的转发中间件，需在过滤回调中排除
   来自转发源的事件，否则会形成无限循环。
