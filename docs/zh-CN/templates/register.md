# Register 模块事件注册器文档

## 概述

`register` 模块提供了两个注册器类——`ModuleEventRegister` 和 `ModuleHandlerRegister`，用于在模块级别**收集**事件声明与处理器定义，并在应用启动时**统一批量注册**到全局的 `EventRegistry` 和 `EventHandlerRegistry` 中。

与直接调用 `event_registry.register()` 或 `await handler_registry.register()` 不同，模块注册器将"定义"与"注册"分离：在模块加载时仅收集（通过装饰器或手动添加），在应用启动阶段一次性完成注册。这种方式使得大型项目可以按模块组织事件和处理器，避免循环导入，同时保持注册时机的可控性。

---

## 核心类

### `ModuleEventRegister`

模块事件注册器，负责收集 `EventDeclaration` 子类并统一注册。

```python
class ModuleEventRegister:
    def __init__(self, name: str) -> None
    def add_event(self, event_decl: Type[EventDeclaration]) -> None
    def event(self, event_cls: EventDeclT) -> EventDeclT
    def register_all_events(self, event_registry: EventRegistry) -> None
    def get_all_event_names(self) -> List[str]
```

| 成员 | 说明 |
| - | - |
| `__init__(name)` | 构造注册器。`name` 为模块名称，用于标识和调试。 |
| `add_event(event_decl)` | 手动添加一个事件声明类。重复添加同一类自动去重。 |
| `event` | **装饰器**。将事件声明类自动添加到注册器中，并原样返回该类（保持类型不变）。 |
| `register_all_events(event_registry)` | 将所有已收集的事件声明一次性注册到给定的 `EventRegistry` 实例中。 |
| `get_all_event_names()` | 返回所有已收集事件的名称列表。 |

---

### `ModuleHandlerRegister`

模块处理器注册器，负责收集 `EventHandler` 子类及其依赖工厂，并统一实例化后注册。

```python
class ModuleHandlerRegister:
    def __init__(self, name: str) -> None
    def add_handler(
        self,
        handler_type: Type[EventHandler],
        depends: Callable[[], Dict[str, Any]]
    ) -> None
    def handler(
        self,
        depends: Callable[[], Dict[str, Any]] = lambda: {}
    ) -> Callable[[HandlerT], HandlerT]
    async def register_all_handlers(
        self, handler_registry: EventHandlerRegistry, *, atomic: bool = False
    ) -> None
```

| 成员 | 说明 |
| - | - |
| `__init__(name)` | 构造注册器。`name` 为模块名称，用于标识和调试。 |
| `add_handler(handler_type, depends)` | 手动添加一个处理器类及其依赖工厂函数。重复添加自动去重。 |
| `handler(depends)` | **装饰器工厂**。返回一个类装饰器，自动将处理器类添加到注册器中。`depends` 为返回依赖字典的可调用对象，字典键对应处理器 `__init__` 的参数名，默认 `lambda: {}`。 |
| `register_all_handlers(handler_registry, *, atomic=False)` | **async**。将所有已收集的处理器**实例化**（通过 `depends` 工厂注入依赖）并一次性注册。`atomic=True` 时启用事务性注册：任一失败则回滚已注册的处理器并抛出异常；默认 `False` 时失败仅记日志并继续。 |

---

## 工作流程

```Text
模块加载阶段                              应用启动阶段
     |                                         |
     |  module_events = ModuleEventRegister()   |
     |  module_handlers = ModuleHandlerRegister()|
     |                                         |
     |  @module_events.event                   |
     |  class MyEvent(EventDeclaration): ...   |
     |                                         |
     |  @module_handlers.handler(depends=...)  |
     |  class MyHandler(EventHandler): ...     |
     |                                         |
     |  # —— 以上仅收集，不注册 ——               |
     |                                         |
     |                                         |  module_events.register_all_events(event_registry)
     |                                         |  await module_handlers.register_all_handlers(handler_registry)
     |                                         |
     |                                         |  # —— 此时才真正注册到全局 ——
```

---

## 使用示例

### 基础用法：事件注册

```python
from infinity_bus import EventDeclaration, EventRegistry
from infinity_bus.templates.register import ModuleEventRegister

# 创建模块级注册器
module_events = ModuleEventRegister("user_module")

# 方式一：装饰器收集
@module_events.event
class UserCreatedEvent(EventDeclaration):
    name = "user.created"
    payload_type = UserCreatedPayload

@module_events.event
class UserDeletedEvent(EventDeclaration):
    name = "user.deleted"
    payload_type = None

# 方式二：手动添加
module_events.add_event(UserLoginEvent)

# 应用启动时一次性注册
event_registry = EventRegistry()
module_events.register_all_events(event_registry)

print(module_events.get_all_event_names())
# ['user.created', 'user.deleted', 'user.login']
```

### 基础用法：处理器注册

```python
from infinity_bus import EventHandler, EventHandlerRegistry
from infinity_bus.templates.register import ModuleHandlerRegister

module_handlers = ModuleHandlerRegister("user_module")

# 通过装饰器收集处理器，并声明依赖
@module_handlers.handler(depends=lambda: {"db": get_db_connection()})
class UserCreatedHandler(EventHandler):
    def __init__(self, db):
        super().__init__(subscriptions=["user.created"])
        self.db = db

    async def handle(self, payload, bus_proxy, raw_event):
        await self.db.insert_user(payload)

# 无额外依赖的处理器（使用默认 depends）
@module_handlers.handler()
class AuditLogHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=[Regex(r"user\..*")])

    async def handle(self, payload, bus_proxy, raw_event):
        print(f"Audit: {raw_event.name}")

# 应用启动时实例化并注册
handler_registry = EventHandlerRegistry()
await module_handlers.register_all_handlers(handler_registry)
```

### 完整模块组织示例

```python
# user_module/events.py
from infinity_bus import EventDeclaration
from infinity_bus.templates.register import ModuleEventRegister

user_events = ModuleEventRegister("user")

@user_events.event
class UserCreated(EventDeclaration):
    name = "user.created"
    payload_type = UserPayload

@user_events.event
class UserUpdated(EventDeclaration):
    name = "user.updated"
    payload_type = UserPayload


# user_module/handlers.py
from infinity_bus import EventHandler
from infinity_bus.templates.register import ModuleHandlerRegister

user_handlers = ModuleHandlerRegister("user")

@user_handlers.handler(depends=lambda: {"user_service": get_user_service()})
class UserEventHandler(EventHandler):
    def __init__(self, user_service):
        super().__init__(subscriptions=["user.created", "user.updated"])
        self.user_service = user_service

    async def handle(self, payload, bus_proxy, raw_event):
        if raw_event.name == "user.created":
            await self.user_service.on_user_created(payload)
        else:
            await self.user_service.on_user_updated(payload)


# app.py（启动时）
from user_module.events import user_events
from user_module.handlers import user_handlers
from infinity_bus import EventRegistry, EventHandlerRegistry, EventBus

event_registry = EventRegistry()
handler_registry = EventHandlerRegistry()

# 按模块批量注册
user_events.register_all_events(event_registry)
await user_handlers.register_all_handlers(handler_registry)

bus = EventBus(event_registry, handler_registry)
```

---

## 设计意图

| 特性 | 说明 |
| - | - |
| **声明与注册分离** | 模块导入时收集声明，应用启动时统一注册，避免模块导入副作用。 |
| **去重保护** | 重复添加同一事件声明或同一 `(处理器类, 依赖工厂)` 组合自动忽略。 |
| **惰性实例化** | 处理器仅在 `register_all_handlers` 调用时才通过依赖工厂实例化，确保依赖在注册时已可用。 |
| **事务性注册** | `register_all_handlers` 支持 `atomic=True` 模式：任一处理器失败时自动回滚已注册的处理器，保证整批成功或整批失败。 |
| **装饰器友好** | 提供 `@register.event` 和 `@register.handler(depends=...)` 两种装饰器风格，保持代码整洁。 |
| **模块隔离** | 每个模块拥有独立的注册器实例，可按需注册部分或全部事件/处理器。 |

---

## 注意事项

1. **依赖工厂的调用时机**：`depends` 工厂函数在 `register_all_handlers()` 中被调用，而非模块导入时。确保工厂所需的资源（如数据库连接）在注册时已初始化。
2. **处理器参数名必须与依赖字典键一致**：`depends` 返回的字典键名需与处理器 `__init__` 的参数名完全匹配，否则实例化时会抛出 `TypeError`。
3. **注册顺序**：建议先将所有模块的事件注册完毕，再注册处理器。虽然注册表本身不强制顺序，但处理器可能依赖事件声明已就绪。
4. **无需手动调用 `register_all_*` 的模块**：若某模块仅有事件或仅有处理器，可只使用对应的注册器，不必强行创建另一个。
5. **重复添加的去重依据**：`ModuleEventRegister` 通过类对象本身去重；`ModuleHandlerRegister` 通过 `(handler_type, depends)` 元组去重。注意不同 `depends` 工厂（即使行为相同）会被视为不同条目。
6. **事务性注册（`atomic=True`）**：当任一处理器注册失败时，所有已注册的处理器会被自动移除（回滚），原始异常向外抛出。适用于需要"整批成功或整批失败"语义的场景。默认 `atomic=False` 保持宽松行为：失败仅记录日志，其余处理器继续注册。

---

## 完整示例

参见 `tests/templates/register_test.py`，其中包含事件注册、处理器注册、去重、装饰器行为、批量注册等场景的完整测试用例。
