# Event / EventDeclaration / EventRegistry 文档

## 概述

事件系统是 EventBus 的类型基础。`EventDeclaration` 声明事件元数据，`Event` 是运行时实例，`EventRegistry` 集中管理所有合法事件类型。

---

## Event

运行时事件实例，由总线在发布时自动创建。

```python
class Event(BaseModel):
    name: str                           # 事件类型名
    data: Optional[BaseModel] = None    # 负载数据（Pydantic 模型实例）
    id: str                             # 事件 UUID（自动生成）
    sources: List[str]                  # 处理链来源记录
    timestamps: List[datetime]          # 各环节时间戳
    event_ids: List[str]                # 事件链 UUID
```

| 字段 | 类型 | 说明 |
| - | - | - |
| `name` | `str` | 事件类型名称，对应 `EventDeclaration.name`。 |
| `data` | `Optional[BaseModel]` | 发布时传入的负载数据。无负载事件为 `None`。 |
| `id` | `str` | 事件唯一标识（UUID hex），自动生成。 |
| `sources` | `List[str]` | 事件经过的处理节点名称列表，用于追踪处理链。 |
| `timestamps` | `List[datetime]` | 每个处理节点的时间戳列表，与 `sources` 一一对应。 |
| `event_ids` | `List[str]` | 事件链中所有事件的列表（包括自己），用于追踪因果。 |

---

## EventDeclaration

事件声明的抽象基类。所有自定义事件类型必须继承此类。

```python
class EventDeclaration(ABC):
    name: ClassVar[str]                              # 事件名称（必须为非空字符串）
    payload_type: ClassVar[Optional[Type[BaseModel]]] = None  # 负载模型类（可选）
```

| 类变量 | 类型 | 说明 |
| - | - | - |
| `name` | `ClassVar[str]` | **必需**。事件类型的唯一标识名，不能为空字符串或纯空格。 |
| `payload_type` | `ClassVar[Optional[Type[BaseModel]]]` | 负载数据的 Pydantic 模型类。`None` 表示该事件无负载。 |

子类化时自动校验 `name` 非空，违反则抛出 `TypeError`。

### 使用示例

```python
from pydantic import BaseModel
from infinity_bus import EventDeclaration

class UserLoginPayload(BaseModel):
    user_id: str
    timestamp: datetime

class UserLoginEvent(EventDeclaration):
    name = "user.login"
    payload_type = UserLoginPayload

# 无负载事件
class HeartbeatEvent(EventDeclaration):
    name = "system.heartbeat"
    # payload_type 默认为 None
```

---

## EventRegistry

事件注册表，管理所有合法的事件声明。发布时用于校验事件类型和负载。

```python
class EventRegistry:
    def __init__(self) -> None
    def register(self, event_decl: Type[EventDeclaration]) -> None
    def unregister(self, event_name: str) -> None
    def get(self, name: str) -> Optional[Type[EventDeclaration]]
    def list_names(self) -> List[str]

    @property
    def version(self) -> int
```

| 方法 / 属性 | 说明 |
| - | - |
| `register(event_decl)` | 注册一个事件声明类。若同名已存在则抛出 `ValueError`。 |
| `unregister(event_name)` | 注销指定名称的事件声明（不存在则静默忽略）。 |
| `get(name)` | 按名称查找事件声明，不存在返回 `None`。 |
| `list_names()` | 返回所有已注册事件名称的列表。 |
| `version` | 注册表版本号，每次增删递增，供 [Router](router.md) 感知变化。 |

### 使用示例

```python
registry = EventRegistry()
registry.register(UserLoginEvent)
registry.register(HeartbeatEvent)

assert registry.get("user.login") is UserLoginEvent
assert registry.get("unknown.event") is None
print(registry.list_names())  # ["user.login", "system.heartbeat"]
```
