# 内置中间件总览

EventBus 提供了 8 个开箱即用的中间件，覆盖日志、指标、限流、转换、屏蔽、递归防护、跨总线转发等常见横切关注点。
所有中间件通过 `MiddlewareChain` 注册，按注册顺序形成洋葱管道。

---

## 快速索引

| 中间件 | 阶段 | 用途 | 文档 |
| - | - | - | - |
| `RateLimitMiddleware` | `before_publish` | 滑动窗口速率限制 | [rate_limit.md](rate_limit.md) |
| `RecursionGuardMiddleware` | `before_publish` | 双重递归检测 | [recursion_guard.md](recursion_guard.md) |
| `EventBlockMiddleware` | `before_publish` | 按规则屏蔽（丢弃）事件 | [event_block.md](event_block.md) |
| `EventTransformMiddleware` | `before_publish` | 转换事件名 / 注入 / 脱敏字段 | [event_transform.md](event_transform.md) |
| `MetricsMiddleware` | `before_publish` + `on_publish` | Prometheus / OTel 风格指标收集 | [metrics.md](metrics.md) |
| `EventForwardMiddleware` | `on_publish` | 单向跨总线事件转发 | [event_forward.md](event_forward.md) |
| `JSONLLoggingMiddleware` | `on_publish` | JSONL 文件持久化日志 | [logging.md](logging.md) |
| `SQLiteLoggingMiddleware` | `on_publish` | SQLite 数据库持久化日志 | [logging.md](logging.md) |

---

## 执行顺序

```text
发布请求
  │
  ▼
┌─ before_publish 链 ─────────────────────────────────┐
│  RateLimitMiddleware    ← 最先限流，超限直接丢弃      │
│  RecursionGuardMiddleware ← 递归检测，防止死循环      │
│  EventBlockMiddleware   ← 按规则屏蔽                  │
│  EventTransformMiddleware ← 重命名 / 注入 / 脱敏      │
│  MetricsMiddleware       ← 指标收集（计时起点）        │
│  核心发布逻辑             ← 校验 → 构造 Event → 入队  │
└──────────────────────────────────────────────────────┘
  │
  ▼
┌─ on_publish 链 ─────────────────────────────────────┐
│  EventForwardMiddleware  ← 转发到其他总线              │
│  MetricsMiddleware       ← 记录完整发布耗时（计时终点）  │
│  JSONLLoggingMiddleware  ← 写入 JSONL 文件             │
│  SQLiteLoggingMiddleware ← 写入 SQLite 数据库          │
└──────────────────────────────────────────────────────┘
```

> **建议注册顺序**：限流 → 递归防护 → 屏蔽 → 转换 → 指标 → 转发 → 日志。
> 指标中间件横跨 `before_publish`（计时起点）和 `on_publish`（计时终点），
> 放置在转换之后可确保转换后的新事件名被正确记录在指标中。
> `on_publish` 阶段的转发和日志中间件顺序：转发在前可确保日志中间仅记录本总线事件。

---

## 全部导出

```python
from infinity_bus.templates.middlewares import (
    # 日志
    JSONLLoggingMiddleware,
    SQLiteLoggingMiddleware,
    LogFallback,
    # 指标
    MetricsMiddleware,
    MetricsSnapshot,
    # 限流
    RateLimitMiddleware,
    # 转换
    EventTransformMiddleware,
    TransformFunc,
    make_rename_transform,
    make_field_inject_transform,
    make_field_redact_transform,
    # 屏蔽
    EventBlockMiddleware,
    BlockPredicate,
    make_blocklist_predicate,
    make_allowlist_predicate,
    # 递归防护
    RecursionGuardMiddleware,
    RecursionDetectedError,
    # 转发
    EventForwardMiddleware,
    EventFilter,
    TargetBusProvider,
    make_event_name_filter,
    make_static_target_provider,
    # 工具
    serialize_data,
)
```

---

## 自定义中间件

所有中间件继承 `Middleware` 抽象基类。自定义中间件只需实现所需钩子：

```python
from infinity_bus import Middleware

class CustomMiddleware(Middleware):
    async def on_setup(self, bus): ...
    async def on_teardown(self, bus): ...
    async def before_publish(self, event_registry, name, source, data, old_event, next): ...
    async def on_publish(self, event, next): ...
    async def on_publish_error(self, error, name, source, data): ...
```

详见 [中间件文档](../../middleware.md)。
