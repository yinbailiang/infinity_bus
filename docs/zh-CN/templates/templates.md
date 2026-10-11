# 高级模板总览

模板层基于 EventBus 核心构建，提供 4 个高级抽象，覆盖常见事件驱动模式。

> 部分模板需要可选依赖。安装方式：
>
> ```bash
> pip install infinity_bus[templates]
> ```

---

## 快速索引

| 模板 | 模式 | 适用场景 | 文档 |
| - | - | - | - |
| `handler` | 函数→处理器 | 快速定义事件处理器、同步/异步函数自动适配、签名校验 | [simple_handler.md](simple_handler.md) |
| `expect` | 一次性监听 | 等待特定事件、测试断言、底层等待逻辑 | [expect.md](expect.md) |
| `request` | 请求-响应 (RPC) | 同步风格的异步调用、服务间通信 | [request.md](request.md) |
| `pipe` | 双向管道 | 流式数据交换、长连接模拟、持久化双向流 | [pipe.md](pipe.md) |
| `register` | 批量注册 + 依赖注入 | 大型项目模块化组织、延迟注册、避免循环导入 | [register.md](register.md) |
| [handlers/](handlers/handlers.md) | 处理器模板集合 | 邮箱模式（串行消费、背压）+ 服务处理器（RPC 服务端） | [handlers 总览](handlers/handlers.md) |
| `idempotency` | 幂等（注入去重） | at-least-once 重复投递去重、跨重启强幂等 | [idempotency.md](idempotency.md) |
| [queues/](queues/queues.md) | 跨进程队列 | 多进程共享总线、fanout 泛洪、补投/不补投、负载重建 | [queues 总览](queues/queues.md) |
| [middlewares/](middlewares/middlewares.md) | 中间件集合 | 日志、限流、转换、屏蔽、递归防护 | [中间件总览](middlewares/middlewares.md) |

---

## 层次关系

```text
                    ┌──────────────┐
                    │   handler    │
                    │ 函数→处理器    │
                    └──────┬───────┘
                           │ 生成 EventHandler 子类
                           ▼
┌─────────────────────────────────────────────────┐
│                    register                     │
│  模块级事件/处理器收集 → 应用启动时批量注册       │
└────────────────────┬────────────────────────────┘
                     │ 注册到
                     ▼
┌─────────────────────────────────────────────────┐
│                   EventBus                      │
│         发布/订阅 · 中间件管道 · 调度循环          │
└──────┬────────────────────────────┬─────────────┘
       │                            │
       ▼                            ▼
┌──────────────┐            ┌──────────────┐
│   request    │            │     pipe     │
│ RPC 调用封装  │◄── 依赖 ──│ 双向管道握手   │
│ (内部用 expect)│           │ (内部用 request│
└──────┬───────┘            │   + expect)   │
       │                    └──────────────┘
       │ 依赖
       ▼
┌──────────────┐
│    expect    │
│ 一次性事件监听 │
└──────────────┘
```

> `handler` 将普通函数转换为 `EventHandler` 子类。`request` 和 `pipe` 内部都依赖 `expect` 实现等待逻辑。`pipe` 同时依赖 `request` 完成握手协议。

---

## 全部导出

```python
from infinity_bus.templates import (
    # handler
    'handler',
    # expect
    'expect', 'OneShotEventHandler', 'temporary_handler',
    # pipe
    'Pipe', 'InProcessPipe', 'InProcessPipeAllocator', 'PipeAllocator',
    'open_pipe', 'expect_pipe', 'get_default_allocator',
    'PipeHandshakeError', 'PipeClosedError', 'PipeTeardownError',
    'PipeLinkedResponse', 'PipeOpenRequest',
    # register
    'ModuleEventRegister', 'ModuleHandlerRegister',
    # request
    'request', 'RequestProtocol', 'ResponseProtocol', 'build_response',
    # mailbox
    'MailboxHandler', 'MailboxConfig', 'StopMailbox',
    # service
    'ServiceHandler', 'process', 'ServiceError',
    'NotFoundError', 'ConflictError', 'InvalidRequestError', 'AccessDeniedError',
    'ProcessorMeta',
    # idempotency（详见 idempotency.md）
    'IdempotencyRecorder', 'IdempotentHandler',
    'InMemoryIdempotencyRecorder', 'SqliteIdempotencyRecorder',
    # queues（详见 queues/queues.md）
    'EventCodec', 'PayloadType', 'RabbitFanoutQueue',
    # middlewares（详见中间件总览）
    'EventBlockMiddleware', 'EventForwardMiddleware', 'EventTransformMiddleware',
    'JSONLLoggingMiddleware', 'SQLiteLoggingMiddleware',
    'MetricsMiddleware', 'MetricsSnapshot',
    'RateLimitMiddleware', 'RecursionGuardMiddleware',
    'RecursionDetectedError',
    'make_rename_transform', 'make_field_inject_transform',
    'make_field_redact_transform', 'make_blocklist_predicate',
    'make_allowlist_predicate', 'make_event_name_filter',
    'make_bidirectional_forward',
    # middlewares 类型别名 & 工具
    'BlockPredicate', 'EventFilter', 'LogFallback',
    'TargetBusProvider', 'TransformFunc', 'serialize_data',
)
```

---

## 选择指南

| 你想做什么 | 用这个 |
| - | - |
| 快速把函数变成事件处理器 | `handler` |
| 发一个请求，等一个响应 | `request` |
| 快速实现 RPC 服务端处理 | `service` |
| 建立长连接，双向收发数据 | `pipe` |
| 等待某个事件发生一次 | `expect` |
| 按模块组织事件和处理器 | `register` |
| 给发布流程加横切逻辑 | [middlewares/](middlewares/middlewares.md) |

---

## handler

```python
from infinity_bus.templates import handler

@handler(UserCreated)
async def send_welcome_email(payload: UserCreatedPayload) -> None:
    print(f"欢迎, {payload.email}!")

handler_registry.register(send_welcome_email())
```

详见 [simple_handler.md](simple_handler.md) 了解签名校验、同步/异步支持和自定义超时。

## expect

```python
from infinity_bus.templates import expect

async with expect(bus_proxy, "user.login") as future:
    await bus_proxy.publish("auth.request", {...})
    event = await future  # 阻塞直到收到 user.login
```

详见 [expect.md](expect.md) 了解过滤、错误处理和高级用法。

## request

```python
from infinity_bus.templates import request

response = await request(
    bus_proxy,
    req_event="order.create",
    req_data={"item": "widget"},
    resp_event="order.created",
    timeout=10.0,
)
```

详见 [request.md](request.md) 了解协议定义和错误处理。

## handlers

`infinity_bus.templates.handlers` 提供两种处理器基类模板：

```python
from infinity_bus.templates import MailboxHandler, ServiceHandler, process

# 服务处理器：RPC 服务端（@process 装饰器）
class OrderService(ServiceHandler):
    @process(OrderCreateReqEvent, OrderCreateRespEvent)
    async def create(self, payload, proxy, raw):
        return {'order_id': 'ord-1'}

# 邮箱模式：独立任务串行消费
class SerialWorker(MailboxHandler):
    async def process(self) -> None:
        while True:
            event, proxy = await self.get()
            ...
```

详见 [handlers 总览](handlers/handlers.md) 了解两种模式的对比与选择。

## pipe

```python
from infinity_bus.templates import open_pipe

async with open_pipe(pipe_id="my_pipe") as pipe:
    await pipe.send(MyData(...))
    reply = await pipe.receive()
```

详见 [pipe.md](pipe.md) 了解分配器和多进程管道。

## register

```python
from infinity_bus.templates import ModuleEventRegister, ModuleHandlerRegister

events = ModuleEventRegister("orders")

@events.event
class OrderCreated(EventDeclaration):
    name = "order.created"
    payload_type = OrderPayload

events.register_all_events(event_registry)
```

详见 [register.md](register.md) 了解事务性注册和错误处理。

## 内置中间件

详见 [middlewares/middlewares.md](middlewares/middlewares.md) 完整文档。

```python
from infinity_bus.templates.middlewares import (
    JSONLLoggingMiddleware,
    RateLimitMiddleware,
    MetricsMiddleware,
)

chain = MiddlewareChain()
await chain.add(RateLimitMiddleware(max_per_second=100))
await chain.add(MetricsMiddleware())
await chain.add(JSONLLoggingMiddleware("events.jsonl"))
```
