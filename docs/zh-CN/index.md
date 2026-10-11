# InfinityBus

**强类型、可扩展的异步事件总线 —— 中间件管道 + 高级模板。**

InfinityBus 是基于 `asyncio` 的事件总线：Pydantic 负载校验、正则订阅匹配、洋葱模型中间件管道，以及 `handler` / `expect` / `request` / `pipe` / `register` / `service` / `mailbox` 等开箱即用的高级模板。

## 安装

```bash
# 核心（发布/订阅 + 中间件管道）
uv add infinity_bus

# 核心 + 高级模板（handler / request / pipe / service / mailbox 等）
uv add infinity_bus --extra templates
```

## 快速入口

- [核心总览](infinity_bus.md) — 架构与组件关系
- [高级模板总览](templates/templates.md) — 高级模板一览
- [README（快速开始）](https://github.com/yinbailiang/event_bus/blob/master/README_zh.md)

## 最简示例

```python
import asyncio
from pydantic import BaseModel
from infinity_bus import EventBus, EventDeclaration, EventHandlerRegistry, EventRegistry
from infinity_bus.templates import handler

class MyPayload(BaseModel):
    message: str

class MyEvent(EventDeclaration):
    name = 'my.event'
    payload_type = MyPayload

@handler(MyEvent)
async def my_handler(payload: MyPayload) -> None:
    print(f'Received: {payload.message}')

async def main() -> None:
    reg = EventRegistry()
    reg.register(MyEvent)
    h_reg = EventHandlerRegistry()
    h_reg.register(my_handler())

    async with EventBus(reg, h_reg) as bus:
        await bus.proxy('cli').publish('my.event', {'message': 'Hello, EventBus!'})
        await asyncio.sleep(0.1)  # 等待处理完成

asyncio.run(main())
```
