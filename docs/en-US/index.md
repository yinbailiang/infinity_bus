# InfinityBus

**Strongly-typed, extensible async event bus — middleware pipeline + advanced templates.**

InfinityBus is an `asyncio`-based event bus: Pydantic payload validation, regex-based
subscription matching, an onion-model middleware pipeline, and batteries-included
templates such as `handler` / `expect` / `request` / `pipe` / `register` / `service` / `mailbox`.

## Installation

```bash
# Core (pub/sub + middleware pipeline only)
uv add infinity_bus

# Core + advanced templates (handler / request / pipe / service / mailbox, etc.)
uv add infinity_bus --extra templates
```

## Quick Links

- [Core Overview](infinity_bus.md) — architecture and component map
- [Templates Overview](templates/templates.md) — the advanced templates
- [README (Quick Start)](https://github.com/yinbailiang/event_bus/blob/master/README.md)

## Minimal Example

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
        await asyncio.sleep(0.1)  # wait for the handler

asyncio.run(main())
```
