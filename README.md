# InfinityBus — Async Event Bus

[![Test](https://github.com/yinbailiang/infinity_bus/actions/workflows/test.yml/badge.svg)](https://github.com/yinbailiang/infinity_bus/actions/workflows/test.yml)
[![Coverage](https://img.shields.io/badge/coverage-90%25+-brightgreen)](ENGINEERING.md)
[![docstring](docs/res/interrogate_badge.svg)](ENGINEERING.md)
[![Pyright](https://img.shields.io/badge/pyright-strict-blue)](ENGINEERING.md)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE.md)
[![PyPI Version](https://img.shields.io/pypi/v/infinity_bus)](https://pypi.org/project/infinity_bus/)
[![Supported Python](https://img.shields.io/badge/python-3.12%2B-blue)](https://pypi.org/project/infinity_bus/)
[![Docs](https://img.shields.io/badge/docs-online-blue)](https://yinbailiang.github.io/infinity_bus/)

**Strongly-typed, extensible async event bus — middleware pipeline + advanced templates.**

> 📖 [中文版](README_zh.md) · 🌐 [Documentation site](https://yinbailiang.github.io/infinity_bus/)

## 📑 Table of Contents

- [✨ Features](#-features)
- [🔍 Comparison](#-comparison)
- [📦 Installation](#-installation)
- [� Migration from v4](#-migration-from-v4)
- [�🚀 Quick Start](#-quick-start)
- [🧱 Architecture](#-architecture)
- [📚 Documentation](#-documentation)
- [🧪 Testing](#-testing)
- [🧑‍💻 Development](#-development)
- [📄 License](#-license)

## ✨ Features

| Category | Capability |
| - | - |
| Type Safety | Pydantic payload validation · pyright **strict** · **zero** `# type: ignore` in business code |
| Flexible Subscription | **Regex**-based event name matching · wildcard handlers |
| Middleware Pipeline | Onion model · `before_publish` / `on_publish` dual hooks · 5+ built-in middlewares |
| Advanced Templates | `handler` simplified API · `expect` one-shot listener · `request` RPC call · `pipe` bidirectional channel · `register` batch registration |
| Production-grade architecture | Graceful shutdown · backpressure control · timeout protection · error isolation · observability |
| Engineering Discipline | 90%+ test coverage · 85%+ docstring coverage · pre-commit automated gating |

> Unlike similar projects: InfinityBus is **extensible** (middleware onion pipeline), rather than hardcoding all functionality in the core class.
> See [Middleware System](docs/en-US/middleware.md) for details.

## 🔍 Comparison

| Feature | InfinityBus | [bubus](https://github.com/browser-use/bubus) | [pyee](https://github.com/jfhbrook/pyee) | [PyPubSub](https://github.com/schollii/pypubsub) |
| - | - | - | - | - |
| Async Native | ✅ asyncio | ✅ asyncio / anyio | ✅ asyncio / trio | ❌ sync only |
| Type Safety | ✅ pyright strict · zero `type: ignore` in business code | ⚠️ pyright strict · ~30 suppressions in business code | ✅ mypy + pyright | ❌ no type annotations |
| Payload Validation | ✅ Pydantic auto-validation | ✅ Pydantic auto-validation | ❌ none | ❌ none (arbitrary objects) |
| Subscription Model | Regex | class name + wildcard `*` | exact string match | topic hierarchy (`a.b.c`) |
| Middleware Pipeline | ✅ onion model dual hooks | ❌ | ❌ | ❌ |
| Advanced Templates | ✅ handler/expect/request/pipe/register | ⚠️ `bus.expect()` only | ❌ | ❌ |
| Event Return Values | ⚠️ via `request` template | ✅ built-in `event.event_result()` | ❌ | ❌ |
| Event Forwarding | ⚠️ EventForwardMiddleware | ✅ built-in `event.forward_to()` | ❌ | ❌ |
| Dependency Count | 1 core (pydantic) + 5 optional | 6 (anyio, aiofiles...) | 0 | 0 |
| Graceful Shutdown | ✅ drain queue + await active tasks | ✅ wait_until_idle + queue close | ⚠️ `wait_for_complete` + `cancel` | N/A (sync) |
| Timeout Protection | ✅ per-handler timeout | ✅ event_result.timeout | ❌ | N/A |
| Error Isolation | ✅ TaskErrorEvent unified reporting | ✅ errors logged, bus uninterrupted | ❌ | N/A |
| Event Dispatch FIFO | ✅ asyncio.Queue guarantees enqueue order | ✅ global lock guarantee | ✅ listeners called in registration order | N/A |
| Handler Execution FIFO | ❌ concurrent tasks | ✅ global lock guarantee | ✅ sequential emit | N/A |
| Slow Handler Alert | ❌ | ✅ 15s timeout alert | ❌ | N/A |
| Recursion Guard | ✅ middleware (configurable) | ✅ built-in (non-configurable) | ❌ | N/A |
| Logging & Auditing | ✅ JSONL + SQLite (middleware) | ✅ built-in JSONL WAL logging | ❌ no built-in | ⚠️ debug tracing |
| Test Coverage | ✅ 94% (391 tests) | ✅ 83% (138 tests) | ✅ 94% (43 tests) | ✅ 86% (167 tests) |
| Python Version | 3.12+ | 3.11+ | 3.12+ | 3.7–3.14 |
| Baseline Version | v5.0.0 `19b750b` | v1.5.6 `7c09342` | v13.0.1 `5157de2` | v4.0.7 `4ec2c47` |

> **Choose InfinityBus when**: you need a middleware-native architecture — hot-reloadable onion pipeline with 8 built-in middlewares (logging, metrics, rate-limit, transform, block, recursion guard, forwarding); you run pyright strict across the entire codebase with literally zero `# type: ignore` in any production code; you want full production-grade features (backpressure, graceful shutdown, regex subscriptions, Pydantic payload validation) with a single core dependency.
>
> **Choose bubus when**: you need built-in event return values and event forwarding; you need Python 3.11 support; you prefer the class-driven API style of `bus.on(EventClass, fn)`.
>
> **Choose pyee when**: you're familiar with the Node.js EventEmitter style; you need both asyncio and trio support; you prefer a minimal API (`ee.on('event', fn)`); the type system has been greatly enhanced since v13, passing both mypy and pyright.
>
> **Choose PyPubSub when**: you don't use asyncio; you need very broad Python version compatibility (3.7–3.14); you prefer traditional topic hierarchy string matching.
>
> Full commits:
> · [InfinityBus `1bae1f0`](https://github.com/yinbailiang/infinity_bus/commit/1bae1f0)
> · [bubus `7c09342`](https://github.com/browser-use/bubus/commit/7c09342)
> · [pyee `5157de2`](https://github.com/jfhbrook/pyee/commit/5157de2) (no coverage in official CI; manually measured via `pytest-cov`)
> · [PyPubSub `4ec2c47`](https://github.com/schollii/pypubsub/commit/4ec2c47)

## 📦 Installation

> We recommend [uv](https://docs.astral.sh/uv/) — a blazing-fast Python package manager, 10–100× faster than pip,
> with automatic venv management, dependency locking, and conflict resolution. No need for separate virtual environment tools.

### pip

```bash
# Core (pub/sub and middleware pipeline only)
pip install infinity_bus

# Core + advanced templates (expect, request, pipe, SQLite logging, etc.)
pip install infinity_bus[templates]
```

### uv (recommended)

```bash
# Core
uv add infinity_bus

# Core + advanced templates
uv add infinity_bus --extra templates
```

> **Note**: Advanced templates (`expect`, `request`, `pipe`, `SQLiteLoggingMiddleware`)
> require the `[templates]` extra. Using them without installing the extra will raise an `ImportError`.

Or install from source:

```bash
git clone https://github.com/yinbailiang/infinity_bus.git
cd infinity_bus
uv sync --extra dev
```

## � Migration from v4

Starting with **v5.0.0**, the import package was renamed from `event_bus` to `infinity_bus`
(the PyPI distribution name remains `infinity_bus`). There is no compatibility alias —
update your imports when upgrading:

| v4 | v5 |
| - | - |
| `from event_bus import EventBus` | `from infinity_bus import EventBus` |
| `from event_bus.templates import handler` | `from infinity_bus.templates import handler` |
| System events `event_bus.__shutdown__` / `event_bus.__task_error__` | `infinity_bus.__shutdown__` / `infinity_bus.__task_error__` |
| Metrics `event_bus_publish_total`, `event_bus_queue_size`, ... | `infinity_bus_publish_total`, `infinity_bus_queue_size`, ... |
| RabbitMQ exchange `event_bus.fanout` | `infinity_bus.fanout` |
| Logger names `event_bus.*` | `infinity_bus.*` |

Class/function names and behavior are unchanged.

## �🚀 Quick Start

### Basic Pub/Sub

> Using the `handler` simplified API requires: `pip install infinity_bus[templates]`
> Or with uv: `uv add infinity_bus --extra templates`

```python
import asyncio
from pydantic import BaseModel
from infinity_bus import (
    EventBus, EventDeclaration,
    EventRegistry, EventHandlerRegistry,
)
from infinity_bus.templates import handler

# 1. Define payload
class MyPayload(BaseModel):
    message: str

# 2. Declare event
class MyEvent(EventDeclaration):
    name = "my.event"
    payload_type = MyPayload

# 3. Implement handler (simplified: function + decorator)
@handler(MyEvent)
async def my_handler(payload: MyPayload) -> None:
    print(f"Received: {payload.message}")

# 4. Wire up and run
async def main():
    reg = EventRegistry()
    reg.register(MyEvent)
    h_reg = EventHandlerRegistry()
    await h_reg.register(my_handler())

    async with EventBus(reg, h_reg) as bus:
        await bus.proxy("cli").publish("my.event", {"message": "Hello, EventBus!"})
        await asyncio.sleep(1)  # wait for handler output

asyncio.run(main())
```

### Request-Response Pattern

> Using advanced templates like `request` / `expect` / `pipe` requires: `pip install infinity_bus[templates]`
> Or with uv: `uv add infinity_bus --extra templates`

```python
import asyncio
from pydantic import BaseModel
from infinity_bus import (
    EventBus, EventDeclaration, EventHandler,
    EventRegistry, EventHandlerRegistry,
)
from infinity_bus.templates.request import (
    request, RequestProtocol, ResponseProtocol,
)

# 1. Define request/response payloads
class GetUserRequest(RequestProtocol):
    user_id: int

class GetUserResponse(ResponseProtocol):
    user_name: str
    email: str

# 2. Declare events
class GetUserRequestEvent(EventDeclaration):
    name = "user.get.request"
    payload_type = GetUserRequest

class GetUserResponseEvent(EventDeclaration):
    name = "user.get.response"
    payload_type = GetUserResponse

# 3. Implement server-side handler
class GetUserHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=["user.get.request"])

    async def handle(self, payload, bus_proxy, raw_event):
        if not isinstance(payload, GetUserRequest):
            return
        resp = GetUserResponse(
            session_id=payload.session_id,
            request_id=payload.request_id,
            success=True,
            user_name="Alice",
            email="alice@example.com",
        )
        await bus_proxy.publish("user.get.response", resp)

# 4. Wire up and run
async def main():
    reg = EventRegistry()
    reg.register(GetUserRequestEvent)
    reg.register(GetUserResponseEvent)
    h_reg = EventHandlerRegistry()
    await h_reg.register(GetUserHandler())

    async with EventBus(reg, h_reg) as bus:
        proxy = bus.proxy("cli")
        resp = await request(
            bus_proxy=proxy,
            req_event="user.get.request",
            req_data={"user_id": 123},
            resp_event="user.get.response",
            timeout=10.0,
        )
        resp.raise_if_failed()
        print(f"User: {resp.user_name} ({resp.email})")

asyncio.run(main())
```

## 🧱 Architecture

| Component | Responsibility |
| - | - |
| **Event** | Runtime event instance with name, payload, and handler chain tracking |
| **EventDeclaration** | Event type metadata declaration (name + optional Pydantic payload model) |
| **EventRegistry** | Central management of registered event declarations; validates on publish |
| **EventHandler** | Handler base class; implement `handle` method for business logic; lifecycle hooks (`on_registered` / `on_activate` / `on_deactivate` / `on_unregistered`) |
| **EventHandlerRegistry** | Manages handler lifecycle: `register` / `unregister` / `activate(bus)` / `deactivate`; routing owned by **Router** |
| **Router** | Activated-only route table (`Router` + `Subscriptions`); precomputed dispatch table, version-aware |
| **EventQueue** | Replaceable dispatch queue abstraction: `EventQueue` ABC + `InMemoryEventQueue` default (config owned by the queue) |
| **EventBus** | Event dispatch hub: concurrency control, error reporting, lifecycle (queue / `router` injectable) |
| **Middleware** | Middleware base class, onion pipeline: `before_publish` / `on_publish` dual hooks |
| **MiddlewareChain** | Chain of responsibility manager; wraps the publish flow in order |
| **templates** | Advanced templates: `handler` simplified API, `expect` listener, `request` RPC, `pipe` channel, `register` batch registration |
| **middlewares** | Built-in middlewares: logging (JSONL+SQLite), rate limiting, transform, block, recursion guard |

## 📚 Documentation

> 🌐 **Online documentation**: **https://yinbailiang.github.io/infinity_bus/** — bilingual (中文 / English), full-text search, dark mode.

| Document | Content |
| - | - |
| [Core Overview](docs/en-US/infinity_bus.md) | Event / EventDeclaration / EventHandler / EventBus / Middleware core concepts |
| [Middleware System](docs/en-US/middleware.md) | `Middleware` base class, `MiddlewareChain` onion pipeline |
| [Advanced Templates](docs/en-US/templates/templates.md) | `expect`, `request`, `pipe`, `register` — the four major templates |
| [Built-in Middlewares](docs/en-US/templates/middlewares/middlewares.md) | Logging, rate limiting, transform, block, recursion guard |
| [Engineering Quality](ENGINEERING.md) | Type safety, test coverage, pre-commit gating, modularity standards |

## 🧪 Testing

### pip

```bash
# Clone and install test dependencies
git clone https://github.com/yinbailiang/infinity_bus.git
cd infinity_bus
pip install -e ".[test]"

# Run all tests
pytest --cov=src -v

# Run template tests only
pytest tests/templates/ -v
```

### uv

```bash
# Clone and sync test dependencies
git clone https://github.com/yinbailiang/infinity_bus.git
cd infinity_bus
uv sync --extra test

# Run all tests
uv run pytest --cov=src -v

# Run template tests only
uv run pytest tests/templates/ -v
```

## 🧑‍💻 Development

> **The sole toolchain for developing this project is [uv](https://docs.astral.sh/uv/).**
> The pre-commit gating invokes tests and code quality tools via `uv run` — please install uv first.

```bash
# 1. Clone and sync all dependencies (auto-creates venv)
git clone https://github.com/yinbailiang/infinity_bus.git
cd infinity_bus
uv sync --extra dev

# 2. Install pre-commit hooks
uv run pre-commit install
```

### Development Loop

```bash
# lint + format
uv run ruff check src/ && uv run ruff format src/ --check

# type check
uv run pyright src/

# test + coverage
uv run pytest --cov=src -v

# run all gates manually
uv run pre-commit run --all-files
```

| Tool | Purpose |
| - | - |
| `uv` | Blazing-fast Python package manager (replaces pip/venv) |
| `ruff` | Lint + formatting |
| `pyright` | Strict type checking |
| `pytest` + `pytest-cov` | Testing + coverage |
| `interrogate` | Docstring coverage |
| `pre-commit` | Pre-commit automated gating |

> 📖 See [CONTRIBUTING.md](CONTRIBUTING.md) for the full guide.

## 📄 License

[MIT](LICENSE.md)

## Part of InfinitySystem

![icon](docs/res/infinity_icon/256x256.png)
