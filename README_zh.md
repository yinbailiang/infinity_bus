# InfinityBus — 异步事件总线

[![Test](https://github.com/yinbailiang/infinity_bus/actions/workflows/test.yml/badge.svg)](https://github.com/yinbailiang/infinity_bus/actions/workflows/test.yml)
[![Coverage](https://img.shields.io/badge/coverage-90%25+-brightgreen)](ENGINEERING.md)
[![docstring](docs/res/interrogate_badge.svg)](ENGINEERING.md)
[![Pyright](https://img.shields.io/badge/pyright-strict-blue)](ENGINEERING.md)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE.md)
[![PyPI Version](https://img.shields.io/pypi/v/infinity_bus)](https://pypi.org/project/infinity_bus/)
[![Supported Python](https://img.shields.io/badge/python-3.12%2B-blue)](https://pypi.org/project/infinity_bus/)
[![Docs](https://img.shields.io/badge/docs-online-blue)](https://yinbailiang.github.io/infinity_bus/)

**强类型、可扩展的异步事件总线——中间件管道 + 高级模板。**

> 📖 [英文版](README.md) · 🌐 [在线文档](https://yinbailiang.github.io/infinity_bus/)

## 📑 目录

- [✨ 特性](#-特性)
- [🔍 同类对比](#-同类对比)
- [📦 安装](#-安装)
- [� 从 v4 迁移](#-从-v4-迁移)
- [�🚀 快速开始](#-快速开始)
- [🧱 架构](#-架构)
- [📚 文档](#-文档)
- [🧪 测试](#-测试)
- [🧑‍💻 开发](#-开发)
- [📄 许可证](#-许可证)

## ✨ 特性

| 类别 | 能力 |
| - | - |
| 类型安全 | Pydantic 负载校验 · pyright **strict** · **业务代码零** `# type: ignore` |
| 灵活订阅 | **正则表达式**匹配事件名 · 通配符处理器 |
| 中间件管道 | 洋葱模型 · `before_publish` / `on_publish` 双钩子 · 5+ 内置中间件 |
| 高级模板 | `handler` 简化 API · `expect` 一次性监听 · `request` RPC 调用 · `pipe` 双向管道 · `register` 批量注册 |
| 生产可靠 | 优雅停机 · 背压控制 · 超时保护 · 错误隔离 · 可观测性 |
| 工程纪律 | 90%+ 测试覆盖 · 85%+ docstring 覆盖 · pre-commit 自动门禁 |

> 和同类项目不同：InfinityBus 是**可扩展的**（中间件洋葱管道），而非把所有功能硬编码在核心类里。
> 详见 [中间件系统](docs/zh-CN/middleware.md)。

## 🔍 同类对比

| 特性 | InfinityBus | [bubus](https://github.com/browser-use/bubus) | [pyee](https://github.com/jfhbrook/pyee) | [PyPubSub](https://github.com/schollii/pypubsub) |
| - | - | - | - | - |
| 异步原生 | ✅ asyncio | ✅ asyncio / anyio | ✅ asyncio / trio | ❌ 纯同步 |
| 类型安全 | ✅ pyright strict · 业务零 `type: ignore` | ⚠️ pyright strict · 业务 ~30 处遮蔽 | ✅ mypy + pyright | ❌ 无类型注解 |
| 负载校验 | ✅ Pydantic 自动校验 | ✅ Pydantic 自动校验 | ❌ 无 | ❌ 无（任意对象） |
| 订阅方式 | 正则表达式 | 类名 + 通配符 `*` | 字符串精确匹配 | 主题层级（`a.b.c`） |
| 中间件管道 | ✅ 洋葱模型双钩子 | ❌ | ❌ | ❌ |
| 高级模板 | ✅ handler/expect/request/pipe/register | ⚠️ 仅 `bus.expect()` | ❌ | ❌ |
| 事件返回值 | ⚠️ 通过 `request` 模板 | ✅ 内置 `event.event_result()` | ❌ | ❌ |
| 事件转发 | ⚠️ EventForwardMiddleware | ✅ 内置 `event.forward_to()` | ❌ | ❌ |
| 依赖数量 | 1 核心（pydantic）+ 5 可选 | 6（anyio, aiofiles...） | 0 | 0 |
| 优雅停机 | ✅ 排空队列 + 等待活跃任务 | ✅ wait_until_idle + 队列关闭 | ⚠️ `wait_for_complete` + `cancel` | N/A（同步） |
| 超时保护 | ✅ 每 handler 独立超时 | ✅ event_result.timeout | ❌ | N/A |
| 错误隔离 | ✅ TaskErrorEvent 统一上报 | ✅ 错误记录不中断总线 | ❌ | N/A |
| 事件分发 FIFO | ✅ asyncio.Queue 保证入队顺序 | ✅ 全局锁保证 | ✅ 按注册顺序调用 | N/A |
| 处理器执行 FIFO | ❌ 并发 task | ✅ 全局锁保证 | ✅ 同步顺序 emit | N/A |
| 慢 handler 告警 | ❌ | ✅ 15s 超时告警 | ❌ | N/A |
| 防递归 | ✅ 中间件（可配置） | ✅ 内置（不可配置） | ❌ | N/A |
| 日志与审计 | ✅ JSONL + SQLite（中间件） | ✅ 内置 JSONL WAL 日志 | ❌ 无内置 | ⚠️ 调试追踪 |
| 测试覆盖 | ✅ 94%（391 tests） | ✅ 83%（138 tests） | ✅ 94%（43 tests） | ✅ 86%（167 tests） |
| Python 版本 | 3.12+ | 3.11+ | 3.12+ | 3.7–3.14 |
| 基准版本 | v5.0.0 `19b750b` | v1.5.6 `7c09342` | v13.0.1 `5157de2` | v4.0.7 `4ec2c47` |

> **选择 InfinityBus 的理由**：你需要中间件原生架构——可热重载的洋葱管道，内置 8 个中间件（日志、指标、限流、转换、屏蔽、防递归、转发）；你要求整个代码库通过 pyright strict 且生产代码**零 `# type: ignore`**；你希望用单一核心依赖获得完整的生产级特性（背压、优雅停机、正则订阅、Pydantic 负载校验）。
>
> **选择 bubus 的理由**：你需要事件自带返回值、事件转发这类开箱即用的功能；你需要支持 Python 3.11；你偏好 `bus.on(EventClass, fn)` 这种类驱动的 API 风格。
>
> **选择 pyee 的理由**：你熟悉 Node.js EventEmitter 风格；你需要同时支持 asyncio 和 trio；你偏好极简 API（`ee.on('event', fn)`）；v13 起类型系统大幅增强，同时通过 mypy 和 pyright 检查。
>
> **选择 PyPubSub 的理由**：你不使用 asyncio；你需要极宽的 Python 版本兼容（3.7–3.14）；你偏好传统的主题层级字符串匹配。
>
> 完整 commit：
> · [InfinityBus `1bae1f0`](https://github.com/yinbailiang/infinity_bus/commit/1bae1f0)
> · [bubus `7c09342`](https://github.com/browser-use/bubus/commit/7c09342)
> · [pyee `5157de2`](https://github.com/jfhbrook/pyee/commit/5157de2)（官方 CI 无覆盖率，此处为手动 `pytest-cov` 测量）
> · [PyPubSub `4ec2c47`](https://github.com/schollii/pypubsub/commit/4ec2c47)

## 📦 安装

> 推荐使用 [uv](https://docs.astral.sh/uv/) —— 极速 Python 包管理器，比 pip 快 10–100 倍，
> 自动管理 venv、锁定依赖、解析冲突。无需单独安装虚拟环境工具。

### pip

```bash
# 核心（仅发布/订阅、中间件管道）
pip install infinity_bus

# 核心 + 高级模板（expect、request、pipe、SQLite 日志等）
pip install infinity_bus[templates]
```

### uv（推荐）

```bash
# 核心
uv add infinity_bus

# 核心 + 高级模板
uv add infinity_bus --extra templates
```

> **提示**：高级模板（`expect`、`request`、`pipe`、`SQLiteLoggingMiddleware`）
> 需要 `[templates]` extra。若未安装而使用这些功能，会收到 `ImportError` 提示。

或从源码安装：

```bash
git clone https://github.com/yinbailiang/infinity_bus.git
cd infinity_bus
uv sync --extra dev
```

## � 从 v4 迁移

自 **v5.0.0** 起，导入包名由 `event_bus` 更名为 `infinity_bus`（PyPI 发行名仍为 `infinity_bus`），**不提供**兼容别名。升级时请更新导入：

| v4 | v5 |
| - | - |
| `from event_bus import EventBus` | `from infinity_bus import EventBus` |
| `from event_bus.templates import handler` | `from infinity_bus.templates import handler` |
| 系统事件 `event_bus.__shutdown__` / `event_bus.__task_error__` | `infinity_bus.__shutdown__` / `infinity_bus.__task_error__` |
| 指标 `event_bus_publish_total`、`event_bus_queue_size` 等 | `infinity_bus_publish_total`、`infinity_bus_queue_size` 等 |
| RabbitMQ exchange `event_bus.fanout` | `infinity_bus.fanout` |
| 日志名 `event_bus.*` | `infinity_bus.*` |

类名 / 函数名与行为完全不变。

## �🚀 快速开始

### 基础发布/订阅

> 使用 `handler` 简化 API 需安装：`pip install infinity_bus[templates]`
> uv 使用 `uv add infinity_bus --extra templates`

```python
import asyncio
from pydantic import BaseModel
from infinity_bus import (
    EventBus, EventDeclaration,
    EventRegistry, EventHandlerRegistry,
)
from infinity_bus.templates import handler

# 1. 定义负载
class MyPayload(BaseModel):
    message: str

# 2. 声明事件
class MyEvent(EventDeclaration):
    name = "my.event"
    payload_type = MyPayload

# 3. 实现处理器（简化版：函数 + 装饰器）
@handler(MyEvent)
async def my_handler(payload: MyPayload) -> None:
    print(f"Received: {payload.message}")

# 4. 组装并运行
async def main():
    reg = EventRegistry()
    reg.register(MyEvent)
    h_reg = EventHandlerRegistry()
    await h_reg.register(my_handler())

    async with EventBus(reg, h_reg) as bus:
        await bus.proxy("cli").publish("my.event", {"message": "Hello, EventBus!"})
        await asyncio.sleep(1)  # 等待处理器输出

asyncio.run(main())
```

### 请求-响应模式

> 使用 `request` / `expect` / `pipe` 等高级模板需安装：`pip install infinity_bus[templates]`
> uv 使用 `uv add infinity_bus --extra templates`

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

# 1. 定义请求/响应负载
class GetUserRequest(RequestProtocol):
    user_id: int

class GetUserResponse(ResponseProtocol):
    user_name: str
    email: str

# 2. 声明事件
class GetUserRequestEvent(EventDeclaration):
    name = "user.get.request"
    payload_type = GetUserRequest

class GetUserResponseEvent(EventDeclaration):
    name = "user.get.response"
    payload_type = GetUserResponse

# 3. 实现服务端处理器
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

# 4. 组装并运行
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

## 🧱 架构

| 组件 | 职责 |
| - | - |
| **Event** | 运行时事件实例，含名称、负载、处理链追踪 |
| **EventDeclaration** | 事件类型元数据声明（名称 + 可选 Pydantic 负载模型） |
| **EventRegistry** | 集中管理已注册的事件声明，发布时校验 |
| **EventHandler** | 处理器基类，实现 `handle` 方法定义业务逻辑；生命周期钩子（`on_registered` / `on_activate` / `on_deactivate` / `on_unregistered`） |
| **EventHandlerRegistry** | 管理处理器生命周期：`register` / `unregister` / `activate(bus)` / `deactivate`；路由由 **Router** 负责 |
| **Router** | 仅含激活订阅的路由表（`Router` + `Subscriptions`）；预计算分派表、版本感知 |
| **EventQueue** | 可替换的派发队列抽象：`EventQueue` ABC + `InMemoryEventQueue` 默认实现（配置归队列自身） |
| **EventBus** | 事件分发中枢：并发控制、错误上报、生命周期（队列 / `router` 可注入） |
| **Middleware** | 中间件基类，洋葱管道：`before_publish` / `on_publish` 双钩子 |
| **MiddlewareChain** | 责任链管理器，按序包裹发布流程 |
| **templates** | 高级模板：`handler` 简化 API、`expect` 监听、`request` RPC、`pipe` 管道、`register` 批量注册 |
| **middlewares** | 内置中间件：日志(JSONL+SQLite)、限流、转换、屏蔽、递归防护 |

## 📚 文档

> 🌐 **在线文档**：**https://yinbailiang.github.io/infinity_bus/** —— 中英双语、全文搜索、暗色模式。

| 文档 | 内容 |
| - | - |
| [核心总览](docs/zh-CN/infinity_bus.md) | Event / EventDeclaration / EventHandler / EventBus / Middleware 核心概念 |
| [中间件系统](docs/zh-CN/middleware.md) | `Middleware` 基类、`MiddlewareChain` 洋葱管道 |
| [高级模板](docs/zh-CN/templates/templates.md) | `expect`、`request`、`pipe`、`register` 四大模板总览 |
| [内置中间件](docs/zh-CN/templates/middlewares/middlewares.md) | 日志、限流、转换、屏蔽、递归防护 |
| [工程质量](ENGINEERING.md) | 类型安全、测试覆盖、pre-commit 门禁、模块化规范 |

## 🧪 测试

### pip

```bash
# 克隆并安装测试依赖
git clone https://github.com/yinbailiang/infinity_bus.git
cd infinity_bus
pip install -e ".[test]"

# 运行全部测试
pytest --cov=src -v

# 仅运行模板测试
pytest tests/templates/ -v
```

### uv

```bash
# 克隆并同步测试依赖
git clone https://github.com/yinbailiang/infinity_bus.git
cd infinity_bus
uv sync --extra test

# 运行全部测试
uv run pytest --cov=src -v

# 仅运行模板测试
uv run pytest tests/templates/ -v
```

## 🧑‍💻 开发

> **开发本项目的唯一工具链是 [uv](https://docs.astral.sh/uv/)。**
> pre-commit 门禁通过 `uv run` 调用测试和代码质量工具，请先安装 uv。

```bash
# 1. 克隆并同步全部依赖（自动创建 venv）
git clone https://github.com/yinbailiang/infinity_bus.git
cd infinity_bus
uv sync --extra dev

# 2. 安装 pre-commit 门禁
uv run pre-commit install
```

### 开发循环

```bash
# lint + 格式化
uv run ruff check src/ && uv run ruff format src/ --check

# 类型检查
uv run pyright src/

# 测试 + 覆盖率
uv run pytest --cov=src -v

# 手动运行全部门禁
uv run pre-commit run --all-files
```

| 工具 | 用途 |
| - | - |
| `uv` | 极速 Python 包管理器（替代 pip/venv） |
| `ruff` | Lint + 格式化 |
| `pyright` | 严格类型检查 |
| `pytest` + `pytest-cov` | 测试 + 覆盖率 |
| `interrogate` | docstring 覆盖率 |
| `pre-commit` | 提交前自动门禁 |

> 📖 完整规范见 [CONTRIBUTING.md](CONTRIBUTING.md)

## 📄 许可证

[MIT](LICENSE.md)

## 此项目属于 InfinitySystem

![icon](docs/res/infinity_icon/256x256.png)
