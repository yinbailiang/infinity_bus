# 工程质量

InfinityBus 是一个工程纪律驱动的项目。以下是所有质量保障措施的完整说明。

---

## 类型安全

### Pyright Strict（生产代码） + Basic（测试）

生产代码 `src/` 启用 [Pyright](https://github.com/microsoft/pyright) **strict** 模式——Python 生态中最严格的类型检查级别；
`tests/` 一并纳入类型检查但运行 **basic** 模式（通过 `strict = ["src"]` 仅对生产代码开严格）。

```toml
# pyproject.toml
[tool.pyright]
typeCheckingMode = "basic"
strict = ["src"]
reportMissingTypeStubs = false
reportMissingImports = "error"
include = ["src", "tests"]
```

**生产代码零遮蔽。** 整个 `src/` 目录没有任何 `# type: ignore` 或 `# pyright: ignore` 注释。

| 指标 | 数值 |
| - | - |
| 类型检查模式 | `src` = `strict` · `tests` = `basic` |
| 生产代码 `# type: ignore` | **0** |

### Ruff Lint

仅忽略两条规则，均有明确理由：

```toml
[tool.ruff.lint]
ignore = [
    "E501",     # 行长度由 formatter 处理
    "ASYNC109", # async 函数的 timeout 参数是有意的 API 设计
]
```

启用规则组：`E`（pycodestyle）、`F`（pyflakes）、`I`（isort）、`ASYNC`（异步最佳实践）、`PLE`（pylint 错误）。

---

## 文档覆盖

使用 [interrogate](https://github.com/econchick/interrogate) 强制公开 API 的 docstring 覆盖。

```toml
[tool.interrogate]
fail-under = 60
ignore-init-method = true
ignore-init-module = true
ignore-magic = true
ignore-private = true
ignore-semiprivate = true
ignore-nested-functions = true
```

| 指标 | 数值 |
| - | - |
| 公开 API docstring 覆盖 | **91.6%** |
| 最低阈值 | 60% |
| 文档文件 | 58 篇 `.md` |

---

## 测试

### 覆盖率

```text
391 passed · 5 deselected(slow) · 0 failed · 9.2s
总覆盖率: 94%（2157 statements / 128 missed）
```

14 个模块达到 **100%** 覆盖，最低模块 > 82%。

### 架构

```text
tests/
├── conftest.py             共享 fixtures、测试用 Payload/Handler、工具函数
├── bus_test.py             EventBus 集成测试
├── event_test.py           Event / Declaration / Registry
├── handler_test.py         EventHandler / Registry
├── router_test.py          Router / Subscriptions
├── middleware_test.py      Middleware / MiddlewareChain
├── queue_test.py           EventQueue / InMemoryEventQueue
└── templates/
    ├── expect_test.py
    ├── idempotency_test.py
    ├── pipe_test.py
    ├── register_test.py
    ├── request_test.py
    ├── simple_handler_test.py
    ├── handlers/
    │   ├── mailbox_test.py
    │   └── service_test.py
    ├── middlewares/
    │   ├── event_block_test.py
    │   ├── event_forward_test.py
    │   ├── event_transform_test.py
    │   ├── logging_test.py
    │   ├── metrics_test.py
    │   ├── rate_limit_test.py
    │   └── recursion_guard_test.py
    └── queues/
        ├── codec_test.py
        └── rabbit/
            ├── rabbitmq_mock_test.py
            └── rabbitmq_queue_test.py   慢测（跨进程集成，CI 以 `-m ""` 全量覆盖）
```

测试结构完全镜像源码结构。`pytest-asyncio` + `asyncio_mode = "auto"`，异步测试零样板代码。

### 运行

```bash
pytest --cov=src -v                     # 全量
pytest -m "not slow"                    # 排除压力测试（默认）
pytest tests/bus_test.py -v             # 单模块
pytest tests/templates/ -v              # 模板层
```

---

## Pre-commit 门禁

> **需要 [uv](https://docs.astral.sh/uv/)**。钩子通过 `uv run` 调用工具链，
> 确保在所有开发环境中使用一致的 venv 和依赖版本。

每次提交自动执行，全部通过才允许 commit：

| 钩子 | 范围 | 作用 |
| - | - | - |
| `ruff-check` | `^src/` | Lint + 自动修复 |
| `ruff-format` | `^src/` | 格式化 |
| `pyright` | `^src/` | `--level error` 类型检查 |
| `interrogate` | `^src/` | docstring 覆盖 >= 60% |
| `pytest-cov` | 全量 | 测试 + 覆盖率 >= 90% |
| `check-ast` | 全项目 | Python 语法校验 |
| `check-toml/yaml/json` | 全项目 | 配置文件校验 |
| `check-merge-conflict` | 全项目 | 合并冲突残留 |
| `end-of-file-fixer` | 非 `.py` | 文件末尾换行 |
| `trailing-whitespace` | 非 `.py` | 行尾空白（`.py` 由 ruff 处理） |

```bash
uv run pre-commit run --all-files   # 手动全量运行
```

---

## 模块化

### KISS 原则

| 指标 | 数值 |
| - | - |
| 最大单文件 | ~446 行 (`bus.py`) |
| 模块平均 | ~181 行/文件 |
| 生产文件数 | 27 个 `.py` |

每个文件一个职责，打开瞬间看完。新增功能不改旧代码。

### 目录结构

```text
src/infinity_bus/
├── __init__.py           公开 API 入口
├── event.py              事件系统（Event / Declaration / Registry）
├── handler.py            处理器系统（EventHandler / Registry / 生命周期钩子）
├── router.py             路由器（Router / Subscriptions 订阅束）
├── queue.py              队列抽象（EventQueue / InMemoryEventQueue / Config）
├── bus.py                事件总线（EventBus / Proxy / 停机）
├── middleware.py         中间件系统（Middleware / Chain / 洋葱模型）
└── templates/
    ├── __init__.py       模板统一出口
    ├── expect.py         一次性事件监听
    ├── idempotency.py    幂等消费（IdempotencyRecorder / IdempotentHandler）
    ├── pipe.py           双向管道
    ├── register.py       模块级批量注册
    ├── request.py        请求-响应 RPC
    ├── simple_handler.py 处理器快速定义装饰模板
    ├── handlers/
    │   ├── mailbox.py    邮箱模板
    │   └── service.py    服务处理器模板
    ├── middlewares/
    │   ├── __init__.py
    │   ├── event_block.py       事件屏蔽
    │   ├── event_forward.py     事件转发
    │   ├── event_transform.py   事件转换
    │   ├── logging.py           JSONL + SQLite 日志
    │   ├── metrics.py           指标上报
    │   ├── rate_limit.py        速率限制
    │   └── recursion_guard.py   递归防护
    └── queues/
        ├── __init__.py
        ├── codec.py             跨进程编解码（EventCodec）
        └── rabbit/
            ├── __init__.py
            └── queue.py         RabbitMQ fanout 事件队列
```

---

## 版本策略

- **SemVer**：`MAJOR.MINOR.PATCH`
- Git tag 触发 PyPI 发布

---

## 提交规范

遵循 [Conventional Commits](https://www.conventionalcommits.org/zh-hans/)：

```text
<type>(<scope>): <subject>
```

| type | 场景 |
| --- | --- |
| `feat` | 新功能 |
| `fix` | Bug 修复 |
| `docs` | 文档变更 |
| `test` | 测试补全 |
| `refactor` | 重构（不改行为） |
| `chore` | 版本号、依赖、构建工具 |

`scope` 对应模块名：`bus`、`event`、`handler`、`middleware`、`templates`、`pipe` 等。

示例：

```text
feat(templates): add handler decorator for simplified event handling
fix(bus): resolve race condition in graceful shutdown
docs(handler): add Chinese and English documentation
test(templates): add 22 test cases for handler decorator
chore: bump version to 1.5.2
```

---

## CI

测试矩阵覆盖 Python 3.12+，每次 push 自动运行 `pytest` + `pre-commit`。

```yaml
# .github/workflows/test.yml
pytest --cov=src --cov-report=xml
```
