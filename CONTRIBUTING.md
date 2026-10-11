# 贡献指南

## 环境搭建

> 本项目唯一合法的 Python 工具链是 [uv](https://docs.astral.sh/uv/)。

```bash
git clone https://github.com/yinbailiang/event_bus.git
cd event_bus
uv sync --extra dev
uv run pre-commit install
```

## 目录约定

```text
src/infinity_bus/    → 源码（pyright strict，零 # type: ignore）
tests/            → 测试（与 src 路径镜像）
docs/             → 文档（中英双语，与 src 模块一一对应）
```

## 开发循环

1. **改代码** — 修改 `src/` 中的实现
2. **写测试** — 在 `tests/` 镜像路径下编写测试
3. **同步文档** — 更新 `docs/` 中对应文档
4. **验证**

   ```bash
   uv run pytest tests/ --cov=infinity_bus --cov-report=term-missing
   uv run pre-commit run --all-files
   ```

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

详见 [ENGINEERING.md#提交规范](ENGINEERING.md#提交规范)。

## 质量门禁

所有提交前自动运行：Ruff → Pyright → interrogate → pytest（覆盖率 ≥ 90%）。

## 发版流程

1. 更新 `pyproject.toml` 和 `src/infinity_bus/__init__.py` 中的版本号
2. 提交：`git commit -m "chore: bump version to X.Y.Z"`
3. 推送：`git push`
4. 打标签：`git tag -a vX.Y.Z` 并 `git push origin vX.Y.Z`
5. CI 自动构建 wheel → GitHub Release → PyPI

详见 [.agents/SKILLS/publish.md](.agents/SKILLS/publish.md)。
