# SKILL: 代码审查前检查

**触发**：提交代码或用户要求 pre-commit 检查。

## 步骤

1. `uv run ruff check .` — lint 检查
2. `uv run ruff format .` — 格式化（仅检查不写入时，为命令追加 `--check`）
3. **零遮蔽检查** — `src/event_bus/` 下不得出现任何 `# type:` 或 `# pyright` 注释
   - Windows: `Select-String -Path src\event_bus\**\*.py -Pattern "# type:|# pyright"` 应无匹配
   - Unix: `grep -r "# type:\|# pyright" src/event_bus/` 应无输出
4. `uv run pyright` — 类型检查（必须零错误）
5. `uv run pytest --cov=event_bus --cov-fail-under=90 -q` — 全量测试
6. `uv run interrogate src/event_bus/` — 公开 API docstring 覆盖率检查
7. `uv run agent-kit update --check` — 规则漂移检查（AGENT.md 与 .agents/ 须与模板一致）

> 如有问题，修复后从第 1 步重新全量运行。
