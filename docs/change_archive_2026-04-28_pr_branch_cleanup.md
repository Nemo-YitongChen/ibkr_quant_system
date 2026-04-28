# 2026-04-28 PR / Branch Cleanup Archive

本归档记录 2026-04-28 对 PR、远端分支和本地 `main` 的收口结果。

## Final State

- `main` 已推送到 `9fcb63b`.
- GitHub open PR 数为 `0`.
- 远端只保留 `origin/main`.
- 主本地 worktree 已快进到 `origin/main`.
- 当前可见验证结果：`401 passed`.

## Preserved Work

已完整保留到 `main` 的工作：

- PR #10 dashboard evidence / action audit / XETRA external data follow-up.
- PR #11 refined progressive optimization plan.
- PR #2 的有效 backlog 文档：
  - `docs/github-issue-backlog.md`
  - `docs/github-issue-backlog.csv`
- dashboard freshness helper tests.
- smoke tests and smoke CI workflow.
- repository structure validation script and CI workflow.
- fill audit chain tests and CI workflow.
- order audit persistence tests and CI workflow.

## Cleanup Decisions

PR #2 的完整分支没有直接合并，因为它基于旧仓库快照，会删除当前主干大量源码、配置和测试，并重新引入 `.venv` / cache 等运行产物。处理方式是只提取仍有价值的 backlog 文档并单独提交到 `main`.

本地旧分支 `fix/realtime5m-md-kwarg` 没有合入，因为当前主干已经包含其有效修复：

- `TradingEngine` 已支持可选 `md`.
- `orders.py` 已包含更完整的 IBKR error 分类。

该旧分支里的 `src/main.py` 会回退当前 bootstrap 入口，因此按过期分支清理。

## Verification

已执行：

```bash
python3 scripts/validate_repo_structure.py
pytest -q -p no:cacheprovider
```

结果：

- repository structure validation passed
- `401 passed in 17.90s`
