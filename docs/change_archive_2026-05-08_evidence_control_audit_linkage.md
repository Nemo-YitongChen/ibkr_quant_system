# 2026-05-08 Evidence Control Audit Linkage

## 背景

PR #13 原本只提供 Evidence Focus lifecycle 后续计划。当前最小可执行 P0 是把 dashboard control action audit 和 evidence focus action lifecycle 接起来，避免 evidence action 永远停留在只读建议态。

## 已完成

- 新增 `src/common/dashboard_control_audit.py`，统一 dashboard control action 脱敏、错误分类、evidence action link 抽取、resolution status 规范化和 linked audit 汇总。
- `src/app/dashboard_control_audit.py` 改为兼容 re-export，旧 import path 不破坏。
- `src/app/supervisor.py` 记录 control action 时保留可选 `evidence_action_id / resolution_status / resolution_note`，并写入 action history/audit JSONL。
- `src/common/evidence_focus_actions.py` 新增 `apply_action_resolutions()`，用 dashboard control audit history 回填 evidence focus action status。
- `src/tools/generate_dashboard.py` 生成 dashboard payload 前应用 action resolution，并把 open urgent action count 接入 ops overview。
- `src/tools/dashboard_blocks.py` 在 Dashboard Control Actions block 暴露 linked action metrics 与最近 resolution。
- Advanced HTML 的控制操作审计表展示 linked action、resolution status 和 resolution note。

## 验证重点

- Control action audit 中敏感 token/account/path 继续脱敏。
- 已被 `ACKNOWLEDGED/APPLIED/REJECTED/SUPERSEDED` 的 urgent evidence action 不再计入 open urgent。
- 未匹配或未知 resolution status 不会误关闭 evidence action。

## 下一步

进入 P1：在 weekly review 中汇总 evidence focus effectiveness，按 action status 回看 blocked-vs-allowed、candidate model 与 post-cost edge 的后续效果。
