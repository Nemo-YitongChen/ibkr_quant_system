# 2026-05-08 Blocked-vs-Allowed Action Mapping Guardrail

## 背景

`weekly_blocked_vs_allowed_expost` 已能生成 ex-post review，并且 weekly review 已能从这些 rows 生成 evidence focus actions。下一步需要把 review label 到 action 的映射锁成测试契约，避免后续 gate 校准误把“样本不足”或“gate 正常”升级成 urgent 调参。

## 已完成

- 新增显式 `action_from_blocked_vs_allowed_review_label()` 映射入口。
- 锁定 `BLOCKED_OUTPERFORMED_ALLOWED -> review_gate_thresholds`，urgency 为 `urgent`。
- 锁定 `INSUFFICIENT_SAMPLE / INSUFFICIENT_OUTCOME_SAMPLE -> collect_more_outcome_samples`，urgency 为 `sample_collection`。
- 锁定 `GATE_OK / BLOCKING_HELPED -> keep_gate_monitor_post_cost`，不生成 urgent。
- `weekly_blocked_vs_allowed_expost` artifact 缺失时，`build_evidence_focus_actions_from_expost(None, ...)` 会生成缺 evidence fallback action，不 crash。
- 空 artifact rows 仍保持空 action list，避免把“没有可行动 review”误报成 artifact 缺失。

## 验证

- `tests/test_investment_evidence.py` 覆盖 gate 有效、gate 过紧、outcome 缺失三类 review label。
- `tests/test_evidence_focus_actions.py` 覆盖 label 到 action/urgency 映射、GATE_OK 非 urgent、missing artifact fallback、空 artifact 非 fallback。

## 下一步

进入 dashboard v2 信息架构收敛：把 home / advanced blocks 分层，避免 evidence、market views、waterfall、control history 在首页堆叠。
