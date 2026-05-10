# 2026-05-08 Evidence Focus Effectiveness

## 背景

Evidence focus action lifecycle 和 dashboard control audit linkage 已经合入。下一步需要让 weekly review 回答 action 是否被处理、是否过期、处理耗时是多少，避免 evidence action 只停留在 dashboard 建议层。

## 已完成

- 新增 `build_evidence_focus_effectiveness_summary()`，统计 new actions、urgent actions、resolved actions、stale urgent actions 和平均处理耗时。
- `ACKNOWLEDGED` 不算 resolved，但不计入 stale urgent。
- `collect_more_outcome_samples` 归为 sample collection，不触发 stale urgent。
- weekly review summary JSON 新增 `evidence_focus_actions` 与 `evidence_focus_effectiveness`。
- weekly tuning dataset JSON 同步写入 evidence focus action 与 effectiveness summary。
- weekly review markdown 新增 `## Evidence Focus Effectiveness` section。
- 新增技术路径校准文档：`docs/evidence_focus_lifecycle_technical_path_2026-05-08.md`。

## 验证

- `tests/test_evidence_focus_actions.py` 覆盖 resolved 计数、acknowledged 非 stale、sample collection 非 stale、stale urgent。
- `tests/test_review_weekly_helpers.py` 覆盖 markdown section 渲染。
- `tests/test_review_weekly_output_support.py` 覆盖 weekly summary / tuning dataset / markdown kwargs 的 bundle 集成。

## 下一步

进入 blocked-vs-allowed action mapping 护栏：锁住 `BLOCKED_OUTPERFORMED_ALLOWED / INSUFFICIENT_SAMPLE / GATE_OK` 到 evidence action 的映射，防止后续 dashboard 和 weekly review 对 gate 信号误判。
