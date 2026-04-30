# Change Archive: 2026-04-30 Weekly Evidence JSON Artifacts

## Context

P2 weekly evidence 已经具备 CSV 输出和 summary JSON 嵌入，但后续 dashboard / review 消费仍需要从大型 `weekly_review_summary.json` 中拼取 evidence 行。PR 5/6 的验收标准还要求独立 JSON artifact：

- `weekly_unified_evidence.json`
- `weekly_blocked_vs_allowed_expost.json`

## Changes

- Added `build_weekly_rows_artifact_payload()` as the shared JSON row artifact contract builder.
- Weekly review now writes standalone JSON artifacts for unified evidence and blocked-vs-allowed ex-post review.
- Dashboard evidence loaders now prefer standalone JSON artifacts before falling back to `weekly_review_summary.json` and CSV.
- Added focused tests for row artifact metadata and dashboard loader precedence.
- Extended the investment workflow smoke expectation to include the two JSON artifacts.

## Validation

Targeted validation for this change should cover:

- `tests/test_review_weekly_output_support.py`
- `tests/test_generate_dashboard_helpers.py`
- `tests/test_investment_workflow_smoke.py`

This keeps PR 5/6 evidence output independently consumable and reduces the need for dashboard code to parse the largest weekly summary artifact just to build evidence views.
