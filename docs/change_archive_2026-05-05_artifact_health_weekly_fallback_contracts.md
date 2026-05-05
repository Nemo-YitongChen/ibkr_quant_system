# 2026-05-05 Artifact Health Weekly Fallback Contracts

## Context

Dashboard 已经消费更多 weekly evidence 输入，但 artifact health 仍主要覆盖核心 summary / execution / risk / governance 产物。

本次小切片把更多真实 fallback section 纳入同一 contract registry，避免 weekly review 半坏时只能从页面空表推断问题。

## Changes

- Added artifact contracts for:
  - `weekly_trading_quality_evidence.csv`
  - `weekly_candidate_model_review.csv`
  - `weekly_attribution_summary.csv`
- Added fallback mapping from `weekly_review_summary.json` sections:
  - `trading_quality_evidence`
  - `candidate_model_review`
  - `attribution_summary`
- Included these artifacts in dashboard review artifact health rows and consistency drift checks.
- Added tests for contract registration, fallback-section loading, and dashboard payload / HTML visibility.

## Validation

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_artifact_health.py tests/test_dashboard_degraded_inputs.py
PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/common/artifact_contracts.py src/common/artifact_health.py src/tools/generate_dashboard.py tests/test_artifact_health.py
```

Result:

- `15 passed`
