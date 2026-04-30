# Change Archive: Investment Evidence Contract

Date: 2026-04-30

## Context

The weekly review pipeline already generated `weekly_unified_evidence.csv` and `weekly_blocked_vs_allowed_expost.csv`, but the schema and aggregation contract lived inside `src/tools/review_weekly_decision_support.py`.

That made the evidence layer harder to reuse from dashboard, tests, and future calibration tools.

## Changes

- Added `src/common/investment_evidence.py`.
- Added `EVIDENCE_COLUMNS` and `normalize_evidence_row()`.
- Added common `build_unified_evidence_rows()` for candidate / decision / order / fill / outcome evidence.
- Added common `build_blocked_vs_allowed_expost_review()` for market / portfolio / block-reason ex-post aggregation.
- Kept the existing private weekly review function names as compatibility wrappers.
- Added `tests/test_investment_evidence.py` contract coverage.

## Validation

```bash
PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/common/investment_evidence.py src/tools/review_weekly_decision_support.py tests/test_investment_evidence.py tests/test_review_investment_weekly.py
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_investment_evidence.py tests/test_review_investment_weekly.py::ReviewInvestmentWeeklyTests::test_unified_evidence_and_blocked_expost_review tests/test_review_investment_weekly.py::ReviewInvestmentWeeklyTests::test_decision_evidence_keeps_candidate_outcomes_without_orders tests/test_pure_strategy_no_trade_loop.py
```

Result:

- `7 passed`

## Operator Notes

This does not add new trading gates. It only stabilizes the evidence contract used to answer:

- whether blocked orders were worse than allowed orders after 5/20/60 days
- whether candidate-only outcomes can still calibrate the model when no orders are submitted
