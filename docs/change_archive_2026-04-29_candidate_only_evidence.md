# 2026-04-29 Candidate-Only Evidence Archive

## Scope

This change strengthens the no-trade optimization loop. Weekly review can now keep model and strategy evidence even when no paper order is submitted or filled.

The goal is to avoid an empty evidence table in fully blocked weeks. Candidate snapshots and 5/20/60d outcomes now remain visible as partial evidence, so strategy scoring and gate calibration can continue while execution stays conservative.

## Implementation

- Extended weekly decision evidence rows with candidate-only fallback rows from `investment_candidate_snapshots`.
- Joined candidate-only rows to `investment_candidate_outcomes` when 5/20/60d labels exist.
- Added `decision_source`, `candidate_only_flag`, and `join_quality` fields to unified evidence rows.
- Preserved existing order/fill evidence behavior and avoided duplicating snapshots already linked to execution parent rows.
- Added dashboard evidence overview counters for:
  - candidate-only rows
  - outcome-labeled rows
  - partial-join rows
- Added dashboard v2 evidence-quality metrics for the same counters.

## Behavior

Candidate-only evidence rows use:

- `decision_source = candidate_snapshot`
- `decision_status = CANDIDATE_SELECTED` for final/short snapshots
- `decision_status = CANDIDATE_RESEARCH` for broad/deep snapshots
- `join_quality = candidate_outcome_only` when outcome labels are available
- `join_quality = candidate_only_pending_outcome` while labels are still maturing

For candidate-only rows, `realized_edge_bps` is a counterfactual post-cost edge based on 20d outcome minus expected cost. It is not a fill-realized execution value; the `join_quality` field makes this distinction explicit.

## Validation

- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/tools/review_weekly_decision_support.py src/tools/review_investment_weekly.py src/common/dashboard_evidence.py src/tools/dashboard_blocks.py tests/test_review_investment_weekly.py tests/test_dashboard_evidence.py tests/test_dashboard_blocks.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_review_investment_weekly.py::ReviewInvestmentWeeklyTests::test_decision_evidence_keeps_candidate_outcomes_without_orders tests/test_review_investment_weekly.py::ReviewInvestmentWeeklyTests::test_unified_evidence_and_blocked_expost_review tests/test_dashboard_evidence.py tests/test_dashboard_blocks.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_review_investment_weekly.py tests/test_review_weekly_helpers.py tests/test_dashboard_evidence.py tests/test_dashboard_blocks.py tests/test_generate_dashboard_helpers.py`

Result: `87 passed`.
