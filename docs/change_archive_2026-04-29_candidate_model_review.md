# 2026-04-29 Candidate Model Review Archive

## Scope

This change upgrades candidate-only evidence from raw rows into an explicit weekly model/strategy calibration review.

The intent is to keep optimizing the strategy when paper orders are fully blocked or no fills exist. Candidate outcomes can still answer whether the signal ranking works and whether expected edge is overstated.

## Implementation

- Added `candidate_model_review` rows built from unified evidence.
- Added weekly CSV artifact: `weekly_candidate_model_review.csv`.
- Added `candidate_model_review` to weekly summary JSON and tuning dataset JSON.
- Added a Markdown section named `Candidate Model Review`.
- Added dashboard v2 evidence-quality metrics for candidate model reviews and warnings.
- Rendered candidate model review rows in advanced dashboard evidence view.

## Review Labels

- `INSUFFICIENT_CANDIDATE_OUTCOME_SAMPLE`: fewer than 3 labeled candidate samples.
- `SIGNAL_RANKING_WORKING`: high-score candidates outperform low-score candidates on 20d outcome.
- `SIGNAL_RANKING_INVERTED`: high-score candidates underperform low-score candidates.
- `EXPECTED_EDGE_OVERSTATED`: realized/counterfactual edge materially lags expected post-cost edge.
- `MIXED_SIGNAL`: sample is mature enough but not conclusive.

## Validation

- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/tools/review_weekly_decision_support.py src/tools/review_weekly_feedback_support.py src/tools/review_investment_weekly.py src/tools/review_weekly_output_support.py src/tools/review_weekly_markdown.py src/tools/generate_dashboard.py src/tools/dashboard_blocks.py tests/test_review_investment_weekly.py tests/test_dashboard_blocks.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_review_investment_weekly.py::ReviewInvestmentWeeklyTests::test_candidate_model_review_scores_no_trade_candidate_evidence tests/test_review_investment_weekly.py::ReviewInvestmentWeeklyTests::test_decision_evidence_keeps_candidate_outcomes_without_orders tests/test_dashboard_blocks.py tests/test_dashboard_evidence.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_review_investment_weekly.py tests/test_review_weekly_helpers.py tests/test_dashboard_evidence.py tests/test_dashboard_blocks.py tests/test_generate_dashboard_helpers.py tests/test_investment_workflow_smoke.py`

Result: `89 passed`.
