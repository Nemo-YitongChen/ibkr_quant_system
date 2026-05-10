# Change Archive: Strategy Parameter Suggestions

Date: 2026-05-09

## Summary

Implemented the first P0 item from `docs/development_report_2026-05-09.md`: weekly review now emits evidence-driven, read-only strategy parameter suggestions.

The new suggestion layer is intentionally governance-first:

- It can suggest one primary strategy field per market / portfolio / week.
- It currently only produces a parameter adjustment for `SIGNAL_RANKING_INVERTED` candidate model review evidence.
- It does not auto-apply changes.
- It includes linked evidence, acceptance rule, rollback note, and effect tracking window.

## Outputs

Weekly review now includes:

- `weekly_strategy_parameter_suggestions.csv`
- `weekly_strategy_parameter_suggestions.json`
- `weekly_review_summary.json -> strategy_parameter_suggestions`
- `weekly_tuning_dataset.json -> strategy_parameter_suggestions`
- `weekly_review.md -> Strategy Parameter Suggestions`

## Technical Details

- Added `src/common/strategy_parameter_suggestions.py`.
- Integrated suggestion rows into `src/tools/review_weekly_output_support.py`.
- Added a markdown section in `src/tools/review_weekly_markdown.py`.
- Kept the suggestion layer read-only with `auto_apply=0` and `read_only=1`.

## Guardrails

- `EXPECTED_EDGE_OVERSTATED` does not mutate signal weights.
- `INSUFFICIENT_CANDIDATE_OUTCOME_SAMPLE` does not produce parameter changes.
- Duplicate candidate review rows for the same market / portfolio / week are capped at one primary suggestion.
- Suggestions read the current strategy defaults and use `strategy_parameter_registry.yaml` for proposed step/bounds.

## Validation

Targeted tests:

- `tests/test_strategy_parameter_suggestions.py`
- `tests/test_review_weekly_output_support.py`
- `tests/test_review_weekly_helpers.py`
- `tests/test_investment_workflow_smoke.py`
- `tests/test_review_investment_weekly.py`
- `tests/test_strategy_parameter_registry.py`
- `tests/test_strategy_config.py`
- `tests/test_signal_fusion.py`
- `tests/test_pure_strategy_signals.py`
