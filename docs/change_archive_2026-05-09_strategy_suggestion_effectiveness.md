# Change Archive: Strategy Suggestion Effectiveness Summary

Date: 2026-05-09

## Summary

Added the effectiveness / resolution summary layer for read-only strategy parameter suggestions.

This completes the next governance step after emitting `weekly_strategy_parameter_suggestions` and linking those suggestions into dashboard control audit history.

## Technical Details

`src/common/strategy_parameter_suggestions.py` now includes:

- `normalize_strategy_parameter_suggestion()`
- `apply_strategy_parameter_suggestion_resolutions()`
- `build_strategy_parameter_suggestion_effectiveness_summary()`

Weekly output now includes:

- `weekly_review_summary.json -> strategy_parameter_suggestion_effectiveness`
- `weekly_tuning_dataset.json -> strategy_parameter_suggestion_effectiveness`
- `weekly_review.md -> Strategy Parameter Suggestion Effectiveness`

## Behavior

The summary tracks:

- total suggestions
- open suggestions
- handled suggestions
- resolved suggestions
- acknowledged / applied / rejected / superseded counts
- stale suggestions
- average resolution hours
- `auto_apply` violations
- read-only count

Dashboard control audit rows can be applied back to suggestions by matching:

`suggestion_id -> linked_strategy_parameter_suggestion_id`

## Guardrails

- Unknown resolution statuses are ignored for suggestion resolution.
- `ACKNOWLEDGED` is handled but not fully resolved.
- `APPLIED / REJECTED / SUPERSEDED` count as resolved.
- `auto_apply != 0` makes the effectiveness summary warn.
- The summary is still read-only and does not write strategy config.

## Validation

Targeted tests:

- `tests/test_strategy_parameter_suggestions.py`
- `tests/test_review_weekly_output_support.py`
- `tests/test_review_weekly_helpers.py`
- `tests/test_investment_workflow_smoke.py`
- `tests/test_dashboard_control_audit.py`
- `tests/test_dashboard_blocks.py`
- `tests/test_generate_dashboard_helpers.py`
- `tests/test_dashboard_rendering.py`
