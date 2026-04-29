# Change Archive: Weekly Support Import Boundary

Date: 2026-04-29

## Context

`review_weekly_decision_support` could not be imported directly because it depended on small helpers from `review_weekly_feedback_support`, while feedback support re-exported decision support helpers later in the same module. That made isolated helper tests fragile and exposed an unnecessary circular import.

## Changes

- Added `src/tools/review_weekly_common_support.py` for low-level weekly review helper functions.
- Moved decision support imports for JSON parsing, numeric coercion, averaging, and portfolio row mapping to the new common support module.
- Moved governance support `_safe_float` import to the new common support module.
- Added `tests/test_review_weekly_support_imports.py` to keep direct imports of decision and feedback support guarded.

## Validation

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_review_weekly_support_imports.py tests/test_review_investment_weekly.py tests/test_review_weekly_helpers.py tests/test_pure_strategy_no_trade_loop.py
```

Result:

```text
63 passed
```

## Operational Impact

No weekly review behavior changed. This reduces helper-module coupling and makes future splits of `review_investment_weekly.py` safer because domain support modules can now be imported and tested independently.
