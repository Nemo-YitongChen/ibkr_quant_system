# 2026-06-16 Opportunity Calibration Trial Plan

## Scope

This change converts read-only opportunity calibration suggestions into a paper-only trial plan. It does not change trading configuration and does not enable automatic application.

## Changes

- `review_opportunity_outcomes` now emits schema `2026Q2.opportunity_outcome_validation.v3`.
- New artifact:
  - `opportunity_outcome_calibration_trial_plan.csv`
- New JSON fields:
  - `calibration_trial_plan_summary`
  - `calibration_trial_plan`
- Dashboard v2 Auto Order block now surfaces trial plan metrics and rows.

## Current Trial Plan

Fresh local artifact result:

- Trial rows: `9`
- Ready for manual review: `8`
- P1 ready for manual review: `7`
- Auto-apply rows: `0`
- HK post-cost threshold paper trials: `2`
- WAIT_PULLBACK near-entry limit trials: `6`
- CN no-action rows: `1`

## Trial Contracts

### HK post-cost threshold paper trial

Field:

```text
auto_order_readiness.max_submit_expected_cost_bps
```

Current value:

```text
35.0 bps
```

Suggested paper-only review value:

```text
55.0 bps
```

Hard requirements:

- Manual review only.
- Market-scoped HK paper trial only.
- Keep all other gates unchanged.
- Require fresh HK BUY plan.
- Require `expected_post_cost_edge_bps >= 0`.
- Require submit quality PASS.
- Require limit order.
- Require Gateway budget OK.
- Require no worse fill/slippage or mature 5/20d outcome degradation.

### WAIT_PULLBACK near-entry limit trial

Field:

```text
opportunity_entry.near_entry_gap_pct
```

Current value:

```text
1.0%
```

Suggested paper-only review values:

- P1 markets: `2.0%`
- ASX P2: `1.5%`
- Max trial value: `3.0%`

Hard requirements:

- Paper only.
- One order per run.
- Max gross order value `100.0`.
- Whole-share feasible.
- Post-cost positive.
- Limit order only.
- Keep risk, edge, liquidity, market-rule, Gateway budget, and submit-quality gates unchanged.

## Interpretation

The system now has a concrete next manual calibration contract instead of a vague recommendation. This makes it possible to review a single field at a time without weakening the rest of the auto-order controls.

This is not a live trading enablement step and not an automatic strategy change.

## Verification

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/test_review_opportunity_outcomes.py \
  tests/test_dashboard_blocks.py \
  tests/test_generate_dashboard_helpers.py
```

Result: `54 passed`.
