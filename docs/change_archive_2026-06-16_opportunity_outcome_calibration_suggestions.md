# 2026-06-16 Opportunity Outcome Calibration Suggestions

## Scope

This change turns fresh opportunity outcome validation into read-only calibration suggestions for HK post-cost edge and multi-market `WAIT_PULLBACK` anchors.

## Changes

- `review_opportunity_outcomes` now writes `opportunity_outcome_validation` schema v2.
- The artifact now includes:
  - `calibration_suggestion_summary`
  - `calibration_suggestions`
  - `opportunity_outcome_calibration_suggestions.csv`
- Dashboard v2 consumes `opportunity_outcome_validation.json` and shows the suggestion counts inside the existing `auto_order_readiness` block.
- No new dashboard block was added.
- All suggestions are explicitly:
  - `read_only=true`
  - `auto_apply=false`
  - `paper_only=true`

## Current Fresh Evidence

Latest all-market validation from local artifacts:

- Validation rows: `14`
- Matched symbols: `78`
- Mature 5d samples: `10,365`
- Mature 20d samples: `7,572`
- Calibration suggestions: `14`
- P1 suggestions: `7`
- `WAIT_PULLBACK_ANCHOR_REVIEW`: `6`
- `HK_POST_COST_THRESHOLD_REVIEW`: `2`
- `WAIT_PULLBACK_NO_ACTION`: `1` for CN, because there are no close `WAIT_PULLBACK` candidate symbols.

## HK Post-Cost Calibration

Both HK portfolios have fresh P1 read-only suggestions:

- `HK:resolved_hk_top100_bluechip`
- `HK:resolved_hk_top100_tech_growth`

Suggested field:

```text
submit_quality.max_expected_cost_bps
```

Action:

```text
prepare_hk_post_cost_threshold_paper_trial
```

Acceptance rule:

- Do not auto-apply.
- Change only one market-specific field in paper.
- Require fresh HK BUY plan.
- Require `expected_post_cost_edge_bps >= 0`.
- Require submit quality PASS.
- Require no negative fill/slippage or 5/20d outcome degradation.

## WAIT_PULLBACK Calibration

Markets with read-only anchor review suggestions:

- HK
- US
- XETRA
- ASX

Suggested field:

```text
opportunity_entry.near_entry_gap_pct
```

Action:

```text
prepare_wait_pullback_near_entry_paper_limit_trial
```

Interpretation:

- Close `WAIT_PULLBACK` symbols have mature positive 5/20d outcomes.
- This is evidence that the anchor may be conservative for several markets.
- It does not justify lowering risk, edge, cost, liquidity, market-rule, Gateway budget, or submit-quality gates.
- Any trial must be small, paper-only, limit-order based, whole-share feasible, and already post-cost positive.

## Dashboard Evidence

The current dashboard Auto Order block now exposes:

- `opportunity_outcome_validation_count=14`
- `opportunity_outcome_matured_5d_sample_count=10365`
- `opportunity_outcome_matured_20d_sample_count=7572`
- `opportunity_calibration_suggestion_count=14`
- `opportunity_calibration_p1_suggestion_count=7`
- `opportunity_calibration_wait_pullback_anchor_review_count=6`
- `opportunity_calibration_hk_post_cost_review_count=2`

## Verification

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/test_review_opportunity_outcomes.py \
  tests/test_dashboard_blocks.py \
  tests/test_generate_dashboard_helpers.py
```

Result: `54 passed`.
