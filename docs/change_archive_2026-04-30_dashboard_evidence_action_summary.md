# Change Archive: 2026-04-30 Dashboard Evidence Action Summary

## Context

The dashboard v2 evidence block already displayed unified evidence and blocked-vs-allowed rows, but it did not clearly separate these cases:

- blocked orders underperformed allowed orders, so the gate is helping
- blocked orders outperformed allowed orders, so the gate may be too tight
- samples are still insufficient, so tuning would be premature
- candidate model review is warning about signal / expected-edge calibration

That ambiguity matters in no-trade or low-trade weeks because the correct action is often to collect more candidate/outcome evidence instead of immediately changing execution gates.

## Changes

- Added blocked-vs-allowed label distribution rows to the dashboard v2 `Trading Quality Evidence` block.
- Added evidence sample readiness metrics:
  - `blocked_review_count`
  - `sample_ready_review_count`
  - `insufficient_sample_count`
  - `too_restrictive_count`
  - `blocking_helped_count`
  - `mixed_review_count`
- Added `primary_action` to distinguish:
  - `review_gate_thresholds`
  - `review_signal_expected_edge`
  - `collect_more_outcome_samples`
  - `hold_parameters_collect_more_evidence`
  - `keep_gate_monitor_post_cost`
  - `build_weekly_unified_evidence`

## Operational Impact

The dashboard can now explain whether a blocked-vs-allowed review is actionable or still sample-starved. This keeps the optimization loop from overreacting when no paper orders have been placed yet, while still surfacing gate calibration warnings once ex-post evidence becomes strong enough.
