# Change Archive: 2026-05-01 Dashboard Evidence Action Labels

## Context

The dashboard v2 evidence block exposed a machine-oriented `primary_action` such as `collect_more_outcome_samples` or `review_gate_thresholds`. That is useful for automation, but less useful for a human scanning the advanced dashboard.

## Changes

- Added `action_label` and `action_note` to the `Trading Quality Evidence` block metrics.
- Mapped evidence actions to concise operator-facing guidance.
- Updated the block summary to show the readable action label.
- Added tests covering model calibration warnings, gate review warnings, insufficient samples, and advanced HTML rendering.

## Operational Impact

The dashboard now explains what to do next without requiring the operator to interpret internal enum values:

- collect more outcome samples when blocked-vs-allowed evidence is sample-starved
- review gate thresholds when blocked orders outperform allowed orders
- review signal expected edge when candidate model warnings dominate
- keep current gates when blocking helped post-cost outcomes
