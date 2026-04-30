# Change Archive: 2026-05-01 Dashboard Evidence Action Top Level

## Context

The evidence next action was available inside the dashboard v2 `Trading Quality Evidence` block, but consumers still had to parse `dashboard_v2_blocks` to find it. The advanced HTML also showed the action only as one metric inside the block.

## Changes

- Added top-level `evidence_action_summary` to `dashboard.json`.
- Extracted status, action label/note, evidence row counts, blocked review counts, and sample readiness counts from the evidence block.
- Rendered the current evidence action at the top of the `Unified Evidence / Blocked vs Allowed` HTML section.
- Added tests for extraction behavior and smoke coverage for the new top-level payload field.

## Operational Impact

Dashboard consumers can now read one stable field to decide whether the next step is:

- collect more outcome samples
- review gate thresholds
- review signal expected edge
- hold parameters and collect more evidence
- keep current gates and monitor post-cost outcomes
