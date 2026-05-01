# Change Archive: 2026-05-01 Evidence Focus Ops Health

## Context

Evidence focus work was visible in simple dashboard sections and the v2 block contract, but urgent evidence work did not influence the top operations overview. Operators could miss a gate or signal-calibration review unless they inspected the evidence sections.

## Changes

- Added optional `evidence_focus_summary` input to the dashboard ops overview builder.
- Added ops overview fields for:
  - evidence focus action count
  - urgent evidence focus count
  - primary market
  - primary action
  - summary text
- Added an `EVIDENCE` WARN alert only when `urgent_action_count > 0`.
- Kept sample-collection-only focus work non-alerting.
- Added `Evidence复核` to the simple ops overview table.
- Added regression tests for urgent evidence alerts and sample-only non-alerting behavior.

## Operational Impact

Urgent evidence work is now visible in the top health summary without changing strategy or execution parameters. Sample-starved evidence remains a data-collection state rather than an operational warning.
