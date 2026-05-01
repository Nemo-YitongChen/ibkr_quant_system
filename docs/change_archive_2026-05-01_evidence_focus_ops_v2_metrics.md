# Change Archive: 2026-05-01 Evidence Focus Ops V2 Metrics

## Context

Urgent evidence focus work was already included in `ops_overview` and the simple ops table. The dashboard v2 `Ops Health` block still omitted those fields from its metrics, so advanced/v2 consumers had to inspect raw ops payload fields.

## Changes

- Added evidence focus metrics to the v2 `Ops Health` block:
  - `evidence_focus_action_count`
  - `evidence_focus_urgent_count`
  - `evidence_focus_primary_market`
  - `evidence_focus_primary_action`
- Updated dashboard block contract tests.

## Operational Impact

The v2 operations health contract now shows urgent evidence work alongside preflight, artifact, governance, stale report, and degraded health metrics. This remains read-only and does not change strategy or execution behavior.
