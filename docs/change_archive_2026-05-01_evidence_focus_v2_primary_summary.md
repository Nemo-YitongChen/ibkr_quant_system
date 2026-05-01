# Change Archive: 2026-05-01 Evidence Focus V2 Primary Summary

## Context

`evidence_focus_summary` existed as a top-level dashboard payload field. The dashboard v2 `Evidence Focus Actions` block still exposed only counts and action rows, so advanced/v2 consumers had to read another payload field to identify the primary market and action.

## Changes

- Aligned the `Evidence Focus Actions` v2 block with `evidence_focus_summary`.
- Added primary summary fields to block metrics:
  - `primary_market`
  - `primary_action`
  - `primary_action_label`
  - `primary_basis`
  - `read_only`
- Used `evidence_focus_summary.summary_text` as the v2 block summary when available.
- Split block rows into structured `summary` and `actions` sections.
- Kept backward compatibility by deriving primary summary fields from the first action row when top-level summary is missing.
- Updated dashboard block tests for summary-backed and fallback behavior.

## Operational Impact

Advanced dashboard consumers can now read the primary evidence work item directly from the v2 block contract. This remains read-only and does not change strategy parameters, execution gates, or queue ranking.
