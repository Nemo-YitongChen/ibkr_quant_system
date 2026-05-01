# Change Archive: 2026-05-01 Evidence Focus Summary

## Context

`evidence_focus_actions` exposed a read-only ranked queue, and dashboard v2 had a dedicated evidence focus block. Consumers still needed to parse rows to answer the simplest operational question: which evidence task is first, and why?

## Changes

- Added `evidence_focus_summary` to the dashboard payload.
- Summarized the ranked evidence queue into:
  - primary market
  - primary action
  - decision basis
  - primary detail
  - focus action count
  - urgent action count
  - gate/signal/missing-evidence/sample counts
  - read-only flag
- Rendered the summary in the simple evidence focus section.
- Added the summary to the Unified Evidence advanced table.
- Added helper tests for prioritized summary generation and empty-input fallback.
- Added smoke coverage to ensure the dashboard payload exposes the summary.

## Operational Impact

Operators and downstream review code can now consume a compact evidence priority summary without re-ranking evidence rows. The summary remains read-only and does not apply strategy or execution parameter changes.
