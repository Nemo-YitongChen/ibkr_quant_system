# Change Archive: 2026-05-01 Evidence Focus Actions

## Context

The simple dashboard could show each market's evidence next step, but operators still had to infer which evidence task should be handled first. The existing `focus_actions` queue only covered portfolio recommendations and governance actions, not evidence work.

## Changes

- Added `evidence_focus_actions` to the dashboard payload.
- Ranked market-level evidence work by action type:
  - gate threshold review
  - signal-to-edge calibration
  - weekly unified evidence rebuild
  - mixed evidence hold/review
  - outcome sample collection
- Skipped passive `monitor` and `keep gate` states so the focus queue only shows actionable evidence work.
- Rendered the queue in the simple dashboard landing section under "今日最该关注的动作 / 研究".
- Added focused helper tests for priority ordering and monitor-only suppression.
- Added smoke coverage to ensure the dashboard payload exposes `evidence_focus_actions`.

## Operational Impact

Evidence review is now visible as a prioritized, read-only work queue. This does not apply parameter changes automatically; it only tells the operator which market-level evidence task should be reviewed first.
