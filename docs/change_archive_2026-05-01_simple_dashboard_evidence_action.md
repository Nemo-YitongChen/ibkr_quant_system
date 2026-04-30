# Change Archive: 2026-05-01 Simple Dashboard Evidence Action

## Context

`evidence_action_summary` was available in `dashboard.json` and the advanced HTML evidence section. The remaining gap was the simple dashboard view: operators scanning the top card still had to open advanced sections to see whether evidence recommended collecting more outcome samples, reviewing gates, or holding parameters.

## Changes

- Attached `evidence_action_summary` to each dashboard card variant.
- Allowed the simple “下一步” row to use evidence action guidance when there is no higher-priority blocker.
- Added evidence action and evidence sample state to the simple “本周策略解释” table.
- Added focused helper tests and smoke coverage for card-level evidence action propagation.

## Operational Impact

No-trade or low-trade weeks now show the correct next step directly in the simple dashboard:

- collect more candidate/outcome samples when blocked-vs-allowed evidence is sample-starved
- review gate thresholds only when ex-post evidence says blocked rows outperformed allowed rows
- review signal expected edge when candidate model warnings dominate

This makes the simple dashboard consistent with the evidence-first optimization workflow.
