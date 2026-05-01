# Change Archive: 2026-05-01 Evidence Focus V2 Block

## Context

`evidence_focus_actions` existed as a read-only dashboard payload field and was rendered in the simple landing view. The dashboard v2 block contract still lacked a dedicated block for this queue, so advanced consumers had to infer evidence priorities from market views or raw payload fields.

## Changes

- Added `Evidence Focus Actions` as a dashboard v2 block.
- Exposed focus queue rows through the v2 block contract.
- Added block metrics for:
  - total focus actions
  - urgent actions
  - gate review actions
  - signal review actions
  - missing evidence actions
  - hold/mixed evidence actions
  - sample collection actions
- Kept sample-collection-only queues as `ok`, matching the existing rule that insufficient samples should not be treated as an immediate parameter-change warning.
- Updated dashboard block and investment workflow smoke tests for the expanded five-block contract.

## Operational Impact

Advanced dashboard consumers can now read a dedicated evidence priority block instead of reconstructing priority from multiple sections. The block remains read-only and does not trigger automatic strategy or execution parameter changes.
