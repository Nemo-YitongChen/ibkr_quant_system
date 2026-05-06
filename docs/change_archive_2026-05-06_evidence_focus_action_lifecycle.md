# 2026-05-06 Evidence Focus Action Lifecycle

## Context

PR #12 started as a P0-P2 implementation plan for evidence focus. Main already had unified evidence and blocked-vs-allowed artifacts, so the real missing P0 layer was a reusable action lifecycle model.

## Changes

- Added `src/common/evidence_focus_actions.py`.
- Added stable evidence action fields:
  - `action_id`
  - `status`
  - `urgency`
  - `linked_evidence_artifact`
  - `linked_evidence_key`
  - `read_only`
- Kept existing dashboard action fields compatible:
  - `market`
  - `action`
  - `primary_action`
  - `basis`
  - `detail`
  - `priority_order`
- Moved dashboard evidence focus action and summary logic behind common lifecycle helpers.
- Updated the PR #12 implementation plan so the current next step is control audit linkage, not unified evidence generation.

## Validation

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_evidence_focus_actions.py tests/test_generate_dashboard_helpers.py tests/test_dashboard_blocks.py
PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/common/evidence_focus_actions.py src/tools/generate_dashboard.py tests/test_evidence_focus_actions.py
```

Result:

- `46 passed`

