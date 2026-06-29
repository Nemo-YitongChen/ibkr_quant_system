# 2026-06-29 Auto-order Supervisor Revision Gate

This change makes automated submit readiness aware of the running Supervisor
code revision. It prevents a stale long-running Supervisor process from
submitting orders after the repository has been updated.

## Problem

The active runtime currently has `python -m src.app.supervisor` PID `77976`
holding `supervisor.lock`. The process started before later auto-order,
shutdown, and dashboard diagnostics were added, so
`supervisor_shutdown_status.json` reports `status=running` but does not include
`code_revision`.

Without a submit gate, another blocker could clear while the old process is
still running, leaving automated submit controlled by stale in-memory code.

## Change

- `review_auto_order_readiness` now loads
  `reports_supervisor/supervisor_shutdown_status.json`.
- The readiness evaluator compares running Supervisor `code_revision` against
  the current repository `HEAD`.
- Supervisor writes `code_revision` as the startup/runtime revision captured
  when the process starts, not as a dynamic `git rev-parse HEAD` lookup. It also
  writes `current_code_revision` for audit. This prevents an old in-memory
  Supervisor from looking fresh after the repository advances.
- If a running/running-degraded Supervisor has no revision or a different
  revision, all submit-enabled portfolios receive a hard block:
  - `supervisor_code_revision_missing`
  - `supervisor_code_revision_mismatch`
- The default policy is explicit:

```yaml
auto_order_readiness:
  block_on_supervisor_code_revision_mismatch: true
```

- Supervisor's internal submit gate passes the same status and current revision
  into the common readiness evaluator.
- Dashboard Auto Order block now exposes:
  - `supervisor_revision_block_count`
  - `supervisor_code_revision_missing_count`
  - `supervisor_code_revision_mismatch_count`

## Runtime verification

Command:

```bash
PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_auto_order_readiness --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001 --out_dir /private/tmp/ibkr_auto_order_revision_gate
```

Observed result:

- `primary_block_reason=supervisor_code_revision_missing`
- `hard_block_counts.supervisor_code_revision_missing=6`
- Detail: `status=running pid=77976 code_revision is missing`

The same payload still reports the other active blockers:

- `weekly_review_stale=6`
- `market_readiness_not_ready=6`
- US `gateway_budget_degraded=2`
- HK `strategy_suggestion_stale=2`

## Trading boundary

This does not submit orders, change portfolio selection, or loosen risk, edge,
cost, liquidity, market-rule, Gateway budget, or submit-quality gates. It is a
submit safety guard: automated submit can proceed only after Supervisor is
running the same code revision as the current repository.
