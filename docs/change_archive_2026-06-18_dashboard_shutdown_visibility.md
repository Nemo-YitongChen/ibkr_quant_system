# 2026-06-18 Dashboard Supervisor Shutdown Visibility

## Context

The prior shutdown diagnostics change writes `supervisor_shutdown_status.json` and append-only `supervisor_shutdown_events.jsonl`. This change makes those artifacts visible in the dashboard so operator decisions do not depend on manually opening runtime files.

## Changes

- `generate_dashboard` now loads:
  - `supervisor_shutdown_status.json`
  - the last 20 rows of `supervisor_shutdown_events.jsonl`
- Dashboard payload now includes:
  - `supervisor_shutdown_status`
  - `supervisor_shutdown_events`
- `ops_overview` now exposes:
  - `supervisor_shutdown_status`
  - `supervisor_shutdown_health_status`
  - `supervisor_shutdown_reason`
  - `supervisor_shutdown_last_signal_name`
  - `supervisor_shutdown_event_count`
- Existing Ops Health v2 block surfaces the same fields as metrics; no new dashboard block was added, so dashboard block count remains stable.
- Simple dashboard mode now includes a `Supervisor` row showing the current shutdown/running state.

## Runtime check

Regenerated the scoped dashboard:

```bash
PYTHONDONTWRITEBYTECODE=1 python -m src.tools.generate_dashboard \
  --config config/supervisor.yaml \
  --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor
```

Current runtime payload shows:

- `supervisor_shutdown_status.status=running`
- `supervisor_shutdown_status.reason=ignored_signal:SIGHUP`
- `ops_overview.supervisor_shutdown_health_status=ready`
- `ops_overview.supervisor_shutdown_event_count=0`

The event count is zero because the currently running Supervisor process was started before the event-history code was deployed. It will start appending events after Supervisor is restarted with the latest code.

## Trading impact

- No IBKR requests are added.
- No YAML config is changed.
- No auto-submit, risk, edge, cost, liquidity, market-rule, Gateway budget, or submit-quality gate is changed.
- This improves operator visibility for unexpected shutdowns, which is required before safely increasing paper auto-order frequency.
