# Automatic Recovery Evidence Refresh

Date: 2026-06-12

## Objective

Remove the manual reply requirement from Gateway-budget recovery without bypassing trading safeguards or consuming the next normal paper-submit execution slot.

## Implemented Flow

1. Supervisor keeps the existing recovery eligibility contract and waits until the projected Gateway-budget recovery time.
2. Once evidence refresh is due, Supervisor runs `src.tools.refresh_ibkr_gateway_budget`.
3. The refresher reads local request telemetry only. It does not connect to IBKR and does not add Gateway load.
4. If the refreshed target-market budget is clear, Supervisor creates a persistent `auto_order_recovery_checkpoint.json`.
5. The checkpoint survives restarts and selects one market/portfolio frontier only.
6. Supervisor forces one target report refresh and one execution evidence dry-run.
7. Recovery execution is always no-submit and is stamped:
   - `execution_purpose=RECOVERY_EVIDENCE`
   - `recovery_evidence_only=true`
   - `consumes_submit_slot=false`
8. Only a valid recovery artifact with no failed IBKR connection completes the checkpoint.
9. A failed or incomplete attempt remains pending and retries after the configured cooldown.
10. The next normal scheduled execution remains eligible to submit if all readiness, risk, edge, cost, market-rule, and budget gates pass.

## Runtime Configuration

`config/supervisor.yaml` now defines:

- `auto_order_recovery_budget_refresh_interval_min: 30`
- `auto_order_recovery_budget_refresh_timeout_sec: 60`
- `auto_order_recovery_budget_window_days: 7`
- `auto_order_recovery_target_retry_interval_min: 60`

## Evidence Revalidation

The local-only refresh generated:

- Window generated at: `2026-06-11T22:50:59.282410+00:00`
- Total Gateway requests: `9753`
- Markets over budget: `4`
- US requests: `3216 / 2000`
- US budget usage: `160.8%`
- US projected recovery: `2026-06-13T23:59:59.999999+00:00`

The target remains blocked until a new local telemetry refresh proves the budget is within policy. Time passage alone does not authorize submit.

## Safety Contract

- Recovery never passes `--submit`.
- Recovery does not relax risk, edge, cost, liquidity, market-rule, submit-quality, preflight, or Gateway-budget gates.
- Broker snapshot and risk guard remain available as protective paths.
- Recovery execution artifacts do not mark the report day as normally executed.
- Gateway-unavailable degraded artifacts cannot complete recovery.
- Existing execution artifacts without the new field remain backward-compatible and consume the normal slot.

## Operator Impact

No manual reply is required at the projected recovery time. Keeping one current Supervisor process running is sufficient. The system will refresh local evidence, retry the single target when eligible, and preserve the next normal submit opportunity.
