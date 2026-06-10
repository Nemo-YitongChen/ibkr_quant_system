# 2026-06-10 Target-Scoped Recovery Execution

## Goal

Convert the minimum-request recovery plan from a dashboard recommendation into
a Supervisor scheduling contract without submitting orders or relaxing trading
quality gates.

## Changes

- Added `evaluate_auto_order_recovery_eligibility()` as the shared recovery
  runtime contract.
- Recovery remains blocked while the Gateway budget is degraded, even after the
  projected recovery timestamp, until fresh budget evidence clears the block.
- Supervisor now suppresses non-target investment report and execution refreshes
  while an actionable recovery plan is active.
- Opportunity scans remain suppressed during targeted recovery.
- An eligible recovery permits only the selected frontier report and one
  execution dry-run.
- Any configured execution submit flag is removed from the recovery run.
- CLI readiness artifacts and dashboard v2 now expose the same structured
  eligibility state and reason.

## Runtime Policy

The only eligible Gateway-backed recovery sequence is:

1. Gateway and preflight blockers are clear.
2. Gateway budget evidence is no longer degraded.
3. The selected frontier still has `PASS` submit quality.
4. The target market and portfolio match the recovery plan.
5. Run the target report.
6. Run target execution without `--submit`.
7. Rebuild local readiness and dashboard artifacts.

All other report, execution, and opportunity work is deferred while this
recovery contract is active. Broker snapshot and risk guard workflows remain
independent so account consistency and protective controls are not weakened.

## Safety Boundary

- Paper-only.
- No order submission.
- Maximum one recovery portfolio.
- No automatic candidate or watchlist promotion.
- No risk, edge, cost, liquidity, market-rule, or submit-quality relaxation.
- No changes to the existing local HK watchlist edits.

## Validation

- Python compile checks passed.
- Full Supervisor test file: `112 passed`.
- Final auto-order, dashboard, workflow smoke, and packaging regression:
  `50 passed`.
- Final Supervisor recovery and readiness regression: `7 passed`.
- Project `.venv` dependency check: no broken requirements.
- Regenerated the real readiness and dashboard artifacts without submitting
  orders or running broad market scans.
