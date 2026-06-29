# 2026-06-29 HK Outcome Revalidation and Supervisor Cycle Resilience

This change revalidates current HK opportunity evidence and reduces avoidable
Supervisor top-level exits from transient cycle errors. It does not submit
orders, relax gates, or change strategy/risk/execution thresholds.

## HK outcome validation

Command used:

```bash
PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_opportunity_outcomes --market HK --market_readiness runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json --weekly_unified_evidence reports_investment_weekly/weekly_unified_evidence.csv --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation
```

Current HK positive post-cost candidates are no longer a clean group-level
support signal:

- `HK:resolved_hk_top100_bluechip`: symbols `2359.HK,0005.HK`, 5d average
  `+65.87bps`, 20d average `-129.20bps`, status `OUTCOME_WEAK_OR_MIXED`.
- `HK:resolved_hk_top100_tech_growth`: symbols `2359.HK,0005.HK`, 5d average
  `+72.54bps`, 20d average `-127.94bps`, status `OUTCOME_WEAK_OR_MIXED`.

The main negative contributor is `2359.HK`, with mature 20d outcomes around
`-650bps` to `-681bps` in the current evidence set. This means the current HK
post-cost group does not justify broadening a post-cost threshold trial.

HK close `WAIT_PULLBACK` remains group-supported:

- `HK:resolved_hk_top100_bluechip`: 5d average `+125.96bps`, 20d average
  `+212.07bps`, status `OUTCOME_SUPPORTS_GROUP`.
- `HK:resolved_hk_top100_tech_growth`: 5d average `+126.74bps`, 20d average
  `+222.09bps`, status `OUTCOME_SUPPORTS_GROUP`.

The `WAIT_PULLBACK` result must remain symbol-aware. `3988.HK`, `2388.HK`,
`1398.HK`, `0939.HK`, `0005.HK`, and `3328.HK` have supportive mature outcome
evidence. `1288.HK` and `2359.HK` remain excluded from any near-entry trial
because their 5d and 20d outcomes are both negative.

## Supervisor shutdown analysis

Runtime evidence shows PID `77976` is still running as `python -m
src.app.supervisor` and holds `supervisor.lock`. The process started on
2026-06-16, so it predates several later shutdown/status improvements. That
explains why the current `supervisor_shutdown_status.json` still lacks
`code_revision` and event-history fields even though recent runtime artifacts
continue to refresh.

There are two distinct "shutdown" cases:

- `src.main` trade-engine child process can be intentionally stopped when no
  configured live trading window is active or market trading is disabled.
- The top-level Supervisor exits only for `--once`, duplicate instance lock,
  SIGINT/SIGTERM, or an unhandled exception.

SIGHUP has already been handled as an ignored signal in newer code, but the
active long-running process still needs a controlled restart before relying on
new status fields from the main loop.

## Cycle resilience change

Supervisor now writes a lightweight heartbeat after successful cycles with
`append_event=false`, keeping `supervisor_shutdown_status.json` fresh without
adding an event-history row every poll.

`run_forever()` now catches exceptions from individual `run_cycle()` calls,
records `running_degraded`, and continues until the configured consecutive
cycle error budget is exhausted. The default is:

```yaml
max_consecutive_cycle_errors_before_shutdown: 3
```

Once the budget is exhausted, Supervisor still exits as `crashed` and preserves
the consecutive error count plus the last cycle error in the final status
artifact. This keeps transient file/database/artifact errors from killing the
main loop immediately while preserving hard failure visibility.

## Trading boundary

This change does not:

- submit paper or live orders
- loosen risk, edge, cost, liquidity, market-rule, Gateway budget, or
  submit-quality gates
- change HK post-cost thresholds
- allow excluded HK symbols into near-entry trials

The next safe trading action is to keep HK post-cost threshold expansion blocked
until fresh symbols regain positive mature 5d/20d evidence, while continuing
paper-only close `WAIT_PULLBACK` research for the outcome-qualified subset only.
