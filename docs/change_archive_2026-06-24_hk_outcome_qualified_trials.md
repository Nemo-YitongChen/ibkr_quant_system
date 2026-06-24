# 2026-06-24 HK Outcome-Qualified Trial Symbols

## Context

HK opportunity outcome validation showed supportive aggregate 5/20d outcomes for
both positive post-cost candidates and close `WAIT_PULLBACK` groups, but the
group-level signal hid weak single-symbol members. In particular, the refreshed
HK close `WAIT_PULLBACK` evidence still supported the group, while `1288.HK` and
`2359.HK` had negative single-symbol 5/20d outcomes.

## Changes

- `review_opportunity_outcomes` now derives `outcome_qualified_symbols` from the
  validation `symbol_rows`.
- A symbol qualifies only when both 5d and 20d sample counts are mature and both
  average outcomes are non-negative.
- Calibration suggestions now expose:
  - `outcome_qualified_symbol_count`
  - `outcome_qualified_symbols`
  - `outcome_excluded_symbol_count`
  - `outcome_excluded_symbols`
- Paper trial rows now use the outcome-qualified subset as `candidate_symbols`
  and retain the original group as `source_candidate_symbols`.
- If a close `WAIT_PULLBACK` group is positive only in aggregate but has no
  qualifying symbol, the tool emits a non-trial review action instead of a
  near-entry trial.

## HK Verification

Refreshed artifact:

`runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation/opportunity_outcome_validation.json`

Latest HK positive post-cost outcome:

- `HK:resolved_hk_top100_bluechip`: 5d `+122.89bps`, 20d `+253.84bps`.
- `HK:resolved_hk_top100_tech_growth`: 5d `+125.19bps`, 20d `+264.69bps`.
- Trial subset: `3988.HK,0005.HK,0939.HK,2388.HK`.
- Excluded: `2359.HK,0992.HK`.

Latest HK close `WAIT_PULLBACK` outcome:

- `HK:resolved_hk_top100_bluechip`: 5d `+125.96bps`, 20d `+212.07bps`.
- `HK:resolved_hk_top100_tech_growth`: 5d `+126.74bps`, 20d `+222.09bps`.
- Trial subset: `3988.HK,2388.HK,1398.HK,0939.HK,0005.HK,3328.HK`.
- Excluded: `1288.HK,2359.HK`.

## Trading Impact

This is a stricter paper-only trial contract. It does not change market config,
does not submit orders, and does not relax risk, edge, cost, liquidity,
market-rule, Gateway budget, or submit-quality gates.

## Shutdown Diagnosis

The current Supervisor process is still alive as `python -m src.app.supervisor`
with PID `77976` and parent PID `1`; it is a detached long-running process. The
latest status is `running / ignored_signal:SIGHUP`, so the recent signal did not
shut it down.

Observed shutdown-like cases are:

- Starting a second Supervisor exits with `SupervisorInstanceLockError` because
  the first instance holds `supervisor.lock`.
- Running with `--once` intentionally exits after one scheduler cycle.
- `SIGINT` / `SIGTERM` request graceful shutdown.
- Unhandled exceptions write `crashed` status and exit.

Because the active process predates code-revision tracking, dashboard currently
shows `running_code_revision_missing`. Restart Supervisor in a controlled window
to load the latest code and start appending shutdown events.
