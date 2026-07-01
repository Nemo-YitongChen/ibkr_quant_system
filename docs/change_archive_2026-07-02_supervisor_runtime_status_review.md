# 2026-07-02 Supervisor runtime status review

## Context

- HK outcome evidence needed a current, repeatable verification path before using it to justify any paper-only entry trial.
- The long-running Supervisor process is still PID `77976`, started on 2026-06-16, and the last runtime status artifact lacks `code_revision/current_code_revision`.
- The existing shutdown status artifact can be misread as a shutdown because its latest reason is `ignored_signal:SIGHUP`; in code, SIGHUP is explicitly ignored and the process keeps running.

## Change

- Added `src.common.supervisor_runtime_status` as a read-only runtime contract builder.
- Added `python -m src.tools.review_supervisor_runtime` to write:
  - `supervisor_runtime_status.json`
  - `supervisor_runtime_status.md`
- The new artifact combines:
  - `supervisor.lock`
  - `supervisor_shutdown_status.json`
  - current git revision
  - PID liveness where the environment permits it
  - restart requirement and recovery refresh block status
- PID liveness now falls back from `os.kill(pid, 0)` to `ps -p <pid>` when kill probing is not permitted.

## Runtime Evidence

Current read-only Supervisor runtime review was written to `/private/tmp/ibkr_supervisor_runtime_status/`:

- `supervisor_status=running`
- `supervisor_reason=ignored_signal:SIGHUP`
- `supervisor_code_revision_status=missing`
- `health_status=warning`
- `restart_required=true`
- `blocks_recovery_refresh=true`
- `next_action=restart_supervisor_current_code`
- `submit_orders=false`

Process evidence:

- `ps -o pid,ppid,etime,stat,command -p 77976` shows `python -m src.app.supervisor` is still running.
- In the managed sandbox, direct `os.kill(pid, 0)` and unapproved `ps` subprocess calls can be denied, so the runtime artifact may show `supervisor_liveness_status=unknown` even while an approved shell `ps -o ... -p` confirms the process exists.

## HK Outcome Evidence

Current HK outcome validation was regenerated to `/private/tmp/ibkr_hk_opportunity_outcomes/` and matches the canonical HK-specific runtime artifact:

- HK positive post-cost candidates are still weak/mixed:
  - `HK:resolved_hk_top100_bluechip`: `5d=65.87bps`, `20d=-129.20bps`
  - `HK:resolved_hk_top100_tech_growth`: `5d=72.54bps`, `20d=-127.94bps`
  - Current symbols: `2359.HK,0005.HK`
- HK close `WAIT_PULLBACK` remains supportive:
  - `HK:resolved_hk_top100_bluechip`: `5d=125.96bps`, `20d=212.07bps`
  - `HK:resolved_hk_top100_tech_growth`: `5d=126.74bps`, `20d=222.09bps`
  - Trial-qualified symbols: `3988.HK,2388.HK,1398.HK,0939.HK,0005.HK,3328.HK`
  - Excluded symbols: `1288.HK,2359.HK`

## Trading Impact

- This does not submit orders, connect to IBKR, start or stop Supervisor, or relax risk/edge/cost/liquidity/market-rule/submit-quality gates.
- HK post-cost threshold expansion is not supported by current mature 20d outcome evidence.
- HK close `WAIT_PULLBACK` supports only a strict paper-only near-entry limit trial after Supervisor is restarted into current code and fresh BUY/execution evidence is regenerated.
- The practical next step remains: gracefully restart Supervisor into current `HEAD`, then refresh weekly review, market readiness, and no-submit execution evidence before considering any paper submit.

## Validation

```text
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_runtime_status.py
4 passed
```

```text
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_review_opportunity_outcomes.py
4 passed
```

```text
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x
777 passed
```
