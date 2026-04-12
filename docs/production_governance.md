# Production Governance

## Goal

This project already contains paper and live execution paths, so production governance should be treated as part of the product rather than a later add-on.

## Operating modes

- `AUTO`: allow automated execution and guard submission
- `REVIEW_ONLY`: continue generating plans but require human confirmation for actions
- `PAUSED`: stop execution while keeping research and review flows available

## Minimum safety gates

Before any live trading session:

1. run supervisor preflight
2. confirm dashboard state and execution mode
3. confirm IBKR connectivity and account scope
4. confirm report freshness and weekly review freshness
5. confirm no critical warning remains unresolved

## Change management

Recommended default policy:

- all source changes should pass automated tests in CI
- config changes that affect execution should be reviewed together with their target market and mode
- live changes should first be exercised through `paper` or `REVIEW_ONLY`
- strategy logic, risk logic and execution logic should not be bundled into one unreviewed change when avoidable

## Runtime data handling

The repo already keeps runtime outputs out of Git. Keep following that rule for:

- `reports_*`
- `runtime_data/`
- `.cache/`
- SQLite runtime databases

If a runtime artifact is needed for regression or investigation, save a purpose-built fixture instead of committing raw runtime directories.

## CI baseline

This repository now includes a GitHub Actions baseline with:

- a `compile-check` job that catches import / syntax / packaging-shape regressions before pytest starts
- a `core-guardrails` job for startup smoke, CLI contract checks, execution audit persistence, fill audit chain, and the minimal investment workflow smoke path
- an `integration-suite` job for cross-tool workflow and supervisor/dashboard contract coverage
- a `full-suite` job that runs the remaining tests after the earlier tiers have already covered guardrails and integration paths

These now live in one Python CI workflow instead of separate compile-only and pytest-only workflows, and the failure ladder is explicit: `compile-check -> core-guardrails -> integration-suite -> full-suite`.

The workflow also uses GitHub Actions `concurrency` so older runs on the same branch or PR are cancelled when a newer push arrives. That keeps the signal current and avoids wasting CI time on superseded runs.

For local verification, the same guardrail set can be run with `pytest -q -p no:cacheprovider -m guardrail`.

The test suite now has three practical local tiers:

- `guardrail`: startup smoke, CLI contract checks, audit persistence, and the minimal investment workflow path
- `integration`: cross-tool workflow and supervisor/dashboard contract tests
- `slow`: the heavier supervisor-cycle coverage that still belongs in the suite but is useful to skip during fast iteration

Useful local commands:

- `pytest -q -p no:cacheprovider -m guardrail`
- `pytest -q -p no:cacheprovider -m integration`
- `pytest -q -p no:cacheprovider -m "not guardrail and not integration"`
- `pytest -q -p no:cacheprovider -m "not slow"`

## Suggested next controls

- add `.env.example` maintenance to change review checklist
- add preflight summary review to live-session checklist
- add explicit rollback notes for switching from `AUTO` back to `REVIEW_ONLY`
- add alerting for stale reports, failed preflight and dashboard mode drift
