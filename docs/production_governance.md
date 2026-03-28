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

This repository now includes a basic GitHub Actions workflow that runs `pytest`.
That is the minimum baseline; the next step should be adding targeted smoke checks for supervisor and report generation paths.

## Suggested next controls

- add `.env.example` maintenance to change review checklist
- add preflight summary review to live-session checklist
- add explicit rollback notes for switching from `AUTO` back to `REVIEW_ONLY`
- add alerting for stale reports, failed preflight and dashboard mode drift
