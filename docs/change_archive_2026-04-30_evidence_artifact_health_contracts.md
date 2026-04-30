# Change Archive: 2026-04-30 Evidence Artifact Health Contracts

## Context

`weekly_unified_evidence.json` and `weekly_blocked_vs_allowed_expost.json` are now emitted as standalone weekly review artifacts. The next operational gap was visibility: dashboard artifact health still did not monitor these new evidence artifacts directly.

## Changes

- Registered `weekly_unified_evidence` and `weekly_blocked_vs_allowed_expost` in `dashboard_artifact_contracts()`.
- Added compatibility fallback from `weekly_review_summary.json` sections for older weekly runs.
- Updated artifact loader support for JSON row artifacts with `rows` and `row_count`.
- Included the new evidence artifacts in dashboard review artifact health and consistency checks.
- Added tests for contract registration, JSON row counting, and summary fallback behavior.

## Operational Impact

Dashboard health can now distinguish three states for weekly evidence:

- Standalone evidence JSON is present and fresh.
- Standalone evidence JSON is missing, but summary fallback rows are available.
- Evidence is missing or stale and should be regenerated.

This makes PR 5/6 evidence artifacts visible as first-class operational contracts instead of hidden implementation details.
