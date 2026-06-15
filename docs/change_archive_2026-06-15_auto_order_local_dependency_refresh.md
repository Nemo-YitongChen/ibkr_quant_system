# 2026-06-15 Auto-Order Local Dependency Refresh

## Goal

Prevent automated paper ordering from remaining blocked by stale local
governance artifacts after the underlying configuration and IB Gateway port are
already healthy.

## Changes

- Supervisor now refreshes the lightweight preflight artifact every six hours.
- Supervisor rebuilds market readiness from existing local report and execution
  artifacts every fifteen minutes.
- A successful preflight refresh forces market readiness to rebuild before the
  auto-order recovery and submit decisions are evaluated.
- If a cycle creates new execution evidence, market readiness is rebuilt again
  before the final auto-order readiness artifact is written.
- Failed dependency refresh attempts are throttled for ten minutes.
- The refresh path is local and read-only:
  - no historical or realtime market-data request;
  - no scanner request;
  - no order submission;
  - no relaxation of risk, edge, cost, market-rule, or submit-quality gates.
- Repeated runtime-root resolution was consolidated into
  `Supervisor._primary_runtime_root()`.
- Artifact interval checks now use the shared
  `supervisor_support.artifact_refresh_due()` helper.

## Real Runtime Evidence

A real one-cycle Supervisor run refreshed:

- preflight: `29 PASS / 0 WARN / 0 FAIL`;
- IB Gateway paper port: `127.0.0.1:4002 PASS`;
- scoped `market_readiness.json`;
- scoped `auto_order_readiness.json`;
- scoped dashboard JSON and HTML.

The stale preflight hard block was removed. The remaining hard blocks now
reflect current trading evidence:

- ASX and one HK portfolio have old execution artifacts that still report
  `IBKR_GATEWAY_UNAVAILABLE`;
- US, XETRA, and other portfolios require a bounded no-submit execution refresh;
- the current US frontier remains below the configured net-edge and edge-margin
  thresholds;
- HK still has stale strategy-suggestion governance work.

## Safety Boundary

This change makes dependency evidence self-maintaining. It does not make a
candidate tradable, increase paper submit capacity, or imply profitable edge.
The next safe execution step is one target-scoped no-submit refresh, followed by
market-readiness and post-cost quality review.

## Validation

- Focused dependency-refresh tests passed.
- Python compilation passed.
- Real Supervisor one-cycle validation completed without submitting orders.
