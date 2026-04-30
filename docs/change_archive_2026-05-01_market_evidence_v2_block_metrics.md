# Change Archive: 2026-05-01 Market Evidence V2 Block Metrics

## Context

Market-level evidence actions were already attached to `market_views` and rendered in the advanced HTML table. The dashboard v2 `market_views` block still treated those fields as passive row data, so its top-level metrics could not answer which markets needed gate review, signal calibration, or more samples.

## Changes

- Enriched dashboard v2 market view rows with `evidence_primary_action` and `evidence_decision_basis` when market summaries are available.
- Added market-level evidence metrics:
  - `evidence_action_market_count`
  - `evidence_row_market_count`
  - `evidence_attention_count`
  - `missing_evidence_market_count`
  - `gate_review_market_count`
  - `signal_review_market_count`
  - `sample_collection_market_count`
- Updated market view block status so missing evidence, gate-review, and signal-review markets surface as warnings.
- Kept sample-starved evidence as non-warning by default, so "collect more outcome samples" does not get misread as an immediate parameter-change signal.
- Added focused dashboard block coverage for market evidence metrics and sample-only non-warning behavior.

## Operational Impact

Operators can now read dashboard v2 block metrics and immediately see whether the next market-level action is to rebuild evidence, review gate thresholds, recalibrate signal-to-edge mapping, or simply continue collecting outcomes.
