# Multi-Market Adaptive Framework

## Objective

This project should not treat every market, account size, and volatility regime as if they were interchangeable.

The target architecture is:

- one research and execution workflow
- multiple market-specific constraint profiles
- one shared market-regime layer
- account-size-aware execution and portfolio preferences

In practice that means:

- trend and relative strength remain the default alpha engine
- high-volatility and sideways regimes should shrink sizing and prefer pullback entries instead of broad chasing
- defensive regimes should reduce exposure before trying to add complexity through shorting
- market rules such as lot sizes, same-day turnaround limits, settlement, and taxes must be first-class inputs

## Why This Matters

The current repository already has:

- market-specific investment configs
- market benchmark regime adaptation
- paper / execution / guard / opportunity / weekly-review workflows
- lot-size-aware execution and execution cost proxies

What it still lacks is a single configuration layer that answers:

- what is operationally allowed in this market
- what is expensive in this market
- what should a small account avoid
- what instrument types should be preferred first

That gap is what `market_structure` is meant to close.

## Market-Level Operating Principles

### US

- treat the US baseline as ETF rotation plus liquid large-cap relative strength
- below the PDT threshold, default to swing / overnight logic instead of margin day-trading behavior
- use market regime to decide whether the system should expand into broader single-name exposure

### HK

- assume higher fixed turnover drag than US because of stamp duty and exchange levies
- assume odd-lot handling is worse than board-lot handling
- prefer ETF and very liquid large-cap names first
- use higher execution conservatism and lower rebalance frequency

### CN / Northbound

- keep the current repository stance of research-only until execution support is formalized
- treat 100-share buy lots, no same-day turnaround, and price-limit behavior as hard design constraints
- bias toward ETF and liquid large-cap names with low turnover

## Framework Layers

### 1. Market Structure

`market_structure` should define:

- settlement cycle
- same-day turnaround allowance
- buy-lot multiple
- odd-lot friction
- price-limit regime
- tax and exchange-fee stack
- small-account preferences
- preferred instrument classes

This layer should be loadable from config, visible in reports, and usable by execution gates.

### 2. Market Regime

The project already has a benchmark-driven regime adaptor.

The next step is to make market regime a shared upstream input for:

- report ranking
- paper sizing
- opportunity gating
- execution readiness
- guard aggressiveness

### 3. Account Profile

Account size is now being formalized as another explicit layer:

- small
- medium
- large

This should drive:

- max positions
- max single-name allocation
- ETF-first vs stock-enabled mode
- manual review thresholds
- expected rebalance frequency

## First Implementation Scope

The first version of `market_structure` in this repository is intentionally narrow:

- add explicit config files for `US`, `HK`, `CN`, `ASX`, and `XETRA`
- load them through a shared `src/common/market_structure.py`
- expose market-structure facts in investment reports
- use the cost rules in the report-side execution-cost proxy
- use the small-account ETF preference as an execution review gate

This keeps the change low-risk while making the new layer immediately visible and operational.

## Planned Next Steps

1. Keep extending `account_profile` from execution into guard / opportunity / weekly review.
2. Add a project-level adaptive strategy spec so regime, ranking, and defensive behavior are documented in one place.
3. Move more execution limits out of market-specific YAML files into `market_structure`.
4. Add explicit ETF stamp-duty exceptions for HK cost modeling at the instrument level.
5. Make market regime a first-class shared input across report / paper / execution / guard.

See also: [`acm_rs_strategy_spec.md`](./acm_rs_strategy_spec.md)

## Reference Notes

The current market-structure defaults in this repository were chosen to match public exchange / regulator guidance at a practical level:

- US: PDT threshold and T+1 settlement
- HK: 0.1% stamp duty for ordinary stock transactions plus exchange levies and post-2025 settlement fee schedule
- CN / Northbound: 100-share buy lots, no same-day turnaround, 10% main-board price-limit baseline

These values are operating assumptions for this codebase, not a replacement for legal or broker documentation.
