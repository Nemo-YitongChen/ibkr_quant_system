# ACM-RS Strategy Spec

## Positioning

`Adaptive Cross-Market Relative Strength Strategy (ACM-RS)` is the project-level strategy framework for this repository.

It is not a promise of a single universal alpha. It is a practical specification for how this project should behave across:

- different markets
- different account sizes
- different market regimes

The design goal is:

- tradable
- backtestable
- explainable
- constraint-aware

## What Is Valid in the Original Analysis

The core direction is usable:

- use medium-term trend / relative strength as the main engine
- use short-term pullback entries only inside names that remain structurally strong
- reduce exposure aggressively in defensive regimes
- do not assume US / HK / CN share the same execution rules or cost model

That direction is consistent with how this repository is already evolving:

- `market_structure` handles market-specific operating constraints
- `account_profile` handles account-size-aware execution limits
- `regime_adaptor` and investment scoring already separate regime from execution

## Corrections Required Before Treating It as a Project Spec

Some points in the original write-up need to be stated more precisely:

- `US PDT` applies to day trading in a **margin account**. It should not be phrased as a blanket rule for every account type.
- `US T+1` should be anchored to the actual change date: `May 28, 2024`.
- `HK stock stamp duty` should use the current baseline of `0.1% per side` for ordinary stock transactions, not the old `0.13%`.
- `HK ETF stamp duty exemption` is real, but should be described as an ETF-specific exception instead of a general Hong Kong equity rule.
- `A-share / Northbound` constraints should be described as a **main board / Stock Connect baseline**, not as a universal rule for every China venue and board.
- the academic evidence should be used as **orientation**, not as a reason to hard-code one fixed threshold set forever.

## Framework Definition

### 1. Regime Layer

The strategy uses three practical states:

- `Uptrend`
  - `Close > MA120`
  - `MA20 > MA120`
- `High-Vol Sideways`
  - `abs(Close / MA120 - 1) <= 3%`
  - `Vol20 > 1.2 * Vol120`
- `Downtrend`
  - `Close < MA120`
  - `MA20 < MA120`

This is an engineering filter, not an academic identity.

### 2. Signal Layer

#### Trend / Relative Strength

Use the default cross-market ranking form:

- `R126`
- `R63`
- `Vol20`

with the composite score:

- `0.5 * z(R126) + 0.3 * z(R63) - 0.2 * z(Vol20)`

and basic filters:

- `Close > MA60`
- `AvgTurnover60` above the configured floor

#### Pullback Module

Use pullback entries only when:

- the name is still above `MA120`
- medium-term strength remains in the better part of the universe
- short-term return has pulled back sharply
- volatility is not already in the worst tail

This module is subordinate to the main trend engine.

### 3. Defensive Layer

When the market is in a defensive regime:

- small accounts should keep gross exposure around `0% - 20%`
- medium accounts around `0% - 30%`
- large accounts around `0% - 40%`

New entries should require a stricter threshold than in normal conditions.

### 4. Execution Layer

Default operating rhythm:

- compute signals after daily close
- rebalance weekly
- allow at most two rebalances per week in high-volatility regimes
- avoid the first minutes after the open
- use staged execution for larger baskets

## Mapping to Current Repository

This repository should map the framework as follows:

- `market_structure`
  - exchange rules, taxes, board lots, same-day turnaround constraints
- `account_profile`
  - small / medium / large account execution limits
- `regime_adaptor`
  - benchmark-based regime inputs
- `investment scoring / report`
  - ranking, execution readiness, cost-aware recommendation output
- `execution / guard / opportunity`
  - tradability and review gates

## Current Implementation Policy

The immediate project policy should be:

1. `ETF-first baseline`
2. `liquid large-cap expansion second`
3. `advanced short / hedging only after market constraints and account profiles are stable`

This is a better fit for the current project than trying to jump straight to a cross-market long/short system.

## Sources Used For the Corrections

- [FINRA PDT guidance](https://www.finra.org/investors/investing/investment-products/stocks/day-trading)
- [SEC T+1 implementation statement, May 28, 2024](https://www.sec.gov/newsroom/press-releases/2024-62)
- [HKEX transaction fees and 0.1% stock stamp duty](https://www.hkex.com.hk/Services/Rules-and-Forms-and-Fees/Fees/Securities-%28Hong-Kong%29/Trading/Transaction?sc_lang=en)
- [HKEX board lot / odd lot FAQ](https://www.hkex.com.hk/Global/Exchange/FAQ/Securities-Market/Trading/Securities-Market-Operations?sc_lang=en)
- [IRD ETF stamp duty exemption FAQ](https://www.ird.gov.hk/eng/faq/ETFs.htm)
- [SSE main board mechanism](https://english.sse.com.cn/start/trading/mechanism/)
- [HKEX Stock Connect investor / participant materials](https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Reference-Materials/Information-Booklet-and-FAQ?sc_lang=en)
