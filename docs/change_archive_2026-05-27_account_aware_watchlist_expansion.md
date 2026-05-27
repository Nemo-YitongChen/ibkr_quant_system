# 2026-05-27 Account-Aware Watchlist Expansion

## Scope

This change connects account-size profiles to the auto-expanded quality-growth watchlists. The goal is to improve paper order frequency only when the candidate set matches the account size, liquidity, whole-share tradability, cost, and evidence constraints.

This does not loosen the auto-submit risk gate, edge gate, submit-quality gate, Gateway budget gate, or market-rule handling.

## What Changed

- `config/account_profiles.yaml` now defines `watchlist_expansion` rules per account band.
- `src/common/account_profile.py` preserves watchlist-expansion overrides in profile summaries.
- `src/common/watchlist_expansion.py` now supports:
  - `max_last_close`
  - account/profile policy overrides
  - deterministic `to_dict()`
  - asset-class preference ordering
  - a new reject reason: `last_close_above_account_cap`
- `src/tools/expand_investment_watchlists.py` now accepts:
  - `--account_equity`
  - `--account_profile`
  - `--account_profile_config`
- Generated watchlists now include the resolved account profile and effective selection policy.

## Current Small-Account Output

The local run used `--account_equity 1000`, which resolves to the `small` profile.

Selected symbols from local candidate evidence:

- US: `SPTM`, `SCHB`
- ASX: none
- HK: none
- XETRA: none

The non-US markets remain empty because current local evidence still fails one or more constraints such as whole-share tradability, expected cost, liquidity, or market-rule feasibility. This is intentional: it preserves order quality instead of increasing frequency by bypassing gates.

## Strategy Path by Account Band

- Small account: keep ETF-first, whole-share-only, low-price, low-cost candidates. Use watchlist expansion for discovery; submit-quality gates still decide whether a candidate can become a paper order.
- Medium account: allow ETF plus high-liquidity equities with stricter cost and data-quality filters.
- Large account: allow a broader ETF/equity basket while keeping per-order and market exposure caps.

## Validation

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_watchlist_expansion.py tests/test_account_profile.py tests/test_remaining_cli.py
PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/common/watchlist_expansion.py src/common/account_profile.py src/tools/expand_investment_watchlists.py
PYTHONDONTWRITEBYTECODE=1 python -m src.tools.expand_investment_watchlists --account_equity 1000
git diff --check
```

Result: `11 passed`; generated watchlist summary selected 2 US symbols.

## Next Steps

- Refresh US report and paper execution dry-run so the next auto-order readiness artifact can evaluate `SPTM` / `SCHB` under current submit-quality gates.
- Keep ASX/HK/XETRA empty until their local candidate evidence clears whole-share/cost/liquidity constraints.
- For medium/large account simulations, run the expansion tool with matching `--account_equity` and compare selected count, expected edge, and rejection reasons before changing live or paper automation limits.
