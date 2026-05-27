# 2026-05-27 Dashboard Market Data Fallback Label Fix

## Issue

GitHub Actions integration tier failed in:

`tests/test_supervisor_cli.py::SupervisorCliTests::test_dashboard_market_data_health_overview_marks_nonresearch_fallback_market_as_attention`

The dashboard labeled XETRA as `研究Fallback` when IBKR historical data was unavailable and the report fell back to yfinance. That was wrong for non-research-only markets: XETRA may use yfinance to reduce IBKR Gateway load, but it is still an execution-capable market and fallback should remain visible as a data attention item.

## Fix

`src/tools/generate_dashboard.py` now marks `research_only_yfinance` for dashboard health only when both conditions are true:

- the market/config allows yfinance fallback, and
- the market structure or report item is explicitly research-only.

This separates two cases:

- CN research-only fallback remains `研究Fallback`.
- XETRA/US/HK/ASX execution-capable fallback becomes `待排查`.

## Trading Impact

No trading gates were loosened. This is dashboard classification only. Execution-capable markets that fall back from IBKR data to yfinance remain visible as requiring attention before automated paper submit.

## Validation

Targeted checks passed:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/test_supervisor_cli.py::SupervisorCliTests::test_dashboard_market_data_health_overview_marks_nonresearch_fallback_market_as_attention \
  tests/test_supervisor_cli.py::SupervisorCliTests::test_dashboard_market_data_health_overview_marks_research_fallback_market

PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/tools/generate_dashboard.py
git diff --check
```
