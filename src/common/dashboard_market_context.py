from __future__ import annotations

from typing import Any, Dict

MARKET_CONTEXT: Dict[str, Dict[str, Any]] = {
    "US": {
        "label": "US",
        "summary": "趋势优先 / 深流动性 / session 风险",
        "context_summary": "趋势优先 / 深流动性 / session 风险",
        "primary_risks": [
            "open_close_slippage",
            "trend_regime_flip",
            "overtrading",
        ],
        "timezone": "America/New_York",
        "settlement_cycle": "T+1",
        "day_turnaround_allowed": True,
        "research_only": False,
        "execution_bias": "liquid_large_cap_etf_first",
        "primary_review_axis": "edge_gate_and_turnover",
    },
    "HK": {
        "label": "HK",
        "summary": "board lot / odd lot / 成本 / sliced limit",
        "context_summary": "board lot / odd lot / 成本 / sliced limit",
        "primary_risks": [
            "board_lot_mismatch",
            "thin_liquidity",
            "cost_buffer",
        ],
        "timezone": "Asia/Hong_Kong",
        "settlement_cycle": "T+2",
        "day_turnaround_allowed": True,
        "research_only": False,
        "execution_bias": "low_turnover_board_lot_aware",
        "primary_review_axis": "board_lot_fee_drag_and_edge_gate",
    },
    "CN": {
        "label": "CN",
        "summary": "research-only / staged / 低频 / 防守预算",
        "context_summary": "research-only / staged / 低频 / 防守预算",
        "primary_risks": [
            "research_only",
            "turnover",
            "defensive_budget",
        ],
        "timezone": "Asia/Shanghai",
        "settlement_cycle": "T+1",
        "day_turnaround_allowed": False,
        "research_only": True,
        "execution_bias": "research_only_etf_large_cap",
        "primary_review_axis": "market_rule_and_research_only_blocks",
    },
}


def market_context(market: str) -> Dict[str, Any]:
    code = str(market or "").strip().upper()
    if code in MARKET_CONTEXT:
        return dict(MARKET_CONTEXT[code])
    return {
        "label": code or "UNKNOWN",
        "summary": "",
        "context_summary": "",
        "primary_risks": [],
    }
