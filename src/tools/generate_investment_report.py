from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Dict, List

from ..analysis.investment import (
    InvestmentPlanConfig,
    InvestmentScoringConfig,
    make_investment_plan,
    score_investment_candidate,
)
from ..analysis.investment_shadow_ml import (
    InvestmentShadowModelConfig,
    apply_investment_shadow_model,
    train_investment_shadow_model,
)
from ..analysis.investment_short import InvestmentShortBookConfig, build_short_book_candidates
from ..analysis.investment_backtest import InvestmentBacktestConfig, compute_investment_backtest_from_bars
from ..analysis.report import write_csv, write_investment_md, write_json
from ..analysis.universe import build_candidates
from ..common.adaptive_strategy import (
    adaptive_strategy_context,
    adaptive_strategy_market_execution_overrides,
    adaptive_strategy_market_plan_overrides,
    adaptive_strategy_market_regime_overrides,
    apply_adaptive_defensive_rank_cap,
    apply_adaptive_strategy_plan_overrides,
    apply_adaptive_strategy_regime_overrides,
    load_adaptive_strategy,
)
from ..common.cli import build_cli_parser, emit_cli_summary
from ..common.logger import get_logger
from ..common.market_structure import MarketStructureConfig, load_market_structure
from ..common.markets import (
    add_market_args,
    infer_market_from_config_path,
    load_market_universe_config,
    load_symbols_from_symbol_master,
    market_config_path,
    resolve_market_code,
    symbol_matches_market,
)
from ..common.runtime_paths import resolve_repo_path
from ..common.storage import Storage
from ..data import MarketDataAdapter
from ..enrichment.providers import EnrichmentProviders
from ..enrichment.yfinance_history import fetch_daily_bars as fetch_daily_bars_yf
from ..ibkr.market_data import MarketDataService
from ..ibkr.universe import UniverseConfig, UniverseService, scanner_location_codes_from_config
from ..offhours.candidates import load_watchlist_symbols, read_recent_symbols_from_audit
from ..offhours.compute_long import compute_long_from_bars
from ..offhours.compute_mid import compute_mid_from_bars
from ..offhours.ib_setup import connect_ib, register_contracts, set_delayed_frozen
from ..strategies.mid_regime import RegimeConfig
from ..strategies.regime_adaptor import RegimeAdaptConfig, RegimeAdaptor

log = get_logger("tools.generate_investment_report")
BASE_DIR = Path(__file__).resolve().parents[2]


def build_parser() -> argparse.ArgumentParser:
    ap = build_cli_parser(
        description="Generate medium/long-term investment candidates from historical data.",
        command="ibkr-quant-report",
        examples=[
            "ibkr-quant-report --market HK --ibkr_config config/ibkr_hk.yaml --top_n 20",
            "ibkr-quant-report --market US --watchlist_yaml config/watchlists/resolved_us_growth.yaml",
        ],
        notes=[
            "Writes investment_report.md, candidate CSVs, and summary JSON files under --out_dir.",
        ],
    )
    add_market_args(ap)
    ap.add_argument("--ibkr_config", default="config/ibkr.yaml", help="Path to the IBKR runtime config yaml.")
    ap.add_argument("--investment_config", default="", help="Path to investment scoring/plan config yaml.")
    ap.add_argument("--out_dir", default="", help="Optional output directory override. Defaults to reports_investment_<market>.")
    ap.add_argument("--top_n", type=int, default=15, help="Number of ranked long ideas to emit.")
    ap.add_argument("--max_universe", type=int, default=1000, help="Maximum candidate universe size before scoring.")
    ap.add_argument("--watchlist_yaml", default="", help="YAML with {symbols: [...]} used to build candidates.")
    ap.add_argument("--db", default="audit.db", help="SQLite audit database used for recent symbols.")
    ap.add_argument("--symbol_master_db", default="", help="SQLite symbol master database used for market universe candidates.")
    ap.add_argument("--market_structure_config", default="", help="Optional path to market structure constraints yaml.")
    ap.add_argument("--adaptive_strategy_config", default="", help="Optional path to adaptive strategy framework yaml.")
    ap.add_argument("--use_audit_recent", action="store_true", default=False, help="Include recent symbols from audit snapshots.")
    ap.add_argument("--audit_limit", type=int, default=500, help="Maximum recent audit symbols to include.")
    ap.add_argument("--request_timeout_sec", type=float, default=15.0, help="Per-request timeout for enrichment requests in seconds.")
    ap.add_argument("--backtest_top_k", type=int, default=10, help="How many ranked symbols to backtest in detail.")
    ap.add_argument("--fundamentals_top_k", type=int, default=20, help="How many symbols to enrich with fundamentals in detail.")
    return ap


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _resolve_project_path(path_str: str) -> str:
    return str(resolve_repo_path(BASE_DIR, path_str))


def _load_yaml(path_str: str) -> Dict[str, Any]:
    import yaml

    path = Path(_resolve_project_path(path_str))
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _slugify_report_name(name: str) -> str:
    s = "".join(ch.lower() if ch.isalnum() else "_" for ch in (name or "").strip())
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_") or "default"


def _extract_vix(bundle: Dict[str, Any]) -> float:
    try:
        return float(bundle.get("markets", {}).get("tickers", {}).get("^VIX", {}).get("close", 0.0) or 0.0)
    except Exception:
        return 0.0


def _macro_high_risk(bundle: Dict[str, Any]) -> bool:
    events = bundle.get("macro_events") or bundle.get("bundle", {}).get("macro_events") or []
    for event in events:
        imp = str(event.get("importance", "")).lower()
        if imp in {"high", "3"}:
            return True
    return False


def _earnings_map(bundle: Dict[str, Any]) -> Dict[str, bool]:
    out: Dict[str, bool] = {}
    earnings = bundle.get("earnings") or bundle.get("bundle", {}).get("earnings") or {}
    if not isinstance(earnings, dict):
        return out
    for sym, info in earnings.items():
        try:
            out[str(sym).upper()] = bool(info.get("in_14d", info.get("in_window", False)))
        except Exception:
            out[str(sym).upper()] = False
    return out


def _market_leaders(bundle: Dict[str, Any]) -> tuple[str, str]:
    tickers = dict(bundle.get("markets", {}).get("tickers", {}) or {})
    rows: List[tuple[str, float]] = []
    for symbol, info in tickers.items():
        if str(symbol).upper() in {"^VIX", "SPY", "QQQ", "IWM"}:
            continue
        try:
            rows.append((str(symbol), float(info.get("ret5d", 0.0) or 0.0)))
        except Exception:
            continue
    rows.sort(key=lambda item: item[1], reverse=True)
    leaders = ",".join(sym for sym, _ in rows[:3])
    laggards = ",".join(sym for sym, _ in rows[-3:]) if len(rows) >= 3 else ""
    return leaders, laggards


def _filter_symbols_for_market(symbols: List[str], market: str) -> List[str]:
    return [str(sym).upper() for sym in symbols if symbol_matches_market(str(sym), market)]


def _rank_sort_key(row: Dict[str, Any]) -> tuple[int, float]:
    return (
        {"ACCUMULATE": 3, "HOLD": 2, "WATCH": 1, "REDUCE": 0}.get(str(row.get("action", "WATCH")).upper(), 1),
        float(row.get("score", 0.0) or 0.0),
    )


def _report_portfolio_id(market: str, watchlist_yaml: str, report_dir: Path) -> str:
    market_code = str(market or "DEFAULT").upper()
    if str(watchlist_yaml or "").strip():
        return f"{market_code}:{_slugify_report_name(Path(watchlist_yaml).stem)}"
    return f"{market_code}:{report_dir.name}"


def _persist_candidate_snapshots(
    storage: Storage,
    *,
    rows: List[Dict[str, Any]],
    stage: str,
    market: str,
    portfolio_id: str,
    report_dir: Path,
    analysis_run_id: str,
    source_reason_map: Dict[str, List[str]],
    plan_map: Dict[str, Dict[str, Any]] | None = None,
) -> None:
    normalized_stage = str(stage or "").strip().lower() or "final"
    plan_map = {str(k).upper(): dict(v) for k, v in (plan_map or {}).items()}
    for idx, row in enumerate(rows, start=1):
        symbol = str(row.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        plan = dict(plan_map.get(symbol) or {})
        source_reasons = [str(item).strip() for item in list(source_reason_map.get(symbol, []) or []) if str(item).strip()]
        storage.insert_investment_candidate_snapshot(
            {
                "snapshot_id": f"{analysis_run_id}|{normalized_stage}|{symbol}",
                "market": str(market or "").upper(),
                "portfolio_id": str(portfolio_id or ""),
                "report_dir": str(report_dir),
                "analysis_run_id": str(analysis_run_id),
                "stage": normalized_stage,
                "symbol": symbol,
                "action": str(row.get("action") or plan.get("action") or "WATCH").upper(),
                "direction": "LONG",
                "score": float(row.get("score", 0.0) or 0.0),
                "model_recommendation_score": float(row.get("model_recommendation_score", row.get("score", 0.0)) or 0.0),
                "execution_score": float(row.get("execution_score", plan.get("execution_score", 0.0)) or 0.0),
                "analyst_recommendation_score": float(
                    row.get("analyst_recommendation_score", row.get("recommendation_score", 0.0)) or 0.0
                ),
                "market_sentiment_score": float(row.get("market_sentiment_score", plan.get("market_sentiment_score", 0.0)) or 0.0),
                "data_quality_score": float(row.get("data_quality_score", plan.get("data_quality_score", 1.0)) or 0.0),
                "source_coverage": float(row.get("source_coverage", plan.get("source_coverage", 1.0)) or 0.0),
                "missing_ratio": float(row.get("missing_ratio", plan.get("missing_ratio", 0.0)) or 0.0),
                "expected_cost_bps": float(row.get("expected_cost_bps", plan.get("expected_cost_bps", 0.0)) or 0.0),
                "cost_penalty": float(row.get("cost_penalty", plan.get("cost_penalty", 0.0)) or 0.0),
                "score_before_cost": float(row.get("score_before_cost", plan.get("score_before_cost", row.get("score", 0.0))) or 0.0),
                "execution_score_before_cost": float(
                    row.get("execution_score_before_cost", plan.get("execution_score_before_cost", row.get("execution_score", 0.0))) or 0.0
                ),
                "expected_edge_threshold": float(
                    row.get("expected_edge_threshold", plan.get("expected_edge_threshold", 0.0)) or 0.0
                ),
                "expected_edge_score": float(
                    row.get("expected_edge_score", plan.get("expected_edge_score", 0.0)) or 0.0
                ),
                "expected_edge_bps": float(
                    row.get("expected_edge_bps", plan.get("expected_edge_bps", 0.0)) or 0.0
                ),
                "scan_tier": str(row.get("scan_tier") or normalized_stage),
                "source_reasons": ",".join(source_reasons),
                "entry_style": str(plan.get("entry_style") or ""),
                "execution_ready": int(bool(row.get("execution_ready", plan.get("execution_ready", False)))),
                "details": {
                    "stage_rank": int(idx),
                    "stage1_rank": int(row.get("stage1_rank", 0) or 0),
                    "last_close": float(row.get("last_close", plan.get("last_close", 0.0)) or 0.0),
                    "market_sentiment": str(row.get("market_sentiment") or plan.get("market_sentiment") or ""),
                    "history_source": str(row.get("history_source") or plan.get("history_source") or ""),
                    "history_bar_count": int(row.get("history_bar_count", plan.get("history_bar_count", 0)) or 0),
                    "history_coverage_ratio": float(
                        row.get("history_coverage_ratio", plan.get("history_coverage_ratio", 0.0)) or 0.0
                    ),
                    "freshness_score": float(row.get("freshness_score", plan.get("freshness_score", 0.0)) or 0.0),
                    "spread_proxy_bps": float(row.get("spread_proxy_bps", plan.get("spread_proxy_bps", 0.0)) or 0.0),
                    "slippage_proxy_bps": float(row.get("slippage_proxy_bps", plan.get("slippage_proxy_bps", 0.0)) or 0.0),
                    "commission_proxy_bps": float(row.get("commission_proxy_bps", plan.get("commission_proxy_bps", 0.0)) or 0.0),
                    "liquidity_score": float(row.get("liquidity_score", plan.get("liquidity_score", 0.0)) or 0.0),
                    "avg_daily_dollar_volume": float(
                        row.get("avg_daily_dollar_volume", plan.get("avg_daily_dollar_volume", 0.0)) or 0.0
                    ),
                    "avg_daily_volume": float(row.get("avg_daily_volume", plan.get("avg_daily_volume", 0.0)) or 0.0),
                    "atr_pct": float(row.get("atr_pct", plan.get("atr_pct", 0.0)) or 0.0),
                    "micro_breakout_5m": float(row.get("micro_breakout_5m", plan.get("micro_breakout_5m", 0.0)) or 0.0),
                    "micro_reversal_5m": float(row.get("micro_reversal_5m", plan.get("micro_reversal_5m", 0.0)) or 0.0),
                    "micro_volume_burst_5m": float(row.get("micro_volume_burst_5m", plan.get("micro_volume_burst_5m", 0.0)) or 0.0),
                    "microstructure_score": float(row.get("microstructure_score", plan.get("microstructure_score", 0.0)) or 0.0),
                    "intraday_history_source": str(row.get("intraday_history_source") or plan.get("intraday_history_source") or ""),
                    "intraday_bar_count": int(row.get("intraday_bar_count", plan.get("intraday_bar_count", 0)) or 0),
                    "returns_bar_count": int(row.get("returns_bar_count", plan.get("returns_bar_count", 0)) or 0),
                    "return_series_60d_json": str(row.get("return_series_60d_json") or plan.get("return_series_60d_json") or ""),
                    "returns_ewma_vol_20d": float(row.get("returns_ewma_vol_20d", plan.get("returns_ewma_vol_20d", 0.0)) or 0.0),
                    "returns_downside_vol_20d": float(
                        row.get("returns_downside_vol_20d", plan.get("returns_downside_vol_20d", 0.0)) or 0.0
                    ),
                    "shadow_ml_score": float(row.get("shadow_ml_score", plan.get("shadow_ml_score", 0.0)) or 0.0),
                    "shadow_ml_return": float(row.get("shadow_ml_return", plan.get("shadow_ml_return", 0.0)) or 0.0),
                    "shadow_ml_positive_prob": float(
                        row.get("shadow_ml_positive_prob", plan.get("shadow_ml_positive_prob", 0.0)) or 0.0
                    ),
                    "weekly_feedback_reason": str(row.get("weekly_feedback_reason", plan.get("weekly_feedback_reason", "")) or ""),
                    "weekly_feedback_penalty_kind": str(
                        row.get("weekly_feedback_penalty_kind", plan.get("weekly_feedback_penalty_kind", "")) or ""
                    ),
                    "weekly_feedback_expected_cost_bps_add": float(
                        row.get("weekly_feedback_expected_cost_bps_add", plan.get("weekly_feedback_expected_cost_bps_add", 0.0)) or 0.0
                    ),
                    "weekly_feedback_slippage_proxy_bps_add": float(
                        row.get("weekly_feedback_slippage_proxy_bps_add", plan.get("weekly_feedback_slippage_proxy_bps_add", 0.0)) or 0.0
                    ),
                    "signal_decision": dict(row.get("signal_decision", {}) or plan.get("signal_decision", {}) or {}),
                    "notes": str(plan.get("notes") or ""),
                },
            }
        )


def _score_ranked_candidates(
    candidates: List[str],
    *,
    long_map: Dict[str, Dict[str, Any]],
    mid_map: Dict[str, Dict[str, Any]],
    quality_map: Dict[str, Dict[str, Any]] | None,
    vix: float,
    market_sentiment_score: float,
    macro_high_risk: bool,
    earnings_map: Dict[str, bool],
    fundamentals_map: Dict[str, Dict[str, Any]] | None,
    recommendations_map: Dict[str, Dict[str, Any]] | None,
    scoring_cfg: InvestmentScoringConfig,
) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []
    fundamentals_map = fundamentals_map or {}
    recommendations_map = recommendations_map or {}
    quality_map = quality_map or {}
    for sym in candidates:
        key = str(sym).upper()
        mid_row = mid_map.get(key)
        long_row = long_map.get(key)
        if mid_row is None or long_row is None:
            continue
        quality = dict(quality_map.get(key) or {})
        scored_row = score_investment_candidate(
            long_row,
            mid_row,
            vix=vix,
            market_sentiment_score=float(market_sentiment_score),
            data_quality_score=float(quality.get("data_quality_score", 1.0) or 0.0),
            source_coverage=float(quality.get("source_coverage", 1.0) or 0.0),
            missing_ratio=float(quality.get("missing_ratio", 0.0) or 0.0),
            history_source=str(quality.get("history_source", "") or ""),
            expected_cost_bps=float(quality.get("expected_cost_bps", 0.0) or 0.0),
            spread_proxy_bps=float(quality.get("spread_proxy_bps", 0.0) or 0.0),
            slippage_proxy_bps=float(quality.get("slippage_proxy_bps", 0.0) or 0.0),
            commission_proxy_bps=float(quality.get("commission_proxy_bps", 0.0) or 0.0),
            liquidity_score=float(quality.get("liquidity_score", 1.0) or 0.0),
            avg_daily_dollar_volume=float(quality.get("avg_daily_dollar_volume", 0.0) or 0.0),
            avg_daily_volume=float(quality.get("avg_daily_volume", 0.0) or 0.0),
            atr_pct=float(quality.get("atr_pct", 0.0) or 0.0),
            micro_breakout_5m=float(quality.get("micro_breakout_5m", 0.0) or 0.0),
            micro_reversal_5m=float(quality.get("micro_reversal_5m", 0.0) or 0.0),
            micro_volume_burst_5m=float(quality.get("micro_volume_burst_5m", 0.0) or 0.0),
            microstructure_score=float(quality.get("microstructure_score", 0.0) or 0.0),
            intraday_history_source=str(quality.get("intraday_history_source", "") or ""),
            intraday_bar_count=float(quality.get("intraday_bar_count", 0.0) or 0.0),
            returns_ewma_vol_20d=float(quality.get("returns_ewma_vol_20d", 0.0) or 0.0),
            returns_downside_vol_20d=float(quality.get("returns_downside_vol_20d", 0.0) or 0.0),
            earnings_in_14d=bool(earnings_map.get(key, False)),
            macro_high_risk=macro_high_risk,
            fundamentals=fundamentals_map.get(key, {}),
            recommendation=recommendations_map.get(key, {}),
            cfg=scoring_cfg,
        )
        scored_row.update(
            {
                "history_bar_count": int(quality.get("history_bar_count", 0) or 0),
                "expected_history_bars": int(quality.get("expected_history_bars", 0) or 0),
                "history_coverage_ratio": float(quality.get("history_coverage_ratio", 0.0) or 0.0),
                "freshness_score": float(quality.get("freshness_score", 0.0) or 0.0),
                "expected_cost_bps": float(quality.get("expected_cost_bps", 0.0) or 0.0),
                "spread_proxy_bps": float(quality.get("spread_proxy_bps", 0.0) or 0.0),
                "slippage_proxy_bps": float(quality.get("slippage_proxy_bps", 0.0) or 0.0),
                "commission_proxy_bps": float(quality.get("commission_proxy_bps", 0.0) or 0.0),
                "cost_penalty_source": str(quality.get("cost_penalty_source", "") or ""),
                "liquidity_score": float(quality.get("liquidity_score", 0.0) or 0.0),
                "avg_daily_dollar_volume": float(quality.get("avg_daily_dollar_volume", 0.0) or 0.0),
                "avg_daily_volume": float(quality.get("avg_daily_volume", 0.0) or 0.0),
                "atr_pct": float(quality.get("atr_pct", 0.0) or 0.0),
                "micro_breakout_5m": float(quality.get("micro_breakout_5m", 0.0) or 0.0),
                "micro_reversal_5m": float(quality.get("micro_reversal_5m", 0.0) or 0.0),
                "micro_volume_burst_5m": float(quality.get("micro_volume_burst_5m", 0.0) or 0.0),
                "microstructure_score": float(quality.get("microstructure_score", 0.0) or 0.0),
                "intraday_history_source": str(quality.get("intraday_history_source", "") or ""),
                "intraday_bar_count": int(quality.get("intraday_bar_count", 0) or 0),
                "returns_bar_count": int(quality.get("returns_bar_count", 0) or 0),
                "return_series_60d_json": str(quality.get("return_series_60d_json", "") or ""),
                "returns_ewma_vol_20d": float(quality.get("returns_ewma_vol_20d", 0.0) or 0.0),
                "returns_downside_vol_20d": float(quality.get("returns_downside_vol_20d", 0.0) or 0.0),
            }
        )
        ranked.append(scored_row)
    ranked.sort(key=_rank_sort_key, reverse=True)
    return ranked


def _parse_signal_penalties(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [dict(row) for row in value if isinstance(row, dict)]
    if not isinstance(value, str) or not value:
        return []
    try:
        data = json.loads(value)
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    return [dict(row) for row in data if isinstance(row, dict)]


def _cost_penalty_from_expected_bps(expected_cost_bps: float, scoring_cfg: InvestmentScoringConfig) -> float:
    scaled = float(expected_cost_bps) / max(float(scoring_cfg.cost_penalty_bps_scale), 1e-6)
    return float(scoring_cfg.cost_penalty_weight) * max(0.0, min(1.0, scaled))


def _execution_cost_penalty_from_expected_bps(expected_cost_bps: float, scoring_cfg: InvestmentScoringConfig) -> float:
    scaled = float(expected_cost_bps) / max(float(scoring_cfg.execution_cost_penalty_bps_scale), 1e-6)
    return float(scoring_cfg.execution_cost_penalty_weight) * max(0.0, min(1.0, scaled))


def _weekly_feedback_penalty_map(weekly_feedback_cfg: Dict[str, Any] | None) -> Dict[str, Dict[str, Any]]:
    weekly_feedback_cfg = dict(weekly_feedback_cfg or {})
    rows = [(dict(row), "signal") for row in _parse_signal_penalties(weekly_feedback_cfg.get("signal_penalties"))]
    rows.extend((dict(row), "execution") for row in _parse_signal_penalties(weekly_feedback_cfg.get("execution_penalties")))
    out: Dict[str, Dict[str, Any]] = {}
    for row, penalty_kind in rows:
        symbol = str(row.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        bucket = out.setdefault(
            symbol,
            {
                "symbol": symbol,
                "score_penalty": 0.0,
                "execution_penalty": 0.0,
                "repeat_count": 0,
                "far_below_count": 0,
                "cooldown_days": 0,
                "expected_cost_bps_add": 0.0,
                "slippage_proxy_bps_add": 0.0,
                "session_count": 0,
                "reason_parts": [],
                "penalty_kinds": [],
            },
        )
        bucket["score_penalty"] = float(bucket.get("score_penalty", 0.0) or 0.0) + float(row.get("score_penalty", 0.0) or 0.0)
        bucket["execution_penalty"] = float(bucket.get("execution_penalty", 0.0) or 0.0) + float(row.get("execution_penalty", 0.0) or 0.0)
        bucket["repeat_count"] = max(int(bucket.get("repeat_count", 0) or 0), int(row.get("repeat_count", row.get("hotspot_count", 0)) or 0))
        bucket["far_below_count"] = max(int(bucket.get("far_below_count", 0) or 0), int(row.get("far_below_count", 0) or 0))
        bucket["cooldown_days"] = max(int(bucket.get("cooldown_days", 0) or 0), int(row.get("cooldown_days", 0) or 0))
        bucket["expected_cost_bps_add"] = float(bucket.get("expected_cost_bps_add", 0.0) or 0.0) + float(row.get("expected_cost_bps_add", 0.0) or 0.0)
        bucket["slippage_proxy_bps_add"] = float(bucket.get("slippage_proxy_bps_add", 0.0) or 0.0) + float(row.get("slippage_proxy_bps_add", 0.0) or 0.0)
        bucket["session_count"] = max(int(bucket.get("session_count", 0) or 0), int(row.get("session_count", 0) or 0))
        reason = str(row.get("reason") or f"weekly_feedback_{penalty_kind}_penalty")
        if reason and reason not in bucket["reason_parts"]:
            bucket["reason_parts"].append(reason)
        if penalty_kind not in bucket["penalty_kinds"]:
            bucket["penalty_kinds"].append(penalty_kind)
    for bucket in out.values():
        bucket["reason"] = "+".join(str(part) for part in list(bucket.get("reason_parts", []))[:3]) or "weekly_feedback_penalty"
        bucket["penalty_kind"] = ",".join(str(part) for part in list(bucket.get("penalty_kinds", [])) if str(part).strip())
    return out


def _action_from_adjusted_score(row: Dict[str, Any], scoring_cfg: InvestmentScoringConfig) -> str:
    score = float(row.get("score", row.get("model_recommendation_score", 0.0)) or 0.0)
    if int(row.get("rebalance_flag", 0) or 0) or score <= float(scoring_cfg.reduce_threshold):
        return "REDUCE"
    if (
        score >= float(scoring_cfg.accumulate_threshold)
        and float(row.get("mid_scale", 0.5) or 0.5) >= float(scoring_cfg.min_mid_scale_accumulate)
        and float(row.get("trend_vs_ma200", 0.0) or 0.0) > 0.0
    ):
        return "ACCUMULATE"
    if score >= float(scoring_cfg.hold_threshold):
        return "HOLD"
    return "WATCH"


def _apply_weekly_feedback_penalties(
    ranked_rows: List[Dict[str, Any]],
    *,
    scoring_cfg: InvestmentScoringConfig,
    weekly_feedback_cfg: Dict[str, Any] | None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    penalty_map = _weekly_feedback_penalty_map(weekly_feedback_cfg)
    if not penalty_map:
        return ranked_rows, {"enabled": False, "penalty_symbol_count": 0, "applied_candidate_count": 0, "top_penalty_symbols": []}

    adjusted: List[Dict[str, Any]] = []
    applied_symbols: List[str] = []
    for base_row in ranked_rows:
        row = dict(base_row)
        symbol = str(row.get("symbol") or "").upper().strip()
        penalty = penalty_map.get(symbol)
        if penalty:
            expected_cost_bps_add = max(0.0, float(penalty.get("expected_cost_bps_add", 0.0) or 0.0))
            slippage_proxy_bps_add = max(0.0, float(penalty.get("slippage_proxy_bps_add", 0.0) or 0.0))
            if expected_cost_bps_add > 0.0 or slippage_proxy_bps_add > 0.0:
                old_expected_cost_bps = max(0.0, float(row.get("expected_cost_bps", 0.0) or 0.0))
                old_slippage_proxy_bps = max(0.0, float(row.get("slippage_proxy_bps", 0.0) or 0.0))
                new_expected_cost_bps = old_expected_cost_bps + expected_cost_bps_add
                new_slippage_proxy_bps = old_slippage_proxy_bps + slippage_proxy_bps_add
                row["expected_cost_bps"] = new_expected_cost_bps
                row["slippage_proxy_bps"] = new_slippage_proxy_bps
                cost_penalty_delta = _cost_penalty_from_expected_bps(new_expected_cost_bps, scoring_cfg) - _cost_penalty_from_expected_bps(old_expected_cost_bps, scoring_cfg)
                execution_cost_penalty_delta = (
                    _execution_cost_penalty_from_expected_bps(new_expected_cost_bps, scoring_cfg)
                    - _execution_cost_penalty_from_expected_bps(old_expected_cost_bps, scoring_cfg)
                )
                if cost_penalty_delta > 0.0:
                    row["score"] = float(row.get("score", 0.0) or 0.0) - cost_penalty_delta
                    row["model_recommendation_score"] = float(row.get("model_recommendation_score", row.get("score", 0.0)) or 0.0) - cost_penalty_delta
                    row["cost_penalty"] = float(row.get("cost_penalty", 0.0) or 0.0) + cost_penalty_delta
                if execution_cost_penalty_delta > 0.0:
                    row["execution_score"] = max(-1.0, min(1.0, float(row.get("execution_score", 0.0) or 0.0) - execution_cost_penalty_delta))
                    row["execution_cost_penalty"] = float(row.get("execution_cost_penalty", 0.0) or 0.0) + execution_cost_penalty_delta
                existing_source = str(row.get("cost_penalty_source", "") or "").strip()
                row["cost_penalty_source"] = f"{existing_source}+weekly_feedback_execution_hotspot".strip("+") if existing_source else "weekly_feedback_execution_hotspot"
            score_penalty = max(0.0, float(penalty.get("score_penalty", 0.0) or 0.0))
            execution_penalty = max(0.0, float(penalty.get("execution_penalty", 0.0) or 0.0))
            row["score"] = float(row.get("score", 0.0) or 0.0) - score_penalty
            row["model_recommendation_score"] = float(row.get("model_recommendation_score", row.get("score", 0.0)) or 0.0) - score_penalty
            row["execution_score"] = max(-1.0, min(1.0, float(row.get("execution_score", 0.0) or 0.0) - execution_penalty))
            row["weekly_feedback_applied"] = 1
            row["weekly_feedback_score_penalty"] = float(score_penalty)
            row["weekly_feedback_execution_penalty"] = float(execution_penalty)
            row["weekly_feedback_reason"] = str(penalty.get("reason") or "weekly_feedback_signal_penalty")
            row["weekly_feedback_repeat_count"] = int(penalty.get("repeat_count", 0) or 0)
            row["weekly_feedback_far_below_count"] = int(penalty.get("far_below_count", 0) or 0)
            row["weekly_feedback_cooldown_days"] = int(penalty.get("cooldown_days", 0) or 0)
            row["weekly_feedback_expected_cost_bps_add"] = float(expected_cost_bps_add)
            row["weekly_feedback_slippage_proxy_bps_add"] = float(slippage_proxy_bps_add)
            row["weekly_feedback_penalty_kind"] = str(penalty.get("penalty_kind") or "")
            previous_action = str(row.get("action") or "").upper()
            row["action"] = _action_from_adjusted_score(row, scoring_cfg)
            row["execution_ready"] = int(
                bool(
                    row["action"] in {"ACCUMULATE", "HOLD"}
                    and float(row.get("execution_score", 0.0) or 0.0) >= float(scoring_cfg.execution_ready_threshold)
                    and float(row.get("data_quality_score", 1.0) or 1.0) >= float(scoring_cfg.execution_min_data_quality)
                    and float(row.get("missing_ratio", 0.0) or 0.0) <= float(scoring_cfg.execution_max_missing_ratio)
                    and not (bool(row.get("earnings_in_14d", False)) and row["action"] == "ACCUMULATE")
                )
            )
            decision = dict(row.get("signal_decision", {}) or {})
            blocked = [str(x).strip() for x in list(decision.get("gates_blocked", []) or []) if str(x).strip()]
            reasons = [str(x).strip() for x in list(decision.get("reasons", []) or []) if str(x).strip()]
            context = dict(decision.get("context", {}) or {})
            context.update(
                {
                    "weekly_feedback_applied": True,
                    "weekly_feedback_score_penalty": float(score_penalty),
                    "weekly_feedback_execution_penalty": float(execution_penalty),
                    "weekly_feedback_repeat_count": int(penalty.get("repeat_count", 0) or 0),
                    "weekly_feedback_cooldown_days": int(penalty.get("cooldown_days", 0) or 0),
                    "weekly_feedback_expected_cost_bps_add": float(expected_cost_bps_add),
                    "weekly_feedback_slippage_proxy_bps_add": float(slippage_proxy_bps_add),
                    "weekly_feedback_penalty_kind": str(penalty.get("penalty_kind") or ""),
                }
            )
            if (previous_action != row["action"] or not bool(row.get("execution_ready", False))) and "weekly_feedback_signal" not in blocked:
                blocked.append("weekly_feedback_signal")
            feedback_reason = (
                f"weekly feedback penalized repeated weak signal score_penalty={score_penalty:.3f} "
                f"execution_penalty={execution_penalty:.3f}"
            )
            if expected_cost_bps_add > 0.0 or slippage_proxy_bps_add > 0.0:
                feedback_reason = (
                    f"{feedback_reason} cost_add_bps={expected_cost_bps_add:.1f} "
                    f"slippage_add_bps={slippage_proxy_bps_add:.1f}"
                )
            if feedback_reason not in reasons:
                reasons.append(feedback_reason)
            decision["action"] = str(row["action"])
            decision["gates_blocked"] = blocked
            decision["reasons"] = reasons
            decision["context"] = context
            row["signal_decision"] = decision
            row["signal_decision_json"] = json.dumps(decision, ensure_ascii=False)
            applied_symbols.append(symbol)
        adjusted.append(row)
    adjusted.sort(key=_rank_sort_key, reverse=True)
    return adjusted, {
        "enabled": True,
        "penalty_symbol_count": int(len(penalty_map)),
        "applied_candidate_count": int(len(applied_symbols)),
        "top_penalty_symbols": list(dict.fromkeys(applied_symbols))[:10],
    }


def _load_daily_bars(symbol: str, data_adapter: MarketDataAdapter, days: int) -> tuple[List[Any], str]:
    return data_adapter.get_daily_bars(symbol, days=days)


def _source_coverage_score(history_source: str) -> float:
    source = str(history_source or "").strip().lower()
    if source == "ibkr":
        return 1.0
    if source.startswith("yfinance"):
        return 0.72
    if source:
        return 0.50
    return 0.0


def _freshness_score_from_bars(daily_bars: List[Any]) -> float:
    if not daily_bars:
        return 0.0
    try:
        last_bar = daily_bars[-1]
        last_ts = getattr(last_bar, "time", None)
        if not isinstance(last_ts, datetime):
            return 0.30
        if last_ts.tzinfo is None:
            last_ts = last_ts.replace(tzinfo=timezone.utc)
        age_days = max(0.0, (datetime.now(timezone.utc) - last_ts.astimezone(timezone.utc)).total_seconds() / 86400.0)
    except Exception:
        return 0.30
    if age_days <= 2.5:
        return 1.0
    if age_days <= 5.0:
        return 0.85
    if age_days <= 10.0:
        return 0.60
    if age_days <= 20.0:
        return 0.35
    return 0.10


def _bar_float(bar: Any, field: str) -> float:
    try:
        return float(getattr(bar, field, 0.0) or 0.0)
    except Exception:
        return 0.0


def _commission_proxy_bps(market: str, market_structure: MarketStructureConfig | None = None) -> float:
    if market_structure is not None:
        return float(market_structure.costs.total_one_side_bps())
    market_code = str(market or "").upper().strip()
    if market_code == "US":
        return 1.0
    if market_code == "HK":
        return 10.0
    if market_code == "ASX":
        return 6.0
    if market_code == "XETRA":
        return 5.0
    if market_code == "CN":
        return 8.0
    return 3.0


def _compute_cost_metrics(
    symbol: str,
    *,
    daily_bars: List[Any],
    market: str,
    market_structure: MarketStructureConfig | None = None,
) -> Dict[str, Any]:
    window = list(daily_bars[-20:])
    if not window:
        return {
            "symbol": str(symbol).upper(),
            "cost_penalty_source": "daily_bar_proxy",
            "atr_pct": 0.0,
            "avg_daily_volume": 0.0,
            "avg_daily_dollar_volume": 0.0,
            "liquidity_score": 0.0,
            "spread_proxy_bps": 0.0,
            "slippage_proxy_bps": 0.0,
            "commission_proxy_bps": round(_commission_proxy_bps(market, market_structure), 6),
            "expected_cost_bps": round(_commission_proxy_bps(market, market_structure), 6),
        }

    closes = [_bar_float(bar, "close") for bar in window if _bar_float(bar, "close") > 0.0]
    volumes = [_bar_float(bar, "volume") for bar in window]
    avg_close = float(sum(closes) / len(closes)) if closes else 0.0
    avg_daily_volume = float(sum(volumes) / len(volumes)) if volumes else 0.0
    avg_daily_dollar_volume = float(
        sum(max(0.0, _bar_float(bar, "close")) * max(0.0, _bar_float(bar, "volume")) for bar in window) / max(1, len(window))
    )

    prev_close = avg_close if avg_close > 0.0 else (_bar_float(window[0], "close") or 0.0)
    true_ranges: List[float] = []
    for bar in window:
        high = _bar_float(bar, "high")
        low = _bar_float(bar, "low")
        close = _bar_float(bar, "close")
        if high <= 0.0 and low <= 0.0 and close <= 0.0:
            continue
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        if close > 0.0:
            prev_close = close
    atr_value = float(sum(true_ranges[-14:]) / len(true_ranges[-14:])) if true_ranges[-14:] else 0.0
    atr_pct = float(atr_value / avg_close) if avg_close > 0.0 else 0.0

    liquidity_log = 0.0
    if avg_daily_dollar_volume > 0.0:
        import math

        liquidity_log = math.log10(max(avg_daily_dollar_volume, 1.0))
    liquidity_score = max(0.0, min(1.0, (liquidity_log - 5.0) / 4.0))
    volatility_multiplier = max(0.40, 1.10 - 0.55 * liquidity_score)
    spread_proxy_bps = max(2.0, min(90.0, 3.0 + atr_pct * 900.0 * volatility_multiplier))
    slippage_proxy_bps = max(3.0, min(140.0, 4.0 + atr_pct * 1600.0 * volatility_multiplier))
    commission_proxy_bps = _commission_proxy_bps(market, market_structure)
    expected_cost_bps = spread_proxy_bps + slippage_proxy_bps + commission_proxy_bps
    return {
        "symbol": str(symbol).upper(),
        "cost_penalty_source": "daily_bar_proxy",
        "atr_pct": round(float(atr_pct), 6),
        "avg_daily_volume": round(float(avg_daily_volume), 6),
        "avg_daily_dollar_volume": round(float(avg_daily_dollar_volume), 6),
        "liquidity_score": round(float(liquidity_score), 6),
        "spread_proxy_bps": round(float(spread_proxy_bps), 6),
        "slippage_proxy_bps": round(float(slippage_proxy_bps), 6),
        "commission_proxy_bps": round(float(commission_proxy_bps), 6),
        "expected_cost_bps": round(float(expected_cost_bps), 6),
    }


def _recent_return_series_from_daily_bars(daily_bars: List[Any], *, limit: int = 60) -> List[float]:
    closes: List[float] = []
    for bar in list(daily_bars or []):
        close = _bar_float(bar, "close")
        if close > 0.0:
            closes.append(float(close))
    returns: List[float] = []
    prev_close = 0.0
    for close in closes:
        if prev_close > 0.0:
            returns.append(float((close / prev_close) - 1.0))
        prev_close = close
    if int(limit) > 0:
        returns = returns[-int(limit) :]
    return returns


def _compute_returns_risk_metrics(symbol: str, *, daily_bars: List[Any]) -> Dict[str, Any]:
    returns = _recent_return_series_from_daily_bars(daily_bars, limit=60)
    ewma_lambda = 0.94
    ewma_var = 0.0
    ewma_weight = 0.0
    downside_sq = 0.0
    downside_count = 0
    for ret in returns:
        ewma_var = float(ewma_lambda * ewma_var + (1.0 - ewma_lambda) * (ret * ret))
        ewma_weight = float(ewma_lambda * ewma_weight + (1.0 - ewma_lambda))
        if ret < 0.0:
            downside_sq += float(ret * ret)
            downside_count += 1
    ewma_vol = (ewma_var / ewma_weight) ** 0.5 if ewma_weight > 0.0 else 0.0
    downside_vol = (downside_sq / max(1, downside_count)) ** 0.5 if downside_count > 0 else 0.0
    return {
        "symbol": str(symbol).upper(),
        "returns_bar_count": int(len(returns)),
        "return_series_60d_json": json.dumps([round(float(x), 8) for x in returns], ensure_ascii=False),
        "returns_ewma_vol_20d": round(float(ewma_vol), 8),
        "returns_downside_vol_20d": round(float(downside_vol), 8),
    }


def _compute_microstructure_metrics(
    symbol: str,
    *,
    data_adapter: Any,
    intraday_need: int = 78,
    fallback_days: int = 5,
) -> Dict[str, Any]:
    bars: List[Any] = []
    source = ""
    try:
        bars, source = data_adapter.get_5m_bars_with_source(symbol, need=intraday_need, fallback_days=fallback_days)
    except Exception as e:
        return {
            "symbol": str(symbol).upper(),
            "intraday_history_source": "",
            "intraday_bar_count": 0,
            "micro_breakout_5m": 0.0,
            "micro_reversal_5m": 0.0,
            "micro_volume_burst_5m": 0.0,
            "microstructure_score": 0.0,
            "microstructure_error": f"{type(e).__name__}: {e}",
        }
    closes = [_bar_float(bar, "close") for bar in list(bars or []) if _bar_float(bar, "close") > 0.0]
    opens = [_bar_float(bar, "open") for bar in list(bars or []) if _bar_float(bar, "open") > 0.0]
    volumes = [max(0.0, _bar_float(bar, "volume")) for bar in list(bars or [])]
    count = int(len(closes))
    breakout = 0.0
    reversal = 0.0
    volume_burst = 0.0
    if count >= 12:
        last_close = float(closes[-1])
        prev_window = [float(value) for value in closes[-13:-1] if float(value) > 0.0]
        if prev_window:
            prev_high = max(prev_window)
            prev_low = min(prev_window)
            if prev_high > 0.0:
                breakout = max(-1.0, min(1.0, (last_close / prev_high) - 1.0))
            if prev_high > prev_low:
                reversal = max(-1.0, min(1.0, ((last_close - prev_low) / (prev_high - prev_low)) * 2.0 - 1.0))
    if len(opens) >= 2 and count >= 2:
        intraday_dir = float(closes[-1] - opens[-1])
        prev_dir = float(closes[-2] - opens[-2])
        if abs(prev_dir) > 1e-9 and abs(intraday_dir) > 1e-9:
            reversal = max(-1.0, min(1.0, reversal * 0.60 + (-prev_dir / abs(prev_dir)) * (intraday_dir / abs(intraday_dir)) * 0.40))
    if len(volumes) >= 12:
        last_volume = float(volumes[-1])
        avg_volume = float(sum(volumes[-12:-1]) / max(1, len(volumes[-12:-1])))
        if avg_volume > 0.0:
            volume_burst = max(-1.0, min(1.0, (last_volume / avg_volume) - 1.0))
    # 微观结构先主要服务执行择时，不让 5m 噪音直接主导中长期 alpha。
    micro_score = max(-1.0, min(1.0, 0.45 * breakout + 0.25 * reversal + 0.30 * volume_burst))
    return {
        "symbol": str(symbol).upper(),
        "intraday_history_source": str(source or ""),
        "intraday_bar_count": count,
        "micro_breakout_5m": round(float(breakout), 8),
        "micro_reversal_5m": round(float(reversal), 8),
        "micro_volume_burst_5m": round(float(volume_burst), 8),
        "microstructure_score": round(float(micro_score), 8),
        "microstructure_error": "",
    }


def _compute_data_quality_metrics(
    symbol: str,
    *,
    daily_bars: List[Any],
    history_source: str,
    shared_days: int,
) -> Dict[str, Any]:
    history_bar_count = int(len(list(daily_bars or [])))
    expected_bars = max(1, int(shared_days or 1))
    history_coverage = max(0.0, min(1.0, float(history_bar_count) / float(expected_bars)))
    missing_ratio = max(0.0, min(1.0, 1.0 - history_coverage))
    source_coverage = _source_coverage_score(history_source)
    freshness_score = _freshness_score_from_bars(daily_bars)
    data_quality_score = max(
        0.0,
        min(
            1.0,
            0.55 * history_coverage
            + 0.30 * source_coverage
            + 0.15 * freshness_score,
        ),
    )
    return {
        "symbol": str(symbol).upper(),
        "history_source": str(history_source or ""),
        "history_bar_count": history_bar_count,
        "expected_history_bars": expected_bars,
        "history_coverage_ratio": round(history_coverage, 6),
        "source_coverage": round(source_coverage, 6),
        "missing_ratio": round(missing_ratio, 6),
        "freshness_score": round(freshness_score, 6),
        "data_quality_score": round(data_quality_score, 6),
    }


def _compute_symbol_features_from_daily_bars(
    symbol: str,
    *,
    daily_bars: List[Any],
    history_source: str,
    market: str,
    market_structure: MarketStructureConfig | None,
    mid_lookback_days: int,
    long_years: int,
    regime_cfg: RegimeConfig,
) -> Dict[str, Any]:
    mid_row = None
    long_row = None
    history_error = ""
    try:
        mid_row = compute_mid_from_bars(
            symbol,
            daily_bars,
            regime_cfg=regime_cfg,
            lookback_days=mid_lookback_days,
        )
        long_row = compute_long_from_bars(symbol, daily_bars, years=long_years)
    except Exception as e:
        history_error = f"{type(e).__name__}: {e}"
    quality_metrics = _compute_data_quality_metrics(
        symbol,
        daily_bars=daily_bars,
        history_source=history_source,
        shared_days=max(mid_lookback_days, 252 * max(1, int(long_years))),
    )
    # 这里把成本代理和数据质量放在同一次历史遍历里完成。
    # 目的不是混淆口径，而是避免为同一批日线重复算两遍，减少协作时的隐式性能坑。
    quality_metrics.update(
        _compute_cost_metrics(
            symbol,
            daily_bars=daily_bars,
            market=market,
            market_structure=market_structure,
        )
    )
    quality_metrics.update(_compute_returns_risk_metrics(symbol, daily_bars=daily_bars))
    return {
        "symbol": symbol,
        "history_source": history_source,
        "mid_row": mid_row,
        "long_row": long_row,
        "history_error": history_error,
        "quality_metrics": quality_metrics,
    }


def _prefetch_symbol_daily_histories(
    symbols: List[str],
    *,
    data_adapter: Any,
    shared_days: int,
    progress_interval: int,
) -> Dict[str, Dict[str, Any]]:
    total = len(symbols)
    out: Dict[str, Dict[str, Any]] = {}
    for idx, symbol in enumerate(symbols, start=1):
        daily_bars: List[Any] = []
        history_source = ""
        history_error = ""
        try:
            daily_bars, history_source = _load_daily_bars(symbol, data_adapter, shared_days)
        except Exception as e:
            history_error = f"{type(e).__name__}: {e}"
        out[str(symbol)] = {
            "symbol": symbol,
            "daily_bars": daily_bars,
            "history_source": history_source,
            "history_error": history_error,
        }
        if idx == 1 or idx % max(1, int(progress_interval)) == 0 or idx == total:
            log.info(
                "Owner-thread history prefetch %s/%s fetched=%s last=%s source=%s",
                idx,
                total,
                int(bool(daily_bars)),
                symbol,
                history_source or "missing",
            )
    return out


def _collect_symbol_feature_results(
    symbols: List[str],
    *,
    data_adapter: Any,
    market: str,
    market_structure: MarketStructureConfig | None = None,
    shared_days: int,
    mid_lookback_days: int,
    long_years: int,
    regime_cfg: RegimeConfig,
    history_workers: int,
    progress_interval: int,
    owner_thread_history: bool,
) -> List[Dict[str, Any]]:
    total = len(symbols)
    if not total:
        return []
    if owner_thread_history:
        prefetched = _prefetch_symbol_daily_histories(
            symbols,
            data_adapter=data_adapter,
            shared_days=shared_days,
            progress_interval=progress_interval,
        )
        if int(history_workers) <= 1:
            results: List[Dict[str, Any]] = []
            for symbol in symbols:
                payload = dict(prefetched.get(symbol, {}) or {})
                result = _compute_symbol_features_from_daily_bars(
                    symbol,
                    daily_bars=list(payload.get("daily_bars", []) or []),
                    history_source=str(payload.get("history_source", "") or ""),
                    market=market,
                    market_structure=market_structure,
                    mid_lookback_days=mid_lookback_days,
                    long_years=long_years,
                    regime_cfg=regime_cfg,
                )
                prefetch_error = str(payload.get("history_error", "") or "")
                if prefetch_error:
                    result["history_error"] = (
                        f"{prefetch_error}; {result['history_error']}"
                        if str(result.get("history_error", "") or "")
                        else prefetch_error
                    )
                results.append(result)
            return results
        results = []
        with ThreadPoolExecutor(max_workers=max(1, int(history_workers))) as executor:
            futures = {
                executor.submit(
                    _compute_symbol_features_from_daily_bars,
                    symbol,
                    daily_bars=list(dict(prefetched.get(symbol, {}) or {}).get("daily_bars", []) or []),
                    history_source=str(dict(prefetched.get(symbol, {}) or {}).get("history_source", "") or ""),
                    market=market,
                    market_structure=market_structure,
                    mid_lookback_days=mid_lookback_days,
                    long_years=long_years,
                    regime_cfg=regime_cfg,
                ): symbol
                for symbol in symbols
            }
            for future in as_completed(futures):
                symbol = futures[future]
                result = dict(future.result() or {})
                payload = dict(prefetched.get(symbol, {}) or {})
                prefetch_error = str(payload.get("history_error", "") or "")
                if prefetch_error:
                    result["history_error"] = (
                        f"{prefetch_error}; {result['history_error']}"
                        if str(result.get("history_error", "") or "")
                        else prefetch_error
                    )
                results.append(result)
        return results

    results = []
    with ThreadPoolExecutor(max_workers=max(1, int(history_workers))) as executor:
        futures = {
            executor.submit(
                _compute_symbol_features,
                symbol,
                data_adapter=data_adapter,
                market=market,
                market_structure=market_structure,
                shared_days=shared_days,
                mid_lookback_days=mid_lookback_days,
                long_years=long_years,
                regime_cfg=regime_cfg,
            ): symbol
            for symbol in symbols
        }
        for future in as_completed(futures):
            results.append(dict(future.result() or {}))
    return results


def _summarize_data_quality(
    feature_results: List[Dict[str, Any]],
    *,
    ranked_rows: List[Dict[str, Any]],
    low_quality_threshold: float,
) -> Dict[str, Any]:
    quality_rows = [dict(row.get("quality_metrics") or {}) for row in list(feature_results or []) if isinstance(row.get("quality_metrics"), dict)]
    ranked_quality_rows = [dict(row) for row in list(ranked_rows or []) if str(row.get("symbol") or "").strip()]

    def _avg(rows: List[Dict[str, Any]], key: str) -> float:
        values = [float(row.get(key, 0.0) or 0.0) for row in rows if row.get(key) is not None]
        if not values:
            return 0.0
        return float(sum(values) / len(values))

    return {
        "feature_symbol_count": int(len(quality_rows)),
        "ranked_symbol_count": int(len(ranked_quality_rows)),
        "avg_data_quality_score": round(_avg(quality_rows, "data_quality_score"), 6),
        "avg_source_coverage": round(_avg(quality_rows, "source_coverage"), 6),
        "avg_missing_ratio": round(_avg(quality_rows, "missing_ratio"), 6),
        "avg_history_coverage_ratio": round(_avg(quality_rows, "history_coverage_ratio"), 6),
        "ranked_avg_data_quality_score": round(_avg(ranked_quality_rows, "data_quality_score"), 6),
        "ranked_avg_missing_ratio": round(_avg(ranked_quality_rows, "missing_ratio"), 6),
        "low_quality_count": int(
            sum(1 for row in quality_rows if float(row.get("data_quality_score", 0.0) or 0.0) < float(low_quality_threshold))
        ),
        "ranked_low_quality_count": int(
            sum(1 for row in ranked_quality_rows if float(row.get("data_quality_score", 0.0) or 0.0) < float(low_quality_threshold))
        ),
        "low_quality_threshold": float(low_quality_threshold),
    }


def _avg_defined(values: List[Any]) -> float:
    nums = [float(value) for value in values if value is not None]
    if not nums:
        return 0.0
    return float(sum(nums) / len(nums))


def _summarize_cost_profile(
    ranked_rows: List[Dict[str, Any]],
    *,
    high_cost_threshold_bps: float,
    low_liquidity_threshold: float = 2_000_000.0,
) -> Dict[str, Any]:
    def _avg(key: str) -> float:
        values = [float(row.get(key, 0.0) or 0.0) for row in list(ranked_rows or []) if row.get(key) is not None]
        if not values:
            return 0.0
        return float(sum(values) / len(values))

    return {
        "ranked_symbol_count": int(len(list(ranked_rows or []))),
        "avg_expected_cost_bps": round(_avg("expected_cost_bps"), 6),
        "avg_spread_proxy_bps": round(_avg("spread_proxy_bps"), 6),
        "avg_slippage_proxy_bps": round(_avg("slippage_proxy_bps"), 6),
        "avg_commission_proxy_bps": round(_avg("commission_proxy_bps"), 6),
        "avg_atr_pct": round(_avg("atr_pct"), 6),
        "avg_daily_dollar_volume": round(_avg("avg_daily_dollar_volume"), 6),
        "high_cost_count": int(
            sum(1 for row in list(ranked_rows or []) if float(row.get("expected_cost_bps", 0.0) or 0.0) >= float(high_cost_threshold_bps))
        ),
        "low_liquidity_count": int(
            sum(
                1
                for row in list(ranked_rows or [])
                if float(row.get("avg_daily_dollar_volume", 0.0) or 0.0) < float(low_liquidity_threshold)
            )
        ),
        "high_cost_threshold_bps": float(high_cost_threshold_bps),
        "low_liquidity_threshold": float(low_liquidity_threshold),
    }


class _YfOnlyMarketData:
    def register(self, symbol: str, contract: Any) -> None:
        return None

    def get_daily_bars(self, symbol: str, days: int) -> List[Any]:
        return fetch_daily_bars_yf(symbol, days=days)


class _YfOnlyDataAdapter:
    def __init__(self) -> None:
        self.md = _YfOnlyMarketData()

    def register(self, symbol: str, contract: Any) -> None:
        return None

    def get_daily_bars(self, symbol: str, days: int) -> tuple[List[Any], str]:
        bars = fetch_daily_bars_yf(symbol, days=days)
        if bars:
            return bars, "yfinance"
        return [], ""


@dataclass
class LayeredScanConfig:
    enabled: bool = True
    include_symbol_master: bool = True
    include_recent: bool = False
    include_scanner: bool = False
    broad_limit: int = 0
    deep_limit: int = 0
    enrichment_limit: int = 0
    history_workers: int = 4
    network_reserve_ratio: float = 0.40
    progress_interval: int = 20
    scanner_limit: int = 24


def _clamp_unit(value: float) -> float:
    return max(-1.0, min(1.0, float(value)))


def _dedupe_keep_order(symbols: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw_symbol in list(symbols or []):
        symbol = str(raw_symbol or "").upper().strip()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def _effective_worker_count(requested_workers: int, reserve_ratio: float) -> int:
    reserve = max(0.0, min(0.85, float(reserve_ratio)))
    effective = int(round(max(1.0, float(requested_workers)) * max(0.15, 1.0 - reserve)))
    return max(1, effective)


def _normalize_market_symbol(raw_symbol: str, market: str) -> str:
    symbol = str(raw_symbol or "").upper().strip()
    if not symbol:
        return ""
    code = str(market or "").upper().strip()
    if code == "HK":
        if symbol.startswith("HK:"):
            symbol = symbol.split(":", 1)[1].strip()
        if symbol.endswith(".HK"):
            symbol = symbol[:-3].strip()
        digits = "".join(ch for ch in symbol if ch.isdigit())
        if digits:
            return f"{digits.zfill(4)}.HK"
    if code == "ASX":
        if symbol.startswith("ASX:") or symbol.startswith("AU:"):
            symbol = symbol.split(":", 1)[1].strip()
        if symbol.endswith(".AX"):
            return symbol
        if "." not in symbol:
            return f"{symbol}.AX"
    if code == "CN":
        if symbol.startswith("CN:") or symbol.startswith("SSE:") or symbol.startswith("SZSE:"):
            symbol = symbol.split(":", 1)[1].strip()
        if symbol.endswith(".SS") or symbol.endswith(".SZ"):
            return symbol
        digits = "".join(ch for ch in symbol if ch.isdigit())
        if len(digits) == 6:
            suffix = ".SS" if digits.startswith(("5", "6", "9")) else ".SZ"
            return f"{digits}{suffix}"
    return symbol


def _scanner_cache_get(db_path: str, codes_key: str, ttl_sec: int) -> List[str]:
    try:
        c = sqlite3.connect(db_path)
        try:
            c.execute(
                """create table if not exists scanner_cache(
                    id integer primary key autoincrement,
                    ts integer not null,
                    codes_key text not null,
                    symbols_json text not null
                )"""
            )
            now = int(time.time())
            row = c.execute(
                "select ts, symbols_json from scanner_cache where codes_key=? and ts>=? order by ts desc limit 1",
                (codes_key, now - int(ttl_sec)),
            ).fetchone()
            if not row:
                return []
            syms = json.loads(row[1]) if row[1] else []
            return [str(x).upper() for x in syms if x] if isinstance(syms, list) else []
        finally:
            c.close()
    except Exception:
        return []


def _scanner_cache_put(db_path: str, codes_key: str, symbols: List[str]) -> None:
    try:
        c = sqlite3.connect(db_path)
        try:
            c.execute(
                """create table if not exists scanner_cache(
                    id integer primary key autoincrement,
                    ts integer not null,
                    codes_key text not null,
                    symbols_json text not null
                )"""
            )
            c.execute(
                "insert into scanner_cache(ts, codes_key, symbols_json) values(?,?,?)",
                (int(time.time()), codes_key, json.dumps(list(symbols or []))),
            )
            c.commit()
        finally:
            c.close()
    except Exception:
        return None


def _scanner_symbols(
    ib: Any,
    *,
    db_path: str,
    market: str,
    ibkr_cfg: Dict[str, Any],
    layered_cfg: LayeredScanConfig,
) -> List[str]:
    if ib is None or not bool(layered_cfg.include_scanner):
        return []
    if not bool(ibkr_cfg.get("scanner_enabled", False)):
        return []

    codes = [str(x).strip() for x in list(ibkr_cfg.get("scanner_codes", ["HOT_BY_VOLUME", "TOP_PERC_GAIN", "TOP_PERC_LOSE"])) if str(x).strip()]
    scanner_location_codes = scanner_location_codes_from_config(ibkr_cfg, default="")
    if not codes or not scanner_location_codes:
        return []

    scanner_limit = max(1, min(int(layered_cfg.scanner_limit or 24), int(layered_cfg.broad_limit or 24)))
    scanner_refresh_sec = int(ibkr_cfg.get("scanner_refresh_sec", 120) or 120)
    scanner_max_codes_per_run = int(ibkr_cfg.get("scanner_max_codes_per_run", 3) or 3)
    scanner_instrument = str(ibkr_cfg.get("scanner_instrument", "STK") or "STK").strip()
    codes_key = (
        f"investment|{str(market).upper()}|{scanner_instrument}|{','.join(scanner_location_codes)}|"
        f"{','.join(codes[:scanner_max_codes_per_run])}|limit={scanner_limit}"
    )
    cached = _scanner_cache_get(db_path, codes_key=codes_key, ttl_sec=scanner_refresh_sec)
    if cached:
        return cached

    try:
        uni = UniverseService(
            ib,
            UniverseConfig(
                max_short_candidates=scanner_limit,
                scanner_enabled=True,
                scanner_instrument=scanner_instrument,
                scanner_location_codes=scanner_location_codes,
                scanner_location_code=str(scanner_location_codes[0]),
                scanner_limit=scanner_limit,
                scanner_refresh_sec=scanner_refresh_sec,
                scanner_max_codes_per_run=scanner_max_codes_per_run,
                scanner_codes=codes,
                seed_symbols=[],
                phase3_enabled=False,
            ),
            storage=Storage(db_path) if db_path else None,
            md=None,
        )
        res = uni.build()
    except Exception as e:
        log.warning("investment scanner failed market=%s: %s %s", market, type(e).__name__, e)
        return []

    normalized = _dedupe_keep_order(
        [
            _normalize_market_symbol(str(raw_symbol), market)
            for raw_symbol in list((res or {}).get("hot", []) or [])
        ]
    )
    filtered = [symbol for symbol in normalized if symbol_matches_market(symbol, market)]
    if filtered:
        _scanner_cache_put(db_path, codes_key=codes_key, symbols=filtered)
    return filtered


def _maybe_collect_research_scanner_symbols(
    *,
    resolved_market: str,
    host: str,
    port: int,
    client_id: int,
    request_timeout_sec: float,
    db_path: str,
    ibkr_cfg: Dict[str, Any],
    layered_cfg: LayeredScanConfig,
) -> List[str]:
    if not bool(layered_cfg.include_scanner) or not bool(ibkr_cfg.get("scanner_enabled", False)):
        return []

    ib = None
    try:
        ib = connect_ib(host, port, client_id, request_timeout=float(request_timeout_sec))
        set_delayed_frozen(ib)
        return _scanner_symbols(
            ib,
            db_path=db_path,
            market=resolved_market,
            ibkr_cfg=ibkr_cfg,
            layered_cfg=layered_cfg,
        )
    except Exception as e:
        log.warning("research-only scanner expansion failed market=%s: %s %s", resolved_market, type(e).__name__, e)
        return []
    finally:
        try:
            if ib is not None:
                ib.disconnect()
        except Exception:
            pass


def _layered_scan_config(
    *,
    market_universe_cfg: Dict[str, Any],
    investment_cfg: Dict[str, Any],
    ibkr_cfg: Dict[str, Any],
    max_universe: int,
    top_n: int,
    fundamentals_top_k: int,
    backtest_top_k: int,
    use_audit_recent: bool,
    research_only_yfinance: bool,
) -> LayeredScanConfig:
    raw: Dict[str, Any] = {}
    raw.update(dict(investment_cfg.get("layered_scan", {}) or {}))
    raw.update(dict(market_universe_cfg.get("layered_scan", {}) or {}))
    broad_limit = min(
        int(max_universe),
        max(int(top_n), int(raw.get("broad_limit", max_universe) or max_universe)),
    )
    deep_default = min(
        broad_limit,
        max(int(top_n) * 4, int(fundamentals_top_k) * 3, int(backtest_top_k) * 4, 24),
    )
    deep_limit = min(broad_limit, max(int(top_n), int(raw.get("deep_limit", deep_default) or deep_default)))
    enrichment_default = min(deep_limit, max(int(top_n) * 3, int(fundamentals_top_k), int(backtest_top_k) * 2, 16))
    enrichment_limit = min(
        deep_limit,
        max(int(top_n), int(raw.get("enrichment_limit", enrichment_default) or enrichment_default)),
    )
    include_scanner_default = bool(ibkr_cfg.get("scanner_enabled", False)) and not bool(research_only_yfinance)
    return LayeredScanConfig(
        enabled=bool(raw.get("enabled", True)),
        include_symbol_master=bool(raw.get("include_symbol_master", True)),
        include_recent=bool(raw.get("include_recent", use_audit_recent)),
        include_scanner=bool(raw.get("include_scanner", include_scanner_default)),
        broad_limit=broad_limit,
        deep_limit=deep_limit,
        enrichment_limit=enrichment_limit,
        history_workers=max(1, int(raw.get("history_workers", 4) or 4)),
        network_reserve_ratio=max(0.0, min(0.85, float(raw.get("network_reserve_ratio", 0.40) or 0.40))),
        progress_interval=max(1, int(raw.get("progress_interval", 20) or 20)),
        scanner_limit=max(1, int(raw.get("scanner_limit", 24) or 24)),
    )


def _build_market_sentiment(bundle: Dict[str, Any], market: str) -> Dict[str, Any]:
    tickers = dict(bundle.get("markets", {}).get("tickers", {}) or {})
    market_code = str(market or "").upper().strip()
    benchmark_symbol = {
        "US": "SPY",
        "HK": "2800.HK",
        "ASX": "VAS.AX",
        "CN": "510300.SS",
        "XETRA": "EXS1.DE",
        "UK": "ISF.L",
    }.get(market_code, "SPY")
    benchmark = dict(tickers.get(benchmark_symbol, {}) or {})
    breadth_samples: List[float] = []
    leadership_samples: List[float] = []
    breadth_positive = 0
    for symbol, info in tickers.items():
        if str(symbol).upper() == "^VIX":
            continue
        try:
            ret1d = float(info.get("ret1d", 0.0) or 0.0)
            breadth_samples.append(ret1d)
            if ret1d > 0.0:
                breadth_positive += 1
        except Exception:
            pass
        try:
            leadership_samples.append(float(info.get("ret5d", 0.0) or 0.0))
        except Exception:
            pass
    breadth = sum(breadth_samples) / float(len(breadth_samples) or 1)
    leadership = sum(leadership_samples) / float(len(leadership_samples) or 1)
    breadth_positive_ratio = float(breadth_positive) / float(len(breadth_samples) or 1)
    breadth_dispersion = 0.0
    if len(breadth_samples) >= 2:
        mean_ret = breadth
        breadth_dispersion = (
            sum((float(sample) - mean_ret) ** 2 for sample in breadth_samples) / float(len(breadth_samples))
        ) ** 0.5
    benchmark_ret1d = float(benchmark.get("ret1d", 0.0) or 0.0)
    benchmark_ret5d = float(benchmark.get("ret5d", 0.0) or 0.0)
    leadership_spread = float(leadership - benchmark_ret5d)
    vix = _extract_vix(bundle)
    macro_risk = _macro_high_risk(bundle)

    score = (
        0.30 * _clamp_unit(benchmark_ret1d * 10.0)
        + 0.18 * _clamp_unit(benchmark_ret5d * 4.0)
        + 0.20 * _clamp_unit((breadth_positive_ratio - 0.5) * 2.0)
        + 0.17 * _clamp_unit(breadth * 10.0)
        + 0.10 * _clamp_unit(leadership_spread * 5.0)
        - 0.05 * _clamp_unit(breadth_dispersion * 12.0)
    )
    if macro_risk:
        score -= 0.20
    if vix >= 25.0:
        score -= 0.25
    elif vix >= 18.0:
        score -= 0.10
    score = _clamp_unit(score)

    if score >= 0.25:
        label = "RISK_ON"
        guidance = "偏积极，优先等待高质量回撤入场。"
    elif score <= -0.25:
        label = "DEFENSIVE"
        guidance = "偏防守，优先观望或放慢加仓速度。"
    else:
        label = "BALANCED"
        guidance = "中性偏谨慎，只保留质量更高的 setup。"

    return {
        "market": market_code,
        "benchmark_symbol": benchmark_symbol,
        "benchmark_ret1d": float(benchmark_ret1d),
        "benchmark_ret5d": float(benchmark_ret5d),
        "breadth_ret1d": float(breadth),
        "breadth_positive_ratio": float(breadth_positive_ratio),
        "breadth_dispersion_1d": float(breadth_dispersion),
        "leaders_ret5d": float(leadership),
        "leadership_spread_5d": float(leadership_spread),
        "score": float(score),
        "label": label,
        "guidance": guidance,
    }


def _compute_symbol_features(
    symbol: str,
    *,
    data_adapter: Any,
    market: str,
    market_structure: MarketStructureConfig | None,
    shared_days: int,
    mid_lookback_days: int,
    long_years: int,
    regime_cfg: RegimeConfig,
) -> Dict[str, Any]:
    daily_bars: List[Any] = []
    history_source = ""
    mid_row = None
    long_row = None
    history_error = ""
    try:
        daily_bars, history_source = _load_daily_bars(symbol, data_adapter, shared_days)
        mid_row = compute_mid_from_bars(
            symbol,
            daily_bars,
            regime_cfg=regime_cfg,
            lookback_days=mid_lookback_days,
        )
        long_row = compute_long_from_bars(symbol, daily_bars, years=long_years)
    except Exception as e:
        history_error = f"{type(e).__name__}: {e}"
    quality_metrics = _compute_data_quality_metrics(
        symbol,
        daily_bars=daily_bars,
        history_source=history_source,
        shared_days=shared_days,
    )
    quality_metrics.update(_compute_cost_metrics(symbol, daily_bars=daily_bars, market=market, market_structure=market_structure))
    quality_metrics.update(_compute_returns_risk_metrics(symbol, daily_bars=daily_bars))
    return {
        "symbol": symbol,
        "history_source": history_source,
        "mid_row": mid_row,
        "long_row": long_row,
        "history_error": history_error,
        "quality_metrics": quality_metrics,
    }


def _market_structure_context(structure: MarketStructureConfig) -> Dict[str, Any]:
    return {
        "market": str(structure.market or ""),
        "market_scope": str(structure.market_scope or ""),
        "benchmark_symbol": str(structure.benchmark_symbol or ""),
        "research_only": bool(structure.research_only),
        "strategy_bias": str(structure.strategy_bias or ""),
        "costs": {
            **structure.costs.__dict__,
            "total_one_side_bps": float(structure.costs.total_one_side_bps()),
        },
        "order_rules": dict(structure.order_rules.__dict__),
        "account_rules": dict(structure.account_rules.__dict__),
        "portfolio_preferences": dict(structure.portfolio_preferences.__dict__),
        "notes": list(structure.notes),
    }


def _cli_summary_payload(
    *,
    market: str,
    portfolio_id: str,
    out_dir: Path,
    candidate_count: int,
    ranked_count: int,
    short_candidate_count: int,
    plan_count: int,
    backtest_count: int,
) -> tuple[Dict[str, Any], Dict[str, Path]]:
    return (
        {
            "market": str(market or "DEFAULT"),
            "portfolio_id": str(portfolio_id or "-"),
            "candidate_count": int(candidate_count),
            "ranked_count": int(ranked_count),
            "short_candidate_count": int(short_candidate_count),
            "plan_count": int(plan_count),
            "backtest_count": int(backtest_count),
        },
        {
            "report_md": out_dir / "investment_report.md",
            "ranked_csv": out_dir / "investment_candidates.csv",
            "plan_csv": out_dir / "investment_plan.csv",
            "backtest_csv": out_dir / "investment_backtest.csv",
            "enrichment_json": out_dir / "enrichment.json",
        },
    )


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)

    market_code = resolve_market_code(getattr(args, "market", ""))
    explicit_cfg = str(args.ibkr_config) if str(args.ibkr_config) != "config/ibkr.yaml" or not market_code else ""
    ibkr_cfg_path = str(market_config_path(BASE_DIR, market_code, explicit_cfg))
    resolved_market = market_code or infer_market_from_config_path(ibkr_cfg_path) or "DEFAULT"
    ibkr_cfg = _load_yaml(ibkr_cfg_path)
    market_universe_cfg = load_market_universe_config(BASE_DIR, resolved_market)
    investment_cfg_path = _resolve_project_path(
        args.investment_config
        or str(
            ibkr_cfg.get(
                "investment_config",
                f"config/investment_{resolved_market.lower()}.yaml" if resolved_market != "DEFAULT" else "config/investment.yaml",
            )
        )
    )
    investment_cfg = _load_yaml(investment_cfg_path)
    strategy_cfg_path = _resolve_project_path(str(ibkr_cfg.get("strategy_config", "config/strategy_defaults.yaml")))
    regime_adaptor_cfg_path = _resolve_project_path(str(ibkr_cfg.get("regime_adaptor_config", "config/regime_adaptor.yaml")))
    market_structure_cfg_path = _resolve_project_path(
        str(
            args.market_structure_config
            or ibkr_cfg.get(
                "market_structure_config",
                f"config/market_structure_{resolved_market.lower()}.yaml" if resolved_market != "DEFAULT" else "config/market_structure.yaml",
            )
        )
    )
    adaptive_strategy_cfg_path = _resolve_project_path(
        str(
            args.adaptive_strategy_config
            or ibkr_cfg.get("adaptive_strategy_config", "config/adaptive_strategy_framework.yaml")
        )
    )
    strategy_cfg = _load_yaml(strategy_cfg_path)
    report_cfg_path = _resolve_project_path(str(ibkr_cfg.get("report_config", "config/report_scoring.yaml")))
    report_cfg = _load_yaml(report_cfg_path)
    risk_cfg_path = _resolve_project_path(str(ibkr_cfg.get("risk_config", "config/risk.yaml")))
    risk_cfg = _load_yaml(risk_cfg_path)
    regime_adaptor_cfg_raw = _load_yaml(regime_adaptor_cfg_path)
    market_structure = load_market_structure(BASE_DIR, resolved_market, market_structure_cfg_path)
    adaptive_strategy = load_adaptive_strategy(BASE_DIR, adaptive_strategy_cfg_path)
    scoring_cfg = InvestmentScoringConfig.from_dict(investment_cfg.get("scoring"))
    plan_cfg = InvestmentPlanConfig.from_dict(investment_cfg.get("plan"))
    plan_cfg = apply_adaptive_strategy_plan_overrides(plan_cfg, adaptive_strategy, market=resolved_market)
    adaptive_market_plan = adaptive_strategy_market_plan_overrides(adaptive_strategy, resolved_market)
    adaptive_market_regime = adaptive_strategy_market_regime_overrides(adaptive_strategy, resolved_market)
    adaptive_market_execution = adaptive_strategy_market_execution_overrides(adaptive_strategy, resolved_market)
    backtest_cfg = InvestmentBacktestConfig.from_dict(investment_cfg.get("backtest"))
    short_book_cfg = InvestmentShortBookConfig.from_dict(investment_cfg.get("short_book"), market=resolved_market)
    shadow_ml_cfg = InvestmentShadowModelConfig.from_dict(investment_cfg.get("shadow_ml"))
    weekly_feedback_cfg = dict(investment_cfg.get("weekly_feedback", {}) or {})

    host = ibkr_cfg["host"]
    port = int(ibkr_cfg["port"])
    client_id = int(ibkr_cfg["client_id"])
    db_path = _resolve_project_path(args.db)
    symbol_master_db_path = _resolve_project_path(args.symbol_master_db or str(market_universe_cfg.get("symbol_master_db", "symbol_master.db")))
    default_watchlist_yaml = str(
        market_universe_cfg.get("report_watchlist_yaml", ibkr_cfg.get("report_watchlist_yaml", ibkr_cfg.get("seed_watchlist_yaml", ""))) or ""
    )
    watchlist_yaml = _resolve_project_path(args.watchlist_yaml or default_watchlist_yaml) if (args.watchlist_yaml or default_watchlist_yaml) else ""
    out_dir_arg = args.out_dir or f"reports_investment_{(resolved_market or 'default').lower()}"
    log.info(
        "Using market=%s IBKR config=%s investment_config=%s market_structure_config=%s adaptive_strategy_config=%s",
        resolved_market,
        ibkr_cfg_path,
        investment_cfg_path,
        market_structure_cfg_path,
        adaptive_strategy_cfg_path,
    )

    research_only_yfinance = bool(
        ibkr_cfg.get("research_only_yfinance", False)
        or market_universe_cfg.get("research_only_yfinance", False)
        or investment_cfg.get("research_only_yfinance", False)
    )
    symbol_master_symbols = load_symbols_from_symbol_master(symbol_master_db_path, resolved_market if resolved_market != "DEFAULT" else "")
    watchlist_symbols = load_watchlist_symbols(watchlist_yaml) if watchlist_yaml else []
    layered_cfg = _layered_scan_config(
        market_universe_cfg=market_universe_cfg,
        investment_cfg=investment_cfg,
        ibkr_cfg=ibkr_cfg,
        max_universe=int(args.max_universe),
        top_n=int(args.top_n),
        fundamentals_top_k=int(args.fundamentals_top_k),
        backtest_top_k=int(args.backtest_top_k),
        use_audit_recent=bool(args.use_audit_recent),
        research_only_yfinance=research_only_yfinance,
    )

    seed_symbols = _dedupe_keep_order(
        list(watchlist_symbols)
        + (list(symbol_master_symbols) if bool(layered_cfg.include_symbol_master) else [])
    )
    recent_symbols = (
        read_recent_symbols_from_audit(db_path, limit=int(args.audit_limit))
        if bool(layered_cfg.include_recent)
        else []
    )
    if resolved_market != "DEFAULT":
        seed_symbols = _filter_symbols_for_market(seed_symbols, resolved_market)
        recent_symbols = _filter_symbols_for_market(recent_symbols, resolved_market)

    universe = build_candidates(
        seed_symbols=[str(sym).upper() for sym in seed_symbols],
        recent_symbols=[str(sym).upper() for sym in recent_symbols],
        scanner_symbols=[],
        blacklist=set(),
        max_n=int(layered_cfg.broad_limit or args.max_universe),
    )
    candidates = universe.symbols
    scanner_symbols: List[str] = []
    uni_rows = [{"symbol": sym, "reasons": ",".join(universe.meta.get(sym, {}).get("reasons", []))} for sym in candidates]
    log.info(
        "Investment layered universe seed=%s recent=%s symbol_master=%s initial=%s broad_limit=%s deep_limit=%s",
        len(seed_symbols),
        len(recent_symbols),
        len(symbol_master_symbols),
        len(candidates),
        layered_cfg.broad_limit,
        layered_cfg.deep_limit,
    )

    ib = None
    try:
        if research_only_yfinance:
            md = _YfOnlyMarketData()
            data_adapter = _YfOnlyDataAdapter()
            log.info("Using research-only yfinance daily bars for market=%s", resolved_market)
            scanner_symbols = _maybe_collect_research_scanner_symbols(
                resolved_market=resolved_market,
                host=str(host),
                port=int(port),
                client_id=int(client_id),
                request_timeout_sec=float(args.request_timeout_sec),
                db_path=db_path,
                ibkr_cfg=ibkr_cfg,
                layered_cfg=layered_cfg,
            )
            if scanner_symbols:
                universe = build_candidates(
                    seed_symbols=[str(sym).upper() for sym in seed_symbols],
                    recent_symbols=[str(sym).upper() for sym in recent_symbols],
                    scanner_symbols=[str(sym).upper() for sym in scanner_symbols],
                    blacklist=set(),
                    max_n=int(layered_cfg.broad_limit or args.max_universe),
                )
                candidates = universe.symbols
                uni_rows = [{"symbol": sym, "reasons": ",".join(universe.meta.get(sym, {}).get("reasons", []))} for sym in candidates]
            log.info(
                "Investment research-only scanner expansion market=%s scanner_symbols=%s final_universe=%s",
                resolved_market,
                len(scanner_symbols),
                len(candidates),
            )
        else:
            ib = connect_ib(host, port, client_id, request_timeout=float(args.request_timeout_sec))
            set_delayed_frozen(ib)
            md = MarketDataService(ib)
            data_adapter = MarketDataAdapter(md)
            scanner_symbols = _scanner_symbols(
                ib,
                db_path=db_path,
                market=resolved_market,
                ibkr_cfg=ibkr_cfg,
                layered_cfg=layered_cfg,
            )
            if scanner_symbols:
                universe = build_candidates(
                    seed_symbols=[str(sym).upper() for sym in seed_symbols],
                    recent_symbols=[str(sym).upper() for sym in recent_symbols],
                    scanner_symbols=[str(sym).upper() for sym in scanner_symbols],
                    blacklist=set(),
                    max_n=int(layered_cfg.broad_limit or args.max_universe),
                )
                candidates = universe.symbols
                uni_rows = [{"symbol": sym, "reasons": ",".join(universe.meta.get(sym, {}).get("reasons", []))} for sym in candidates]
            register_contracts(ib, md, candidates)
            log.info(
                "Investment scanner expansion market=%s scanner_symbols=%s final_universe=%s",
                resolved_market,
                len(scanner_symbols),
                len(candidates),
            )

        base_regime_cfg = RegimeConfig(**(strategy_cfg.get("mid_regime", {}) or {}))
        base_regime_cfg = apply_adaptive_strategy_regime_overrides(
            base_regime_cfg,
            adaptive_strategy,
            market=resolved_market,
        )
        regime_adaptor = RegimeAdaptor(
            market=resolved_market,
            base_cfg=base_regime_cfg,
            adapt_cfg=RegimeAdaptConfig.from_dict(regime_adaptor_cfg_raw.get("regime_adaptor")),
        )
        adapted_regime_cfg = regime_adaptor.refresh_if_due(md, force=True)

        providers = EnrichmentProviders()
        bundle = {
            "asof_utc": EnrichmentProviders._utc_now().isoformat(),
            "earnings": {},
            "macro_events": providers.fetch_macro_calendar(days_ahead=7),
            "markets": providers.fetch_market_snapshot(market=resolved_market),
            "market_news": providers.fetch_market_news(market=resolved_market),
            "fundamentals": {},
            "macro_indicators": providers.fetch_macro_indicators(),
        }
        market_sentiment = _build_market_sentiment(bundle, resolved_market)
        bundle["market_sentiment"] = dict(market_sentiment)
        vix = _extract_vix(bundle)
        macro_high_risk = _macro_high_risk(bundle)
        earnings_map: Dict[str, bool] = {}
        fundamentals_map: Dict[str, Dict[str, Any]] = {}
        recommendations_map: Dict[str, Dict[str, Any]] = {}
        market_leaders, market_laggards = _market_leaders(bundle)
        history_source_counts = {"ibkr": 0, "yfinance": 0, "missing": 0}
        quality_map: Dict[str, Dict[str, Any]] = {}

        mid_rows: List[Dict[str, Any]] = []
        long_rows: List[Dict[str, Any]] = []
        mid_lookback_days = int(investment_cfg.get("data", {}).get("mid_lookback_days", 180))
        long_years = int(investment_cfg.get("data", {}).get("long_years", 5))
        shared_days = max(mid_lookback_days, 252 * long_years)
        history_workers = _effective_worker_count(layered_cfg.history_workers, layered_cfg.network_reserve_ratio)
        total_candidates = len(candidates)
        if total_candidates:
            owner_thread_history = isinstance(getattr(data_adapter, "md", None), MarketDataService)
            feature_results = _collect_symbol_feature_results(
                candidates,
                data_adapter=data_adapter,
                market=resolved_market,
                market_structure=market_structure,
                shared_days=shared_days,
                mid_lookback_days=mid_lookback_days,
                long_years=long_years,
                regime_cfg=adapted_regime_cfg,
                history_workers=history_workers,
                progress_interval=layered_cfg.progress_interval,
                owner_thread_history=owner_thread_history,
            )
            for idx, result in enumerate(feature_results, start=1):
                sym = str(result.get("symbol", "") or "")
                history_source = str(result.get("history_source") or "").strip().lower()
                source_key = history_source or "missing"
                if source_key.startswith("yfinance"):
                    source_key = "yfinance"
                history_source_counts[source_key] = history_source_counts.get(source_key, 0) + 1
                quality_metrics = dict(result.get("quality_metrics") or {})
                if sym:
                    quality_map[sym.upper()] = quality_metrics
                mid_row = result.get("mid_row")
                long_row = result.get("long_row")
                if isinstance(mid_row, dict):
                    mid_rows.append(mid_row)
                if isinstance(long_row, dict):
                    long_rows.append(long_row)
                if str(result.get("history_error") or "").strip():
                    log.warning("history compute warning for %s: %s", sym, result["history_error"])
                if idx == 1 or idx % layered_cfg.progress_interval == 0 or idx == total_candidates:
                    log.info(
                        "Historical feature progress %s/%s mid_ok=%s long_ok=%s last=%s workers=%s mode=%s",
                        idx,
                        total_candidates,
                        len(mid_rows),
                        len(long_rows),
                        sym,
                        history_workers,
                        "owner-thread-prefetch" if owner_thread_history else "threaded-direct",
                    )

        mid_map = {str(row["symbol"]).upper(): row for row in mid_rows}
        long_map = {str(row["symbol"]).upper(): row for row in long_rows}

        broad_ranked = _score_ranked_candidates(
            candidates,
            long_map=long_map,
            mid_map=mid_map,
            quality_map=quality_map,
            vix=vix,
            market_sentiment_score=float(market_sentiment.get("score", 0.0) or 0.0),
            macro_high_risk=macro_high_risk,
            earnings_map=earnings_map,
            fundamentals_map=fundamentals_map,
            recommendations_map=recommendations_map,
            scoring_cfg=scoring_cfg,
        )
        broad_ranked, weekly_feedback_broad_summary = _apply_weekly_feedback_penalties(
            broad_ranked,
            scoring_cfg=scoring_cfg,
            weekly_feedback_cfg=weekly_feedback_cfg,
        )
        stage1_rank_map: Dict[str, int] = {}
        for idx, row in enumerate(broad_ranked, start=1):
            row.setdefault("market", str(resolved_market).upper())
            row["stage1_rank"] = idx
            row["scan_tier"] = "broad"
            row["market_sentiment"] = str(market_sentiment.get("label", ""))
            row["market_sentiment_score"] = float(market_sentiment.get("score", 0.0) or 0.0)
            stage1_rank_map[str(row["symbol"]).upper()] = idx
            if isinstance(row.get("signal_decision"), dict):
                row["signal_decision_json"] = json.dumps(row["signal_decision"], ensure_ascii=False)

        deep_pool_symbols = [str(row["symbol"]).upper() for row in broad_ranked[: max(0, int(layered_cfg.deep_limit))]]
        enrichment_symbols = [str(symbol).upper() for symbol in deep_pool_symbols[: max(0, int(layered_cfg.enrichment_limit))]]
        if enrichment_symbols:
            log.info(
                "Fetching deep enrichment market=%s stage2=%s earnings=%s",
                resolved_market,
                len(deep_pool_symbols),
                len(enrichment_symbols),
            )
            bundle["earnings"] = providers.fetch_earnings_calendar(enrichment_symbols, days_ahead=14)
            earnings_map = _earnings_map(bundle)
        recommendations_map = providers.fetch_recommendation_trends(
            enrichment_symbols,
            max_symbols=max(1, len(enrichment_symbols)),
        ) if enrichment_symbols else {}

        fundamentals = providers.fetch_fundamentals(
            enrichment_symbols,
            max_symbols=max(1, len(enrichment_symbols)),
        ) if enrichment_symbols else {}
        fundamentals_map = {str(symbol).upper(): dict(info) for symbol, info in fundamentals.items()}
        microstructure_map: Dict[str, Dict[str, Any]] = {}
        for symbol in enrichment_symbols:
            metrics = _compute_microstructure_metrics(symbol, data_adapter=data_adapter)
            microstructure_map[str(symbol).upper()] = dict(metrics)
            quality_bucket = dict(quality_map.get(str(symbol).upper(), {}) or {})
            quality_bucket.update(metrics)
            quality_map[str(symbol).upper()] = quality_bucket
        deep_ranked = _score_ranked_candidates(
            deep_pool_symbols or candidates,
            long_map=long_map,
            mid_map=mid_map,
            quality_map=quality_map,
            vix=vix,
            market_sentiment_score=float(market_sentiment.get("score", 0.0) or 0.0),
            macro_high_risk=macro_high_risk,
            earnings_map=earnings_map,
            fundamentals_map=fundamentals_map,
            recommendations_map=recommendations_map,
            scoring_cfg=scoring_cfg,
        )
        deep_ranked, weekly_feedback_deep_summary = _apply_weekly_feedback_penalties(
            deep_ranked,
            scoring_cfg=scoring_cfg,
            weekly_feedback_cfg=weekly_feedback_cfg,
        )
        deep_ranked, adaptive_strategy_summary = apply_adaptive_defensive_rank_cap(
            deep_ranked,
            adaptive_strategy,
        )
        for row in deep_ranked:
            symbol = str(row["symbol"]).upper()
            row.setdefault("market", str(resolved_market).upper())
            row["stage1_rank"] = int(stage1_rank_map.get(symbol, 0) or 0)
            row["scan_tier"] = "deep" if symbol in set(enrichment_symbols) else "deep_pool"
            row["market_sentiment"] = str(market_sentiment.get("label", ""))
            row["market_sentiment_score"] = float(market_sentiment.get("score", 0.0) or 0.0)
            row["adaptive_strategy_market_profile"] = str(adaptive_market_plan.get("profile_key", "") or "")
            row["adaptive_strategy_market_profile_label"] = str(adaptive_market_plan.get("profile_label", "") or "")
            if isinstance(row.get("signal_decision"), dict):
                row["signal_decision_json"] = json.dumps(row["signal_decision"], ensure_ascii=False)

        ranked = deep_ranked[: int(args.top_n)]
        weekly_feedback_summary = {
            "enabled": bool(weekly_feedback_cfg),
            "configured_penalty_symbols": int(weekly_feedback_deep_summary.get("penalty_symbol_count", 0) or 0),
            "configured_signal_penalty_symbols": int(len({str(row.get("symbol") or "").upper() for row in _parse_signal_penalties(weekly_feedback_cfg.get("signal_penalties")) if str(row.get("symbol") or "").strip()})),
            "configured_execution_penalty_symbols": int(len({str(row.get("symbol") or "").upper() for row in _parse_signal_penalties(weekly_feedback_cfg.get("execution_penalties")) if str(row.get("symbol") or "").strip()})),
            "applied_candidate_count": int(sum(1 for row in ranked if int(row.get("weekly_feedback_applied", 0) or 0) == 1)),
            "top_penalty_symbols": sorted(
                {
                    str(row.get("symbol") or "").upper()
                    for row in ranked
                    if int(row.get("weekly_feedback_applied", 0) or 0) == 1
                }
            )[:10],
            "top_execution_penalty_symbols": sorted(
                {
                    str(row.get("symbol") or "").upper()
                    for row in ranked
                    if float(row.get("weekly_feedback_expected_cost_bps_add", 0.0) or 0.0) > 0.0
                }
            )[:10],
        }

        backtests: List[Dict[str, Any]] = []
        for row in ranked[: max(0, int(args.backtest_top_k))]:
            symbol = str(row["symbol"]).upper()
            try:
                backtest_bars, _ = _load_daily_bars(
                    symbol,
                    data_adapter,
                    int(investment_cfg.get("backtest", {}).get("history_days", 252 * 6)),
                )
                bt = compute_investment_backtest_from_bars(
                    symbol,
                    backtest_bars,
                    scoring_cfg=scoring_cfg,
                    regime_cfg=adapted_regime_cfg,
                    cfg=backtest_cfg,
                )
                backtests.append(bt)
            except Exception as e:
                log.warning("investment backtest failed for %s: %s %s", symbol, type(e).__name__, e)
        backtest_map = {str(row["symbol"]).upper(): row for row in backtests}

        for row in ranked:
            symbol = str(row["symbol"]).upper()
            row.update(backtest_map.get(symbol, {}))
            row.update(fundamentals.get(symbol, {}))
            row.update(recommendations_map.get(symbol, {}))
            row["market_sentiment"] = str(market_sentiment.get("label", ""))
            row["market_sentiment_score"] = float(market_sentiment.get("score", 0.0) or 0.0)
            row["adaptive_strategy_market_profile"] = str(adaptive_market_plan.get("profile_key", "") or "")
            row["adaptive_strategy_market_profile_label"] = str(adaptive_market_plan.get("profile_label", "") or "")
            if isinstance(row.get("signal_decision"), dict):
                row["signal_decision_json"] = json.dumps(row["signal_decision"], ensure_ascii=False)

        short_universe_symbols = _dedupe_keep_order(
            list(scanner_symbols)
            + list(recent_symbols)
            + list(watchlist_symbols)
            + list(candidates[: max(0, int(layered_cfg.deep_limit or 0))])
        )
        if ib is not None and short_universe_symbols:
            register_contracts(ib, md, short_universe_symbols)
        short_ranked = build_short_book_candidates(
            short_universe_symbols,
            market=resolved_market,
            base_dir=BASE_DIR,
            data_adapter=md,
            bundle=bundle,
            strategy_cfg=strategy_cfg,
            report_cfg=report_cfg,
            risk_cfg=risk_cfg,
            adapted_regime_cfg=adapted_regime_cfg,
            short_book_cfg=short_book_cfg,
        )
        for row in short_ranked:
            row["market_sentiment"] = str(market_sentiment.get("label", ""))
            row["market_sentiment_score"] = float(market_sentiment.get("score", 0.0) or 0.0)
            row["adaptive_strategy_market_profile"] = str(adaptive_market_plan.get("profile_key", "") or "")
            row["adaptive_strategy_market_profile_label"] = str(adaptive_market_plan.get("profile_label", "") or "")
        short_plans = [make_investment_plan(row, vix=vix, cfg=plan_cfg) for row in short_ranked]
        for plan in short_plans:
            plan["market_sentiment"] = str(market_sentiment.get("label", ""))
            plan["market_sentiment_score"] = float(market_sentiment.get("score", 0.0) or 0.0)
            plan["market_sentiment_guidance"] = str(market_sentiment.get("guidance", "") or "")

        out_dir = Path(out_dir_arg)
        if not out_dir.is_absolute():
            out_dir = BASE_DIR / out_dir
        if watchlist_yaml:
            out_dir = out_dir / _slugify_report_name(Path(watchlist_yaml).stem)
        else:
            out_dir = out_dir / f"market_{resolved_market.lower()}"
        os.makedirs(out_dir, exist_ok=True)
        analysis_run_id = f"{resolved_market}-{int(time.time() * 1000)}-{os.getpid()}"
        portfolio_id = _report_portfolio_id(resolved_market, watchlist_yaml, out_dir)
        source_reason_map = {
            str(symbol).upper(): [str(item).strip() for item in list(meta.get("reasons", []) or []) if str(item).strip()]
            for symbol, meta in dict(universe.meta or {}).items()
        }
        storage = Storage(db_path)
        shadow_training_rows = storage.get_investment_snapshot_training_rows(
            market=resolved_market,
            horizon_days=int(shadow_ml_cfg.horizon_days),
            stages=list(shadow_ml_cfg.stage_values),
            direction="LONG",
            limit=int(shadow_ml_cfg.max_training_rows),
        )
        shadow_model = train_investment_shadow_model(shadow_training_rows, cfg=shadow_ml_cfg)
        broad_ranked, shadow_broad_summary = apply_investment_shadow_model(
            broad_ranked,
            model=shadow_model,
            cfg=shadow_ml_cfg,
        )
        deep_ranked, shadow_deep_summary = apply_investment_shadow_model(
            deep_ranked,
            model=shadow_model,
            cfg=shadow_ml_cfg,
        )
        ranked, shadow_summary = apply_investment_shadow_model(
            ranked,
            model=shadow_model,
            cfg=shadow_ml_cfg,
        )
        data_quality_summary = _summarize_data_quality(
            feature_results if total_candidates else [],
            ranked_rows=ranked,
            low_quality_threshold=float(scoring_cfg.low_data_quality_threshold),
        )
        cost_summary = _summarize_cost_profile(
            ranked,
            high_cost_threshold_bps=float(scoring_cfg.high_expected_cost_bps),
        )
        data_quality_summary["history_source_counts"] = dict(history_source_counts)
        data_quality_summary["market"] = str(resolved_market).upper()
        data_quality_summary["portfolio_id"] = str(portfolio_id or "")
        data_quality_summary["analysis_run_id"] = str(analysis_run_id)
        cost_summary["market"] = str(resolved_market).upper()
        cost_summary["portfolio_id"] = str(portfolio_id or "")
        cost_summary["analysis_run_id"] = str(analysis_run_id)

        write_csv(str(out_dir / "universe_candidates.csv"), uni_rows)
        write_csv(str(out_dir / "investment_broad_candidates.csv"), broad_ranked)
        write_csv(str(out_dir / "investment_deep_candidates.csv"), deep_ranked)
        write_csv(str(out_dir / "investment_candidates.csv"), ranked)
        write_csv(str(out_dir / "investment_short_candidates.csv"), short_ranked)
        write_csv(str(out_dir / "investment_short_plan.csv"), short_plans)
        write_csv(str(out_dir / "investment_backtest.csv"), backtests)
        write_json(str(out_dir / "enrichment.json"), bundle)
        write_json(str(out_dir / "fundamentals.json"), fundamentals)
        write_json(str(out_dir / "market_sentiment.json"), market_sentiment)
        write_json(str(out_dir / "investment_data_quality_summary.json"), data_quality_summary)
        write_json(str(out_dir / "investment_cost_summary.json"), cost_summary)
        write_json(
            str(out_dir / "investment_weekly_feedback_summary.json"),
            {
                "weekly_feedback": weekly_feedback_cfg,
                "summary": weekly_feedback_summary,
                "broad_summary": weekly_feedback_broad_summary,
                "deep_summary": weekly_feedback_deep_summary,
            },
        )
        write_json(
            str(out_dir / "investment_shadow_model_summary.json"),
            {
                "model": shadow_model,
                "summary": shadow_summary,
                "broad_summary": shadow_broad_summary,
                "deep_summary": shadow_deep_summary,
            },
        )
        write_json(
            str(out_dir / "investment_adaptive_strategy_summary.json"),
            {
                "adaptive_strategy": adaptive_strategy_context(adaptive_strategy),
                "summary": adaptive_strategy_summary,
                "active_market_plan": adaptive_market_plan,
                "active_market_regime": adaptive_market_regime,
                "active_market_execution": adaptive_market_execution,
            },
        )

        plans = [make_investment_plan(row, vix=vix, cfg=plan_cfg) for row in ranked]
        for plan in plans:
            symbol = str(plan["symbol"]).upper()
            bt = backtest_map.get(symbol, {})
            if bt:
                plan.update(
                    {
                        "bt_signal_samples": int(bt.get("bt_signal_samples", 0) or 0),
                        "bt_avg_ret_30d": float(bt.get("bt_avg_ret_30d", 0.0) or 0.0),
                        "bt_avg_ret_60d": float(bt.get("bt_avg_ret_60d", 0.0) or 0.0),
                        "bt_avg_ret_90d": float(bt.get("bt_avg_ret_90d", 0.0) or 0.0),
                    }
                )
            plan["market_sentiment"] = str(market_sentiment.get("label", ""))
            plan["market_sentiment_score"] = float(market_sentiment.get("score", 0.0) or 0.0)
            plan["market_sentiment_guidance"] = str(market_sentiment.get("guidance", "") or "")
            if isinstance(plan.get("signal_decision"), dict):
                plan["signal_decision_json"] = json.dumps(plan["signal_decision"], ensure_ascii=False)
        write_csv(str(out_dir / "investment_plan.csv"), plans)
        plan_map = {str(plan.get("symbol") or "").upper(): dict(plan) for plan in plans}
        _persist_candidate_snapshots(
            storage,
            rows=broad_ranked,
            stage="broad",
            market=resolved_market,
            portfolio_id=portfolio_id,
            report_dir=out_dir,
            analysis_run_id=analysis_run_id,
            source_reason_map=source_reason_map,
        )
        _persist_candidate_snapshots(
            storage,
            rows=deep_ranked,
            stage="deep",
            market=resolved_market,
            portfolio_id=portfolio_id,
            report_dir=out_dir,
            analysis_run_id=analysis_run_id,
            source_reason_map=source_reason_map,
        )
        _persist_candidate_snapshots(
            storage,
            rows=ranked,
            stage="final",
            market=resolved_market,
            portfolio_id=portfolio_id,
            report_dir=out_dir,
            analysis_run_id=analysis_run_id,
            source_reason_map=source_reason_map,
            plan_map=plan_map,
        )
        _persist_candidate_snapshots(
            storage,
            rows=short_ranked,
            stage="short",
            market=resolved_market,
            portfolio_id=portfolio_id,
            report_dir=out_dir,
            analysis_run_id=analysis_run_id,
            source_reason_map=source_reason_map,
            plan_map={str(plan.get("symbol") or "").upper(): dict(plan) for plan in short_plans},
        )

        context = {
            "summary": {
                "vix": float(vix),
                "macro_high_risk": bool(macro_high_risk),
                "earnings_risk_count": int(sum(1 for val in earnings_map.values() if val)),
                "candidate_count": int(len(candidates)),
                "broad_ranked_count": int(len(broad_ranked)),
                "deep_ranked_count": int(len(deep_ranked)),
                "deep_pool_count": int(len(deep_pool_symbols)),
                "enrichment_count": int(len(enrichment_symbols)),
                "mid_ok": int(len(mid_rows)),
                "long_ok": int(len(long_rows)),
                "ranked_count": int(len(ranked)),
                "plan_count": int(len(plans)),
                "backtest_count": int(len(backtests)),
                "scanner_candidate_count": int(len(scanner_symbols)),
                "short_candidate_count": int(len(short_ranked)),
                "history_workers": int(history_workers),
                "market_leaders": market_leaders,
                "market_laggards": market_laggards,
                "macro_indicators": dict(bundle.get("macro_indicators", {}) or {}),
                "market_news": list(bundle.get("market_news", []) or []),
                "market_sentiment_label": str(market_sentiment.get("label", "") or ""),
                "market_sentiment_score": float(market_sentiment.get("score", 0.0) or 0.0),
                "market_sentiment_guidance": str(market_sentiment.get("guidance", "") or ""),
                "breadth_positive_ratio": float(market_sentiment.get("breadth_positive_ratio", 0.0) or 0.0),
                "breadth_dispersion_1d": float(market_sentiment.get("breadth_dispersion_1d", 0.0) or 0.0),
                "leadership_spread_5d": float(market_sentiment.get("leadership_spread_5d", 0.0) or 0.0),
                "recommendation_coverage": int(len(recommendations_map)),
                "history_source_ibkr": int(history_source_counts.get("ibkr", 0) or 0),
                "history_source_yfinance": int(history_source_counts.get("yfinance", 0) or 0),
                "history_source_missing": int(history_source_counts.get("missing", 0) or 0),
                "avg_data_quality_score": float(data_quality_summary.get("avg_data_quality_score", 0.0) or 0.0),
                "avg_source_coverage": float(data_quality_summary.get("avg_source_coverage", 0.0) or 0.0),
                "avg_missing_ratio": float(data_quality_summary.get("avg_missing_ratio", 0.0) or 0.0),
                "low_quality_count": int(data_quality_summary.get("low_quality_count", 0) or 0),
                "ranked_low_quality_count": int(data_quality_summary.get("ranked_low_quality_count", 0) or 0),
                "low_quality_threshold": float(data_quality_summary.get("low_quality_threshold", 0.0) or 0.0),
                "avg_expected_cost_bps": float(cost_summary.get("avg_expected_cost_bps", 0.0) or 0.0),
                "avg_spread_proxy_bps": float(cost_summary.get("avg_spread_proxy_bps", 0.0) or 0.0),
                "avg_slippage_proxy_bps": float(cost_summary.get("avg_slippage_proxy_bps", 0.0) or 0.0),
                "avg_commission_proxy_bps": float(cost_summary.get("avg_commission_proxy_bps", 0.0) or 0.0),
                "avg_daily_dollar_volume": float(cost_summary.get("avg_daily_dollar_volume", 0.0) or 0.0),
                "high_cost_count": int(cost_summary.get("high_cost_count", 0) or 0),
                "low_liquidity_count": int(cost_summary.get("low_liquidity_count", 0) or 0),
                "high_cost_threshold_bps": float(cost_summary.get("high_cost_threshold_bps", 0.0) or 0.0),
                "shadow_ml_enabled": bool(shadow_summary.get("enabled", False)),
                "shadow_ml_reason": str(shadow_summary.get("reason", "") or ""),
                "shadow_ml_model_version": str(shadow_summary.get("model_version", shadow_model.get("model_version", "")) or ""),
                "shadow_ml_feature_count": int(shadow_summary.get("feature_count", shadow_model.get("feature_count", 0)) or 0),
                "shadow_ml_training_samples": int(shadow_summary.get("training_samples", 0) or 0),
                "shadow_ml_horizon_days": int(shadow_summary.get("horizon_days", 0) or 0),
                "shadow_ml_avg_score": float(shadow_summary.get("avg_shadow_ml_score", 0.0) or 0.0),
                "shadow_ml_avg_return": float(shadow_summary.get("avg_shadow_ml_return", 0.0) or 0.0),
                "shadow_ml_avg_positive_prob": float(shadow_summary.get("avg_shadow_ml_positive_prob", 0.0) or 0.0),
                "shadow_ml_train_directional_accuracy": float(
                    shadow_summary.get("train_directional_accuracy", shadow_model.get("train_directional_accuracy", 0.0)) or 0.0
                ),
                "weekly_feedback_enabled": bool(weekly_feedback_summary.get("enabled", False)),
                "weekly_feedback_penalty_symbols": int(weekly_feedback_summary.get("configured_penalty_symbols", 0) or 0),
                "weekly_feedback_signal_penalty_symbols": int(weekly_feedback_summary.get("configured_signal_penalty_symbols", 0) or 0),
                "weekly_feedback_execution_penalty_symbols": int(weekly_feedback_summary.get("configured_execution_penalty_symbols", 0) or 0),
                "weekly_feedback_applied_candidates": int(weekly_feedback_summary.get("applied_candidate_count", 0) or 0),
                "adaptive_strategy_enabled": bool(adaptive_strategy_summary.get("enabled", False)),
                "adaptive_strategy_defensive_caps": int(adaptive_strategy_summary.get("defensive_cap_count", 0) or 0),
                "avg_microstructure_score": float(_avg_defined([row.get("microstructure_score") for row in ranked]) or 0.0),
                "avg_micro_breakout_5m": float(_avg_defined([row.get("micro_breakout_5m") for row in ranked]) or 0.0),
                "avg_micro_reversal_5m": float(_avg_defined([row.get("micro_reversal_5m") for row in ranked]) or 0.0),
                "avg_micro_volume_burst_5m": float(_avg_defined([row.get("micro_volume_burst_5m") for row in ranked]) or 0.0),
                "avg_returns_ewma_vol_20d": float(_avg_defined([row.get("returns_ewma_vol_20d") for row in ranked]) or 0.0),
                "avg_returns_downside_vol_20d": float(_avg_defined([row.get("returns_downside_vol_20d") for row in ranked]) or 0.0),
                "analysis_run_id": analysis_run_id,
                "portfolio_id": portfolio_id,
                "data_warning": (
                    f"{resolved_market} 的 IBKR 历史行情不可用，当前已回退到 yfinance 免费日线。"
                    if history_source_counts.get("yfinance", 0) and not history_source_counts.get("ibkr", 0)
                    else f"{resolved_market} 历史行情样本为 0，疑似缺少该市场历史数据权限或订阅。"
                    if candidates and not mid_rows and not long_rows
                    else ""
                ),
            },
            "investment_config": investment_cfg,
            "market_profile": dict(investment_cfg.get("market_profile", {}) or {}),
            "market_structure": _market_structure_context(market_structure),
            "adaptive_strategy": adaptive_strategy_context(adaptive_strategy),
        }
        write_investment_md(
            str(out_dir / "investment_report.md"),
            "Investment Candidate Report",
            list(ranked) + list(short_ranked),
            list(plans) + list(short_plans),
            context,
        )
        summary_fields, artifact_fields = _cli_summary_payload(
            market=resolved_market,
            portfolio_id=portfolio_id,
            out_dir=out_dir,
            candidate_count=len(candidates),
            ranked_count=len(ranked),
            short_candidate_count=len(short_ranked),
            plan_count=len(plans),
            backtest_count=len(backtests),
        )
        emit_cli_summary(
            command="ibkr-quant-report",
            headline="investment report generated",
            summary=summary_fields,
            artifacts=artifact_fields,
        )
        log.info("Wrote investment report -> %s (ranked=%s plans=%s)", out_dir / "investment_report.md", len(ranked), len(plans))
    finally:
        if ib is not None:
            try:
                ib.disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    main()
