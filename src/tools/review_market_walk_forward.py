from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import fields, replace
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional

from ..analysis.report import write_csv, write_json
from ..common.adaptive_strategy import (
    AdaptiveStrategyConfig,
    MarketProfileConfig,
    adaptive_strategy_market_profile,
    adaptive_strategy_config_path,
    load_adaptive_strategy,
)
from ..common.cli import build_cli_parser, emit_cli_summary
from ..common.cli_contracts import ArtifactBundle, WalkForwardSummary
from ..common.markets import add_market_args, resolve_market_code
from ..common.runtime_paths import resolve_repo_path

UTC = timezone.utc
BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_OUT_DIR = "reports_walk_forward"


def build_parser() -> argparse.ArgumentParser:
    parser = build_cli_parser(
        description="Run market-level surrogate walk-forward tuning from weekly tuning history.",
        command="ibkr-quant-walk-forward",
        examples=[
            "ibkr-quant-walk-forward --db audit.db",
            "ibkr-quant-walk-forward --markets US,HK,CN --train_weeks 8 --validate_weeks 4",
            "ibkr-quant-walk-forward --market HK --out_dir reports_walk_forward/hk",
        ],
        notes=[
            "Uses weekly_tuning_history as a surrogate dataset; it does not replay a full bar-level simulator.",
            "Writes summary JSON, market summary CSV, candidate/window CSVs, patch CSV, and a markdown report into --out_dir.",
        ],
    )
    add_market_args(parser)
    parser.add_argument("--markets", default="", help="Comma-separated market list. Overrides --market when provided.")
    parser.add_argument("--db", default="audit.db", help="SQLite audit database that contains investment_weekly_tuning_history.")
    parser.add_argument("--out_dir", default=DEFAULT_OUT_DIR, help="Directory for generated walk-forward artifacts.")
    parser.add_argument(
        "--adaptive_strategy_config",
        default="config/adaptive_strategy_framework.yaml",
        help="Adaptive strategy framework YAML used as the current parameter baseline.",
    )
    parser.add_argument("--min_weeks", type=int, default=10, help="Minimum weekly samples required per market.")
    parser.add_argument("--train_weeks", type=int, default=8, help="Training window length in weeks.")
    parser.add_argument("--validate_weeks", type=int, default=4, help="Validation window length in weeks.")
    parser.add_argument("--step_weeks", type=int, default=2, help="Walk-forward step size in weeks.")
    return parser


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value if value is not None else default)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value if value is not None else default)
    except Exception:
        return int(default)


def _mean(values: Iterable[float]) -> float:
    rows = [float(v) for v in list(values or [])]
    if not rows:
        return 0.0
    return float(sum(rows) / len(rows))


def _median(values: Iterable[float]) -> float:
    rows = [float(v) for v in list(values or [])]
    if not rows:
        return 0.0
    return float(median(rows))


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(value)))


def _parse_ts(raw: Any) -> Optional[datetime]:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (str(table),),
    ).fetchone()
    return bool(row)


def _parse_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
    except Exception:
        return {}
    return dict(data) if isinstance(data, dict) else {}


def _market_list_from_args(market: str = "", markets: str = "") -> List[str]:
    explicit = [resolve_market_code(item) for item in str(markets or "").split(",") if resolve_market_code(item)]
    if explicit:
        return sorted(dict.fromkeys(explicit))
    one = resolve_market_code(market)
    return [one] if one else []


def _profile_to_dict(profile: MarketProfileConfig) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for field in fields(MarketProfileConfig):
        value = getattr(profile, field.name)
        if isinstance(value, (int, float)) and value is not None:
            out[field.name] = float(value)
    return out


def _apply_profile_patch(profile: MarketProfileConfig, changes: Dict[str, float]) -> MarketProfileConfig:
    current = profile
    for field_name, value in dict(changes or {}).items():
        if field_name == "no_trade_band_pct":
            value = _clamp(value, 0.01, 0.12)
        elif field_name == "turnover_penalty_scale":
            value = _clamp(value, 0.05, 0.60)
        elif field_name == "min_expected_edge_bps":
            value = _clamp(value, 8.0, 40.0)
        elif field_name == "edge_cost_buffer_bps":
            value = _clamp(value, 2.0, 20.0)
        elif field_name == "regime_risk_on_threshold":
            value = _clamp(value, 0.35, 0.75)
        elif field_name == "regime_hard_risk_off_threshold":
            value = _clamp(value, 0.10, 0.50)
        current = replace(current, **{field_name: value})
    return current


def _candidate_family_label(family: str) -> str:
    family_code = str(family or "").strip().upper()
    if family_code == "BASELINE":
        return "Keep baseline"
    if family_code == "EXECUTION_RELAX":
        return "Relax execution gate"
    if family_code == "EXECUTION_TIGHTEN":
        return "Tighten execution gate"
    if family_code == "TURNOVER_TIGHTEN":
        return "Tighten turnover control"
    if family_code == "TURNOVER_RELAX":
        return "Relax turnover control"
    if family_code == "REGIME_DEFENSIVE":
        return "More defensive regime"
    if family_code == "REGIME_AGGRESSIVE":
        return "More aggressive regime"
    return family_code or "-"


def _build_candidate_set(profile: MarketProfileConfig) -> List[Dict[str, Any]]:
    band_step = max(0.002, _safe_float(profile.no_trade_band_pct, 0.04) * 0.12)
    turnover_step = max(0.02, _safe_float(profile.turnover_penalty_scale, 0.18) * 0.12)
    edge_step = 2.0
    buffer_step = 1.0
    risk_on_step = 0.02
    hard_off_step = 0.02

    specs = [
        ("BASELINE", {}),
        (
            "EXECUTION_RELAX",
            {
                "min_expected_edge_bps": _safe_float(profile.min_expected_edge_bps, 18.0) - edge_step,
                "edge_cost_buffer_bps": _safe_float(profile.edge_cost_buffer_bps, 6.0) - buffer_step,
            },
        ),
        (
            "EXECUTION_TIGHTEN",
            {
                "min_expected_edge_bps": _safe_float(profile.min_expected_edge_bps, 18.0) + edge_step,
                "edge_cost_buffer_bps": _safe_float(profile.edge_cost_buffer_bps, 6.0) + buffer_step,
            },
        ),
        (
            "TURNOVER_TIGHTEN",
            {
                "no_trade_band_pct": _safe_float(profile.no_trade_band_pct, 0.04) + band_step,
                "turnover_penalty_scale": _safe_float(profile.turnover_penalty_scale, 0.18) + turnover_step,
            },
        ),
        (
            "TURNOVER_RELAX",
            {
                "no_trade_band_pct": _safe_float(profile.no_trade_band_pct, 0.04) - band_step,
                "turnover_penalty_scale": _safe_float(profile.turnover_penalty_scale, 0.18) - turnover_step,
            },
        ),
        (
            "REGIME_DEFENSIVE",
            {
                "regime_risk_on_threshold": _safe_float(profile.regime_risk_on_threshold, 0.50) + risk_on_step,
                "regime_hard_risk_off_threshold": _safe_float(profile.regime_hard_risk_off_threshold, 0.25) + hard_off_step,
                "no_trade_band_pct": _safe_float(profile.no_trade_band_pct, 0.04) + (band_step * 0.5),
            },
        ),
        (
            "REGIME_AGGRESSIVE",
            {
                "regime_risk_on_threshold": _safe_float(profile.regime_risk_on_threshold, 0.50) - risk_on_step,
                "regime_hard_risk_off_threshold": _safe_float(profile.regime_hard_risk_off_threshold, 0.25) - hard_off_step,
                "no_trade_band_pct": _safe_float(profile.no_trade_band_pct, 0.04) - (band_step * 0.5),
            },
        ),
    ]

    base_values = _profile_to_dict(profile)
    out: List[Dict[str, Any]] = []
    for family, changes in specs:
        tuned_profile = _apply_profile_patch(profile, changes)
        tuned_values = _profile_to_dict(tuned_profile)
        patch_rows: List[Dict[str, Any]] = []
        for key, suggested in tuned_values.items():
            current = base_values.get(key)
            if current is None:
                continue
            if abs(float(suggested) - float(current)) < 1e-12:
                continue
            patch_rows.append(
                {
                    "field": key,
                    "current_value": float(current),
                    "suggested_value": float(suggested),
                    "delta": float(suggested - current),
                }
            )
        summary = "; ".join(
            f"{item['field']} {item['current_value']:.4f}->{item['suggested_value']:.4f}" if abs(item["suggested_value"]) < 1.0
            else f"{item['field']} {item['current_value']:.2f}->{item['suggested_value']:.2f}"
            for item in patch_rows
        ) or "keep baseline"
        out.append(
            {
                "family": family,
                "family_label": _candidate_family_label(family),
                "profile": tuned_profile,
                "patch_rows": patch_rows,
                "patch_summary": summary,
                "complexity": len(patch_rows),
            }
        )
    return out


def _load_weekly_tuning_history(db_path: Path, markets: List[str] | None = None) -> List[Dict[str, Any]]:
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(str(path))
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        if not _table_exists(conn, "investment_weekly_tuning_history"):
            return []
        query = """
            SELECT week_label, week_start, window_start, window_end, ts, market, portfolio_id,
                   active_market_profile, dominant_driver, market_profile_tuning_action,
                   weekly_return, max_drawdown, turnover, outcome_sample_count,
                   signal_quality_score, execution_cost_gap, execution_gate_blocked_weight,
                   strategy_control_weight_delta, risk_overlay_weight_delta,
                   risk_feedback_action, execution_feedback_action,
                   market_profile_ready_for_manual_apply, details
            FROM investment_weekly_tuning_history
        """
        params: List[Any] = []
        wanted = [resolve_market_code(item) for item in list(markets or []) if resolve_market_code(item)]
        if wanted:
            query += " WHERE market IN ({})".format(",".join("?" for _ in wanted))
            params.extend(wanted)
        query += " ORDER BY week_start ASC, ts ASC, id ASC"
        rows = conn.execute(query, params).fetchall()
    finally:
        conn.close()

    out: List[Dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        details = _parse_json_dict(row.get("details"))
        merged = dict(details)
        merged.update(row)
        merged["market"] = resolve_market_code(str(merged.get("market") or ""))
        merged["portfolio_id"] = str(merged.get("portfolio_id") or "").strip()
        merged["active_market_profile"] = str(
            merged.get("active_market_profile")
            or merged.get("adaptive_strategy_active_market_profile")
            or merged.get("market")
            or "DEFAULT"
        ).strip().upper()
        merged["week_start"] = str(merged.get("week_start") or "").strip()
        merged["week_label"] = str(merged.get("week_label") or "").strip()
        out.append(merged)
    return out


def _aggregate_market_week_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for raw in list(rows or []):
        row = dict(raw)
        market = resolve_market_code(str(row.get("market") or ""))
        week_start = str(row.get("week_start") or "").strip()
        if not market or not week_start:
            continue
        buckets[(market, week_start)].append(row)

    out: List[Dict[str, Any]] = []
    for (market, week_start), bucket in sorted(buckets.items(), key=lambda item: (item[0][0], item[0][1])):
        profile_counter = Counter(str(row.get("active_market_profile") or market).strip().upper() for row in bucket)
        active_profile = profile_counter.most_common(1)[0][0] if profile_counter else market
        week_label = str(bucket[0].get("week_label") or "")
        out.append(
            {
                "market": market,
                "week_start": week_start,
                "week_label": week_label,
                "active_market_profile": active_profile,
                "portfolio_count": int(len(bucket)),
                "weekly_return": _mean(_safe_float(row.get("weekly_return"), 0.0) for row in bucket),
                "max_drawdown": _mean(_safe_float(row.get("max_drawdown"), 0.0) for row in bucket),
                "turnover": _mean(_safe_float(row.get("turnover"), 0.0) for row in bucket),
                "outcome_sample_count": int(sum(_safe_int(row.get("outcome_sample_count"), 0) for row in bucket)),
                "signal_quality_score": _mean(_safe_float(row.get("signal_quality_score"), 0.0) for row in bucket),
                "execution_gate_blocked_weight": _mean(
                    _safe_float(row.get("execution_gate_blocked_weight"), 0.0) for row in bucket
                ),
                "strategy_control_weight_delta": _mean(
                    _safe_float(row.get("strategy_control_weight_delta"), 0.0) for row in bucket
                ),
                "risk_overlay_weight_delta": _mean(
                    _safe_float(row.get("risk_overlay_weight_delta"), 0.0) for row in bucket
                ),
                "avg_execution_capture_bps": _mean(
                    _safe_float(row.get("avg_execution_capture_bps"), 0.0) for row in bucket
                ),
                "matured_20d_avg_realized_edge_bps": _mean(
                    _safe_float(row.get("matured_20d_avg_realized_edge_bps"), 0.0) for row in bucket
                ),
                "avg_actual_slippage_bps": _mean(
                    _safe_float(row.get("avg_actual_slippage_bps"), 0.0) for row in bucket
                ),
                "avg_expected_cost_bps": _mean(
                    _safe_float(row.get("avg_expected_cost_bps"), 0.0) for row in bucket
                ),
                "outcome_selected_spread_5d_bps": _mean(
                    _safe_float(row.get("outcome_selected_spread_5d_bps"), 0.0) for row in bucket
                ),
                "outcome_selected_spread_20d_bps": _mean(
                    _safe_float(row.get("outcome_selected_spread_20d_bps"), 0.0) for row in bucket
                ),
                "outcome_selected_spread_60d_bps": _mean(
                    _safe_float(row.get("outcome_selected_spread_60d_bps"), 0.0) for row in bucket
                ),
                "blocked_20d_avg_counterfactual_edge_bps": _mean(
                    _safe_float(row.get("blocked_20d_avg_counterfactual_edge_bps"), 0.0) for row in bucket
                ),
                "execution_cost_gap": _mean(_safe_float(row.get("execution_cost_gap"), 0.0) for row in bucket),
                "market_profile_tuning_action": str(bucket[-1].get("market_profile_tuning_action") or ""),
                "dominant_driver": str(bucket[-1].get("dominant_driver") or ""),
            }
        )
    return out


def _build_reference(profile: MarketProfileConfig, rows: List[Dict[str, Any]]) -> Dict[str, float]:
    rebalance_window_days = max(10.0, _safe_float(profile.rebalance_window_days, 21.0))
    turnover_target = max(0.10, 5.0 / rebalance_window_days)
    heuristic_slippage_target = max(2.0, _safe_float(profile.edge_cost_buffer_bps, 6.0) * 0.75)
    median_slippage = _median(_safe_float(row.get("avg_actual_slippage_bps"), 0.0) for row in rows)
    slippage_target = (
        heuristic_slippage_target
        if median_slippage <= 0.0
        else max(2.0, min(heuristic_slippage_target, median_slippage))
    )
    return {
        "turnover_target": turnover_target,
        "drawdown_warn": max(0.01, abs(_safe_float(profile.regime_drawdown_warn, -0.03))),
        "slippage_target": slippage_target,
        "signal_quality_floor": max(0.35, _median(_safe_float(row.get("signal_quality_score"), 0.0) for row in rows)),
        "opportunity_baseline": max(
            15.0,
            _median(_safe_float(row.get("outcome_selected_spread_20d_bps"), 0.0) for row in rows),
        ),
        "capture_baseline": max(
            6.0,
            _median(_safe_float(row.get("avg_execution_capture_bps"), 0.0) for row in rows),
        ),
        "blocked_baseline": max(
            8.0,
            _median(_safe_float(row.get("blocked_20d_avg_counterfactual_edge_bps"), 0.0) for row in rows),
        ),
        "strategy_control_floor": max(
            0.02,
            _median(abs(_safe_float(row.get("strategy_control_weight_delta"), 0.0)) for row in rows),
        ),
        "risk_overlay_floor": max(
            0.02,
            _median(abs(_safe_float(row.get("risk_overlay_weight_delta"), 0.0)) for row in rows),
        ),
    }


def _row_pressures(row: Dict[str, Any], ref: Dict[str, float]) -> Dict[str, float]:
    weekly_return = _safe_float(row.get("weekly_return"), 0.0)
    max_drawdown = abs(min(_safe_float(row.get("max_drawdown"), 0.0), 0.0))
    signal_quality = _safe_float(row.get("signal_quality_score"), 0.0)
    outcome_20 = _safe_float(row.get("outcome_selected_spread_20d_bps"), 0.0)
    outcome_60 = _safe_float(row.get("outcome_selected_spread_60d_bps"), 0.0)
    capture_bps = _safe_float(row.get("avg_execution_capture_bps"), 0.0)
    slippage_bps = _safe_float(row.get("avg_actual_slippage_bps"), 0.0)
    turnover = _safe_float(row.get("turnover"), 0.0)
    blocked_weight = _safe_float(row.get("execution_gate_blocked_weight"), 0.0)
    blocked_counterfactual = _safe_float(row.get("blocked_20d_avg_counterfactual_edge_bps"), 0.0)
    strategy_control = abs(_safe_float(row.get("strategy_control_weight_delta"), 0.0))
    risk_overlay = abs(_safe_float(row.get("risk_overlay_weight_delta"), 0.0))

    opportunity_pressure = (
        max(0.0, outcome_20) / max(20.0, ref["opportunity_baseline"])
        + max(0.0, outcome_60) / max(30.0, ref["opportunity_baseline"] * 1.5)
        + max(0.0, capture_bps) / max(8.0, ref["capture_baseline"])
        + max(0.0, signal_quality - ref["signal_quality_floor"]) * 4.0
        + max(0.0, weekly_return) * 40.0
    )
    blocked_opportunity = (
        max(0.0, blocked_counterfactual) / max(10.0, ref["blocked_baseline"])
    ) * (1.0 + max(0.0, blocked_weight) * 4.0)
    cost_pressure = (
        max(0.0, slippage_bps - ref["slippage_target"]) / max(ref["slippage_target"], 1.0)
        + max(0.0, _safe_float(row.get("avg_expected_cost_bps"), 0.0) - ref["slippage_target"] * 0.75) / max(ref["slippage_target"], 1.0)
    )
    turnover_pressure = max(0.0, turnover - ref["turnover_target"]) / max(ref["turnover_target"], 0.05)
    risk_pressure = (
        max(0.0, max_drawdown - ref["drawdown_warn"]) / max(ref["drawdown_warn"], 0.01)
        + max(0.0, risk_overlay - ref["risk_overlay_floor"]) / max(ref["risk_overlay_floor"], 0.01)
        + max(0.0, -weekly_return) * 60.0
    )
    strategy_pressure = (
        max(0.0, strategy_control - ref["strategy_control_floor"]) / max(ref["strategy_control_floor"], 0.01)
        + max(0.0, risk_overlay - ref["risk_overlay_floor"]) / max(ref["risk_overlay_floor"], 0.01)
    )
    return {
        "opportunity_pressure": opportunity_pressure,
        "blocked_opportunity": blocked_opportunity,
        "cost_pressure": cost_pressure,
        "turnover_pressure": turnover_pressure,
        "risk_pressure": risk_pressure,
        "strategy_pressure": strategy_pressure,
    }


def _base_row_score(row: Dict[str, Any]) -> float:
    return (
        (_safe_float(row.get("weekly_return"), 0.0) * 10000.0 * 0.08)
        - (abs(min(_safe_float(row.get("max_drawdown"), 0.0), 0.0)) * 10000.0 * 0.03)
        + (_safe_float(row.get("signal_quality_score"), 0.0) * 12.0)
        + (_safe_float(row.get("outcome_selected_spread_20d_bps"), 0.0) * 0.06)
        + (_safe_float(row.get("outcome_selected_spread_60d_bps"), 0.0) * 0.04)
        + (_safe_float(row.get("avg_execution_capture_bps"), 0.0) * 0.10)
        - (_safe_float(row.get("avg_actual_slippage_bps"), 0.0) * 0.06)
    )


def _candidate_bonus(family: str, pressures: Dict[str, float]) -> float:
    opportunity = _safe_float(pressures.get("opportunity_pressure"), 0.0)
    blocked = _safe_float(pressures.get("blocked_opportunity"), 0.0)
    cost = _safe_float(pressures.get("cost_pressure"), 0.0)
    turnover = _safe_float(pressures.get("turnover_pressure"), 0.0)
    risk = _safe_float(pressures.get("risk_pressure"), 0.0)
    strategy = _safe_float(pressures.get("strategy_pressure"), 0.0)
    family_code = str(family or "").strip().upper()
    if family_code == "EXECUTION_RELAX":
        return (blocked * 4.0) + (opportunity * 2.0) - (cost * 3.0) - risk
    if family_code == "EXECUTION_TIGHTEN":
        return (cost * 4.0) + (risk * 2.0) - (blocked * 3.5) - opportunity
    if family_code == "TURNOVER_TIGHTEN":
        return (turnover * 5.0) + (cost * 3.0) + (risk * 0.8) - (opportunity * 1.8)
    if family_code == "TURNOVER_RELAX":
        return (opportunity * 2.2) + (blocked * 0.5) - (turnover * 2.5) - (cost * 1.5)
    if family_code == "REGIME_DEFENSIVE":
        return (risk * 4.0) + (strategy * 2.0) - (opportunity * 3.0)
    if family_code == "REGIME_AGGRESSIVE":
        return (opportunity * 2.0) - (risk * 4.0) - (strategy * 2.0) - (turnover * 1.5) - cost
    return 0.0


def _score_candidate_rows(candidate: Dict[str, Any], rows: List[Dict[str, Any]], ref: Dict[str, float]) -> float:
    if not rows:
        return 0.0
    family = str(candidate.get("family") or "")
    scores = []
    for row in list(rows or []):
        pressures = _row_pressures(row, ref)
        scores.append(_base_row_score(row) + _candidate_bonus(family, pressures))
    return float(_mean(scores))


def _window_slice_summary(rows: List[Dict[str, Any]], ref: Dict[str, float]) -> Dict[str, float]:
    if not rows:
        return {
            "turnover": 0.0,
            "turnover_target": float(ref.get("turnover_target", 0.0) or 0.0),
            "signal_quality_score": 0.0,
            "outcome_selected_spread_5d_bps": 0.0,
            "outcome_selected_spread_20d_bps": 0.0,
            "outcome_selected_spread_60d_bps": 0.0,
            "matured_20d_avg_realized_edge_bps": 0.0,
        }
    return {
        "turnover": _mean(_safe_float(row.get("turnover"), 0.0) for row in rows),
        "turnover_target": float(ref.get("turnover_target", 0.0) or 0.0),
        "signal_quality_score": _mean(_safe_float(row.get("signal_quality_score"), 0.0) for row in rows),
        "outcome_selected_spread_5d_bps": _mean(_safe_float(row.get("outcome_selected_spread_5d_bps"), 0.0) for row in rows),
        "outcome_selected_spread_20d_bps": _mean(_safe_float(row.get("outcome_selected_spread_20d_bps"), 0.0) for row in rows),
        "outcome_selected_spread_60d_bps": _mean(_safe_float(row.get("outcome_selected_spread_60d_bps"), 0.0) for row in rows),
        "matured_20d_avg_realized_edge_bps": _mean(
            _safe_float(row.get("matured_20d_avg_realized_edge_bps"), 0.0) for row in rows
        ),
    }


def _max_consecutive_true(flags: Iterable[bool]) -> int:
    best = 0
    current = 0
    for flag in list(flags or []):
        if bool(flag):
            current += 1
            best = max(best, current)
        else:
            current = 0
    return int(best)


def _acceptance_status(
    *,
    best_candidate: Dict[str, Any],
    family_rows: List[Dict[str, Any]],
    baseline_mean_validation: float,
    window_count: int,
) -> Dict[str, Any]:
    best_family = str(best_candidate.get("family") or "BASELINE")
    improvement = float(best_candidate.get("mean_validation_score", 0.0) or 0.0) - float(baseline_mean_validation)
    win_rate = float(best_candidate.get("selected_window_rate", 0.0) or 0.0)
    min_windows_pass = int(window_count) >= 3
    post_cost_pass = improvement >= 0.50
    consecutive_stable_windows = _max_consecutive_true(
        float(row.get("validation_improvement", 0.0) or 0.0) >= -0.25
        for row in sorted(family_rows, key=lambda item: int(item.get("window_index", 0) or 0))
    )
    stability_pass = consecutive_stable_windows >= 3
    outcome_support_pass = (
        float(best_candidate.get("mean_validate_outcome_5d_bps", 0.0) or 0.0) > 0.0
        and float(best_candidate.get("mean_validate_outcome_20d_bps", 0.0) or 0.0) > 0.0
        and float(best_candidate.get("mean_validate_outcome_60d_bps", 0.0) or 0.0) > 0.0
    )
    turnover_target = max(0.05, float(best_candidate.get("mean_validate_turnover_target", 0.0) or 0.0))
    turnover_guard_pass = (
        float(best_candidate.get("mean_validate_turnover", 0.0) or 0.0) <= turnover_target * 1.25
        or improvement >= 1.00
    )
    rules = {
        "min_validation_windows": bool(min_windows_pass),
        "post_cost_improvement": bool(post_cost_pass),
        "stability": bool(stability_pass),
        "outcome_support_5_20_60": bool(outcome_support_pass),
        "turnover_guard": bool(turnover_guard_pass),
    }
    accepted = (
        best_family != "BASELINE"
        and all(bool(value) for value in rules.values())
        and win_rate >= 0.50
    )
    if best_family == "BASELINE":
        status = "KEEP_BASELINE"
        reason = "baseline remains the most stable candidate."
    elif accepted:
        status = "RECOMMEND_PATCH"
        reason = "candidate passed minimum windows, post-cost improvement, stability, outcome support, and turnover guard."
    elif improvement > 0.0:
        status = "WATCH"
        reason = "candidate improved validation, but failed one or more acceptance checks."
    else:
        status = "KEEP_BASELINE"
        reason = "candidate failed to clear post-cost validation improvement over baseline."
    failed_rules = [name for name, passed in rules.items() if not passed]
    return {
        "status": status,
        "status_reason": reason,
        "accepted": bool(accepted),
        "win_rate": float(win_rate),
        "improvement_score": float(improvement),
        "consecutive_stable_windows": int(consecutive_stable_windows),
        "acceptance_failed_rules": ",".join(failed_rules),
        "acceptance_rules": rules,
    }


def _build_walk_forward_windows(
    market: str,
    rows: List[Dict[str, Any]],
    candidates: List[Dict[str, Any]],
    profile: MarketProfileConfig,
    *,
    train_weeks: int,
    validate_weeks: int,
    step_weeks: int,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    candidate_rows: List[Dict[str, Any]] = []
    window_rows: List[Dict[str, Any]] = []
    total = len(rows)
    if total < max(1, train_weeks + validate_weeks):
        return candidate_rows, window_rows
    for start in range(0, total - train_weeks - validate_weeks + 1, max(1, step_weeks)):
        train_slice = rows[start : start + train_weeks]
        validate_slice = rows[start + train_weeks : start + train_weeks + validate_weeks]
        ref = _build_reference(profile, train_slice)
        validate_summary = _window_slice_summary(validate_slice, ref)
        best_validation_family = "BASELINE"
        best_validation_score = None
        for candidate in list(candidates or []):
            train_score = _score_candidate_rows(candidate, train_slice, ref)
            validation_score = _score_candidate_rows(candidate, validate_slice, ref)
            baseline_validation = _score_candidate_rows(candidates[0], validate_slice, ref)
            family = str(candidate.get("family") or "")
            row = {
                "market": market,
                "window_index": int(len(window_rows) + 1),
                "train_week_start": str(train_slice[0].get("week_start") or ""),
                "train_week_end": str(train_slice[-1].get("week_start") or ""),
                "validate_week_start": str(validate_slice[0].get("week_start") or ""),
                "validate_week_end": str(validate_slice[-1].get("week_start") or ""),
                "candidate_family": family,
                "candidate_family_label": str(candidate.get("family_label") or ""),
                "patch_summary": str(candidate.get("patch_summary") or ""),
                "train_score": float(train_score),
                "validation_score": float(validation_score),
                "train_post_cost_score": float(train_score),
                "validation_post_cost_score": float(validation_score),
                "baseline_validation_score": float(baseline_validation),
                "validation_improvement": float(validation_score - baseline_validation),
                "validate_turnover": float(validate_summary.get("turnover", 0.0) or 0.0),
                "validate_turnover_target": float(validate_summary.get("turnover_target", 0.0) or 0.0),
                "validate_signal_quality_score": float(validate_summary.get("signal_quality_score", 0.0) or 0.0),
                "validate_outcome_5d_bps": float(validate_summary.get("outcome_selected_spread_5d_bps", 0.0) or 0.0),
                "validate_outcome_20d_bps": float(validate_summary.get("outcome_selected_spread_20d_bps", 0.0) or 0.0),
                "validate_outcome_60d_bps": float(validate_summary.get("outcome_selected_spread_60d_bps", 0.0) or 0.0),
                "validate_realized_edge_20d_bps": float(validate_summary.get("matured_20d_avg_realized_edge_bps", 0.0) or 0.0),
            }
            candidate_rows.append(row)
            if best_validation_score is None or validation_score > best_validation_score:
                best_validation_score = validation_score
                best_validation_family = family
            elif best_validation_score is not None and abs(validation_score - best_validation_score) < 1e-12:
                incumbent = next(
                    (
                        item
                        for item in list(candidates or [])
                        if str(item.get("family") or "") == best_validation_family
                    ),
                    {},
                )
                if int(candidate.get("complexity") or 0) < int(incumbent.get("complexity") or 0):
                    best_validation_family = family
        window_rows.append(
            {
                "market": market,
                "window_index": int(len(window_rows) + 1),
                "train_week_start": str(train_slice[0].get("week_start") or ""),
                "train_week_end": str(train_slice[-1].get("week_start") or ""),
                "validate_week_start": str(validate_slice[0].get("week_start") or ""),
                "validate_week_end": str(validate_slice[-1].get("week_start") or ""),
                "selected_candidate_family": best_validation_family,
                "selected_candidate_label": _candidate_family_label(best_validation_family),
                "validate_turnover": float(validate_summary.get("turnover", 0.0) or 0.0),
                "validate_turnover_target": float(validate_summary.get("turnover_target", 0.0) or 0.0),
                "validate_signal_quality_score": float(validate_summary.get("signal_quality_score", 0.0) or 0.0),
                "validate_outcome_5d_bps": float(validate_summary.get("outcome_selected_spread_5d_bps", 0.0) or 0.0),
                "validate_outcome_20d_bps": float(validate_summary.get("outcome_selected_spread_20d_bps", 0.0) or 0.0),
                "validate_outcome_60d_bps": float(validate_summary.get("outcome_selected_spread_60d_bps", 0.0) or 0.0),
                "validate_realized_edge_20d_bps": float(validate_summary.get("matured_20d_avg_realized_edge_bps", 0.0) or 0.0),
            }
        )
    return candidate_rows, window_rows


def _candidate_patch_rows(market: str, profile_key: str, candidate: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for raw in list(candidate.get("patch_rows") or []):
        row = dict(raw)
        rows.append(
            {
                "market": market,
                "profile": profile_key,
                "candidate_family": str(candidate.get("family") or ""),
                "candidate_family_label": str(candidate.get("family_label") or ""),
                "config_path": f"market_profiles.{profile_key}.{row['field']}",
                "field": str(row["field"]),
                "current_value": float(row["current_value"]),
                "suggested_value": float(row["suggested_value"]),
                "delta": float(row["delta"]),
            }
        )
    return rows


def build_market_walk_forward_report(
    db_path: str | Path,
    *,
    adaptive_strategy_config: str | Path = "config/adaptive_strategy_framework.yaml",
    markets: List[str] | None = None,
    min_weeks: int = 10,
    train_weeks: int = 8,
    validate_weeks: int = 4,
    step_weeks: int = 2,
) -> Dict[str, Any]:
    config_path = adaptive_strategy_config_path(BASE_DIR, str(adaptive_strategy_config))
    cfg = load_adaptive_strategy(BASE_DIR, str(config_path))
    history_rows = _load_weekly_tuning_history(Path(db_path), markets=markets)
    market_week_rows = _aggregate_market_week_rows(history_rows)

    by_market: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in list(market_week_rows or []):
        by_market[str(row.get("market") or "")].append(dict(row))

    summary_rows: List[Dict[str, Any]] = []
    candidate_summary_rows: List[Dict[str, Any]] = []
    window_rows_out: List[Dict[str, Any]] = []
    patch_rows: List[Dict[str, Any]] = []

    wanted_markets = list(markets or [])
    if not wanted_markets:
        wanted_markets = sorted(by_market)

    for market in wanted_markets:
        market_code = resolve_market_code(market)
        rows = sorted(by_market.get(market_code, []), key=lambda item: str(item.get("week_start") or ""))
        profile_key, profile = adaptive_strategy_market_profile(cfg, market_code)
        candidates = _build_candidate_set(profile)
        if len(rows) < max(1, int(min_weeks)):
            summary_rows.append(
                {
                    "market": market_code,
                    "profile": profile_key,
                    "sample_weeks": int(len(rows)),
                    "window_count": 0,
                    "selected_candidate_family": "BASELINE",
                    "selected_candidate_label": _candidate_family_label("BASELINE"),
                    "baseline_validation_score": 0.0,
                    "tuned_validation_score": 0.0,
                    "improvement_score": 0.0,
                    "win_rate": 0.0,
                    "status": "INSUFFICIENT_HISTORY",
                    "status_reason": f"need >= {int(min_weeks)} weeks, got {int(len(rows))}",
                    "patch_summary": "insufficient history",
                }
            )
            continue

        candidate_rows, window_rows = _build_walk_forward_windows(
            market_code,
            rows,
            candidates,
            profile,
            train_weeks=int(train_weeks),
            validate_weeks=int(validate_weeks),
            step_weeks=int(step_weeks),
        )
        window_rows_out.extend(window_rows)
        if not candidate_rows:
            summary_rows.append(
                {
                    "market": market_code,
                    "profile": profile_key,
                    "sample_weeks": int(len(rows)),
                    "window_count": 0,
                    "selected_candidate_family": "BASELINE",
                    "selected_candidate_label": _candidate_family_label("BASELINE"),
                    "baseline_validation_score": 0.0,
                    "tuned_validation_score": 0.0,
                    "improvement_score": 0.0,
                    "win_rate": 0.0,
                    "status": "INSUFFICIENT_HISTORY",
                    "status_reason": "not enough windows for requested train/validate lengths",
                    "patch_summary": "insufficient windows",
                }
            )
            continue

        grouped_candidates: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in list(candidate_rows or []):
            grouped_candidates[str(row.get("candidate_family") or "")].append(dict(row))

        best_candidate: Dict[str, Any] | None = None
        baseline_mean_validation = _mean(
            float(row.get("validation_score", 0.0) or 0.0) for row in grouped_candidates.get("BASELINE", [])
        )
        for candidate in list(candidates or []):
            family = str(candidate.get("family") or "")
            family_rows = grouped_candidates.get(family, [])
            if not family_rows:
                continue
            window_count = len(family_rows)
            validation_mean = _mean(float(row.get("validation_score", 0.0) or 0.0) for row in family_rows)
            train_mean = _mean(float(row.get("train_score", 0.0) or 0.0) for row in family_rows)
            improvement_mean = _mean(float(row.get("validation_improvement", 0.0) or 0.0) for row in family_rows)
            selected_windows = sum(
                1
                for window in list(window_rows or [])
                if str(window.get("selected_candidate_family") or "") == family
            )
            candidate_summary_rows.append(
                {
                    "market": market_code,
                    "profile": profile_key,
                    "candidate_family": family,
                    "candidate_family_label": str(candidate.get("family_label") or ""),
                    "window_count": int(window_count),
                    "selected_window_count": int(selected_windows),
                    "selected_window_rate": float(selected_windows / max(1, len(window_rows))),
                    "mean_train_score": float(train_mean),
                    "mean_validation_score": float(validation_mean),
                    "mean_validation_improvement": float(improvement_mean),
                    "mean_validate_turnover": float(_mean(_safe_float(row.get("validate_turnover"), 0.0) for row in family_rows)),
                    "mean_validate_turnover_target": float(_mean(_safe_float(row.get("validate_turnover_target"), 0.0) for row in family_rows)),
                    "mean_validate_outcome_5d_bps": float(_mean(_safe_float(row.get("validate_outcome_5d_bps"), 0.0) for row in family_rows)),
                    "mean_validate_outcome_20d_bps": float(_mean(_safe_float(row.get("validate_outcome_20d_bps"), 0.0) for row in family_rows)),
                    "mean_validate_outcome_60d_bps": float(_mean(_safe_float(row.get("validate_outcome_60d_bps"), 0.0) for row in family_rows)),
                    "mean_validate_realized_edge_20d_bps": float(_mean(_safe_float(row.get("validate_realized_edge_20d_bps"), 0.0) for row in family_rows)),
                    "patch_summary": str(candidate.get("patch_summary") or ""),
                }
            )
            current_eval = {
                "family": family,
                "family_label": str(candidate.get("family_label") or ""),
                "patch_summary": str(candidate.get("patch_summary") or ""),
                "patch_rows": list(candidate.get("patch_rows") or []),
                "window_count": int(window_count),
                "selected_window_count": int(selected_windows),
                "selected_window_rate": float(selected_windows / max(1, len(window_rows))),
                "mean_train_score": float(train_mean),
                "mean_validation_score": float(validation_mean),
                "mean_validation_improvement": float(improvement_mean),
                "mean_validate_turnover": float(_mean(_safe_float(row.get("validate_turnover"), 0.0) for row in family_rows)),
                "mean_validate_turnover_target": float(_mean(_safe_float(row.get("validate_turnover_target"), 0.0) for row in family_rows)),
                "mean_validate_outcome_5d_bps": float(_mean(_safe_float(row.get("validate_outcome_5d_bps"), 0.0) for row in family_rows)),
                "mean_validate_outcome_20d_bps": float(_mean(_safe_float(row.get("validate_outcome_20d_bps"), 0.0) for row in family_rows)),
                "mean_validate_outcome_60d_bps": float(_mean(_safe_float(row.get("validate_outcome_60d_bps"), 0.0) for row in family_rows)),
                "mean_validate_realized_edge_20d_bps": float(_mean(_safe_float(row.get("validate_realized_edge_20d_bps"), 0.0) for row in family_rows)),
                "complexity": int(candidate.get("complexity") or 0),
            }
            if best_candidate is None:
                best_candidate = current_eval
                continue
            if current_eval["mean_validation_score"] > float(best_candidate.get("mean_validation_score", 0.0)):
                best_candidate = current_eval
            elif abs(current_eval["mean_validation_score"] - float(best_candidate.get("mean_validation_score", 0.0))) < 1e-12:
                if current_eval["complexity"] < int(best_candidate.get("complexity", 0)):
                    best_candidate = current_eval

        best_candidate = dict(best_candidate or {})
        best_family = str(best_candidate.get("family") or "BASELINE")
        acceptance = _acceptance_status(
            best_candidate=best_candidate,
            family_rows=grouped_candidates.get(best_family, []),
            baseline_mean_validation=baseline_mean_validation,
            window_count=len(window_rows),
        )
        improvement = float(acceptance.get("improvement_score", 0.0) or 0.0)
        win_rate = float(acceptance.get("win_rate", 0.0) or 0.0)
        status = str(acceptance.get("status") or "KEEP_BASELINE")
        status_reason = str(acceptance.get("status_reason") or "baseline remains the most stable validation candidate.")
        patch_rows.extend(_candidate_patch_rows(market_code, profile_key, best_candidate))
        summary_rows.append(
            {
                "market": market_code,
                "profile": profile_key,
                "sample_weeks": int(len(rows)),
                "window_count": int(len(window_rows)),
                "selected_candidate_family": best_family,
                "selected_candidate_label": str(best_candidate.get("family_label") or _candidate_family_label(best_family)),
                "baseline_validation_score": float(baseline_mean_validation),
                "tuned_validation_score": float(best_candidate.get("mean_validation_score", 0.0) or 0.0),
                "improvement_score": float(improvement),
                "win_rate": float(win_rate),
                "consecutive_stable_windows": int(acceptance.get("consecutive_stable_windows", 0) or 0),
                "acceptance_failed_rules": str(acceptance.get("acceptance_failed_rules") or ""),
                "acceptance_rules": dict(acceptance.get("acceptance_rules") or {}),
                "mean_validate_turnover": float(best_candidate.get("mean_validate_turnover", 0.0) or 0.0),
                "mean_validate_turnover_target": float(best_candidate.get("mean_validate_turnover_target", 0.0) or 0.0),
                "mean_validate_outcome_5d_bps": float(best_candidate.get("mean_validate_outcome_5d_bps", 0.0) or 0.0),
                "mean_validate_outcome_20d_bps": float(best_candidate.get("mean_validate_outcome_20d_bps", 0.0) or 0.0),
                "mean_validate_outcome_60d_bps": float(best_candidate.get("mean_validate_outcome_60d_bps", 0.0) or 0.0),
                "mean_validate_realized_edge_20d_bps": float(best_candidate.get("mean_validate_realized_edge_20d_bps", 0.0) or 0.0),
                "status": status,
                "status_reason": status_reason,
                "patch_summary": str(best_candidate.get("patch_summary") or "keep baseline"),
            }
        )

    summary_rows.sort(key=lambda row: (str(row.get("market") or ""), str(row.get("profile") or "")))
    candidate_summary_rows.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            -float(row.get("mean_validation_score", 0.0) or 0.0),
            int(row.get("window_count", 0) or 0),
        )
    )
    patch_rows.sort(key=lambda row: (str(row.get("market") or ""), str(row.get("config_path") or "")))
    best_summary = max(summary_rows, key=lambda row: float(row.get("improvement_score", 0.0) or 0.0), default={})
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "config_path": str(config_path),
        "min_weeks": int(min_weeks),
        "train_weeks": int(train_weeks),
        "validate_weeks": int(validate_weeks),
        "step_weeks": int(step_weeks),
        "summary_rows": summary_rows,
        "candidate_summary_rows": candidate_summary_rows,
        "window_rows": window_rows_out,
        "patch_rows": patch_rows,
        "summary": {
            "market_count": int(len(summary_rows)),
            "window_count": int(len(window_rows_out)),
            "recommended_patch_count": int(sum(1 for row in summary_rows if str(row.get("status") or "") == "RECOMMEND_PATCH")),
            "watch_count": int(sum(1 for row in summary_rows if str(row.get("status") or "") == "WATCH")),
            "best_market": str(best_summary.get("market") or "-"),
            "best_candidate": str(best_summary.get("selected_candidate_family") or "BASELINE"),
        },
    }


def _write_walk_forward_markdown(
    path: Path,
    *,
    report: Dict[str, Any],
) -> None:
    summary_rows = list(report.get("summary_rows") or [])
    patch_rows = list(report.get("patch_rows") or [])
    candidate_rows = list(report.get("candidate_summary_rows") or [])
    window_rows = list(report.get("window_rows") or [])
    lines = [
        "# Market Walk-Forward Tuning",
        "",
        f"- Generated: {str(report.get('generated_at') or '')}",
        f"- Config: {str(report.get('config_path') or '')}",
        f"- Train / validate / step: {int(report.get('train_weeks', 0) or 0)} / {int(report.get('validate_weeks', 0) or 0)} / {int(report.get('step_weeks', 0) or 0)}",
        "",
        "## Market Summary",
        "",
    ]
    if not summary_rows:
        lines.append("- No qualifying weekly tuning history was found.")
    else:
        for row in summary_rows:
            lines.append(
                "- {market} [{status}] {candidate}: validation {tuned:.2f} vs baseline {base:.2f} "
                "(delta {delta:.2f}, win_rate {win_rate:.0%}) | {reason}".format(
                    market=str(row.get("market") or "-"),
                    status=str(row.get("status") or "-"),
                    candidate=str(row.get("selected_candidate_label") or "-"),
                    tuned=float(row.get("tuned_validation_score", 0.0) or 0.0),
                    base=float(row.get("baseline_validation_score", 0.0) or 0.0),
                    delta=float(row.get("improvement_score", 0.0) or 0.0),
                    win_rate=float(row.get("win_rate", 0.0) or 0.0),
                    reason=str(row.get("status_reason") or ""),
                )
            )
            if str(row.get("acceptance_failed_rules") or "").strip():
                lines.append(f"  failed_rules: {str(row.get('acceptance_failed_rules') or '')}")
            lines.append(
                "  acceptance: stable_windows={stable} turnover={turnover:.3f}/{target:.3f} "
                "outcome_5/20/60={o5:.1f}/{o20:.1f}/{o60:.1f} realized_20d={realized:.1f}".format(
                    stable=int(row.get("consecutive_stable_windows", 0) or 0),
                    turnover=float(row.get("mean_validate_turnover", 0.0) or 0.0),
                    target=float(row.get("mean_validate_turnover_target", 0.0) or 0.0),
                    o5=float(row.get("mean_validate_outcome_5d_bps", 0.0) or 0.0),
                    o20=float(row.get("mean_validate_outcome_20d_bps", 0.0) or 0.0),
                    o60=float(row.get("mean_validate_outcome_60d_bps", 0.0) or 0.0),
                    realized=float(row.get("mean_validate_realized_edge_20d_bps", 0.0) or 0.0),
                )
            )
            lines.append(f"  patch: {str(row.get('patch_summary') or 'keep baseline')}")
    lines.extend(["", "## Patch Rows", ""])
    if not patch_rows:
        lines.append("- No patch rows were recommended.")
    else:
        for row in patch_rows:
            lines.append(
                "- {market} {field}: {current:.4f} -> {suggested:.4f} ({delta:+.4f})".format(
                    market=str(row.get("market") or "-"),
                    field=str(row.get("field") or "-"),
                    current=float(row.get("current_value", 0.0) or 0.0),
                    suggested=float(row.get("suggested_value", 0.0) or 0.0),
                    delta=float(row.get("delta", 0.0) or 0.0),
                )
            )
    lines.extend(["", "## Candidate Summary", ""])
    if not candidate_rows:
        lines.append("- No candidate summaries were generated.")
    else:
        for row in candidate_rows:
            lines.append(
                "- {market} {family}: validation {score:.2f}, improvement {delta:.2f}, selected {wins}/{windows} windows".format(
                    market=str(row.get("market") or "-"),
                    family=str(row.get("candidate_family_label") or "-"),
                    score=float(row.get("mean_validation_score", 0.0) or 0.0),
                    delta=float(row.get("mean_validation_improvement", 0.0) or 0.0),
                    wins=int(row.get("selected_window_count", 0) or 0),
                    windows=int(row.get("window_count", 0) or 0),
                )
            )
    lines.extend(["", "## Window Winners", ""])
    if not window_rows:
        lines.append("- No walk-forward windows were generated.")
    else:
        for row in window_rows:
            lines.append(
                "- {market} window {idx}: {candidate} ({start} -> {end})".format(
                    market=str(row.get("market") or "-"),
                    idx=int(row.get("window_index", 0) or 0),
                    candidate=str(row.get("selected_candidate_label") or "-"),
                    start=str(row.get("validate_week_start") or "-"),
                    end=str(row.get("validate_week_end") or "-"),
                )
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _cli_summary_payload(report: Dict[str, Any], out_dir: Path) -> tuple[Dict[str, Any], Dict[str, Path]]:
    summary = dict(report.get("summary") or {})
    summary_contract = WalkForwardSummary(
        market_count=int(summary.get("market_count", 0) or 0),
        window_count=int(summary.get("window_count", 0) or 0),
        recommended_patch_count=int(summary.get("recommended_patch_count", 0) or 0),
        best_market=str(summary.get("best_market") or "-"),
        best_candidate=str(summary.get("best_candidate") or "BASELINE"),
    )
    artifacts = ArtifactBundle(
        summary_json=out_dir / "market_walk_forward_summary.json",
        summary_csv=out_dir / "market_walk_forward_summary.csv",
        rows_csv=out_dir / "market_walk_forward_candidate_summary.csv",
        report_md=out_dir / "market_walk_forward.md",
    )
    return summary_contract.to_dict(), artifacts.to_dict()


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    db_path = resolve_repo_path(BASE_DIR, str(args.db))
    out_dir = resolve_repo_path(BASE_DIR, str(args.out_dir))
    out_dir.mkdir(parents=True, exist_ok=True)
    markets = _market_list_from_args(getattr(args, "market", ""), getattr(args, "markets", ""))
    report = build_market_walk_forward_report(
        db_path,
        adaptive_strategy_config=resolve_repo_path(BASE_DIR, str(args.adaptive_strategy_config)),
        markets=markets,
        min_weeks=int(args.min_weeks),
        train_weeks=int(args.train_weeks),
        validate_weeks=int(args.validate_weeks),
        step_weeks=int(args.step_weeks),
    )
    summary_rows = list(report.get("summary_rows") or [])
    candidate_rows = list(report.get("candidate_summary_rows") or [])
    window_rows = list(report.get("window_rows") or [])
    patch_rows = list(report.get("patch_rows") or [])
    write_csv(str(out_dir / "market_walk_forward_summary.csv"), summary_rows)
    write_csv(str(out_dir / "market_walk_forward_candidate_summary.csv"), candidate_rows)
    write_csv(str(out_dir / "market_walk_forward_windows.csv"), window_rows)
    write_csv(str(out_dir / "market_walk_forward_patch_recommendations.csv"), patch_rows)
    write_json(str(out_dir / "market_walk_forward_summary.json"), report)
    _write_walk_forward_markdown(out_dir / "market_walk_forward.md", report=report)
    summary_fields, artifact_fields = _cli_summary_payload(report, out_dir)
    emit_cli_summary(
        command="ibkr-quant-walk-forward",
        headline="market walk-forward tuning complete",
        summary=summary_fields,
        artifacts=artifact_fields,
    )


if __name__ == "__main__":
    main()
