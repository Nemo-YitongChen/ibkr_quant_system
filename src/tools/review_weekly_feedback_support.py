from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List

from ..analysis.investment_portfolio import InvestmentPaperConfig
from ..common.account_profile import load_account_profiles, resolved_account_profile_summary
from ..common.adaptive_strategy import (
    adaptive_strategy_context,
    adaptive_strategy_effective_controls_human_note,
    load_adaptive_strategy,
)
from ..common.strategy_parameter_registry import (
    StrategyParameterRegistry,
    load_strategy_parameter_registry,
    strategy_parameter_field_meta,
    strategy_parameter_priority,
    strategy_parameter_proposed_value,
)
from ..common.market_structure import load_market_structure, market_structure_summary
from ..common.markets import market_config_path, resolve_market_code
from ..common.runtime_paths import resolve_repo_path
from ..common.storage import Storage
from ..portfolio.investment_allocator import InvestmentExecutionConfig
from .review_weekly_io import load_json_file as _load_json_file, load_yaml_file as _load_yaml_file
from .review_weekly_thresholds import (
    feedback_action_field,
    feedback_automation_basis_label,
    feedback_automation_mode_label,
    feedback_automation_thresholds,
    feedback_kind_label,
    feedback_maturity_label,
    feedback_threshold_effect_bucket,
)

BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_PAPER_CFG = InvestmentPaperConfig()
DEFAULT_EXECUTION_CFG = InvestmentExecutionConfig()
_STRATEGY_PARAMETER_REGISTRY: StrategyParameterRegistry | None = None


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _feedback_confidence_label(value: float) -> str:
    value = _clamp(value, 0.0, 1.0)
    if value >= 0.75:
        return "HIGH"
    if value >= 0.45:
        return "MEDIUM"
    return "LOW"


def _feedback_confidence(
    sample_ratio: float,
    magnitude_ratio: float,
    persistence_ratio: float = 0.0,
    structure_ratio: float = 0.0,
) -> float:
    sample_ratio = _clamp(sample_ratio, 0.0, 1.0)
    magnitude_ratio = _clamp(magnitude_ratio, 0.0, 1.0)
    persistence_ratio = _clamp(persistence_ratio, 0.0, 1.0)
    structure_ratio = _clamp(structure_ratio, 0.0, 1.0)
    return round(
        float(
            0.40 * sample_ratio
            + 0.25 * magnitude_ratio
            + 0.20 * persistence_ratio
            + 0.15 * structure_ratio
        ),
        6,
    )


def _select_feedback_calibration_rows(rows: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], str, str]:
    actionable_actions = {"ACCUMULATE", "HOLD", "REDUCE", "BUY", "SELL"}
    enriched: List[Dict[str, Any]] = []
    for raw in list(rows or []):
        item = dict(raw)
        details = _parse_json_dict(item.get("details"))
        item["_details"] = details
        item["_stage"] = str(details.get("stage") or "").strip().lower()
        item["_action"] = str(details.get("action") or "").strip().upper()
        enriched.append(item)

    action_rows = [row for row in enriched if str(row.get("_action") or "") in actionable_actions]
    final_rows = [row for row in enriched if str(row.get("_stage") or "") in {"final", "short"}]
    final_action_rows = [row for row in final_rows if str(row.get("_action") or "") in actionable_actions]
    candidates = [
        ("FINAL_ACTIONABLE", final_action_rows),
        ("FINAL_ONLY", final_rows),
        ("ACTIONABLE_ONLY", action_rows),
        ("ALL", enriched),
    ]
    preferred_horizons = (20, 60, 5)
    for scope, scope_rows in candidates:
        for horizon in preferred_horizons:
            horizon_rows = [row for row in scope_rows if int(row.get("horizon_days") or 0) == int(horizon)]
            if len(horizon_rows) >= 6:
                return horizon_rows, scope, str(horizon)
    for scope, scope_rows in candidates:
        if scope_rows:
            return list(scope_rows), scope, "ALL"
    return [], "ALL", "ALL"


def _score_alignment_score(rows: List[Dict[str, Any]]) -> tuple[float, float]:
    scored_rows: List[tuple[float, float]] = []
    for row in list(rows or []):
        details = dict(row.get("_details") or {})
        score = float(details.get("model_recommendation_score", details.get("score", 0.0)) or 0.0)
        future_return = float(row.get("future_return") or 0.0)
        scored_rows.append((score, future_return))
    if len(scored_rows) < 6:
        return 0.5, 0.0
    scored_rows.sort(key=lambda item: item[0])
    bucket_size = max(2, len(scored_rows) // 3)
    bottom = scored_rows[:bucket_size]
    top = scored_rows[-bucket_size:]
    top_avg = _avg_defined([item[1] for item in top]) or 0.0
    bottom_avg = _avg_defined([item[1] for item in bottom]) or 0.0
    gap = float(top_avg - bottom_avg)
    score = _clamp(0.5 + gap / 0.20, 0.0, 1.0)
    return float(score), float(gap)


def _build_feedback_calibration_rows(outcome_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for raw in list(outcome_rows or []):
        portfolio_id = str(raw.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        grouped.setdefault(portfolio_id, []).append(dict(raw))

    scope_labels = {
        "FINAL_ACTIONABLE": "final 可执行候选",
        "FINAL_ONLY": "final 候选",
        "ACTIONABLE_ONLY": "可执行候选",
        "ALL": "全部回标",
    }
    rows: List[Dict[str, Any]] = []
    now_utc = datetime.now(timezone.utc)
    for portfolio_id, portfolio_rows in grouped.items():
        selected_rows, selected_scope, selected_horizon = _select_feedback_calibration_rows(portfolio_rows)
        if not selected_rows:
            continue
        sample_count = int(len(selected_rows))
        latest_outcome_ts = max(str(row.get("outcome_ts") or "") for row in selected_rows)
        latest_outcome_dt = None
        if latest_outcome_ts:
            try:
                latest_outcome_dt = datetime.fromisoformat(latest_outcome_ts.replace("Z", "+00:00"))
            except Exception:
                latest_outcome_dt = None
        age_days = max(0.0, float((now_utc - latest_outcome_dt).days)) if latest_outcome_dt is not None else 999.0

        positive_count = int(
            sum(1 for row in selected_rows if str(row.get("outcome_label") or "").upper() in {"POSITIVE", "OUTPERFORM"})
        )
        broken_count = int(sum(1 for row in selected_rows if str(row.get("outcome_label") or "").upper() == "BROKEN"))
        negative_count = int(
            sum(1 for row in selected_rows if str(row.get("outcome_label") or "").upper() in {"NEGATIVE", "BROKEN"})
        )
        avg_future_return = _avg_defined([float(row.get("future_return") or 0.0) for row in selected_rows]) or 0.0
        avg_max_drawdown = _avg_defined([float(row.get("max_drawdown") or 0.0) for row in selected_rows]) or 0.0
        avg_model_score = _avg_defined(
            [
                float(
                    dict(row.get("_details") or {}).get(
                        "model_recommendation_score",
                        dict(row.get("_details") or {}).get("score", 0.0),
                    )
                    or 0.0
                )
                for row in selected_rows
            ]
        ) or 0.0
        avg_execution_score = _avg_defined(
            [float(dict(row.get("_details") or {}).get("execution_score", 0.0) or 0.0) for row in selected_rows]
        ) or 0.0
        positive_rate = float(positive_count / max(sample_count, 1))
        broken_rate = float(broken_count / max(sample_count, 1))
        negative_rate = float(negative_count / max(sample_count, 1))
        sample_ratio = _clamp(sample_count / 24.0, 0.0, 1.0)
        return_health = _clamp((avg_future_return + 0.05) / 0.15, 0.0, 1.0)
        drawdown_health = _clamp((avg_max_drawdown + 0.18) / 0.18, 0.0, 1.0)
        freshness_ratio = _clamp((90.0 - age_days) / 90.0, 0.0, 1.0)
        score_alignment_score, score_alignment_gap = _score_alignment_score(selected_rows)

        signal_quality_score = _clamp(
            0.30 * positive_rate
            + 0.20 * (1.0 - broken_rate)
            + 0.20 * return_health
            + 0.15 * drawdown_health
            + 0.15 * score_alignment_score,
            0.0,
            1.0,
        )
        shadow_threshold_relax_support = _clamp(
            0.35 * positive_rate
            + 0.20 * return_health
            + 0.15 * drawdown_health
            + 0.15 * sample_ratio
            + 0.15 * score_alignment_score,
            0.0,
            1.0,
        )
        shadow_weak_signal_support = _clamp(
            0.35 * broken_rate
            + 0.20 * negative_rate
            + 0.15 * (1.0 - return_health)
            + 0.15 * (1.0 - drawdown_health)
            + 0.15 * (1.0 - score_alignment_score),
            0.0,
            1.0,
        )
        risk_tighten_support = _clamp(
            0.35 * broken_rate
            + 0.20 * (1.0 - drawdown_health)
            + 0.20 * (1.0 - return_health)
            + 0.15 * (1.0 - signal_quality_score)
            + 0.10 * (1.0 - freshness_ratio),
            0.0,
            1.0,
        )
        risk_relax_support = _clamp(
            0.30 * positive_rate
            + 0.20 * return_health
            + 0.20 * drawdown_health
            + 0.15 * signal_quality_score
            + 0.15 * sample_ratio,
            0.0,
            1.0,
        )
        execution_support = _clamp(
            0.35 * signal_quality_score
            + 0.20 * positive_rate
            + 0.20 * drawdown_health
            + 0.15 * sample_ratio
            + 0.10 * score_alignment_score,
            0.0,
            1.0,
        )
        calibration_confidence = _feedback_confidence(
            sample_ratio=sample_ratio,
            magnitude_ratio=max(abs(signal_quality_score - 0.5) * 2.0, abs(risk_tighten_support - 0.5) * 2.0),
            persistence_ratio=score_alignment_score,
            structure_ratio=freshness_ratio,
        )
        if signal_quality_score >= 0.65:
            calibration_reason = "近期候选回标整体偏强，说明 alpha 仍有一定稳定性，适合更积极地校准阈值与执行参数。"
        elif signal_quality_score <= 0.35:
            calibration_reason = "近期候选回标偏弱且 downside 偏大，说明策略仍需要保守处理，自动调参应偏谨慎。"
        else:
            calibration_reason = "近期候选回标强弱参半，适合维持中性校准，逐步累积更多样本后再放大自动调参。"

        rows.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(selected_rows[0].get("market") or ""),
                "selection_scope": selected_scope,
                "selection_scope_label": scope_labels.get(selected_scope, selected_scope),
                "selected_horizon_days": str(selected_horizon),
                "outcome_sample_count": sample_count,
                "latest_outcome_ts": latest_outcome_ts,
                "outcome_positive_rate": round(float(positive_rate), 6),
                "outcome_broken_rate": round(float(broken_rate), 6),
                "outcome_negative_rate": round(float(negative_rate), 6),
                "avg_future_return": round(float(avg_future_return), 6),
                "avg_max_drawdown": round(float(avg_max_drawdown), 6),
                "avg_model_score": round(float(avg_model_score), 6),
                "avg_execution_score": round(float(avg_execution_score), 6),
                "score_alignment_score": round(float(score_alignment_score), 6),
                "score_alignment_gap": round(float(score_alignment_gap), 6),
                "signal_quality_score": round(float(signal_quality_score), 6),
                "shadow_threshold_relax_support": round(float(shadow_threshold_relax_support), 6),
                "shadow_weak_signal_support": round(float(shadow_weak_signal_support), 6),
                "risk_tighten_support": round(float(risk_tighten_support), 6),
                "risk_relax_support": round(float(risk_relax_support), 6),
                "execution_support": round(float(execution_support), 6),
                "calibration_confidence": round(float(calibration_confidence), 6),
                "calibration_confidence_label": _feedback_confidence_label(calibration_confidence),
                "calibration_reason": calibration_reason,
            }
        )
    rows.sort(
        key=lambda row: (
            -int(row.get("outcome_sample_count", 0) or 0),
            -float(row.get("signal_quality_score", 0.0) or 0.0),
            str(row.get("portfolio_id") or ""),
        )
    )
    return rows


def _avg_defined(values: List[Any]) -> float | None:
    defined: List[float] = []
    for value in values:
        if value in (None, ""):
            continue
        try:
            defined.append(float(value))
        except Exception:
            continue
    if not defined:
        return None
    return float(sum(defined) / len(defined))


def _parse_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not isinstance(value, str) or not value:
        return {}
    try:
        data = json.loads(value)
        return dict(data) if isinstance(data, dict) else {}
    except Exception:
        return {}


def _parse_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return list(value)
    if not isinstance(value, str) or not value:
        return []
    try:
        data = json.loads(value)
        return list(data) if isinstance(data, list) else []
    except Exception:
        return []


def _parse_shadow_metric(text: str, key: str, operator: str = "=") -> float | None:
    match = re.search(rf"{re.escape(key)}\s*{re.escape(operator)}\s*([-+]?\d+(?:\.\d+)?)", str(text or ""))
    if not match:
        return None
    try:
        return float(match.group(1))
    except Exception:
        return None


def _latest_report_dir(runs_by_portfolio: Dict[str, List[Dict[str, Any]]], portfolio_id: str) -> str:
    rows = list(runs_by_portfolio.get(portfolio_id) or [])
    if not rows:
        return ""
    latest = rows[-1]
    details = _parse_json_dict(latest.get("details"))
    return str(details.get("report_dir") or latest.get("report_dir") or "").strip()


def _load_market_sentiment(report_dir: str) -> Dict[str, Any]:
    if not report_dir:
        return {}
    path = Path(report_dir) / "market_sentiment.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return dict(data) if isinstance(data, dict) else {}
    except Exception:
        return {}


def _report_json(report_dir: str, name: str) -> Dict[str, Any]:
    if not report_dir:
        return {}
    path = resolve_repo_path(BASE_DIR, report_dir) / name
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return dict(data) if isinstance(data, dict) else {}
    except Exception:
        return {}


def _load_report_data_warning(report_dir: str) -> str:
    if not report_dir:
        return ""
    path = resolve_repo_path(BASE_DIR, report_dir) / "investment_report.md"
    if not path.exists():
        return ""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return ""
    for line in lines[:80]:
        text = str(line or "").strip()
        if text.startswith("数据提醒:"):
            return text.removeprefix("数据提醒:").strip()
    return ""


def _market_research_only_yfinance(market_code: str) -> bool:
    code = resolve_market_code(str(market_code or ""))
    if not code:
        return False
    universe_cfg = _load_yaml_file(resolve_repo_path(BASE_DIR, f"config/markets/{code.lower()}/universe.yaml"))
    ibkr_cfg = _load_yaml_file(market_config_path(BASE_DIR, code))
    investment_cfg_path = str(ibkr_cfg.get("investment_config", f"config/investment_{code.lower()}.yaml") or "")
    investment_cfg = _load_yaml_file(resolve_repo_path(BASE_DIR, investment_cfg_path))
    return bool(
        universe_cfg.get("research_only_yfinance", False)
        or ibkr_cfg.get("research_only_yfinance", False)
        or investment_cfg.get("research_only_yfinance", False)
    )


def _load_ibkr_history_probe_market_map(preflight_dir: Path) -> Dict[str, Dict[str, Any]]:
    summary = _load_json_file(preflight_dir / "ibkr_history_probe_summary.json")
    out: Dict[str, Dict[str, Any]] = {}
    for raw in list(summary.get("market_summary", []) or []):
        row = dict(raw or {})
        market = resolve_market_code(str(row.get("market") or ""))
        if market:
            out[market] = row
    return out


def _build_market_data_gate_map(
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
    *,
    preflight_dir: Path,
) -> Dict[str, Dict[str, Any]]:
    probe_market_map = _load_ibkr_history_probe_market_map(preflight_dir)
    out: Dict[str, Dict[str, Any]] = {}
    for portfolio_id, rows in list(runs_by_portfolio.items()):
        if not rows:
            continue
        latest = dict(rows[-1] or {})
        market = resolve_market_code(str(latest.get("market") or ""))
        report_dir = _latest_report_dir(runs_by_portfolio, portfolio_id)
        report_path = resolve_repo_path(BASE_DIR, report_dir) if report_dir else Path()
        data_quality = _load_json_file(report_path / "investment_data_quality_summary.json") if report_dir else {}
        counts = dict(data_quality.get("history_source_counts", {}) or {})
        ibkr_count = int(counts.get("ibkr", 0) or 0)
        yfinance_count = int(counts.get("yfinance", 0) or 0)
        missing_count = int(counts.get("missing", 0) or 0)
        warning_text = _load_report_data_warning(report_dir)
        research_only = _market_research_only_yfinance(market)
        probe_row = dict(probe_market_map.get(market, {}) or {})
        probe_status = str(probe_row.get("status_label") or "").strip()
        probe_diagnosis = str(probe_row.get("diagnosis") or "").strip()

        status_code = "UNKNOWN"
        status_label = "未检查"
        reason = "当前还没有足够的市场数据诊断结果，先保持保守。"
        if research_only and (yfinance_count > 0 or "yfinance" in warning_text.lower()):
            status_code = "RESEARCH_FALLBACK"
            status_label = "研究Fallback"
            reason = "当前市场配置为 research-only fallback，周报反馈更适合作为研究建议，不直接自动应用。"
        elif probe_status == "权限待补":
            status_code = "ATTENTION"
            status_label = "待排查"
            reason = probe_diagnosis or "IBKR 历史权限待补，当前不适合直接自动放大 weekly feedback。"
        elif ibkr_count <= 0 and yfinance_count > 0:
            status_code = "ATTENTION"
            status_label = "待排查"
            reason = "当前历史数据主要依赖 yfinance fallback，优先排查 IBKR 历史权限、订阅或合约覆盖。"
        elif missing_count > 0 and ibkr_count <= 0:
            status_code = "ATTENTION"
            status_label = "待排查"
            reason = "当前仍存在历史缺失且 IBKR 主源不可用，先避免自动放大调参。"
        elif ibkr_count > 0 or probe_status == "正常":
            status_code = "OK"
            status_label = "IBKR正常"
            reason = "当前历史数据主源稳定，可继续按常规流程观察 weekly feedback。"
        out[str(portfolio_id)] = {
            "portfolio_id": str(portfolio_id),
            "market": market,
            "status_code": status_code,
            "status_label": status_label,
            "reason": reason,
            "report_data_warning": warning_text,
            "research_only_yfinance": int(research_only),
            "probe_status_label": probe_status,
            "probe_diagnosis": probe_diagnosis,
            "history_ibkr_count": ibkr_count,
            "history_yfinance_count": yfinance_count,
            "history_missing_count": missing_count,
        }
    return out


def _resolve_labeling_summary_dir(base_dir: Path, path_str: str, market_filter: str) -> Path | None:
    raw_candidates = [str(path_str or "").strip()] if str(path_str or "").strip() else [
        "reports_investment_labeling",
        "reports_investment_labels",
    ]
    suffixes = []
    if market_filter:
        suffixes.append(market_filter.lower())
    suffixes.extend(["all", ""])
    for raw in raw_candidates:
        base = resolve_repo_path(base_dir, raw)
        for suffix in suffixes:
            candidate = (base / suffix) if suffix else base
            if (candidate / "investment_candidate_outcomes_summary.json").exists():
                return candidate
    return None


def _build_shadow_review_order_rows(execution_orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for raw in execution_orders:
        details = _parse_json_dict(raw.get("details"))
        reason_text = (
            str(details.get("shadow_review_reason") or "").strip()
            or str(details.get("manual_review_reason") or "").strip()
            or str(raw.get("reason") or "").strip()
        )
        shadow_status = str(details.get("shadow_review_status") or "").strip().upper()
        reason_blob = " ".join(
            [
                str(raw.get("reason") or ""),
                str(details.get("manual_review_reason") or ""),
                str(details.get("shadow_review_reason") or ""),
            ]
        ).lower()
        if shadow_status != "REVIEW_REQUIRED" and "shadow" not in reason_blob:
            continue
        shadow_score = _parse_shadow_metric(reason_text, "score")
        shadow_prob = _parse_shadow_metric(reason_text, "prob")
        shadow_samples = _parse_shadow_metric(reason_text, "samples")
        score_threshold = _parse_shadow_metric(reason_text, "shadow_score", "<")
        prob_threshold = _parse_shadow_metric(reason_text, "shadow_prob", "<")
        score_gap = None
        prob_gap = None
        if shadow_score is not None and score_threshold is not None:
            score_gap = float(score_threshold) - float(shadow_score)
        if shadow_prob is not None and prob_threshold is not None:
            prob_gap = float(prob_threshold) - float(shadow_prob)
        score_blocked = score_threshold is not None
        prob_blocked = prob_threshold is not None
        near_miss = False
        far_below = False
        if score_gap is not None and score_gap > 0:
            near_miss = near_miss or score_gap <= 0.05
            far_below = far_below or score_gap >= 0.12
        if prob_gap is not None and prob_gap > 0:
            near_miss = near_miss or prob_gap <= 0.08
            far_below = far_below or prob_gap >= 0.18
        rows.append(
            {
                "portfolio_id": str(raw.get("portfolio_id") or ""),
                "market": str(raw.get("market") or ""),
                "ts": str(raw.get("ts") or ""),
                "symbol": str(raw.get("symbol") or "").upper(),
                "action": str(raw.get("action") or ""),
                "status": str(raw.get("status") or ""),
                "order_value": float(raw.get("order_value") or 0.0),
                "shadow_review_status": shadow_status or "REVIEW_REQUIRED",
                "shadow_review_reason": reason_text,
                "shadow_score": shadow_score,
                "shadow_prob": shadow_prob,
                "shadow_samples": int(shadow_samples) if shadow_samples is not None else 0,
                "score_threshold": score_threshold,
                "prob_threshold": prob_threshold,
                "score_gap": score_gap,
                "prob_gap": prob_gap,
                "score_blocked": int(bool(score_blocked)),
                "prob_blocked": int(bool(prob_blocked)),
                "near_miss": int(bool(near_miss)),
                "far_below": int(bool(far_below)),
            }
        )
    rows.sort(
        key=lambda row: (
            str(row.get("portfolio_id") or ""),
            str(row.get("ts") or ""),
            str(row.get("symbol") or ""),
        ),
        reverse=True,
    )
    return rows


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (str(table),),
    ).fetchone()
    return bool(row)


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except Exception:
        return False
    return any(str(row[1] or "") == str(column) for row in rows)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _median(values: List[Any]) -> float | None:
    nums = sorted(float(v) for v in values if v is not None)
    if not nums:
        return None
    mid = len(nums) // 2
    if len(nums) % 2 == 1:
        return float(nums[mid])
    return float((nums[mid - 1] + nums[mid]) / 2.0)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def _parse_ts(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _seconds_between(start_ts: Any, end_ts: Any) -> float | None:
    start_dt = _parse_ts(start_ts)
    end_dt = _parse_ts(end_ts)
    if start_dt is None or end_dt is None:
        return None
    return max(0.0, float((end_dt - start_dt).total_seconds()))


def _portfolio_row_map(rows: List[Dict[str, Any]] | None) -> Dict[str, Dict[str, Any]]:
    return {
        str(row.get("portfolio_id") or "").strip(): dict(row)
        for row in list(rows or [])
        if str(row.get("portfolio_id") or "").strip()
    }


def _feedback_calibration_support(
    calibration_row: Dict[str, Any] | None,
    *,
    feedback_kind: str,
    action: str,
) -> Dict[str, Any]:
    if not calibration_row:
        return {
            "score": 0.5,
            "label": "MEDIUM",
            "sample_count": 0,
            "selected_horizon_days": "",
            "selection_scope_label": "-",
            "reason": "暂无可用 outcome 回标样本，当前按中性校准处理。",
        }
    feedback_kind = str(feedback_kind or "").strip().lower()
    action = str(action or "").strip().upper()
    if feedback_kind == "shadow" and action == "REVIEW_THRESHOLD":
        score = float(calibration_row.get("shadow_threshold_relax_support", 0.5) or 0.5)
        reason = "这里优先看近期 outcome 是否支持放宽 shadow 阈值；候选越强，放宽阈值越有依据。"
    elif feedback_kind == "shadow" and action == "WEAK_SIGNAL":
        score = float(calibration_row.get("shadow_weak_signal_support", 0.5) or 0.5)
        reason = "这里优先看近期 outcome 是否支持继续惩罚弱信号；候选越弱，惩罚越有依据。"
    elif feedback_kind == "risk" and action == "TIGHTEN":
        score = float(calibration_row.get("risk_tighten_support", 0.5) or 0.5)
        reason = "这里优先看近期 outcome 的 drawdown / broken 情况；downside 越差，收紧风险预算越有依据。"
    elif feedback_kind == "risk" and action == "RELAX":
        score = float(calibration_row.get("risk_relax_support", 0.5) or 0.5)
        reason = "这里优先看近期 outcome 是否足够稳；回标越稳，适度放宽风险预算越有依据。"
    else:
        score = float(calibration_row.get("execution_support", calibration_row.get("signal_quality_score", 0.5)) or 0.5)
        reason = "这里优先看近期 outcome 是否说明 alpha 仍然存在；alpha 越稳定，execution 调参越值得自动应用。"
    return {
        "score": _clamp(score, 0.0, 1.0),
        "label": _feedback_confidence_label(score),
        "sample_count": int(calibration_row.get("outcome_sample_count", 0) or 0),
        "selected_horizon_days": str(calibration_row.get("selected_horizon_days", "") or ""),
        "selection_scope_label": str(calibration_row.get("selection_scope_label", "") or "-"),
        "reason": f"{str(calibration_row.get('calibration_reason', '') or '').strip()} {reason}".strip(),
    }


def _apply_outcome_calibration(base_confidence: float, calibration_score: float) -> float:
    if base_confidence <= 0.0:
        return 0.0
    multiplier = 0.70 + 0.60 * _clamp(calibration_score, 0.0, 1.0)
    return round(_clamp(base_confidence * multiplier, 0.0, 1.0), 6)


def _attribution_control_split_text(
    *,
    strategy_delta: float,
    risk_delta: float,
    gate_weight: float,
    gate_value: float,
    gate_ratio: float,
) -> str:
    strategy_delta = max(0.0, float(strategy_delta or 0.0))
    risk_delta = max(0.0, float(risk_delta or 0.0))
    gate_weight = max(0.0, float(gate_weight or 0.0))
    gate_value = max(0.0, float(gate_value or 0.0))
    gate_ratio = max(0.0, float(gate_ratio or 0.0))
    if max(strategy_delta, risk_delta, gate_weight, gate_ratio) <= 1e-9 and gate_value <= 1e-9:
        return "策略/风险/执行本周都没有明显压缩。"
    execution_text = f"执行 {gate_weight:.1%}"
    if gate_value > 1e-9 or gate_ratio > 1e-9:
        execution_text += f"（blocked {gate_value:.2f} / {gate_ratio:.0%}）"
    return " | ".join(
        [
            f"策略 {strategy_delta:.1%}",
            f"风险 {risk_delta:.1%}",
            execution_text,
        ]
    )


def _feedback_control_driver_context(
    *,
    strategy_delta: float,
    risk_delta: float,
    execution_gate_weight: float,
    execution_gate_ratio: float = 0.0,
    execution_gate_value: float = 0.0,
) -> Dict[str, Any]:
    strategy_delta = max(0.0, float(strategy_delta or 0.0))
    risk_delta = max(0.0, float(risk_delta or 0.0))
    execution_gate_weight = max(0.0, float(execution_gate_weight or 0.0))
    execution_gate_ratio = max(0.0, float(execution_gate_ratio or 0.0))
    execution_gate_value = max(0.0, float(execution_gate_value or 0.0))
    dominant_value = max(strategy_delta, risk_delta, execution_gate_weight)
    driver = ""
    if dominant_value > 1e-9:
        if strategy_delta >= risk_delta and strategy_delta >= execution_gate_weight:
            driver = "STRATEGY"
        elif risk_delta >= strategy_delta and risk_delta >= execution_gate_weight:
            driver = "RISK"
        else:
            driver = "EXECUTION"
    driver_label = {
        "STRATEGY": "策略主动控仓",
        "RISK": "风险 overlay",
        "EXECUTION": "执行 gate",
    }.get(driver, "")
    return {
        "feedback_control_driver": driver,
        "feedback_control_driver_label": driver_label,
        "feedback_control_driver_weight": float(dominant_value),
        "strategy_control_weight_delta": float(strategy_delta),
        "risk_overlay_weight_delta": float(risk_delta),
        "execution_gate_blocked_weight": float(execution_gate_weight),
        "execution_gate_blocked_order_ratio": float(execution_gate_ratio),
        "execution_gate_blocked_order_value": float(execution_gate_value),
        "feedback_control_split_text": _attribution_control_split_text(
            strategy_delta=strategy_delta,
            risk_delta=risk_delta,
            gate_weight=execution_gate_weight,
            gate_value=execution_gate_value,
            gate_ratio=execution_gate_ratio,
        ),
    }


def _feedback_threshold_trend_bucket(action: str, *, same_action_weeks: int, distinct_actions: int) -> str:
    action_code = str(action or "").strip().upper()
    if distinct_actions >= 3:
        return "反复切换"
    if action_code == "RELAX_AUTO_APPLY" and same_action_weeks >= 2:
        return "连续放宽"
    if action_code == "TIGHTEN_AUTO_APPLY" and same_action_weeks >= 2:
        return "连续收紧"
    if action_code == "KEEP_CONSERVATIVE" and same_action_weeks >= 2:
        return "持续保守"
    if action_code == "KEEP_BASE" and same_action_weeks >= 2:
        return "维持基线"
    return "本周更新"


def _market_feedback_kind_keys(rows: List[Dict[str, Any]] | None) -> List[tuple[str, str]]:
    keys = {
        (
            resolve_market_code(str(row.get("market") or "")),
            str(row.get("feedback_kind") or "").strip().lower(),
        )
        for row in list(rows or [])
        if resolve_market_code(str(row.get("market") or "")) and str(row.get("feedback_kind") or "").strip()
    }
    return sorted(keys)


def _feedback_threshold_history_context_rows(
    storage: Storage,
    feedback_threshold_rows: List[Dict[str, Any]],
    *,
    limit: int = 12,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for market, feedback_kind in _market_feedback_kind_keys(feedback_threshold_rows):
        history_rows = storage.get_recent_investment_feedback_threshold_history(
            market,
            feedback_kind=feedback_kind,
            limit=max(2, int(limit)),
        )
        if not history_rows:
            continue
        history_rows = sorted(
            list(history_rows or []),
            key=lambda item: (str(item.get("week_start", "") or ""), str(item.get("ts", "") or "")),
            reverse=True,
        )
        current = dict(history_rows[0])
        previous = dict(history_rows[1]) if len(history_rows) > 1 else {}
        current_action = str(current.get("suggestion_action") or "").strip().upper()
        same_action_weeks = 0
        for item in history_rows:
            if str(item.get("suggestion_action") or "").strip().upper() != current_action:
                break
            same_action_weeks += 1
        recent_actions = [
            str(item.get("suggestion_action") or "").strip().upper()
            for item in history_rows[:4]
            if str(item.get("suggestion_action") or "").strip()
        ]
        action_chain = " -> ".join(
            f"{str(item.get('week_label', '') or '-')}:{str(item.get('suggestion_action', '') or '-')}"
            for item in reversed(history_rows[:4])
        ) or "-"
        out.append(
            {
                "market": market,
                "feedback_kind": feedback_kind,
                "history_rows": history_rows,
                "current": current,
                "previous": previous,
                "current_action": current_action,
                "same_action_weeks": int(same_action_weeks),
                "recent_actions": recent_actions,
                "action_chain": action_chain,
                "current_details": dict(current.get("details_json", {}) or {}),
                "cohort_rows_asc": list(reversed(history_rows[:same_action_weeks])),
            }
        )
    return out


def _feedback_threshold_effect_label(action: str, signal: str) -> tuple[str, str]:
    action_code = str(action or "").strip().upper()
    signal_text = str(signal or "").strip()
    if action_code == "RELAX_AUTO_APPLY":
        if signal_text == "持续改善":
            return "放宽后改善", "这条市场阈值放宽后，自动应用效果仍在继续改善。"
        if signal_text == "需复核":
            return "放宽后恶化", "放宽后出现恶化信号，下一步应优先考虑收回门槛。"
        if signal_text == "稳定跟踪":
            return "放宽后稳定", "放宽后还算稳定，但改善力度还不足以继续放宽。"
        return "放宽后观察中", "放宽后样本还不够多，先继续观察。"
    if action_code == "TIGHTEN_AUTO_APPLY":
        if signal_text == "持续改善":
            return "收紧后改善", "收紧后改善，说明之前的自动应用门槛可能偏松。"
        if signal_text == "需复核":
            return "收紧后仍恶化", "即使已经收紧，效果仍偏弱，需要进一步复核。"
        if signal_text in {"稳定跟踪", "观察中"}:
            return "收紧后稳定", "收紧后已趋稳，先保持当前保守阈值。"
        return "收紧后观察中", "收紧后样本还不足，继续观察。"
    if action_code == "KEEP_CONSERVATIVE":
        return "保守观察", "当前仍以保守观察为主，样本还不支持更激进动作。"
    return "基线观察", "当前主要维持基线阈值，等待更多效果样本。"


def _build_feedback_threshold_history_overview(
    db_path: Path,
    feedback_threshold_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not feedback_threshold_rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    for context in _feedback_threshold_history_context_rows(storage, feedback_threshold_rows):
        market = str(context.get("market") or "")
        feedback_kind = str(context.get("feedback_kind") or "")
        history_rows = list(context.get("history_rows") or [])
        current = dict(context.get("current") or {})
        previous = dict(context.get("previous") or {})
        current_action = str(context.get("current_action") or "")
        same_action_weeks = int(context.get("same_action_weeks", 0) or 0)
        recent_actions = list(context.get("recent_actions") or [])
        transition = "首次建议"
        if previous:
            previous_action = str(previous.get("suggestion_action") or "").strip().upper()
            transition = "动作变化" if previous_action != current_action else "持续试运行"
        action_chain = str(context.get("action_chain") or "-")
        current_details = dict(context.get("current_details") or {})
        out.append(
            {
                "market": market,
                "feedback_kind": feedback_kind,
                "feedback_kind_label": str(current.get("feedback_kind_label") or feedback_kind),
                "current_action": current_action or "-",
                "current_label": str(current.get("suggestion_label") or current_action or "-"),
                "summary_signal": str(current.get("summary_signal") or "-"),
                "transition": transition,
                "same_action_weeks": int(same_action_weeks),
                "weeks_tracked": int(len(history_rows)),
                "trend_bucket": _feedback_threshold_trend_bucket(
                    current_action,
                    same_action_weeks=int(same_action_weeks),
                    distinct_actions=len(set(recent_actions)),
                ),
                "threshold_snapshot": (
                    f"conf {float(current.get('base_auto_confidence', 0.0) or 0.0):.2f}->"
                    f"{float(current.get('suggested_auto_confidence', 0.0) or 0.0):.2f} | "
                    f"base {float(current.get('base_auto_base_confidence', 0.0) or 0.0):.2f}->"
                    f"{float(current.get('suggested_auto_base_confidence', 0.0) or 0.0):.2f}"
                ),
                "action_chain": action_chain,
                "reason": str(current_details.get("reason") or current.get("reason") or "-"),
            }
        )
    out.sort(
        key=lambda row: (
            0 if str(row.get("trend_bucket") or "") == "连续收紧" else 1 if str(row.get("trend_bucket") or "") == "反复切换" else 2,
            -int(row.get("same_action_weeks", 0) or 0),
            str(row.get("market") or ""),
            str(row.get("feedback_kind_label") or ""),
        )
    )
    return out


def _build_feedback_threshold_effect_overview(
    db_path: Path,
    feedback_threshold_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not feedback_threshold_rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    for context in _feedback_threshold_history_context_rows(storage, feedback_threshold_rows):
        market = str(context.get("market") or "")
        feedback_kind = str(context.get("feedback_kind") or "")
        history_rows = list(context.get("history_rows") or [])
        current = dict(context.get("current") or {})
        current_action = str(context.get("current_action") or "")
        same_action_weeks = int(context.get("same_action_weeks", 0) or 0)
        effect_label, effect_reason = _feedback_threshold_effect_label(
            current_action,
            str(current.get("summary_signal") or ""),
        )
        current_details = dict(context.get("current_details") or {})
        out.append(
            {
                "market": market,
                "feedback_kind": feedback_kind,
                "feedback_kind_label": str(current.get("feedback_kind_label") or feedback_kind),
                "current_action": current_action or "-",
                "current_label": str(current.get("suggestion_label") or current_action or "-"),
                "summary_signal": str(current.get("summary_signal") or "-"),
                "effect_label": effect_label,
                "effect_reason": effect_reason,
                "same_action_weeks": int(same_action_weeks),
                "weeks_tracked": int(len(history_rows)),
                "tracked_count": int(float(current.get("tracked_count", 0) or 0)),
                "avg_active_weeks": float(current.get("avg_active_weeks", 0.0) or 0.0),
                "threshold_snapshot": (
                    f"conf {float(current.get('base_auto_confidence', 0.0) or 0.0):.2f}->"
                    f"{float(current.get('suggested_auto_confidence', 0.0) or 0.0):.2f} | "
                    f"calib {float(current.get('base_auto_calibration_score', 0.0) or 0.0):.2f}->"
                    f"{float(current.get('suggested_auto_calibration_score', 0.0) or 0.0):.2f}"
                ),
                "action_chain": str(context.get("action_chain") or "-"),
                "reason": str(current_details.get("reason") or current.get("reason") or "-"),
            }
        )
    out.sort(
        key=lambda row: (
            0 if str(row.get("effect_label") or "") in {"放宽后恶化", "收紧后仍恶化"} else 1 if str(row.get("effect_label") or "") in {"放宽后观察中", "收紧后观察中"} else 2,
            -int(row.get("same_action_weeks", 0) or 0),
            str(row.get("market") or ""),
            str(row.get("feedback_kind_label") or ""),
        )
    )
    return out


def _feedback_threshold_cohort_milestone(cohort_rows_asc: List[Dict[str, Any]], week_offset: int) -> str:
    if len(cohort_rows_asc) <= week_offset:
        return "-"
    target = dict(cohort_rows_asc[week_offset])
    effect_label, _ = _feedback_threshold_effect_label(
        str(target.get("suggestion_action") or ""),
        str(target.get("summary_signal") or ""),
    )
    return effect_label or "-"


def _feedback_threshold_cohort_diagnosis(
    action: str,
    *,
    latest_effect: str,
    effect_w1: str,
    effect_w2: str,
    effect_w4: str,
) -> str:
    action_code = str(action or "").strip().upper()
    buckets = {
        feedback_threshold_effect_bucket(latest_effect),
        feedback_threshold_effect_bucket(effect_w1),
        feedback_threshold_effect_bucket(effect_w2),
        feedback_threshold_effect_bucket(effect_w4),
    }
    if action_code == "RELAX_AUTO_APPLY":
        if "恶化" in buckets:
            return "放宽后出现恶化，建议优先收回阈值。"
        if "改善" in buckets:
            return "放宽后已看到改善，可继续试运行并跟踪。"
        if "稳定" in buckets:
            return "放宽后暂时稳定，先继续观察。"
        return "放宽后样本仍少，继续等待。"
    if action_code == "TIGHTEN_AUTO_APPLY":
        if "恶化" in buckets:
            return "收紧后仍恶化，需要进一步复核。"
        if "改善" in buckets or "稳定" in buckets:
            return "收紧后已趋稳，可继续保持保守。"
        return "收紧后样本仍少，继续等待。"
    if action_code == "KEEP_CONSERVATIVE":
        return "当前仍处于保守观察阶段。"
    return "当前主要维持基线阈值。"


def _build_feedback_threshold_cohort_overview(
    db_path: Path,
    feedback_threshold_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not feedback_threshold_rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    for context in _feedback_threshold_history_context_rows(storage, feedback_threshold_rows):
        market = str(context.get("market") or "")
        feedback_kind = str(context.get("feedback_kind") or "")
        current = dict(context.get("current") or {})
        current_action = str(context.get("current_action") or "")
        cohort_rows_asc = list(context.get("cohort_rows_asc") or [])
        if not cohort_rows_asc:
            continue
        latest_effect, _ = _feedback_threshold_effect_label(
            current_action,
            str(current.get("summary_signal") or ""),
        )
        effect_w1 = _feedback_threshold_cohort_milestone(cohort_rows_asc, 1)
        effect_w2 = _feedback_threshold_cohort_milestone(cohort_rows_asc, 2)
        effect_w4 = _feedback_threshold_cohort_milestone(cohort_rows_asc, 4)
        out.append(
            {
                "market": market,
                "feedback_kind": feedback_kind,
                "feedback_kind_label": str(current.get("feedback_kind_label") or feedback_kind),
                "cohort_action": current_action or "-",
                "cohort_label": str(current.get("suggestion_label") or current_action or "-"),
                "baseline_week": str(cohort_rows_asc[0].get("week_label") or "-"),
                "cohort_weeks": int(len(cohort_rows_asc)),
                "tracked_count": int(float(current.get("tracked_count", 0) or 0)),
                "avg_active_weeks": float(current.get("avg_active_weeks", 0.0) or 0.0),
                "latest_effect": latest_effect,
                "effect_w1": effect_w1,
                "effect_w2": effect_w2,
                "effect_w4": effect_w4,
                "diagnosis": _feedback_threshold_cohort_diagnosis(
                    current_action,
                    latest_effect=latest_effect,
                    effect_w1=effect_w1,
                    effect_w2=effect_w2,
                    effect_w4=effect_w4,
                ),
                "action_chain": " -> ".join(
                    f"{str(item.get('week_label', '') or '-')}:{str(item.get('suggestion_action', '') or '-')}"
                    for item in cohort_rows_asc[:4]
                ) or "-",
            }
        )
    out.sort(
        key=lambda row: (
            0 if feedback_threshold_effect_bucket(row.get("latest_effect")) == "恶化" else 1 if feedback_threshold_effect_bucket(row.get("latest_effect")) == "观察中" else 2,
            -int(row.get("cohort_weeks", 0) or 0),
            str(row.get("market") or ""),
            str(row.get("feedback_kind_label") or ""),
        )
    )
    return out


def _build_feedback_threshold_trial_alert_row(
    row: Dict[str, Any],
    *,
    cohort_weeks: int,
    stage_label: str,
    action_label: str,
    next_check: str,
) -> Dict[str, Any]:
    return {
        "market": str(row.get("market") or ""),
        "feedback_kind": str(row.get("feedback_kind") or ""),
        "feedback_kind_label": str(row.get("feedback_kind_label") or "-"),
        "cohort_label": str(row.get("cohort_label") or "-"),
        "baseline_week": str(row.get("baseline_week") or "-"),
        "cohort_weeks": int(cohort_weeks),
        "stage_label": str(stage_label or ""),
        "action_label": str(action_label or ""),
        "latest_effect": str(row.get("latest_effect") or "-"),
        "effect_w1": str(row.get("effect_w1") or "-"),
        "effect_w2": str(row.get("effect_w2") or "-"),
        "diagnosis": str(row.get("diagnosis") or "-"),
        "next_check": str(next_check or ""),
    }


def _build_feedback_threshold_trial_alert_overview(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for raw in list(rows or []):
        row = dict(raw)
        cohort_action = str(row.get("cohort_action") or "").strip().upper()
        cohort_weeks = int(row.get("cohort_weeks", 0) or 0)
        if cohort_action not in {"RELAX_AUTO_APPLY", "TIGHTEN_AUTO_APPLY"}:
            continue
        if cohort_weeks <= 0 or cohort_weeks > 2:
            continue
        stage_label = "新进入观察期" if cohort_weeks == 1 else "持续观察期"
        action_label = "放宽试运行" if cohort_action == "RELAX_AUTO_APPLY" else "收紧试运行"
        next_check = "优先确认是否恶化" if cohort_action == "RELAX_AUTO_APPLY" else "优先确认是否已趋稳"
        out.append(
            _build_feedback_threshold_trial_alert_row(
                row,
                cohort_weeks=cohort_weeks,
                stage_label=stage_label,
                action_label=action_label,
                next_check=next_check,
            )
        )
    out.sort(
        key=lambda row: (
            0 if str(row.get("cohort_weeks") or 0) == "1" else 1,
            0 if str(row.get("action_label") or "") == "放宽试运行" else 1,
            str(row.get("market") or ""),
            str(row.get("feedback_kind_label") or ""),
        )
    )
    return out


def _feedback_history_state_label(row: Dict[str, Any]) -> str:
    alert_bucket = str(row.get("alert_bucket", "") or "").strip().upper()
    if alert_bucket in {"ACTIVE", "READY", "SOON"}:
        return alert_bucket
    return str(row.get("calibration_apply_mode", "") or "HOLD").strip().upper() or "HOLD"


def _feedback_effect_snapshot_from_history_row(row: Dict[str, Any]) -> Dict[str, Any]:
    details = dict(row.get("details_json", {}) or {})
    if not details and row.get("details"):
        details = _parse_json_dict(row.get("details"))
    return dict(details.get("effect_snapshot", {}) or {})


def _feedback_effect_compare_snapshot(
    feedback_kind: str,
    baseline: Dict[str, Any],
    current: Dict[str, Any],
) -> tuple[str, str]:
    kind = str(feedback_kind or "").strip().lower()
    if not baseline or not current:
        return "-", "-"
    if kind == "execution":
        gap_delta = float(current.get("execution_cost_gap", 0.0) or 0.0) - float(baseline.get("execution_cost_gap", 0.0) or 0.0)
        actual_delta = float(current.get("avg_actual_slippage_bps", 0.0) or 0.0) - float(
            baseline.get("avg_actual_slippage_bps", 0.0) or 0.0
        )
        if gap_delta <= -2.0 and actual_delta <= -3.0:
            label = "改善"
        elif gap_delta >= 2.0 and actual_delta >= 3.0:
            label = "恶化"
        else:
            label = "稳定"
        metric = f"gapΔ={gap_delta:+.2f} / slipΔ={actual_delta:+.1f}bps"
        return label, metric
    if kind == "risk":
        scale_delta = float(current.get("latest_dynamic_scale", 1.0) or 1.0) - float(
            baseline.get("latest_dynamic_scale", 1.0) or 1.0
        )
        stress_delta = float(current.get("latest_stress_worst_loss", 0.0) or 0.0) - float(
            baseline.get("latest_stress_worst_loss", 0.0) or 0.0
        )
        corr_delta = float(current.get("latest_avg_pair_correlation", 0.0) or 0.0) - float(
            baseline.get("latest_avg_pair_correlation", 0.0) or 0.0
        )
        if scale_delta >= 0.03 and stress_delta <= -0.01 and corr_delta <= -0.03:
            label = "改善"
        elif scale_delta <= -0.03 and (stress_delta >= 0.01 or corr_delta >= 0.03):
            label = "恶化"
        else:
            label = "稳定"
        metric = f"scaleΔ={scale_delta:+.2f} / stressΔ={stress_delta:+.1%} / corrΔ={corr_delta:+.2f}"
        return label, metric
    positive_delta = float(current.get("outcome_positive_rate", 0.0) or 0.0) - float(
        baseline.get("outcome_positive_rate", 0.0) or 0.0
    )
    broken_delta = float(current.get("outcome_broken_rate", 0.0) or 0.0) - float(
        baseline.get("outcome_broken_rate", 0.0) or 0.0
    )
    align_delta = float(current.get("score_alignment_score", 0.0) or 0.0) - float(
        baseline.get("score_alignment_score", 0.0) or 0.0
    )
    if positive_delta >= 0.05 and broken_delta <= -0.03 and align_delta >= 0.04:
        label = "改善"
    elif positive_delta <= -0.05 and broken_delta >= 0.03 and align_delta <= -0.04:
        label = "恶化"
    else:
        label = "稳定"
    metric = f"posΔ={positive_delta:+.1%} / brokenΔ={broken_delta:+.1%} / alignΔ={align_delta:+.2f}"
    return label, metric


def _feedback_effect_milestone(active_rows_asc: List[Dict[str, Any]], feedback_kind: str, week_offset: int) -> str:
    if len(active_rows_asc) <= week_offset:
        return "-"
    baseline = _feedback_effect_snapshot_from_history_row(active_rows_asc[0])
    current = _feedback_effect_snapshot_from_history_row(active_rows_asc[week_offset])
    label, metric = _feedback_effect_compare_snapshot(feedback_kind, baseline, current)
    if label == "-" and metric == "-":
        return "-"
    return f"{label} ({metric})"


def _feedback_effect_bucket(text: Any) -> str:
    value = str(text or "").strip()
    if "恶化" in value:
        return "恶化"
    if "改善" in value:
        return "改善"
    if "稳定" in value:
        return "稳定"
    if value in {"-", ""}:
        return "无样本"
    return "观察中"


def _market_portfolio_feedback_kind_keys(
    rows: List[Dict[str, Any]] | None,
) -> List[tuple[str, str, str]]:
    keys = {
        (
            resolve_market_code(str(row.get("market") or "")),
            str(row.get("portfolio_id") or "").strip(),
            str(row.get("feedback_kind") or "").strip().lower(),
        )
        for row in list(rows or [])
        if resolve_market_code(str(row.get("market") or ""))
        and str(row.get("portfolio_id") or "").strip()
        and str(row.get("feedback_kind") or "").strip()
    }
    return sorted(keys)


def _feedback_automation_history_context_rows(
    storage: Storage,
    feedback_automation_rows: List[Dict[str, Any]],
    *,
    limit: int = 12,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for market, portfolio_id, feedback_kind in _market_portfolio_feedback_kind_keys(feedback_automation_rows):
        history_rows = storage.get_recent_investment_feedback_automation_history(
            market,
            portfolio_id,
            feedback_kind=feedback_kind,
            limit=max(2, int(limit)),
        )
        if not history_rows:
            continue
        history_rows = sorted(
            list(history_rows or []),
            key=lambda row: (str(row.get("week_start", "") or ""), str(row.get("ts", "") or "")),
            reverse=True,
        )
        current = dict(history_rows[0])
        current_state = _feedback_history_state_label(current)
        current_mode = str(current.get("calibration_apply_mode", "") or "").strip().upper()
        if current_state != "ACTIVE" and current_mode != "AUTO_APPLY":
            continue
        active_weeks = 0
        for history_row in history_rows:
            row_state = _feedback_history_state_label(history_row)
            row_mode = str(history_row.get("calibration_apply_mode", "") or "").strip().upper()
            if row_state != "ACTIVE" and row_mode != "AUTO_APPLY":
                break
            active_weeks += 1
        active_rows_asc = list(reversed(history_rows[:active_weeks]))
        out.append(
            {
                "market": market,
                "portfolio_id": portfolio_id,
                "feedback_kind": feedback_kind,
                "history_rows": history_rows,
                "current": current,
                "current_state": current_state,
                "active_weeks": int(active_weeks),
                "active_rows_asc": active_rows_asc,
                "baseline_week": str(active_rows_asc[0].get("week_label", "") or "-") if active_rows_asc else "-",
                "details": dict(current.get("details_json", {}) or {}),
            }
        )
    return out


def _build_feedback_automation_effect_row(context: Dict[str, Any]) -> Dict[str, Any]:
    market = str(context.get("market") or "")
    portfolio_id = str(context.get("portfolio_id") or "")
    feedback_kind = str(context.get("feedback_kind") or "")
    current = dict(context.get("current") or {})
    current_state = str(context.get("current_state") or "")
    active_weeks = int(context.get("active_weeks", 0) or 0)
    active_rows_asc = list(context.get("active_rows_asc") or [])
    baseline_week = str(context.get("baseline_week") or "-")
    details = dict(context.get("details") or {})
    effect_label = "观察中"
    effect_metric = "-"
    reason = str(details.get("automation_reason") or details.get("feedback_reason") or "-")
    driver = str(details.get("feedback_action") or current.get("feedback_action") or "-")
    if len(active_rows_asc) >= 2:
        baseline_snapshot = _feedback_effect_snapshot_from_history_row(active_rows_asc[0])
        latest_snapshot = _feedback_effect_snapshot_from_history_row(active_rows_asc[-1])
        compare_label, compare_metric = _feedback_effect_compare_snapshot(feedback_kind, baseline_snapshot, latest_snapshot)
        if compare_label != "-":
            effect_label = compare_label
            effect_metric = compare_metric
    return {
        "market": market,
        "portfolio_id": portfolio_id,
        "feedback_kind": feedback_kind,
        "feedback_kind_label": str(current.get("feedback_kind_label", "") or feedback_kind),
        "current_state": current_state,
        "current_mode": str(current.get("calibration_apply_mode_label", "") or "-"),
        "baseline_week": baseline_week,
        "active_weeks": int(active_weeks),
        "effect_label": effect_label,
        "effect_metric": effect_metric,
        "effect_w1": _feedback_effect_milestone(active_rows_asc, feedback_kind, 1),
        "effect_w2": _feedback_effect_milestone(active_rows_asc, feedback_kind, 2),
        "effect_w4": _feedback_effect_milestone(active_rows_asc, feedback_kind, 4),
        "driver": driver or "-",
        "reason": reason,
    }


def _build_feedback_automation_effect_overview(
    db_path: Path,
    feedback_automation_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not feedback_automation_rows:
        return []
    storage = Storage(str(db_path))
    rows: List[Dict[str, Any]] = []
    for context in _feedback_automation_history_context_rows(storage, feedback_automation_rows):
        rows.append(_build_feedback_automation_effect_row(context))
    rows.sort(
        key=lambda row: (
            0 if str(row.get("effect_label", "") or "") == "恶化" else 1 if str(row.get("effect_label", "") or "") == "观察中" else 2,
            -int(row.get("active_weeks", 0) or 0),
            str(row.get("market", "") or ""),
            str(row.get("feedback_kind_label", "") or ""),
            str(row.get("portfolio_id", "") or ""),
        )
    )
    return rows


def _feedback_effect_summary_signal(row: Dict[str, Any]) -> str:
    tracked = int(row.get("tracked_count", 0) or 0)
    if int(row.get("latest_deteriorated_count", 0) or 0) > 0:
        return "需复核"
    if int(row.get("latest_improved_count", 0) or 0) >= max(1, tracked // 2):
        return "持续改善"
    if int(row.get("latest_stable_count", 0) or 0) > 0:
        return "稳定跟踪"
    return "观察中"


def _feedback_maturity_alert_bucket(row: Dict[str, Any], *, now_dt: datetime | None = None) -> str:
    now_utc = now_dt or datetime.now(timezone.utc)
    apply_mode = str(row.get("calibration_apply_mode") or "").strip().upper()
    maturity_label = str(row.get("outcome_maturity_label") or "UNKNOWN").strip().upper()
    pending_count = int(float(row.get("outcome_pending_sample_count", 0) or 0))
    ready_end_text = str(row.get("outcome_ready_estimate_end_ts") or "").strip()
    days_until_ready = 999
    if ready_end_text:
        try:
            ready_end_ts = datetime.fromisoformat(ready_end_text.replace("Z", "+00:00"))
            if ready_end_ts.tzinfo is None:
                ready_end_ts = ready_end_ts.replace(tzinfo=timezone.utc)
            days_until_ready = max(0, (ready_end_ts.date() - now_utc.date()).days)
        except Exception:
            days_until_ready = 999
    if apply_mode == "AUTO_APPLY" and maturity_label in {"MATURE", "LATE"}:
        return "ACTIVE"
    if apply_mode != "AUTO_APPLY" and maturity_label in {"MATURE", "LATE"}:
        return "READY"
    if apply_mode != "AUTO_APPLY" and pending_count > 0 and days_until_ready <= 2:
        return "SOON"
    return ""


def _feedback_effect_snapshot(
    row: Dict[str, Any],
    *,
    feedback_calibration_map: Dict[str, Dict[str, Any]] | None = None,
    risk_review_map: Dict[str, Dict[str, Any]] | None = None,
    execution_feedback_map: Dict[str, Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    portfolio_id = str(row.get("portfolio_id") or "").strip()
    feedback_kind = str(row.get("feedback_kind") or "").strip().lower()
    if not portfolio_id or not feedback_kind:
        return {}
    if feedback_kind == "execution":
        item = dict((execution_feedback_map or {}).get(portfolio_id, {}) or {})
        if not item:
            return {}
        return {
            "snapshot_kind": "execution",
            "planned_execution_cost_total": float(item.get("planned_execution_cost_total", 0.0) or 0.0),
            "execution_cost_total": float(item.get("execution_cost_total", 0.0) or 0.0),
            "execution_cost_gap": float(item.get("execution_cost_gap", 0.0) or 0.0),
            "avg_expected_cost_bps": float(item.get("avg_expected_cost_bps", 0.0) or 0.0),
            "avg_actual_slippage_bps": float(item.get("avg_actual_slippage_bps", 0.0) or 0.0),
            "dominant_execution_session_label": str(item.get("dominant_execution_session_label") or ""),
            "execution_feedback_action": str(item.get("execution_feedback_action") or ""),
        }
    if feedback_kind == "risk":
        item = dict((risk_review_map or {}).get(portfolio_id, {}) or {})
        if not item:
            return {}
        return {
            "snapshot_kind": "risk",
            "latest_dynamic_scale": float(item.get("latest_dynamic_scale", 1.0) or 1.0),
            "latest_dynamic_net_exposure": float(item.get("latest_dynamic_net_exposure", 0.0) or 0.0),
            "latest_dynamic_gross_exposure": float(item.get("latest_dynamic_gross_exposure", 0.0) or 0.0),
            "latest_avg_pair_correlation": float(item.get("latest_avg_pair_correlation", 0.0) or 0.0),
            "latest_stress_worst_loss": float(item.get("latest_stress_worst_loss", 0.0) or 0.0),
            "dominant_risk_driver": str(item.get("dominant_risk_driver") or ""),
            "risk_diagnosis": str(item.get("risk_diagnosis") or ""),
        }
    item = dict((feedback_calibration_map or {}).get(portfolio_id, {}) or {})
    if not item:
        return {}
    return {
        "snapshot_kind": "shadow",
        "outcome_sample_count": int(item.get("outcome_sample_count", 0) or 0),
        "outcome_positive_rate": float(item.get("outcome_positive_rate", 0.0) or 0.0),
        "outcome_broken_rate": float(item.get("outcome_broken_rate", 0.0) or 0.0),
        "avg_future_return": float(item.get("avg_future_return", 0.0) or 0.0),
        "avg_max_drawdown": float(item.get("avg_max_drawdown", 0.0) or 0.0),
        "score_alignment_score": float(item.get("score_alignment_score", 0.0) or 0.0),
        "calibration_confidence": float(item.get("calibration_confidence", 0.0) or 0.0),
        "calibration_reason": str(item.get("calibration_reason") or ""),
    }


def _persist_feedback_automation_history(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    week_label: str,
    week_start: str,
    window_start: str,
    window_end: str,
    feedback_calibration_map: Dict[str, Dict[str, Any]] | None = None,
    risk_review_map: Dict[str, Dict[str, Any]] | None = None,
    execution_feedback_map: Dict[str, Dict[str, Any]] | None = None,
) -> None:
    if not rows:
        return
    storage = Storage(str(db_path))
    review_ts = datetime.now(timezone.utc).isoformat()
    for raw in list(rows or []):
        row = dict(raw)
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        feedback_kind = str(row.get("feedback_kind") or "").strip().lower()
        if not portfolio_id or not feedback_kind:
            continue
        details_row = dict(row)
        effect_snapshot = _feedback_effect_snapshot(
            row,
            feedback_calibration_map=feedback_calibration_map,
            risk_review_map=risk_review_map,
            execution_feedback_map=execution_feedback_map,
        )
        if effect_snapshot:
            details_row["effect_snapshot"] = effect_snapshot
            details_row["effect_snapshot_week_label"] = str(week_label or "").strip()
        storage.upsert_investment_feedback_automation_history(
            {
                "week_label": str(week_label or "").strip(),
                "week_start": str(week_start or "").strip(),
                "window_start": str(window_start or "").strip(),
                "window_end": str(window_end or "").strip(),
                "ts": review_ts,
                "market": str(row.get("market") or "").upper(),
                "portfolio_id": portfolio_id,
                "feedback_kind": feedback_kind,
                "feedback_kind_label": str(row.get("feedback_kind_label") or ""),
                "feedback_action": str(row.get("feedback_action") or ""),
                "calibration_apply_mode": str(row.get("calibration_apply_mode") or ""),
                "calibration_apply_mode_label": str(row.get("calibration_apply_mode_label") or ""),
                "calibration_basis": str(row.get("calibration_basis") or ""),
                "calibration_basis_label": str(row.get("calibration_basis_label") or ""),
                "feedback_base_confidence": float(row.get("feedback_base_confidence", 0.0) or 0.0),
                "feedback_calibration_score": float(row.get("feedback_calibration_score", 0.0) or 0.0),
                "feedback_confidence": float(row.get("feedback_confidence", 0.0) or 0.0),
                "feedback_sample_count": int(float(row.get("feedback_sample_count", 0) or 0)),
                "feedback_calibration_sample_count": int(float(row.get("feedback_calibration_sample_count", 0) or 0)),
                "outcome_maturity_ratio": float(row.get("outcome_maturity_ratio", 0.0) or 0.0),
                "outcome_maturity_label": str(row.get("outcome_maturity_label") or ""),
                "outcome_pending_sample_count": int(float(row.get("outcome_pending_sample_count", 0) or 0)),
                "outcome_ready_estimate_end_ts": str(row.get("outcome_ready_estimate_end_ts") or ""),
                "alert_bucket": _feedback_maturity_alert_bucket(row, now_dt=datetime.now(timezone.utc)),
                "details": details_row,
            }
        )


def _persist_feedback_threshold_history(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    week_label: str,
    week_start: str,
    window_start: str,
    window_end: str,
) -> None:
    if not rows:
        return
    storage = Storage(str(db_path))
    review_ts = datetime.now(timezone.utc).isoformat()
    for raw in list(rows or []):
        row = dict(raw)
        market = resolve_market_code(str(row.get("market") or ""))
        feedback_kind = str(row.get("feedback_kind") or "").strip().lower()
        if not market or not feedback_kind:
            continue
        storage.upsert_investment_feedback_threshold_history(
            {
                "week_label": str(week_label or "").strip(),
                "week_start": str(week_start or "").strip(),
                "window_start": str(window_start or "").strip(),
                "window_end": str(window_end or "").strip(),
                "ts": review_ts,
                "market": market,
                "feedback_kind": feedback_kind,
                "feedback_kind_label": str(row.get("feedback_kind_label") or feedback_kind),
                "suggestion_action": str(row.get("suggestion_action") or ""),
                "suggestion_label": str(row.get("suggestion_label") or ""),
                "summary_signal": str(row.get("summary_signal") or ""),
                "tracked_count": int(float(row.get("tracked_count", 0) or 0)),
                "avg_active_weeks": float(row.get("avg_active_weeks", 0.0) or 0.0),
                "base_auto_confidence": float(row.get("base_auto_confidence", 0.0) or 0.0),
                "suggested_auto_confidence": float(row.get("suggested_auto_confidence", 0.0) or 0.0),
                "base_auto_base_confidence": float(row.get("base_auto_base_confidence", 0.0) or 0.0),
                "suggested_auto_base_confidence": float(row.get("suggested_auto_base_confidence", 0.0) or 0.0),
                "base_auto_calibration_score": float(row.get("base_auto_calibration_score", 0.0) or 0.0),
                "suggested_auto_calibration_score": float(row.get("suggested_auto_calibration_score", 0.0) or 0.0),
                "base_auto_maturity_ratio": float(row.get("base_auto_maturity_ratio", 0.0) or 0.0),
                "suggested_auto_maturity_ratio": float(row.get("suggested_auto_maturity_ratio", 0.0) or 0.0),
                "details": row,
            }
        )


def _persist_market_profile_patch_history(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    week_label: str,
    week_start: str,
    window_start: str,
    window_end: str,
) -> None:
    if not rows:
        return
    storage = Storage(str(db_path))
    review_ts = datetime.now(timezone.utc).isoformat()
    for raw in list(rows or []):
        row = dict(raw)
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        market = resolve_market_code(str(row.get("market") or ""))
        if not portfolio_id or not market:
            continue
        tuning_action = str(row.get("market_profile_tuning_action") or "").strip().upper()
        storage.upsert_investment_market_profile_patch_history(
            {
                "week_label": str(week_label or "").strip(),
                "week_start": str(week_start or "").strip(),
                "window_start": str(window_start or "").strip(),
                "window_end": str(window_end or "").strip(),
                "ts": review_ts,
                "market": market,
                "portfolio_id": portfolio_id,
                "profile": str(row.get("adaptive_strategy_active_market_profile") or ""),
                "tuning_target": str(row.get("market_profile_tuning_target") or ""),
                "tuning_action": tuning_action,
                "tuning_bias": str(row.get("market_profile_tuning_bias") or ""),
                "review_required": int(tuning_action in {"REVIEW_EXECUTION_GATE", "REVIEW_REGIME_PLAN"}),
                "details": row,
            }
        )


def _persist_weekly_tuning_history(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    week_label: str,
    week_start: str,
    window_start: str,
    window_end: str,
) -> None:
    if not rows:
        return
    storage = Storage(str(db_path))
    review_ts = datetime.now(timezone.utc).isoformat()
    for raw in list(rows or []):
        row = dict(raw)
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        market = resolve_market_code(str(row.get("market") or ""))
        if not portfolio_id or not market:
            continue
        storage.upsert_investment_weekly_tuning_history(
            {
                "week_label": str(week_label or "").strip(),
                "week_start": str(week_start or "").strip(),
                "window_start": str(window_start or "").strip(),
                "window_end": str(window_end or "").strip(),
                "ts": review_ts,
                "market": market,
                "portfolio_id": portfolio_id,
                "active_market_profile": str(row.get("adaptive_strategy_active_market_profile") or ""),
                "dominant_driver": str(row.get("dominant_driver") or ""),
                "market_profile_tuning_action": str(row.get("market_profile_tuning_action") or ""),
                "weekly_return": float(row.get("weekly_return", 0.0) or 0.0),
                "max_drawdown": float(row.get("max_drawdown", 0.0) or 0.0),
                "turnover": float(row.get("turnover", 0.0) or 0.0),
                "outcome_sample_count": int(row.get("outcome_sample_count", 0) or 0),
                "signal_quality_score": float(row.get("signal_quality_score", 0.0) or 0.0),
                "execution_cost_gap": float(row.get("execution_cost_gap", 0.0) or 0.0),
                "execution_gate_blocked_weight": float(row.get("execution_gate_blocked_weight", 0.0) or 0.0),
                "strategy_control_weight_delta": float(row.get("strategy_control_weight_delta", 0.0) or 0.0),
                "risk_overlay_weight_delta": float(row.get("risk_overlay_weight_delta", 0.0) or 0.0),
                "risk_feedback_action": str(row.get("risk_feedback_action") or ""),
                "execution_feedback_action": str(row.get("execution_feedback_action") or ""),
                "shadow_apply_mode": str(row.get("shadow_apply_mode") or ""),
                "risk_apply_mode": str(row.get("risk_apply_mode") or ""),
                "execution_apply_mode": str(row.get("execution_apply_mode") or ""),
                "market_profile_ready_for_manual_apply": int(row.get("market_profile_ready_for_manual_apply", 0) or 0),
                "details": row,
            }
        )


def _persist_weekly_decision_evidence_history(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    week_label: str,
    week_start: str,
    window_start: str,
    window_end: str,
) -> None:
    if not rows:
        return
    storage = Storage(str(db_path))
    review_ts = datetime.now(timezone.utc).isoformat()
    for raw in list(rows or []):
        row = dict(raw)
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        market = resolve_market_code(str(row.get("market") or ""))
        parent_order_key = str(row.get("parent_order_key") or "").strip()
        if not portfolio_id or not market or not parent_order_key:
            continue
        storage.upsert_investment_weekly_decision_evidence_history(
            {
                "week_label": str(week_label or "").strip(),
                "week_start": str(week_start or "").strip(),
                "window_start": str(window_start or "").strip(),
                "window_end": str(window_end or "").strip(),
                "ts": review_ts,
                "market": market,
                "portfolio_id": portfolio_id,
                "run_id": str(row.get("run_id") or ""),
                "parent_order_key": parent_order_key,
                "symbol": str(row.get("symbol") or "").upper(),
                "action": str(row.get("action") or ""),
                "decision_status": str(row.get("decision_status") or ""),
                "candidate_snapshot_id": str(row.get("candidate_snapshot_id") or ""),
                "candidate_stage": str(row.get("candidate_stage") or ""),
                "order_value": float(row.get("order_value", 0.0) or 0.0),
                "fill_notional": float(row.get("fill_notional", 0.0) or 0.0),
                "signal_score": float(row.get("signal_score", 0.0) or 0.0),
                "expected_edge_bps": float(row.get("expected_edge_bps", 0.0) or 0.0),
                "expected_cost_bps": float(row.get("expected_cost_bps", 0.0) or 0.0),
                "edge_gate_threshold_bps": float(row.get("edge_gate_threshold_bps", 0.0) or 0.0),
                "blocked_market_rule_order_count": int(row.get("blocked_market_rule_order_count", 0) or 0),
                "blocked_edge_order_count": int(row.get("blocked_edge_order_count", 0) or 0),
                "blocked_gate_order_count": int(row.get("blocked_gate_order_count", 0) or 0),
                "dynamic_liquidity_bucket": str(row.get("dynamic_liquidity_bucket") or ""),
                "dynamic_order_adv_pct": float(row.get("dynamic_order_adv_pct", 0.0) or 0.0),
                "slice_count": int(row.get("slice_count", 0) or 0),
                "strategy_control_weight_delta": float(row.get("strategy_control_weight_delta", 0.0) or 0.0),
                "risk_overlay_weight_delta": float(row.get("risk_overlay_weight_delta", 0.0) or 0.0),
                "risk_market_profile_budget_weight_delta": float(
                    row.get("risk_market_profile_budget_weight_delta", 0.0) or 0.0
                ),
                "risk_throttle_weight_delta": float(row.get("risk_throttle_weight_delta", 0.0) or 0.0),
                "risk_recovery_weight_credit": float(row.get("risk_recovery_weight_credit", 0.0) or 0.0),
                "execution_gate_blocked_weight": float(row.get("execution_gate_blocked_weight", 0.0) or 0.0),
                "realized_slippage_bps": (
                    float(row.get("realized_slippage_bps", 0.0) or 0.0)
                    if row.get("realized_slippage_bps") not in (None, "")
                    else None
                ),
                "realized_edge_bps": (
                    float(row.get("realized_edge_bps", 0.0) or 0.0)
                    if row.get("realized_edge_bps") not in (None, "")
                    else None
                ),
                "execution_capture_bps": (
                    float(row.get("execution_capture_bps", 0.0) or 0.0)
                    if row.get("execution_capture_bps") not in (None, "")
                    else None
                ),
                "first_fill_delay_seconds": (
                    float(row.get("first_fill_delay_seconds", 0.0) or 0.0)
                    if row.get("first_fill_delay_seconds") not in (None, "")
                    else None
                ),
                "outcome_5d_bps": (
                    float(row.get("outcome_5d_bps", 0.0) or 0.0)
                    if row.get("outcome_5d_bps") not in (None, "")
                    else None
                ),
                "outcome_20d_bps": (
                    float(row.get("outcome_20d_bps", 0.0) or 0.0)
                    if row.get("outcome_20d_bps") not in (None, "")
                    else None
                ),
                "outcome_60d_bps": (
                    float(row.get("outcome_60d_bps", 0.0) or 0.0)
                    if row.get("outcome_60d_bps") not in (None, "")
                    else None
                ),
                "details": row,
            }
        )


def _build_feedback_effect_market_summary_row(
    row: Dict[str, Any],
    grouped: Dict[tuple[str, str], Dict[str, Any]],
) -> None:
    market = str(row.get("market", "") or "-")
    feedback_kind_label = str(row.get("feedback_kind_label", "") or "-")
    key = (market, feedback_kind_label)
    item = grouped.setdefault(
        key,
        {
            "market": market,
            "feedback_kind": str(row.get("feedback_kind", "") or ""),
            "feedback_kind_label": feedback_kind_label,
            "tracked_count": 0,
            "latest_improved_count": 0,
            "latest_deteriorated_count": 0,
            "latest_stable_count": 0,
            "latest_observe_count": 0,
            "w1_improved_count": 0,
            "w2_improved_count": 0,
            "w4_improved_count": 0,
            "w1_deteriorated_count": 0,
            "w2_deteriorated_count": 0,
            "w4_deteriorated_count": 0,
            "active_weeks_total": 0,
            "top_portfolios": [],
        },
    )
    item["tracked_count"] = int(item.get("tracked_count", 0) or 0) + 1
    item["active_weeks_total"] = int(item.get("active_weeks_total", 0) or 0) + int(row.get("active_weeks", 0) or 0)
    latest_bucket = _feedback_effect_bucket(row.get("effect_label"))
    if latest_bucket == "改善":
        item["latest_improved_count"] = int(item.get("latest_improved_count", 0) or 0) + 1
    elif latest_bucket == "恶化":
        item["latest_deteriorated_count"] = int(item.get("latest_deteriorated_count", 0) or 0) + 1
    elif latest_bucket == "稳定":
        item["latest_stable_count"] = int(item.get("latest_stable_count", 0) or 0) + 1
    else:
        item["latest_observe_count"] = int(item.get("latest_observe_count", 0) or 0) + 1
    for horizon in ("w1", "w2", "w4"):
        bucket = _feedback_effect_bucket(row.get(f"effect_{horizon}"))
        if bucket == "改善":
            item[f"{horizon}_improved_count"] = int(item.get(f"{horizon}_improved_count", 0) or 0) + 1
        elif bucket == "恶化":
            item[f"{horizon}_deteriorated_count"] = int(item.get(f"{horizon}_deteriorated_count", 0) or 0) + 1
    top_portfolios = list(item.get("top_portfolios", []) or [])
    top_portfolios.append(f"{str(row.get('portfolio_id', '') or '-')}: {_feedback_effect_bucket(row.get('effect_label'))}")
    item["top_portfolios"] = top_portfolios[:5]


def _build_feedback_effect_market_summary(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[str, str], Dict[str, Any]] = {}
    for raw in list(rows or []):
        _build_feedback_effect_market_summary_row(dict(raw), grouped)

    out = list(grouped.values())
    for row in out:
        tracked = int(row.get("tracked_count", 0) or 0)
        row["avg_active_weeks"] = float(row.get("active_weeks_total", 0) or 0) / float(tracked or 1)
        row["summary_signal"] = _feedback_effect_summary_signal(row)
        row["top_portfolios_text"] = " / ".join(list(row.get("top_portfolios", []) or [])[:3]) or "-"
    out.sort(
        key=lambda row: (
            0 if str(row.get("summary_signal", "") or "") == "需复核" else 1 if str(row.get("summary_signal", "") or "") == "观察中" else 2,
            str(row.get("market", "") or ""),
            str(row.get("feedback_kind_label", "") or ""),
        )
    )
    return out


def _build_feedback_maturity_map(labeling_skip_rows: List[Dict[str, Any]]) -> Dict[tuple[str, str], Dict[str, Any]]:
    grouped: Dict[tuple[str, str], Dict[str, Any]] = {}
    for row in list(labeling_skip_rows or []):
        if str(row.get("skip_reason") or "").strip().upper() != "INSUFFICIENT_FORWARD_BARS":
            continue
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        horizon_days = str(row.get("horizon_days") or "").strip()
        key = (portfolio_id, horizon_days)
        item = grouped.setdefault(
            key,
            {
                "portfolio_id": portfolio_id,
                "horizon_days": horizon_days,
                "pending_skip_count": 0,
                "min_remaining_forward_bars": 0,
                "max_remaining_forward_bars": 0,
                "estimated_ready_start_ts": "",
                "estimated_ready_end_ts": "",
            },
        )
        pending_skip_count = int(float(row.get("skip_count", 0) or 0))
        item["pending_skip_count"] = int(item.get("pending_skip_count", 0) or 0) + pending_skip_count
        min_remaining = int(float(row.get("min_remaining_forward_bars", 0) or 0))
        max_remaining = int(float(row.get("max_remaining_forward_bars", 0) or 0))
        current_min = int(item.get("min_remaining_forward_bars", 0) or 0)
        current_max = int(item.get("max_remaining_forward_bars", 0) or 0)
        if min_remaining > 0:
            item["min_remaining_forward_bars"] = min_remaining if current_min <= 0 else min(current_min, min_remaining)
        if max_remaining > 0:
            item["max_remaining_forward_bars"] = max(current_max, max_remaining)
        ready_start = str(row.get("estimated_ready_start_ts") or "")
        ready_end = str(row.get("estimated_ready_end_ts") or "")
        current_ready_start = str(item.get("estimated_ready_start_ts") or "")
        current_ready_end = str(item.get("estimated_ready_end_ts") or "")
        if ready_start and (not current_ready_start or ready_start < current_ready_start):
            item["estimated_ready_start_ts"] = ready_start
        if ready_end and (not current_ready_end or ready_end > current_ready_end):
            item["estimated_ready_end_ts"] = ready_end
    return grouped


def _feedback_maturity_info(
    portfolio_id: str,
    horizon_days: str,
    calibration_sample_count: int,
    maturity_map: Dict[tuple[str, str], Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    key = (str(portfolio_id or "").strip(), str(horizon_days or "").strip())
    info = dict((maturity_map or {}).get(key, {}) or {})
    pending_skip_count = int(info.get("pending_skip_count", 0) or 0)
    total_known = int(calibration_sample_count) + pending_skip_count
    maturity_ratio = float(calibration_sample_count / total_known) if total_known > 0 else 0.0
    maturity_label = feedback_maturity_label(maturity_ratio, pending_skip_count, int(calibration_sample_count))
    return {
        "pending_skip_count": pending_skip_count,
        "min_remaining_forward_bars": int(info.get("min_remaining_forward_bars", 0) or 0),
        "max_remaining_forward_bars": int(info.get("max_remaining_forward_bars", 0) or 0),
        "estimated_ready_start_ts": str(info.get("estimated_ready_start_ts") or ""),
        "estimated_ready_end_ts": str(info.get("estimated_ready_end_ts") or ""),
        "maturity_ratio": round(_clamp(maturity_ratio, 0.0, 1.0), 6),
        "maturity_label": maturity_label,
        "maturity_known": int(total_known > 0),
    }


def _feedback_automation_maturity_note(
    calibration_horizon_days: str,
    maturity_info: Dict[str, Any],
) -> str:
    pending_skip_count = int(maturity_info.get("pending_skip_count", 0) or 0)
    if pending_skip_count <= 0:
        return ""
    remaining_min = int(maturity_info.get("min_remaining_forward_bars", 0) or 0)
    remaining_max = int(maturity_info.get("max_remaining_forward_bars", 0) or 0)
    ready_start = str(maturity_info.get("estimated_ready_start_ts") or "")
    ready_end = str(maturity_info.get("estimated_ready_end_ts") or "")
    return (
        f"当前仍有 {pending_skip_count} 条 {calibration_horizon_days or '-'} 日 outcome 样本待成熟"
        f"（remaining={remaining_min}-{remaining_max}，ready={ready_start[:10] or '-'}->{ready_end[:10] or '-'}）。"
    )


def _feedback_automation_apply_decision(
    *,
    kind: str,
    action: str,
    thresholds: Dict[str, float],
    outcome_ready: bool,
    base_confidence: float,
    final_confidence: float,
    calibration_score: float,
    feedback_sample_count: int,
    pending_skip_count: int,
    auto_maturity_ready: bool,
    suggest_maturity_ready: bool,
    maturity_note: str,
) -> Dict[str, str]:
    kind_label = feedback_kind_label(kind)
    if not action or action in {"HOLD", "KEEP_OBSERVING", "NONE"}:
        return {
            "apply_mode": "HOLD",
            "basis": "NO_SIGNAL",
            "reason": f"本周没有形成明确的{kind_label}调参动作，先继续观察。",
        }

    if outcome_ready:
        if (
            final_confidence >= float(thresholds["auto_confidence"])
            and calibration_score >= float(thresholds["auto_calibration_score"])
            and feedback_sample_count >= int(thresholds["auto_feedback_samples"])
            and auto_maturity_ready
        ):
            decision = {
                "apply_mode": "AUTO_APPLY",
                "basis": "OUTCOME_CALIBRATED",
                "reason": f"{kind_label}本周动作明确，且 outcome 校准样本已支持自动应用；paper 可自动落盘，live 保留人工确认。",
            }
        elif (
            final_confidence >= float(thresholds["suggest_confidence"])
            and (
                calibration_score >= float(thresholds["suggest_calibration_score"])
                or feedback_sample_count >= int(thresholds["suggest_feedback_samples"])
            )
            and suggest_maturity_ready
        ):
            decision = {
                "apply_mode": "SUGGEST_ONLY",
                "basis": "OUTCOME_CALIBRATED",
                "reason": f"{kind_label}已有一定 outcome 支持，但强度还不够稳，建议先人工确认，不直接自动放大。",
            }
        else:
            decision = {
                "apply_mode": "HOLD",
                "basis": "OUTCOME_CALIBRATED",
                "reason": f"{kind_label}虽然已有 outcome 校准样本，但本周置信度仍偏弱，先继续观察，避免过早自动改参数。",
            }
        if pending_skip_count > 0 and not auto_maturity_ready and decision["apply_mode"] == "SUGGEST_ONLY":
            decision["reason"] = f"{kind_label}已有 outcome 支持，但样本仍在持续成熟中，建议先人工确认。{maturity_note}"
        elif pending_skip_count > 0 and not suggest_maturity_ready:
            decision["apply_mode"] = "HOLD"
            decision["reason"] = f"{kind_label}仍有较多 outcome 样本待成熟，先继续观察，避免过早自动改参数。{maturity_note}"
        return decision

    if (
        base_confidence >= float(thresholds["auto_base_confidence"])
        and feedback_sample_count >= int(thresholds["auto_feedback_samples"])
        and auto_maturity_ready
    ):
        decision = {
            "apply_mode": "AUTO_APPLY",
            "basis": "BASE_WEEKLY",
            "reason": f"{kind_label}的周报样本已经足够强，虽然 outcome 样本还没完全成熟，paper 先自动应用，后续再用 outcome 继续校准。",
        }
    elif (
        base_confidence >= float(thresholds["suggest_base_confidence"])
        and feedback_sample_count >= int(thresholds["suggest_feedback_samples"])
        and suggest_maturity_ready
    ):
        decision = {
            "apply_mode": "SUGGEST_ONLY",
            "basis": "BASE_WEEKLY",
            "reason": f"{kind_label}已有周报层面的调整依据，但 outcome 校准样本还不够，建议先人工确认。",
        }
    else:
        decision = {
            "apply_mode": "HOLD",
            "basis": "NO_SIGNAL",
            "reason": f"{kind_label}当前样本仍偏少或置信度偏弱，暂时不建议自动改参数。",
        }
    if pending_skip_count > 0 and not auto_maturity_ready and decision["apply_mode"] == "SUGGEST_ONLY":
        decision["reason"] = f"{kind_label}周报信号已经出现，但 outcome 样本还在成熟中，建议先人工确认。{maturity_note}"
    elif pending_skip_count > 0 and not suggest_maturity_ready:
        decision["apply_mode"] = "HOLD"
        decision["reason"] = f"{kind_label}仍缺少足够成熟的 outcome 样本，先继续观察。{maturity_note}"
    return decision


def _feedback_market_data_gate_decision(
    portfolio_id: str,
    market_data_gate_map: Dict[str, Dict[str, Any]] | None,
    decision: Dict[str, str],
) -> Dict[str, Any]:
    market_data_gate = dict((market_data_gate_map or {}).get(portfolio_id, {}) or {})
    market_data_gate_status = str(market_data_gate.get("status_code") or "UNKNOWN").strip().upper() or "UNKNOWN"
    market_data_gate_label = str(market_data_gate.get("status_label") or "未检查").strip() or "未检查"
    market_data_gate_reason = str(market_data_gate.get("reason") or "").strip()
    apply_mode = str(decision.get("apply_mode") or "")
    basis = str(decision.get("basis") or "")
    reason = str(decision.get("reason") or "")

    if market_data_gate_status in {"ATTENTION", "RESEARCH_FALLBACK"}:
        if apply_mode == "AUTO_APPLY":
            apply_mode = "SUGGEST_ONLY"
            basis = "DATA_HEALTH_GATED"
            if market_data_gate_status == "ATTENTION":
                reason = f"{market_data_gate_reason} 当前先降为人工确认，不直接 AUTO_APPLY。"
            else:
                reason = f"{market_data_gate_reason} 当前先按研究/人工确认口径处理，不直接 AUTO_APPLY。"
        elif apply_mode == "SUGGEST_ONLY":
            basis = "DATA_HEALTH_GATED"
            if market_data_gate_reason:
                reason = f"{reason} {market_data_gate_reason}"
        elif market_data_gate_reason:
            reason = f"{reason} {market_data_gate_reason}"

    return {
        "decision": {
            "apply_mode": apply_mode,
            "basis": basis,
            "reason": reason,
        },
        "market_data_gate_status": market_data_gate_status,
        "market_data_gate_label": market_data_gate_label,
        "market_data_gate_reason": market_data_gate_reason,
        "market_data_probe_status_label": str(market_data_gate.get("probe_status_label") or ""),
    }


def _build_feedback_automation_row(
    feedback_kind: str,
    row: Dict[str, Any],
    maturity_map: Dict[tuple[str, str], Dict[str, Any]] | None = None,
    threshold_overrides: Dict[str, Dict[str, Dict[str, float]]] | None = None,
    market_data_gate_map: Dict[str, Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    kind = str(feedback_kind or "").strip().lower()
    action_field = feedback_action_field(kind)
    action = str(row.get(action_field) or "").strip().upper()
    portfolio_id = str(row.get("portfolio_id") or "")
    base_confidence = float(row.get("feedback_base_confidence", 0.0) or 0.0)
    final_confidence = float(row.get("feedback_confidence", 0.0) or 0.0)
    calibration_score = float(row.get("feedback_calibration_score", 0.5) or 0.5)
    feedback_sample_count = int(row.get("feedback_sample_count", 0) or 0)
    calibration_sample_count = int(row.get("feedback_calibration_sample_count", 0) or 0)
    calibration_horizon_days = str(row.get("feedback_calibration_horizon_days") or "")
    thresholds = feedback_automation_thresholds(
        kind,
        market=str(row.get("market") or ""),
        threshold_overrides=threshold_overrides,
    )
    outcome_ready = calibration_sample_count >= int(thresholds["ready_outcome_samples"])
    maturity_info = _feedback_maturity_info(
        portfolio_id,
        calibration_horizon_days,
        calibration_sample_count,
        maturity_map,
    )
    maturity_ratio = float(maturity_info.get("maturity_ratio", 0.0) or 0.0)
    pending_skip_count = int(maturity_info.get("pending_skip_count", 0) or 0)
    maturity_known = bool(int(maturity_info.get("maturity_known", 0) or 0))
    auto_maturity_ready = (not maturity_known) or maturity_ratio >= float(thresholds["auto_maturity_ratio"])
    suggest_maturity_ready = (not maturity_known) or maturity_ratio >= float(thresholds["suggest_maturity_ratio"])
    maturity_note = _feedback_automation_maturity_note(calibration_horizon_days, maturity_info)
    decision = _feedback_automation_apply_decision(
        kind=kind,
        action=action,
        thresholds=thresholds,
        outcome_ready=outcome_ready,
        base_confidence=base_confidence,
        final_confidence=final_confidence,
        calibration_score=calibration_score,
        feedback_sample_count=feedback_sample_count,
        pending_skip_count=pending_skip_count,
        auto_maturity_ready=auto_maturity_ready,
        suggest_maturity_ready=suggest_maturity_ready,
        maturity_note=maturity_note,
    )
    gated = _feedback_market_data_gate_decision(portfolio_id, market_data_gate_map, decision)
    gated_decision = dict(gated.get("decision") or {})
    apply_mode = str(gated_decision.get("apply_mode") or "")
    basis = str(gated_decision.get("basis") or "")
    reason = str(gated_decision.get("reason") or "")
    market_data_gate_status = str(gated.get("market_data_gate_status") or "")
    market_data_gate_label = str(gated.get("market_data_gate_label") or "")
    market_data_gate_reason = str(gated.get("market_data_gate_reason") or "")
    market_data_probe_status_label = str(gated.get("market_data_probe_status_label") or "")

    return {
        "portfolio_id": portfolio_id,
        "market": str(row.get("market") or ""),
        "feedback_kind": kind,
        "feedback_kind_label": feedback_kind_label(kind),
        "feedback_action": action,
        "feedback_scope": str(row.get("feedback_scope") or ""),
        "feedback_sample_count": int(feedback_sample_count),
        "feedback_base_confidence": float(base_confidence),
        "feedback_base_confidence_label": str(row.get("feedback_base_confidence_label") or _feedback_confidence_label(base_confidence)),
        "feedback_calibration_score": float(calibration_score),
        "feedback_calibration_label": str(row.get("feedback_calibration_label") or "MEDIUM"),
        "feedback_calibration_sample_count": int(calibration_sample_count),
        "feedback_calibration_horizon_days": calibration_horizon_days,
        "feedback_calibration_scope": str(row.get("feedback_calibration_scope") or ""),
        "feedback_confidence": float(final_confidence),
        "feedback_confidence_label": str(row.get("feedback_confidence_label") or _feedback_confidence_label(final_confidence)),
        "outcome_maturity_known": int(maturity_known),
        "outcome_maturity_ratio": float(maturity_ratio),
        "outcome_maturity_label": str(maturity_info.get("maturity_label") or "UNKNOWN"),
        "outcome_pending_sample_count": int(pending_skip_count),
        "outcome_pending_min_remaining_bars": int(maturity_info.get("min_remaining_forward_bars", 0) or 0),
        "outcome_pending_max_remaining_bars": int(maturity_info.get("max_remaining_forward_bars", 0) or 0),
        "outcome_ready_estimate_start_ts": str(maturity_info.get("estimated_ready_start_ts") or ""),
        "outcome_ready_estimate_end_ts": str(maturity_info.get("estimated_ready_end_ts") or ""),
        "calibration_apply_mode": apply_mode,
        "calibration_apply_mode_label": feedback_automation_mode_label(apply_mode),
        "calibration_basis": basis,
        "calibration_basis_label": feedback_automation_basis_label(basis),
        "paper_auto_apply_enabled": int(apply_mode == "AUTO_APPLY"),
        "live_confirmation_required": int(apply_mode in {"AUTO_APPLY", "SUGGEST_ONLY"}),
        "automation_reason": reason,
        "feedback_reason": str(row.get("feedback_reason") or ""),
        "market_data_gate_status": market_data_gate_status,
        "market_data_gate_label": market_data_gate_label,
        "market_data_gate_reason": market_data_gate_reason,
        "market_data_probe_status_label": market_data_probe_status_label,
        "auto_threshold_snapshot_json": json.dumps(thresholds, ensure_ascii=False, sort_keys=True),
    }


def _build_feedback_automation_rows(
    shadow_feedback_rows: List[Dict[str, Any]],
    risk_feedback_rows: List[Dict[str, Any]],
    execution_feedback_rows: List[Dict[str, Any]],
    labeling_skip_rows: List[Dict[str, Any]] | None = None,
    threshold_overrides: Dict[str, Dict[str, Dict[str, float]]] | None = None,
    market_data_gate_map: Dict[str, Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    maturity_map = _build_feedback_maturity_map(list(labeling_skip_rows or []))
    rows: List[Dict[str, Any]] = []
    for kind, group in (
        ("shadow", shadow_feedback_rows),
        ("risk", risk_feedback_rows),
        ("execution", execution_feedback_rows),
    ):
        for raw in list(group or []):
            row = _build_feedback_automation_row(
                kind,
                dict(raw),
                maturity_map,
                threshold_overrides,
                market_data_gate_map,
            )
            if not str(row.get("portfolio_id") or "").strip():
                continue
            rows.append(row)
    rows.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
            0 if str(row.get("feedback_kind") or "") == "execution" else 1 if str(row.get("feedback_kind") or "") == "risk" else 2,
        )
    )
    return rows


def _shadow_review_recommendation(rows: List[Dict[str, Any]]) -> Dict[str, str]:
    if not rows:
        return {"shadow_review_action": "NONE", "shadow_review_reason": "本周没有 shadow review 拦单。"}
    count = max(1, len(rows))
    near_miss_rate = sum(int(row.get("near_miss", 0) or 0) for row in rows) / count
    far_below_rate = sum(int(row.get("far_below", 0) or 0) for row in rows) / count
    avg_score_gap = _avg_defined([row.get("score_gap") for row in rows])
    avg_prob_gap = _avg_defined([row.get("prob_gap") for row in rows])
    repeat_symbols = sum(
        1
        for symbol in {str(row.get("symbol") or "") for row in rows}
        if sum(1 for r in rows if str(r.get("symbol") or "") == symbol) >= 2
    )
    if near_miss_rate >= 0.6 and (avg_score_gap is None or avg_score_gap <= 0.05) and (avg_prob_gap is None or avg_prob_gap <= 0.08):
        return {
            "shadow_review_action": "REVIEW_THRESHOLD",
            "shadow_review_reason": "多数 shadow review 拦单接近阈值，建议复核 burn-in 阈值是否偏严。",
        }
    if far_below_rate >= 0.5 or repeat_symbols >= 2:
        return {
            "shadow_review_action": "WEAK_SIGNAL",
            "shadow_review_reason": "被拦标的多次出现且 shadow 分数明显低于阈值，更像信号持续偏弱，应先优化选股/特征。",
        }
    return {
        "shadow_review_action": "KEEP_OBSERVING",
        "shadow_review_reason": "当前样本显示 shadow review 仍有观察价值，先继续累积样本再调整阈值。",
    }


def _build_shadow_review_summary_rows(shadow_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in shadow_rows:
        grouped.setdefault(str(row.get("portfolio_id") or ""), []).append(row)
    out: List[Dict[str, Any]] = []
    for portfolio_id, rows in grouped.items():
        rows = list(rows)
        rows.sort(key=lambda row: str(row.get("ts") or ""), reverse=True)
        symbol_counts: Dict[str, int] = {}
        for row in rows:
            symbol = str(row.get("symbol") or "").upper()
            if symbol:
                symbol_counts[symbol] = int(symbol_counts.get(symbol, 0)) + 1
        repeated_symbols = sorted([symbol for symbol, count in symbol_counts.items() if count >= 2])
        rec = _shadow_review_recommendation(rows)
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(rows[0].get("market") or ""),
                "shadow_review_count": int(len(rows)),
                "distinct_symbols": int(len(symbol_counts)),
                "repeated_symbol_count": int(len(repeated_symbols)),
                "repeated_symbols": ",".join(repeated_symbols[:8]),
                "score_block_count": int(sum(int(row.get("score_blocked", 0) or 0) for row in rows)),
                "prob_block_count": int(sum(int(row.get("prob_blocked", 0) or 0) for row in rows)),
                "near_miss_count": int(sum(int(row.get("near_miss", 0) or 0) for row in rows)),
                "far_below_count": int(sum(int(row.get("far_below", 0) or 0) for row in rows)),
                "avg_shadow_score": _avg_defined([row.get("shadow_score") for row in rows]),
                "avg_shadow_prob": _avg_defined([row.get("shadow_prob") for row in rows]),
                "avg_shadow_samples": _avg_defined([row.get("shadow_samples") for row in rows]),
                "avg_score_gap": _avg_defined([row.get("score_gap") for row in rows]),
                "avg_prob_gap": _avg_defined([row.get("prob_gap") for row in rows]),
                "latest_shadow_ts": str(rows[0].get("ts") or ""),
                "latest_shadow_symbol": str(rows[0].get("symbol") or ""),
                "shadow_review_action": rec["shadow_review_action"],
                "shadow_review_reason": rec["shadow_review_reason"],
            }
        )
    out.sort(key=lambda row: (-int(row.get("shadow_review_count", 0) or 0), str(row.get("portfolio_id") or "")))
    return out


def _build_shadow_signal_penalties(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        grouped.setdefault(symbol, []).append(row)

    out: List[Dict[str, Any]] = []
    for symbol, symbol_rows in grouped.items():
        count = len(symbol_rows)
        far_below_count = sum(int(row.get("far_below", 0) or 0) for row in symbol_rows)
        if count < 2 and far_below_count <= 0:
            continue
        avg_score_gap = _avg_defined([row.get("score_gap") for row in symbol_rows]) or 0.0
        avg_prob_gap = _avg_defined([row.get("prob_gap") for row in symbol_rows]) or 0.0
        score_penalty = _clamp(0.03 + avg_score_gap * 0.45 + max(0, count - 1) * 0.01, 0.03, 0.12)
        execution_penalty = _clamp(0.02 + avg_prob_gap * 0.18 + max(0, count - 1) * 0.01, 0.02, 0.10)
        cooldown_days = int(min(28, 7 + max(0, count - 1) * 7 + far_below_count * 3))
        out.append(
            {
                "symbol": symbol,
                "repeat_count": int(count),
                "far_below_count": int(far_below_count),
                "avg_score_gap": float(avg_score_gap),
                "avg_prob_gap": float(avg_prob_gap),
                "score_penalty": round(float(score_penalty), 6),
                "execution_penalty": round(float(execution_penalty), 6),
                "cooldown_days": int(cooldown_days),
                "reason": "repeat_shadow_weak_signal",
            }
        )
    out.sort(
        key=lambda row: (
            -int(row.get("repeat_count", 0) or 0),
            -float(row.get("score_penalty", 0.0) or 0.0),
            str(row.get("symbol") or ""),
        )
    )
    return out


def _build_shadow_feedback_rows(
    shadow_rows: List[Dict[str, Any]],
    shadow_summary_rows: List[Dict[str, Any]],
    feedback_calibration_map: Dict[str, Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    shadow_rows_by_portfolio: Dict[str, List[Dict[str, Any]]] = {}
    for row in shadow_rows:
        shadow_rows_by_portfolio.setdefault(str(row.get("portfolio_id") or ""), []).append(row)

    out: List[Dict[str, Any]] = []
    for summary_row in shadow_summary_rows:
        portfolio_id = str(summary_row.get("portfolio_id") or "")
        rows = list(shadow_rows_by_portfolio.get(portfolio_id) or [])
        action = str(summary_row.get("shadow_review_action") or "").upper()
        avg_score_gap = float(summary_row.get("avg_score_gap") or 0.0)
        avg_prob_gap = float(summary_row.get("avg_prob_gap") or 0.0)
        avg_shadow_samples = float(summary_row.get("avg_shadow_samples") or 0.0)
        repeat_count = int(summary_row.get("repeated_symbol_count") or 0)
        signal_penalties: List[Dict[str, Any]] = []
        execution_shadow_score_delta = 0.0
        execution_shadow_prob_delta = 0.0
        scoring_accumulate_threshold_delta = 0.0
        scoring_execution_ready_threshold_delta = 0.0
        plan_review_window_days_delta = 0
        feedback_reason = str(summary_row.get("shadow_review_reason") or "")

        if action == "REVIEW_THRESHOLD":
            if avg_score_gap > 0.0:
                execution_shadow_score_delta = -_clamp(max(0.01, avg_score_gap * 0.50), 0.01, 0.03)
            if avg_prob_gap > 0.0:
                execution_shadow_prob_delta = -_clamp(max(0.01, avg_prob_gap * 0.50), 0.01, 0.04)
        elif action == "WEAK_SIGNAL":
            scoring_accumulate_threshold_delta = _clamp(max(0.01, avg_score_gap * 0.20), 0.01, 0.04)
            scoring_execution_ready_threshold_delta = _clamp(max(0.01, avg_prob_gap * 0.20), 0.01, 0.04)
            plan_review_window_days_delta = 7
            signal_penalties = _build_shadow_signal_penalties(rows)

        base_confidence = (
            _feedback_confidence(
                sample_ratio=float(len(rows) / 6.0),
                magnitude_ratio=max(
                    avg_score_gap / 0.12 if avg_score_gap > 0.0 else 0.0,
                    avg_prob_gap / 0.18 if avg_prob_gap > 0.0 else 0.0,
                ),
                persistence_ratio=float(repeat_count / 3.0),
                structure_ratio=float(avg_shadow_samples / 24.0),
            )
            if action
            else 0.0
        )
        calibration_info = _feedback_calibration_support(
            dict((feedback_calibration_map or {}).get(portfolio_id, {}) or {}),
            feedback_kind="shadow",
            action=action,
        )
        confidence = _apply_outcome_calibration(base_confidence, float(calibration_info.get("score", 0.5) or 0.5))

        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(summary_row.get("market") or ""),
                "shadow_review_action": action,
                "feedback_scope": "paper_only",
                "execution_shadow_score_delta": round(float(execution_shadow_score_delta), 6),
                "execution_shadow_prob_delta": round(float(execution_shadow_prob_delta), 6),
                "scoring_accumulate_threshold_delta": round(float(scoring_accumulate_threshold_delta), 6),
                "scoring_execution_ready_threshold_delta": round(float(scoring_execution_ready_threshold_delta), 6),
                "plan_review_window_days_delta": int(plan_review_window_days_delta),
                "signal_penalty_symbol_count": int(len(signal_penalties)),
                "signal_penalty_symbols": ",".join(str(row.get("symbol") or "") for row in signal_penalties[:12]),
                "signal_penalties_json": json.dumps(signal_penalties, ensure_ascii=False),
                "feedback_sample_count": int(len(rows)),
                "feedback_base_confidence": float(base_confidence),
                "feedback_base_confidence_label": _feedback_confidence_label(base_confidence),
                "feedback_calibration_score": float(calibration_info.get("score", 0.5) or 0.5),
                "feedback_calibration_label": str(calibration_info.get("label", "MEDIUM") or "MEDIUM"),
                "feedback_calibration_sample_count": int(calibration_info.get("sample_count", 0) or 0),
                "feedback_calibration_horizon_days": str(calibration_info.get("selected_horizon_days", "") or ""),
                "feedback_calibration_scope": str(calibration_info.get("selection_scope_label", "") or "-"),
                "feedback_calibration_reason": str(calibration_info.get("reason", "") or ""),
                "feedback_confidence": float(confidence),
                "feedback_confidence_label": _feedback_confidence_label(confidence),
                "feedback_reason": feedback_reason,
            }
        )
    out.sort(key=lambda row: (-int(row.get("signal_penalty_symbol_count", 0) or 0), str(row.get("portfolio_id") or "")))
    return out


def _build_risk_feedback_rows(
    risk_review_rows: List[Dict[str, Any]],
    attribution_rows: List[Dict[str, Any]] | None = None,
    feedback_calibration_map: Dict[str, Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    attribution_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in list(attribution_rows or [])
        if str(row.get("portfolio_id") or "").strip()
    }
    out: List[Dict[str, Any]] = []
    for row in risk_review_rows:
        driver = str(row.get("dominant_risk_driver") or "").upper()
        risk_overlay_runs = int(row.get("risk_overlay_runs") or 0)
        latest_corr = float(row.get("latest_avg_pair_correlation", 0.0) or 0.0)
        latest_stress = float(row.get("latest_stress_worst_loss", 0.0) or 0.0)
        latest_top_sector = float(row.get("latest_top_sector_share", 0.0) or 0.0)
        latest_dynamic_net = float(row.get("latest_dynamic_net_exposure", 0.0) or 0.0)
        latest_dynamic_gross = float(row.get("latest_dynamic_gross_exposure", 0.0) or 0.0)
        attribution = dict(attribution_map.get(str(row.get("portfolio_id") or ""), {}) or {})
        control_context = _feedback_control_driver_context(
            strategy_delta=float(attribution.get("strategy_control_weight_delta", 0.0) or 0.0),
            risk_delta=float(attribution.get("risk_overlay_weight_delta", 0.0) or 0.0),
            execution_gate_weight=float(attribution.get("execution_gate_blocked_weight", 0.0) or 0.0),
            execution_gate_ratio=float(attribution.get("execution_gate_blocked_order_ratio", 0.0) or 0.0),
            execution_gate_value=float(attribution.get("execution_gate_blocked_order_value", 0.0) or 0.0),
        )
        control_driver = str(control_context.get("feedback_control_driver", "") or "")
        strategy_delta = float(control_context.get("strategy_control_weight_delta", 0.0) or 0.0)
        risk_delta = float(control_context.get("risk_overlay_weight_delta", 0.0) or 0.0)
        gate_weight = float(control_context.get("execution_gate_blocked_weight", 0.0) or 0.0)
        control_driver_reason = ""

        action = "HOLD"
        feedback_reason = str(row.get("risk_diagnosis") or "")
        max_single_delta = 0.0
        max_sector_delta = 0.0
        max_net_delta = 0.0
        max_gross_delta = 0.0
        max_short_delta = 0.0
        correlation_soft_limit_delta = 0.0

        if driver == "CORRELATION":
            action = "TIGHTEN"
            max_single_delta = -_clamp(0.01 + max(0.0, latest_corr - 0.62) * 0.10, 0.01, 0.04)
            max_sector_delta = -_clamp(
                0.02 + max(0.0, latest_top_sector - 0.40) * 0.18 + max(0.0, latest_corr - 0.62) * 0.08,
                0.02,
                0.10,
            )
            max_net_delta = -_clamp(0.02 + max(0.0, latest_corr - 0.62) * 0.12, 0.02, 0.08)
            max_gross_delta = -_clamp(0.02 + max(0.0, latest_corr - 0.62) * 0.15, 0.02, 0.10)
            max_short_delta = -_clamp(0.01 + max(0.0, latest_corr - 0.62) * 0.08, 0.01, 0.05)
            correlation_soft_limit_delta = -0.03
        elif driver == "STRESS":
            action = "TIGHTEN"
            max_net_delta = -_clamp(0.03 + max(0.0, latest_stress - 0.085) * 0.90, 0.03, 0.12)
            max_gross_delta = -_clamp(0.04 + max(0.0, latest_stress - 0.085) * 1.10, 0.04, 0.14)
            max_short_delta = -_clamp(0.02 + max(0.0, latest_stress - 0.085) * 0.50, 0.02, 0.08)
            max_single_delta = -_clamp(0.01 + max(0.0, latest_stress - 0.085) * 0.18, 0.01, 0.04)
        elif driver == "EXPOSURE_BUDGET" and latest_corr <= 0.45 and latest_stress <= 0.06:
            action = "RELAX"
            max_single_delta = _clamp(0.01 + max(0.0, 0.72 - latest_dynamic_net) * 0.06, 0.01, 0.03)
            max_sector_delta = _clamp(0.02 + max(0.0, 0.76 - latest_dynamic_gross) * 0.08, 0.02, 0.05)
            max_net_delta = _clamp(0.03 + max(0.0, 0.72 - latest_dynamic_net) * 0.16, 0.03, 0.08)
            max_gross_delta = _clamp(0.03 + max(0.0, 0.78 - latest_dynamic_gross) * 0.18, 0.03, 0.10)
            max_short_delta = _clamp(0.01, 0.01, 0.03)
            correlation_soft_limit_delta = 0.02
            feedback_reason = "组合风险预算偏紧，但相关性和 stress 仍在可接受范围，适度放宽预算以减少资金闲置。"

        if (
            action in {"TIGHTEN", "RELAX"}
            and control_driver == "STRATEGY"
            and strategy_delta >= max(0.05, risk_delta + 0.02)
        ):
            action = "HOLD"
            max_single_delta = 0.0
            max_sector_delta = 0.0
            max_net_delta = 0.0
            max_gross_delta = 0.0
            max_short_delta = 0.0
            correlation_soft_limit_delta = 0.0
            control_driver_reason = (
                "本周更明显的压仓来自策略主动控仓，先复核 regime/target invested weight，暂不直接改风险预算。"
            )
            feedback_reason = (
                f"{feedback_reason.rstrip('。')}。{control_driver_reason}"
                f"（{str(control_context.get('feedback_control_split_text') or '')}）"
            )
        elif (
            action == "RELAX"
            and control_driver == "EXECUTION"
            and gate_weight >= max(0.02, risk_delta + 0.01)
        ):
            action = "HOLD"
            max_single_delta = 0.0
            max_sector_delta = 0.0
            max_net_delta = 0.0
            max_gross_delta = 0.0
            max_short_delta = 0.0
            correlation_soft_limit_delta = 0.0
            control_driver_reason = "当前低仓位更像执行 gate 阻断，而不是风险预算过紧，先复核执行门槛。"
            feedback_reason = (
                f"{feedback_reason.rstrip('。')}。{control_driver_reason}"
                f"（{str(control_context.get('feedback_control_split_text') or '')}）"
            )
        elif action in {"TIGHTEN", "RELAX"} and control_driver == "RISK" and risk_delta > 1e-9:
            control_driver_reason = (
                f"本周主要压缩来自风险 overlay（{str(control_context.get('feedback_control_split_text') or '')}），继续沿风险预算方向调整更一致。"
            )
            feedback_reason = f"{feedback_reason.rstrip('。')}。{control_driver_reason}"

        severity_ratio = 0.0
        if driver == "CORRELATION":
            severity_ratio = max(0.0, latest_corr - 0.62) / 0.12
        elif driver == "STRESS":
            severity_ratio = max(0.0, latest_stress - 0.085) / 0.06
        elif driver == "EXPOSURE_BUDGET":
            severity_ratio = max(max(0.0, 0.72 - latest_dynamic_net), max(0.0, 0.78 - latest_dynamic_gross)) / 0.24
        if action != "HOLD" and control_driver == "RISK":
            severity_ratio = max(severity_ratio, min(1.0, risk_delta / 0.10))
        base_confidence = (
            _feedback_confidence(
                sample_ratio=float(risk_overlay_runs / 4.0),
                magnitude_ratio=severity_ratio,
                persistence_ratio=float((1.0 - min(1.0, float(row.get("latest_dynamic_scale", 1.0) or 1.0))) / 0.30)
                if driver in {"CORRELATION", "STRESS"}
                else 0.0,
                structure_ratio=1.0 if driver in {"CORRELATION", "STRESS", "EXPOSURE_BUDGET"} else 0.0,
            )
            if action != "HOLD"
            else 0.0
        )
        calibration_info = _feedback_calibration_support(
            dict((feedback_calibration_map or {}).get(str(row.get("portfolio_id") or ""), {}) or {}),
            feedback_kind="risk",
            action=action,
        )
        confidence = _apply_outcome_calibration(base_confidence, float(calibration_info.get("score", 0.5) or 0.5))

        out.append(
            {
                "portfolio_id": str(row.get("portfolio_id") or ""),
                "market": str(row.get("market") or ""),
                "feedback_scope": "paper_only",
                "risk_feedback_action": action,
                "paper_max_single_weight_delta": round(float(max_single_delta), 6),
                "paper_max_sector_weight_delta": round(float(max_sector_delta), 6),
                "paper_max_net_exposure_delta": round(float(max_net_delta), 6),
                "paper_max_gross_exposure_delta": round(float(max_gross_delta), 6),
                "paper_max_short_exposure_delta": round(float(max_short_delta), 6),
                "paper_correlation_soft_limit_delta": round(float(correlation_soft_limit_delta), 6),
                "feedback_sample_count": int(risk_overlay_runs),
                "feedback_base_confidence": float(base_confidence),
                "feedback_base_confidence_label": _feedback_confidence_label(base_confidence),
                "feedback_calibration_score": float(calibration_info.get("score", 0.5) or 0.5),
                "feedback_calibration_label": str(calibration_info.get("label", "MEDIUM") or "MEDIUM"),
                "feedback_calibration_sample_count": int(calibration_info.get("sample_count", 0) or 0),
                "feedback_calibration_horizon_days": str(calibration_info.get("selected_horizon_days", "") or ""),
                "feedback_calibration_scope": str(calibration_info.get("selection_scope_label", "") or "-"),
                "feedback_calibration_reason": str(calibration_info.get("reason", "") or ""),
                "feedback_confidence": float(confidence),
                "feedback_confidence_label": _feedback_confidence_label(confidence),
                "feedback_reason": feedback_reason,
                "feedback_control_driver": str(control_context.get("feedback_control_driver", "") or ""),
                "feedback_control_driver_label": str(control_context.get("feedback_control_driver_label", "") or ""),
                "feedback_control_driver_weight": float(control_context.get("feedback_control_driver_weight", 0.0) or 0.0),
                "feedback_control_split_text": str(control_context.get("feedback_control_split_text", "") or ""),
                "feedback_control_driver_reason": control_driver_reason,
                "strategy_control_weight_delta": float(strategy_delta),
                "risk_overlay_weight_delta": float(risk_delta),
                "execution_gate_blocked_weight": float(gate_weight),
            }
        )
    out.sort(
        key=lambda row: (
            0
            if str(row.get("risk_feedback_action", "") or "") == "TIGHTEN"
            else 1 if str(row.get("risk_feedback_action", "") or "") == "RELAX" else 2,
            str(row.get("portfolio_id", "") or ""),
        )
    )
    return out




def _resolve_project_path(path_str: str) -> Path:
    return resolve_repo_path(BASE_DIR, path_str)


def _runtime_config_paths_for_market(market: str) -> Dict[str, Path]:
    market_code = resolve_market_code(str(market or ""))
    ibkr_cfg = _load_yaml_file(market_config_path(BASE_DIR, market_code)) if market_code else {}
    return {
        "market_structure": _resolve_project_path(
            str(
                ibkr_cfg.get(
                    "market_structure_config",
                    f"config/market_structure_{market_code.lower()}.yaml" if market_code else "config/market_structure.yaml",
                )
            )
        ),
        "account_profile": _resolve_project_path(
            str(ibkr_cfg.get("account_profile_config", "config/account_profiles.yaml"))
        ),
        "adaptive_strategy": _resolve_project_path(
            str(ibkr_cfg.get("adaptive_strategy_config", "config/adaptive_strategy_framework.yaml"))
        ),
        "investment_paper": _resolve_project_path(
            str(
                ibkr_cfg.get(
                    "investment_paper_config",
                    f"config/investment_paper_{market_code.lower()}.yaml" if market_code else "config/investment_paper.yaml",
                )
            )
        ),
        "investment_execution": _resolve_project_path(
            str(
                ibkr_cfg.get(
                    "investment_execution_config",
                    f"config/investment_execution_{market_code.lower()}.yaml" if market_code else "config/investment_execution.yaml",
                )
            )
        ),
    }


def _strategy_parameter_registry() -> StrategyParameterRegistry:
    global _STRATEGY_PARAMETER_REGISTRY
    if _STRATEGY_PARAMETER_REGISTRY is None:
        _STRATEGY_PARAMETER_REGISTRY = load_strategy_parameter_registry(BASE_DIR)
    return _STRATEGY_PARAMETER_REGISTRY


def _calibration_patch_field_meta(field: str) -> Dict[str, Any]:
    return strategy_parameter_field_meta(field, registry=_strategy_parameter_registry())


def _calibration_patch_priority(scope: str, field: str) -> tuple[int, str]:
    return strategy_parameter_priority(scope, field, registry=_strategy_parameter_registry())


def _calibration_patch_value(field: str, current_value: Any, change_hint: str) -> Any:
    return strategy_parameter_proposed_value(
        field,
        current_value,
        change_hint,
        registry=_strategy_parameter_registry(),
    )


def _calibration_runtime_market_cache(market: str) -> Dict[str, Any]:
    runtime_paths = _runtime_config_paths_for_market(market)
    execution_payload = dict(_load_yaml_file(runtime_paths["investment_execution"]).get("execution") or {})
    paper_payload = dict(_load_yaml_file(runtime_paths["investment_paper"]).get("paper") or {})
    return {
        "runtime_paths": runtime_paths,
        "adaptive_strategy_cfg": load_adaptive_strategy(BASE_DIR, str(runtime_paths["adaptive_strategy"])),
        "execution_payload": execution_payload,
        "paper_payload": paper_payload,
    }


def _calibration_current_config_value(
    *,
    market: str,
    profile: str,
    scope: str,
    field: str,
    market_cache: Dict[str, Any],
) -> tuple[str, str, Any]:
    runtime_paths = dict(market_cache.get("runtime_paths") or {})
    scope_code = str(scope or "").strip().upper()
    field_name = str(field or "").strip()
    if scope_code == "ADAPTIVE_STRATEGY":
        cfg = market_cache.get("adaptive_strategy_cfg")
        profile_key = str(profile or market or "DEFAULT").strip().upper()
        profile_cfg = None
        if cfg is not None:
            profile_cfg = cfg.market_profiles.get(profile_key) or cfg.market_profiles.get(resolve_market_code(market)) or cfg.market_profiles.get("DEFAULT")
        current_value = getattr(profile_cfg, field_name, None) if profile_cfg is not None else None
        return str(runtime_paths.get("adaptive_strategy") or ""), f"market_profiles.{profile_key}.{field_name}", current_value
    if scope_code == "EXECUTION":
        payload = dict(market_cache.get("execution_payload") or {})
        current_value = payload.get(field_name, getattr(DEFAULT_EXECUTION_CFG, field_name, None))
        return str(runtime_paths.get("investment_execution") or ""), f"execution.{field_name}", current_value
    if scope_code == "PAPER":
        payload = dict(market_cache.get("paper_payload") or {})
        current_value = payload.get(field_name, getattr(DEFAULT_PAPER_CFG, field_name, None))
        return str(runtime_paths.get("investment_paper") or ""), f"paper.{field_name}", current_value
    return "", "", None


def _build_calibration_patch_item(
    *,
    portfolio_id: str,
    market: str,
    profile: str,
    scope: str,
    scope_label: str,
    config_scope: str,
    field: str,
    change_hint: str,
    change_hint_label: str,
    source_kind: str,
    source_signal: str,
    source_signal_label: str,
    source_note: str,
    current_summary: str,
    market_cache: Dict[str, Any],
) -> Dict[str, Any]:
    config_file, config_path, current_value = _calibration_current_config_value(
        market=market,
        profile=profile,
        scope=config_scope,
        field=field,
        market_cache=market_cache,
    )
    if current_value in (None, ""):
        return {}
    suggested_value = _calibration_patch_value(field, current_value, change_hint)
    try:
        delta_value = round(float(suggested_value) - float(current_value), 6)
    except Exception:
        delta_value = 0.0
    field_meta = _calibration_patch_field_meta(field)
    priority_rank, priority_label = _calibration_patch_priority(scope, field)
    suggestion_summary = (
        f"{market}/{profile} 建议调整 {field}: {current_value} -> {suggested_value} "
        f"（{scope_label} / {source_signal_label}）"
    )
    return {
        "portfolio_id": portfolio_id,
        "market": market,
        "adaptive_strategy_active_market_profile": profile,
        "scope": scope,
        "scope_label": scope_label,
        "config_scope": config_scope,
        "config_file": config_file,
        "config_path": config_path,
        "field": field,
        "field_label": str(field_meta.get("field_label") or field),
        "current_value": current_value,
        "suggested_value": suggested_value,
        "delta_value": delta_value,
        "change_hint": change_hint,
        "change_hint_label": change_hint_label,
        "priority_rank": int(priority_rank),
        "priority_label": priority_label,
        "source_kind": source_kind,
        "source_signal": source_signal,
        "source_signal_label": source_signal_label,
        "source_note": source_note,
        "current_summary": current_summary,
        "auto_apply": 0,
        "suggestion_summary": suggestion_summary,
    }


def _build_weekly_calibration_patch_suggestion_rows(
    strategy_context_rows: List[Dict[str, Any]],
    *,
    edge_calibration_rows: List[Dict[str, Any]],
    slicing_calibration_rows: List[Dict[str, Any]],
    risk_calibration_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    context_map = {
        str(row.get("portfolio_id") or "").strip(): dict(row)
        for row in list(strategy_context_rows or [])
        if str(row.get("portfolio_id") or "").strip()
    }
    market_cache: Dict[str, Dict[str, Any]] = {}
    out: List[Dict[str, Any]] = []

    def cached_market_payload(market_code: str) -> Dict[str, Any]:
        code = resolve_market_code(str(market_code or ""))
        if code not in market_cache:
            market_cache[code] = _calibration_runtime_market_cache(code)
        return market_cache[code]

    for raw in list(edge_calibration_rows or []):
        row = dict(raw)
        if str(row.get("edge_gate_quality") or "").strip().upper() != "GATE_TOO_TIGHT":
            continue
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        market = resolve_market_code(str(row.get("market") or ""))
        if not portfolio_id or not market:
            continue
        context = dict(context_map.get(portfolio_id) or {})
        profile = str(context.get("adaptive_strategy_active_market_profile") or market).strip().upper()
        payload = cached_market_payload(market)
        current_summary = str(context.get("adaptive_strategy_active_market_execution_summary") or "")
        note = str(row.get("edge_calibration_note") or "").strip()
        out.append(
            _build_calibration_patch_item(
                portfolio_id=portfolio_id, market=market, profile=profile,
                scope="EXECUTION_GATE", scope_label="执行门槛", config_scope="ADAPTIVE_STRATEGY",
                field="edge_cost_buffer_bps", change_hint="RELAX_LOWER", change_hint_label="按放松方向温和下调",
                source_kind="EDGE_CALIBRATION", source_signal="GATE_TOO_TIGHT", source_signal_label="edge gate 偏紧",
                source_note=note, current_summary=current_summary, market_cache=payload,
            )
        )
        out.append(
            _build_calibration_patch_item(
                portfolio_id=portfolio_id, market=market, profile=profile,
                scope="EXECUTION_GATE", scope_label="执行门槛", config_scope="ADAPTIVE_STRATEGY",
                field="min_expected_edge_bps", change_hint="RELAX_LOWER", change_hint_label="按放松方向温和下调",
                source_kind="EDGE_CALIBRATION", source_signal="GATE_TOO_TIGHT", source_signal_label="edge gate 偏紧",
                source_note=note, current_summary=current_summary, market_cache=payload,
            )
        )

    slicing_priority = {"NEED_MORE_SLICING": 0, "DELAY_HEAVY": 1, "POSSIBLY_TOO_CONSERVATIVE": 2}
    best_slicing_rows: Dict[str, Dict[str, Any]] = {}
    for raw in list(slicing_calibration_rows or []):
        row = dict(raw)
        assessment = str(row.get("slicing_assessment") or "").strip().upper()
        if assessment not in slicing_priority:
            continue
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        best = best_slicing_rows.get(portfolio_id)
        if best is None:
            best_slicing_rows[portfolio_id] = row
            continue
        current_key = (slicing_priority.get(assessment, 99), -int(row.get("sample_count", 0) or 0), -int(row.get("filled_sample_count", 0) or 0))
        best_key = (
            slicing_priority.get(str(best.get("slicing_assessment") or "").strip().upper(), 99),
            -int(best.get("sample_count", 0) or 0),
            -int(best.get("filled_sample_count", 0) or 0),
        )
        if current_key < best_key:
            best_slicing_rows[portfolio_id] = row

    for portfolio_id, row in sorted(best_slicing_rows.items()):
        market = resolve_market_code(str(row.get("market") or ""))
        if not market:
            continue
        context = dict(context_map.get(portfolio_id) or {})
        profile = str(context.get("adaptive_strategy_active_market_profile") or market).strip().upper()
        payload = cached_market_payload(market)
        assessment = str(row.get("slicing_assessment") or "").strip().upper()
        current_summary = str(context.get("adaptive_strategy_active_market_execution_summary") or "")
        note = str(row.get("slicing_calibration_note") or "").strip()
        bucket = str(row.get("dynamic_liquidity_bucket") or "").strip().upper() or "-"
        if assessment == "POSSIBLY_TOO_CONSERVATIVE":
            out.append(_build_calibration_patch_item(
                portfolio_id=portfolio_id, market=market, profile=profile,
                scope="SLICING_RELAX", scope_label="执行切片放宽", config_scope="EXECUTION",
                field="adv_split_trigger_pct", change_hint="INCREASE", change_hint_label="优先抬高 split trigger",
                source_kind="SLICING_CALIBRATION", source_signal=assessment, source_signal_label=f"{bucket} bucket 可能过保守",
                source_note=note, current_summary=current_summary, market_cache=payload,
            ))
            out.append(_build_calibration_patch_item(
                portfolio_id=portfolio_id, market=market, profile=profile,
                scope="SLICING_RELAX", scope_label="执行切片放宽", config_scope="EXECUTION",
                field="adv_max_participation_pct", change_hint="INCREASE", change_hint_label="温和抬高 ADV 上限",
                source_kind="SLICING_CALIBRATION", source_signal=assessment, source_signal_label=f"{bucket} bucket 可能过保守",
                source_note=note, current_summary=current_summary, market_cache=payload,
            ))
        elif assessment == "DELAY_HEAVY":
            out.append(_build_calibration_patch_item(
                portfolio_id=portfolio_id, market=market, profile=profile,
                scope="SLICING_RELAX", scope_label="执行切片放宽", config_scope="EXECUTION",
                field="limit_price_buffer_bps", change_hint="INCREASE", change_hint_label="温和抬高 limit buffer",
                source_kind="SLICING_CALIBRATION", source_signal=assessment, source_signal_label=f"{bucket} bucket 成交偏慢",
                source_note=note, current_summary=current_summary, market_cache=payload,
            ))
            out.append(_build_calibration_patch_item(
                portfolio_id=portfolio_id, market=market, profile=profile,
                scope="SLICING_RELAX", scope_label="执行切片放宽", config_scope="EXECUTION",
                field="adv_split_trigger_pct", change_hint="INCREASE", change_hint_label="优先抬高 split trigger",
                source_kind="SLICING_CALIBRATION", source_signal=assessment, source_signal_label=f"{bucket} bucket 成交偏慢",
                source_note=note, current_summary=current_summary, market_cache=payload,
            ))
        elif assessment == "NEED_MORE_SLICING":
            out.append(_build_calibration_patch_item(
                portfolio_id=portfolio_id, market=market, profile=profile,
                scope="SLICING_TIGHTEN", scope_label="执行切片收紧", config_scope="EXECUTION",
                field="adv_split_trigger_pct", change_hint="REDUCE", change_hint_label="优先降低 split trigger",
                source_kind="SLICING_CALIBRATION", source_signal=assessment, source_signal_label=f"{bucket} bucket 需要更细拆单",
                source_note=note, current_summary=current_summary, market_cache=payload,
            ))
            out.append(_build_calibration_patch_item(
                portfolio_id=portfolio_id, market=market, profile=profile,
                scope="SLICING_TIGHTEN", scope_label="执行切片收紧", config_scope="EXECUTION",
                field="adv_max_participation_pct", change_hint="REDUCE", change_hint_label="温和降低 ADV 上限",
                source_kind="SLICING_CALIBRATION", source_signal=assessment, source_signal_label=f"{bucket} bucket 需要更细拆单",
                source_note=note, current_summary=current_summary, market_cache=payload,
            ))
            out.append(_build_calibration_patch_item(
                portfolio_id=portfolio_id, market=market, profile=profile,
                scope="SLICING_TIGHTEN", scope_label="执行切片收紧", config_scope="EXECUTION",
                field="max_slices_per_symbol", change_hint="INCREASE", change_hint_label="必要时提高 slices 上限",
                source_kind="SLICING_CALIBRATION", source_signal=assessment, source_signal_label=f"{bucket} bucket 需要更细拆单",
                source_note=note, current_summary=current_summary, market_cache=payload,
            ))

    throttle_field_map = {
        "CORRELATION": ("correlation_soft_limit", "INCREASE", "按放松方向温和上调"),
        "STRESS": ("stress_loss_soft_limit", "INCREASE", "按放松方向温和上调"),
        "RETURNS_VAR": ("stress_loss_soft_limit", "INCREASE", "按放松方向温和上调"),
        "ATR": ("portfolio_atr_soft_limit", "INCREASE", "按放松方向温和上调"),
        "LIQUIDITY": ("portfolio_liquidity_soft_floor", "REDUCE", "按放松方向温和下调"),
        "CONCENTRATION": ("sector_concentration_soft_limit", "INCREASE", "按放松方向温和上调"),
        "SENTIMENT": ("market_sentiment_soft_floor", "REDUCE", "按放松方向温和下调"),
    }
    for raw in list(risk_calibration_rows or []):
        row = dict(raw)
        target = str(row.get("risk_calibration_target") or "").strip().upper()
        if target not in {"BUDGET_TOO_TIGHT", "THROTTLE_TOO_TIGHT", "RECOVERY_HELPING"}:
            continue
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        market = resolve_market_code(str(row.get("market") or ""))
        if not portfolio_id or not market:
            continue
        context = dict(context_map.get(portfolio_id) or {})
        profile = str(context.get("adaptive_strategy_active_market_profile") or market).strip().upper()
        payload = cached_market_payload(market)
        note = str(row.get("risk_calibration_note") or "").strip()
        if target == "BUDGET_TOO_TIGHT":
            out.append(_build_calibration_patch_item(
                portfolio_id=portfolio_id, market=market, profile=profile,
                scope="RISK_BUDGET", scope_label="风险预算", config_scope="ADAPTIVE_STRATEGY",
                field="risk_budget_net_exposure", change_hint="INCREASE", change_hint_label="优先抬高 net budget",
                source_kind="RISK_CALIBRATION", source_signal=target, source_signal_label="budget 层偏紧",
                source_note=note, current_summary=str(context.get("adaptive_strategy_market_profile_note") or ""), market_cache=payload,
            ))
            out.append(_build_calibration_patch_item(
                portfolio_id=portfolio_id, market=market, profile=profile,
                scope="RISK_BUDGET", scope_label="风险预算", config_scope="ADAPTIVE_STRATEGY",
                field="risk_budget_gross_exposure", change_hint="INCREASE", change_hint_label="再抬高 gross budget",
                source_kind="RISK_CALIBRATION", source_signal=target, source_signal_label="budget 层偏紧",
                source_note=note, current_summary=str(context.get("adaptive_strategy_market_profile_note") or ""), market_cache=payload,
            ))
        elif target == "THROTTLE_TOO_TIGHT":
            dominant_layer = str(row.get("latest_dominant_throttle_layer") or "").strip().upper()
            field_name, change_hint, change_hint_label = throttle_field_map.get(
                dominant_layer, ("correlation_soft_limit", "INCREASE", "按放松方向温和上调")
            )
            out.append(_build_calibration_patch_item(
                portfolio_id=portfolio_id, market=market, profile=profile,
                scope="RISK_THROTTLE", scope_label="风险 throttle", config_scope="PAPER",
                field=field_name, change_hint=change_hint, change_hint_label=change_hint_label,
                source_kind="RISK_CALIBRATION", source_signal=target,
                source_signal_label=f"{str(row.get('latest_dominant_throttle_layer_label') or dominant_layer or '-') } 层偏紧",
                source_note=note, current_summary=str(row.get("latest_dominant_throttle_layer_label") or dominant_layer or ""), market_cache=payload,
            ))
        elif target == "RECOVERY_HELPING":
            out.append(_build_calibration_patch_item(
                portfolio_id=portfolio_id, market=market, profile=profile,
                scope="RISK_RECOVERY", scope_label="风险恢复", config_scope="ADAPTIVE_STRATEGY",
                field="risk_recovery_max_bonus", change_hint="INCREASE", change_hint_label="温和抬高 recovery bonus",
                source_kind="RISK_CALIBRATION", source_signal=target, source_signal_label="recovery 正在改善收益恢复",
                source_note=note, current_summary=str(context.get("adaptive_strategy_market_profile_note") or ""), market_cache=payload,
            ))

    out = [row for row in out if row]
    out.sort(
        key=lambda row: (
            int(row.get("priority_rank", 99) or 99),
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
            str(row.get("scope") or ""),
            str(row.get("field") or ""),
        )
    )
    return out




def _candidate_snapshot_stage_priority(stage: str) -> int:
    normalized = str(stage or "").strip().lower()
    if normalized in {"final", "short"}:
        return 3
    if normalized == "deep":
        return 2
    if normalized == "broad":
        return 1
    return 0


def _is_selected_snapshot_stage(stage: str) -> bool:
    return str(stage or "").strip().lower() in {"final", "short"}


def _preferred_snapshot_stages_for_order(row: Dict[str, Any]) -> List[str]:
    target_weight = _safe_float(row.get("target_weight"), 0.0)
    target_qty = _safe_float(row.get("target_qty"), 0.0)
    if target_weight < 0.0 or target_qty < 0.0:
        return ["short", "final", "deep", "broad"]
    return ["final", "deep", "broad", "short"]

from .review_weekly_execution_support import (
    _SESSION_LABELS,
    _apply_execution_broker_summary_context,
    _build_execution_analysis_bundle,
    _build_execution_effect_rows,
    _build_execution_feedback_rows,
    _build_execution_gate_rows,
    _build_execution_hotspot_penalties,
    _build_execution_hotspot_rows,
    _build_execution_parent_rows,
    _build_execution_session_rows,
    _build_planned_execution_cost_rows,
    _build_weekly_blocked_edge_attribution_rows,
    _build_weekly_edge_realization_rows,
    _build_weekly_outcome_spread_rows,
    _filter_execution_metric_rows,
    _is_execution_gate_status,
    _link_execution_orders_to_candidate_snapshots,
    _market_from_portfolio_or_symbol,
)
from .review_weekly_governance_support import (
    _build_weekly_control_timeseries_rows,
    _build_weekly_patch_governance_summary_rows,
    _build_weekly_tuning_history_overview,
    _patch_review_kind_label,
    _patch_review_week_start_dt,
    _weekly_tuning_history_trend_label,
)
from .review_weekly_decision_support import (
    _apply_market_profile_tuning_context,
    _build_blocked_vs_allowed_expost_rows,
    _build_candidate_model_review_rows,
    _build_market_profile_patch_readiness,
    _build_trading_quality_evidence_rows,
    _build_unified_evidence_rows,
    _build_weekly_decision_evidence_history_overview,
    _build_weekly_decision_evidence_rows,
    _build_weekly_decision_evidence_summary_rows,
    _build_weekly_edge_calibration_rows,
    _build_weekly_portfolio_summary_rows,
    _build_weekly_risk_calibration_rows,
    _build_weekly_slicing_calibration_rows,
    _build_weekly_tuning_dataset_rows,
    _build_weekly_tuning_dataset_summary,
    _decision_summary_by_week,
    _latest_risk_overlay,
    _market_profile_patch_conflict,
    _persist_trading_quality_evidence,
    _risk_driver_and_diagnosis,
    _risk_overlay_from_history_row,
)
from .review_weekly_strategy_support import (
    _active_market_profile_note,
    _active_market_strategy_field,
    _augment_summary_rows_with_strategy_context,
    _build_attribution_rows,
    _build_market_profile_tuning_summary,
    _build_risk_review_rows,
    _execution_gate_summary,
    _strategy_effective_controls_note,
    _weekly_strategy_note,
)
