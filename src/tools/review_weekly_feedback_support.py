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


def _build_execution_hotspot_penalties(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        grouped.setdefault(symbol, []).append(dict(row))

    out: List[Dict[str, Any]] = []
    for symbol, symbol_rows in grouped.items():
        symbol_rows.sort(
            key=lambda item: (
                -float(item.get("pressure_score", 0.0) or 0.0),
                -float(item.get("avg_actual_slippage_bps", 0.0) or 0.0),
                str(item.get("session_label") or ""),
            )
        )
        hotspot_count = int(len(symbol_rows))
        investigate_count = int(
            sum(1 for row in symbol_rows if str(row.get("hotspot_action") or "").upper() == "INVESTIGATE_EXECUTION")
        )
        avg_pressure = _avg_defined([row.get("pressure_score") for row in symbol_rows]) or 0.0
        avg_expected_bps = _avg_defined([row.get("avg_expected_cost_bps") for row in symbol_rows]) or 0.0
        avg_actual_bps = _avg_defined([row.get("avg_actual_slippage_bps") for row in symbol_rows]) or 0.0
        avg_bps_deviation = max(0.0, avg_actual_bps - avg_expected_bps)
        session_labels = sorted(
            {
                str(row.get("session_label") or "").strip()
                for row in symbol_rows
                if str(row.get("session_label") or "").strip()
            }
        )
        if investigate_count <= 0 and hotspot_count < 2 and avg_bps_deviation < 2.0:
            continue

        expected_cost_bps_add = _clamp(2.0 + avg_bps_deviation * 0.60 + max(0, hotspot_count - 1) * 1.50, 2.0, 18.0)
        slippage_proxy_bps_add = _clamp(1.5 + avg_bps_deviation * 0.80 + investigate_count * 1.50, 1.5, 20.0)
        execution_penalty = _clamp(0.02 + avg_pressure * 0.08 + investigate_count * 0.01, 0.02, 0.12)
        score_penalty = _clamp(execution_penalty * 0.35, 0.0, 0.05)
        out.append(
            {
                "symbol": symbol,
                "hotspot_count": hotspot_count,
                "investigate_count": investigate_count,
                "session_count": int(len(session_labels)),
                "session_labels": ",".join(session_labels[:6]),
                "avg_pressure": round(float(avg_pressure), 6),
                "avg_expected_cost_bps": round(float(avg_expected_bps), 6),
                "avg_actual_slippage_bps": round(float(avg_actual_bps), 6),
                "avg_bps_deviation": round(float(avg_bps_deviation), 6),
                "score_penalty": round(float(score_penalty), 6),
                "execution_penalty": round(float(execution_penalty), 6),
                "expected_cost_bps_add": round(float(expected_cost_bps_add), 6),
                "slippage_proxy_bps_add": round(float(slippage_proxy_bps_add), 6),
                "reason": "repeat_execution_hotspot",
            }
        )
    out.sort(
        key=lambda row: (
            -int(row.get("investigate_count", 0) or 0),
            -float(row.get("execution_penalty", 0.0) or 0.0),
            -float(row.get("expected_cost_bps_add", 0.0) or 0.0),
            str(row.get("symbol") or ""),
        )
    )
    return out


def _market_from_portfolio_or_symbol(portfolio_id: str, symbol: str = "") -> str:
    text = str(portfolio_id or "").strip().upper()
    if ":" in text:
        return resolve_market_code(text.split(":", 1)[0])
    symbol_text = str(symbol or "").strip().upper()
    if symbol_text.endswith(".HK"):
        return "HK"
    if symbol_text.endswith(".AX"):
        return "ASX"
    if symbol_text.endswith(".DE"):
        return "XETRA"
    if symbol_text.endswith(".SS") or symbol_text.endswith(".SZ"):
        return "CN"
    return "US" if symbol_text else ""


_SESSION_LABELS = {
    "OPEN": "开盘",
    "MIDDAY": "午盘",
    "CLOSE": "尾盘",
    "UNKNOWN": "未知时段",
}


def _execution_session_profile_from_order(row: Dict[str, Any]) -> Dict[str, str]:
    details = _parse_json_dict(row.get("details"))
    plan_row = dict(details.get("plan_row") or {}) if isinstance(details.get("plan_row"), dict) else {}
    session_bucket = str(
        details.get("session_bucket")
        or plan_row.get("session_bucket")
        or row.get("session_bucket")
        or ""
    ).strip().upper()
    if session_bucket not in {"OPEN", "MIDDAY", "CLOSE"}:
        session_bucket = "UNKNOWN"
    session_label = str(
        details.get("session_label")
        or plan_row.get("session_label")
        or _SESSION_LABELS.get(session_bucket, "未知时段")
    ).strip() or _SESSION_LABELS.get(session_bucket, "未知时段")
    execution_style = str(
        details.get("execution_style")
        or plan_row.get("execution_style")
        or row.get("execution_style")
        or ""
    ).strip()
    return {
        "session_bucket": session_bucket,
        "session_label": session_label,
        "execution_style": execution_style,
    }


def _planned_cost_metrics_from_order(row: Dict[str, Any]) -> Dict[str, Any]:
    details = _parse_json_dict(row.get("details"))
    plan_row = dict(details.get("plan_row") or {}) if isinstance(details.get("plan_row"), dict) else {}

    def _pick(key: str, default: Any = 0.0) -> Any:
        direct = details.get(key)
        if direct not in (None, ""):
            return direct
        nested = plan_row.get(key)
        if nested not in (None, ""):
            return nested
        raw = row.get(key)
        if raw not in (None, ""):
            return raw
        return default

    order_value = abs(float(_pick("order_value", row.get("order_value") or 0.0) or 0.0))
    spread_bps = float(_pick("spread_proxy_bps", 0.0) or 0.0)
    slippage_bps = float(_pick("slippage_proxy_bps", 0.0) or 0.0)
    commission_bps = float(_pick("commission_proxy_bps", 0.0) or 0.0)
    expected_cost_bps = float(_pick("expected_cost_bps", spread_bps + slippage_bps + commission_bps) or 0.0)
    expected_spread_cost = details.get("expected_spread_cost")
    expected_slippage_cost = details.get("expected_slippage_cost")
    expected_commission_cost = details.get("expected_commission_cost")
    expected_cost_value = details.get("expected_cost_value")
    return {
        "order_value": float(order_value),
        "expected_cost_bps": float(expected_cost_bps),
        "expected_spread_cost": float(
            expected_spread_cost if expected_spread_cost not in (None, "") else order_value * spread_bps / 10000.0
        ),
        "expected_slippage_cost": float(
            expected_slippage_cost if expected_slippage_cost not in (None, "") else order_value * slippage_bps / 10000.0
        ),
        "expected_commission_cost": float(
            expected_commission_cost if expected_commission_cost not in (None, "") else order_value * commission_bps / 10000.0
        ),
        "expected_cost_value": float(
            expected_cost_value if expected_cost_value not in (None, "") else order_value * expected_cost_bps / 10000.0
        ),
        "execution_style": str(_pick("execution_style", "") or ""),
    }


def _build_planned_execution_cost_rows(execution_orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in execution_orders:
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        status = str(row.get("status") or "").strip().upper()
        if status.startswith("BLOCKED"):
            continue
        submitted_like = int(row.get("broker_order_id") or 0) > 0 or status in {
            "CREATED",
            "SUBMITTED",
            "PRESUBMITTED",
            "FILLED",
            "PARTIAL",
            "PARTIALLY_FILLED",
        } or status.startswith("ERROR_")
        metrics = _planned_cost_metrics_from_order(row)
        bucket = grouped.setdefault(
            portfolio_id,
            {
                "portfolio_id": portfolio_id,
                "market": str(row.get("market") or ""),
                "all_order_rows": 0,
                "submitted_order_rows": 0,
                "all_order_value": 0.0,
                "submitted_order_value": 0.0,
                "all_expected_spread_cost": 0.0,
                "all_expected_slippage_cost": 0.0,
                "all_expected_commission_cost": 0.0,
                "all_expected_cost_total": 0.0,
                "all_expected_cost_bps_numerator": 0.0,
                "submitted_expected_spread_cost": 0.0,
                "submitted_expected_slippage_cost": 0.0,
                "submitted_expected_commission_cost": 0.0,
                "submitted_expected_cost_total": 0.0,
                "submitted_expected_cost_bps_numerator": 0.0,
                "_all_style_counts": {},
                "_submitted_style_counts": {},
            },
        )
        bucket["all_order_rows"] = int(bucket["all_order_rows"]) + 1
        bucket["all_order_value"] = float(bucket["all_order_value"]) + float(metrics["order_value"])
        bucket["all_expected_spread_cost"] = float(bucket["all_expected_spread_cost"]) + float(metrics["expected_spread_cost"])
        bucket["all_expected_slippage_cost"] = float(bucket["all_expected_slippage_cost"]) + float(metrics["expected_slippage_cost"])
        bucket["all_expected_commission_cost"] = float(bucket["all_expected_commission_cost"]) + float(metrics["expected_commission_cost"])
        bucket["all_expected_cost_total"] = float(bucket["all_expected_cost_total"]) + float(metrics["expected_cost_value"])
        bucket["all_expected_cost_bps_numerator"] = float(bucket["all_expected_cost_bps_numerator"]) + float(metrics["expected_cost_bps"]) * float(metrics["order_value"])
        style = str(metrics.get("execution_style") or "")
        if style:
            style_counts = dict(bucket.get("_all_style_counts") or {})
            style_counts[style] = int(style_counts.get(style, 0)) + 1
            bucket["_all_style_counts"] = style_counts
        if submitted_like:
            bucket["submitted_order_rows"] = int(bucket["submitted_order_rows"]) + 1
            bucket["submitted_order_value"] = float(bucket["submitted_order_value"]) + float(metrics["order_value"])
            bucket["submitted_expected_spread_cost"] = float(bucket["submitted_expected_spread_cost"]) + float(metrics["expected_spread_cost"])
            bucket["submitted_expected_slippage_cost"] = float(bucket["submitted_expected_slippage_cost"]) + float(metrics["expected_slippage_cost"])
            bucket["submitted_expected_commission_cost"] = float(bucket["submitted_expected_commission_cost"]) + float(metrics["expected_commission_cost"])
            bucket["submitted_expected_cost_total"] = float(bucket["submitted_expected_cost_total"]) + float(metrics["expected_cost_value"])
            bucket["submitted_expected_cost_bps_numerator"] = float(bucket["submitted_expected_cost_bps_numerator"]) + float(metrics["expected_cost_bps"]) * float(metrics["order_value"])
            if style:
                submitted_style_counts = dict(bucket.get("_submitted_style_counts") or {})
                submitted_style_counts[style] = int(submitted_style_counts.get(style, 0)) + 1
                bucket["_submitted_style_counts"] = submitted_style_counts

    out: List[Dict[str, Any]] = []
    for portfolio_id, bucket in grouped.items():
        submitted_rows = int(bucket.get("submitted_order_rows", 0) or 0)
        submitted_value = float(bucket.get("submitted_order_value", 0.0) or 0.0)
        use_submitted = submitted_rows > 0 and submitted_value > 0.0
        basis_prefix = "submitted" if use_submitted else "all"
        basis_label = "submitted_orders" if use_submitted else "planned_orders"
        value = float(bucket.get(f"{basis_prefix}_order_value", 0.0) or 0.0)
        numerator = float(bucket.get(f"{basis_prefix}_expected_cost_bps_numerator", 0.0) or 0.0)
        style_counts = dict(bucket.get("_submitted_style_counts") or {}) if use_submitted else dict(bucket.get("_all_style_counts") or {})
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(bucket.get("market") or ""),
                "planned_cost_basis": basis_label,
                "planned_order_rows": int(bucket.get(f"{basis_prefix}_order_rows", 0) or 0),
                "planned_order_value": float(value),
                "planned_spread_cost_total": float(bucket.get(f"{basis_prefix}_expected_spread_cost", 0.0) or 0.0),
                "planned_slippage_cost_total": float(bucket.get(f"{basis_prefix}_expected_slippage_cost", 0.0) or 0.0),
                "planned_commission_cost_total": float(bucket.get(f"{basis_prefix}_expected_commission_cost", 0.0) or 0.0),
                "planned_execution_cost_total": float(bucket.get(f"{basis_prefix}_expected_cost_total", 0.0) or 0.0),
                "avg_expected_cost_bps": float(numerator / value) if value > 0.0 else None,
                "execution_style_breakdown": ",".join(f"{name}:{style_counts[name]}" for name in sorted(style_counts)),
            }
        )
    out.sort(key=lambda row: str(row.get("portfolio_id") or ""))
    return out


def _is_execution_gate_status(status: str) -> bool:
    normalized = str(status or "").strip().upper()
    return normalized.startswith("BLOCKED") or normalized in {"DEFERRED_RISK_ALERT", "REVIEW_REQUIRED"}


def _build_execution_gate_rows(execution_orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in execution_orders:
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        order_value = abs(float(row.get("order_value") or 0.0))
        bucket = grouped.setdefault(
            portfolio_id,
            {
                "portfolio_id": portfolio_id,
                "market": str(row.get("market") or _market_from_portfolio_or_symbol(portfolio_id, str(row.get("symbol") or ""))),
                "execution_order_count": 0,
                "execution_order_value": 0.0,
                "blocked_order_count": 0,
                "blocked_order_value": 0.0,
            },
        )
        bucket["execution_order_count"] = int(bucket["execution_order_count"]) + 1
        bucket["execution_order_value"] = float(bucket["execution_order_value"]) + float(order_value)
        if not _is_execution_gate_status(str(row.get("status") or "")):
            continue
        bucket["blocked_order_count"] = int(bucket["blocked_order_count"]) + 1
        bucket["blocked_order_value"] = float(bucket["blocked_order_value"]) + float(order_value)

    out: List[Dict[str, Any]] = []
    for portfolio_id, bucket in grouped.items():
        total_count = int(bucket.get("execution_order_count", 0) or 0)
        total_value = float(bucket.get("execution_order_value", 0.0) or 0.0)
        blocked_count = int(bucket.get("blocked_order_count", 0) or 0)
        blocked_value = float(bucket.get("blocked_order_value", 0.0) or 0.0)
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(bucket.get("market") or ""),
                "execution_order_count": total_count,
                "execution_order_value": total_value,
                "blocked_order_count": blocked_count,
                "blocked_order_value": blocked_value,
                "blocked_order_ratio": float(blocked_count / total_count) if total_count > 0 else 0.0,
                "blocked_order_value_ratio": float(blocked_value / total_value) if total_value > 0.0 else 0.0,
            }
        )
    out.sort(key=lambda row: str(row.get("portfolio_id") or ""))
    return out


def _build_execution_effect_rows(
    fill_rows: List[Dict[str, Any]],
    commission_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    commission_by_exec: Dict[str, float] = {}
    for row in commission_rows:
        exec_id = str(row.get("exec_id") or "").strip()
        if not exec_id:
            continue
        commission_by_exec[exec_id] = float(commission_by_exec.get(exec_id, 0.0)) + float(row.get("value") or 0.0)

    grouped: Dict[str, Dict[str, Any]] = {}
    for row in fill_rows:
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        market = _market_from_portfolio_or_symbol(portfolio_id, str(row.get("symbol") or ""))
        bucket = grouped.setdefault(
            portfolio_id,
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "fill_count": 0,
                "fill_notional": 0.0,
                "commission_total": 0.0,
                "slippage_cost_total": 0.0,
                "_slippage_samples": [],
            },
        )
        fill_notional = abs(float(row.get("qty") or 0.0)) * abs(float(row.get("price") or 0.0))
        actual_slippage_bps = row.get("actual_slippage_bps")
        bucket["fill_count"] = int(bucket["fill_count"]) + 1
        bucket["fill_notional"] = float(bucket["fill_notional"]) + fill_notional
        commission = float(commission_by_exec.get(str(row.get("exec_id") or "").strip(), 0.0))
        bucket["commission_total"] = float(bucket["commission_total"]) + commission
        if actual_slippage_bps not in (None, ""):
            slip = float(actual_slippage_bps or 0.0)
            bucket["_slippage_samples"].append(slip)
            bucket["slippage_cost_total"] = float(bucket["slippage_cost_total"]) + fill_notional * slip / 10000.0

    out: List[Dict[str, Any]] = []
    for portfolio_id, bucket in grouped.items():
        slippage_samples = [float(v) for v in list(bucket.pop("_slippage_samples", []) or [])]
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(bucket.get("market") or ""),
                "fill_count": int(bucket.get("fill_count", 0) or 0),
                "fill_notional": float(bucket.get("fill_notional", 0.0) or 0.0),
                "commission_total": float(bucket.get("commission_total", 0.0) or 0.0),
                "slippage_cost_total": float(bucket.get("slippage_cost_total", 0.0) or 0.0),
                "execution_cost_total": float(bucket.get("commission_total", 0.0) or 0.0) + float(bucket.get("slippage_cost_total", 0.0) or 0.0),
                "avg_actual_slippage_bps": _avg_defined(slippage_samples),
            }
        )
    out.sort(key=lambda row: str(row.get("portfolio_id") or ""))
    return out


def _build_execution_session_rows(
    execution_orders: List[Dict[str, Any]],
    fill_rows: List[Dict[str, Any]],
    commission_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    commission_by_exec: Dict[str, float] = {}
    for row in commission_rows:
        exec_id = str(row.get("exec_id") or "").strip()
        if not exec_id:
            continue
        commission_by_exec[exec_id] = float(commission_by_exec.get(exec_id, 0.0)) + float(row.get("value") or 0.0)

    order_meta_by_broker: Dict[int, Dict[str, Any]] = {}
    grouped: Dict[tuple[str, str], Dict[str, Any]] = {}

    def _bucket(portfolio_id: str, market: str, session_bucket: str, session_label: str) -> Dict[str, Any]:
        return grouped.setdefault(
            (portfolio_id, session_bucket),
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "session_bucket": session_bucket,
                "session_label": session_label,
                "all_order_rows": 0,
                "submitted_order_rows": 0,
                "all_order_value": 0.0,
                "submitted_order_value": 0.0,
                "all_expected_spread_cost": 0.0,
                "all_expected_slippage_cost": 0.0,
                "all_expected_commission_cost": 0.0,
                "all_expected_cost_total": 0.0,
                "all_expected_cost_bps_numerator": 0.0,
                "submitted_expected_spread_cost": 0.0,
                "submitted_expected_slippage_cost": 0.0,
                "submitted_expected_commission_cost": 0.0,
                "submitted_expected_cost_total": 0.0,
                "submitted_expected_cost_bps_numerator": 0.0,
                "fill_count": 0,
                "fill_notional": 0.0,
                "commission_total": 0.0,
                "slippage_cost_total": 0.0,
                "_slippage_samples": [],
                "_slippage_dev_samples": [],
                "_style_counts_all": {},
                "_style_counts_submitted": {},
            },
        )

    for row in execution_orders:
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        broker_order_id = int(row.get("broker_order_id") or 0)
        if broker_order_id > 0:
            order_meta_by_broker[broker_order_id] = dict(row)
        status = str(row.get("status") or "").strip().upper()
        if status.startswith("BLOCKED"):
            continue
        session = _execution_session_profile_from_order(row)
        metrics = _planned_cost_metrics_from_order(row)
        bucket = _bucket(
            portfolio_id,
            str(row.get("market") or _market_from_portfolio_or_symbol(portfolio_id, str(row.get("symbol") or ""))),
            session["session_bucket"],
            session["session_label"],
        )
        submitted_like = (
            broker_order_id > 0
            or status in {"CREATED", "SUBMITTED", "PRESUBMITTED", "FILLED", "PARTIAL", "PARTIALLY_FILLED"}
            or status.startswith("ERROR_")
        )
        bucket["all_order_rows"] = int(bucket["all_order_rows"]) + 1
        bucket["all_order_value"] = float(bucket["all_order_value"]) + float(metrics["order_value"])
        bucket["all_expected_spread_cost"] = float(bucket["all_expected_spread_cost"]) + float(metrics["expected_spread_cost"])
        bucket["all_expected_slippage_cost"] = float(bucket["all_expected_slippage_cost"]) + float(metrics["expected_slippage_cost"])
        bucket["all_expected_commission_cost"] = float(bucket["all_expected_commission_cost"]) + float(metrics["expected_commission_cost"])
        bucket["all_expected_cost_total"] = float(bucket["all_expected_cost_total"]) + float(metrics["expected_cost_value"])
        bucket["all_expected_cost_bps_numerator"] = float(bucket["all_expected_cost_bps_numerator"]) + float(metrics["expected_cost_bps"]) * float(metrics["order_value"])
        style = str(session.get("execution_style") or metrics.get("execution_style") or "").strip()
        if style:
            style_counts_all = dict(bucket.get("_style_counts_all") or {})
            style_counts_all[style] = int(style_counts_all.get(style, 0)) + 1
            bucket["_style_counts_all"] = style_counts_all
        if submitted_like:
            bucket["submitted_order_rows"] = int(bucket["submitted_order_rows"]) + 1
            bucket["submitted_order_value"] = float(bucket["submitted_order_value"]) + float(metrics["order_value"])
            bucket["submitted_expected_spread_cost"] = float(bucket["submitted_expected_spread_cost"]) + float(metrics["expected_spread_cost"])
            bucket["submitted_expected_slippage_cost"] = float(bucket["submitted_expected_slippage_cost"]) + float(metrics["expected_slippage_cost"])
            bucket["submitted_expected_commission_cost"] = float(bucket["submitted_expected_commission_cost"]) + float(metrics["expected_commission_cost"])
            bucket["submitted_expected_cost_total"] = float(bucket["submitted_expected_cost_total"]) + float(metrics["expected_cost_value"])
            bucket["submitted_expected_cost_bps_numerator"] = float(bucket["submitted_expected_cost_bps_numerator"]) + float(metrics["expected_cost_bps"]) * float(metrics["order_value"])
            if style:
                style_counts_submitted = dict(bucket.get("_style_counts_submitted") or {})
                style_counts_submitted[style] = int(style_counts_submitted.get(style, 0)) + 1
                bucket["_style_counts_submitted"] = style_counts_submitted

    for row in fill_rows:
        broker_order_id = int(row.get("order_id") or 0)
        order_meta = dict(order_meta_by_broker.get(broker_order_id) or {})
        portfolio_id = str(order_meta.get("portfolio_id") or row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        session = _execution_session_profile_from_order(order_meta)
        market = str(order_meta.get("market") or _market_from_portfolio_or_symbol(portfolio_id, str(row.get("symbol") or "")))
        bucket = _bucket(portfolio_id, market, session["session_bucket"], session["session_label"])
        fill_notional = abs(float(row.get("qty") or 0.0)) * abs(float(row.get("price") or 0.0))
        actual_slippage_bps = row.get("actual_slippage_bps")
        slippage_dev_bps = row.get("slippage_bps_deviation")
        commission = float(commission_by_exec.get(str(row.get("exec_id") or "").strip(), 0.0))
        bucket["fill_count"] = int(bucket["fill_count"]) + 1
        bucket["fill_notional"] = float(bucket["fill_notional"]) + float(fill_notional)
        bucket["commission_total"] = float(bucket["commission_total"]) + float(commission)
        if actual_slippage_bps not in (None, ""):
            slip = float(actual_slippage_bps or 0.0)
            bucket["_slippage_samples"].append(slip)
            bucket["slippage_cost_total"] = float(bucket["slippage_cost_total"]) + float(fill_notional) * slip / 10000.0
        if slippage_dev_bps not in (None, ""):
            bucket["_slippage_dev_samples"].append(float(slippage_dev_bps or 0.0))

    session_sort_order = {"OPEN": 0, "MIDDAY": 1, "CLOSE": 2, "UNKNOWN": 3}
    out: List[Dict[str, Any]] = []
    for (_, session_bucket), bucket in grouped.items():
        submitted_rows = int(bucket.get("submitted_order_rows", 0) or 0)
        submitted_value = float(bucket.get("submitted_order_value", 0.0) or 0.0)
        use_submitted = submitted_rows > 0 and submitted_value > 0.0
        basis_prefix = "submitted" if use_submitted else "all"
        basis_label = "submitted_orders" if use_submitted else "planned_orders"
        value = float(bucket.get(f"{basis_prefix}_order_value", 0.0) or 0.0)
        numerator = float(bucket.get(f"{basis_prefix}_expected_cost_bps_numerator", 0.0) or 0.0)
        style_counts = dict(bucket.get("_style_counts_submitted") or {}) if use_submitted else dict(bucket.get("_style_counts_all") or {})
        slippage_samples = [float(v) for v in list(bucket.pop("_slippage_samples", []) or [])]
        slippage_dev_samples = [float(v) for v in list(bucket.pop("_slippage_dev_samples", []) or [])]
        planned_execution_cost_total = float(bucket.get(f"{basis_prefix}_expected_cost_total", 0.0) or 0.0)
        execution_cost_total = float(bucket.get("commission_total", 0.0) or 0.0) + float(bucket.get("slippage_cost_total", 0.0) or 0.0)
        out.append(
            {
                "portfolio_id": str(bucket.get("portfolio_id") or ""),
                "market": str(bucket.get("market") or ""),
                "session_bucket": str(session_bucket),
                "session_label": str(bucket.get("session_label") or _SESSION_LABELS.get(session_bucket, "未知时段")),
                "planned_cost_basis": basis_label,
                "planned_order_rows": int(bucket.get(f"{basis_prefix}_order_rows", 0) or 0),
                "submitted_order_rows": int(submitted_rows),
                "planned_order_value": float(value),
                "planned_spread_cost_total": float(bucket.get(f"{basis_prefix}_expected_spread_cost", 0.0) or 0.0),
                "planned_slippage_cost_total": float(bucket.get(f"{basis_prefix}_expected_slippage_cost", 0.0) or 0.0),
                "planned_commission_cost_total": float(bucket.get(f"{basis_prefix}_expected_commission_cost", 0.0) or 0.0),
                "planned_execution_cost_total": float(planned_execution_cost_total),
                "avg_expected_cost_bps": float(numerator / value) if value > 0.0 else None,
                "fill_count": int(bucket.get("fill_count", 0) or 0),
                "fill_notional": float(bucket.get("fill_notional", 0.0) or 0.0),
                "commission_total": float(bucket.get("commission_total", 0.0) or 0.0),
                "slippage_cost_total": float(bucket.get("slippage_cost_total", 0.0) or 0.0),
                "execution_cost_total": float(execution_cost_total),
                "execution_cost_gap": float(execution_cost_total - planned_execution_cost_total),
                "avg_actual_slippage_bps": _avg_defined(slippage_samples),
                "avg_slippage_bps_deviation": _avg_defined(slippage_dev_samples),
                "execution_style_breakdown": ",".join(f"{name}:{style_counts[name]}" for name in sorted(style_counts)),
            }
        )
    out.sort(key=lambda row: (str(row.get("portfolio_id") or ""), session_sort_order.get(str(row.get("session_bucket") or ""), 9)))
    return out


def _build_execution_hotspot_rows(
    execution_orders: List[Dict[str, Any]],
    fill_rows: List[Dict[str, Any]],
    commission_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    commission_by_exec: Dict[str, float] = {}
    for row in commission_rows:
        exec_id = str(row.get("exec_id") or "").strip()
        if not exec_id:
            continue
        commission_by_exec[exec_id] = float(commission_by_exec.get(exec_id, 0.0)) + float(row.get("value") or 0.0)

    order_meta_by_broker: Dict[int, Dict[str, Any]] = {}
    grouped: Dict[tuple[str, str, str], Dict[str, Any]] = {}

    def _bucket(portfolio_id: str, market: str, symbol: str, session_bucket: str, session_label: str) -> Dict[str, Any]:
        return grouped.setdefault(
            (portfolio_id, session_bucket, symbol),
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "symbol": symbol,
                "session_bucket": session_bucket,
                "session_label": session_label,
                "all_order_rows": 0,
                "submitted_order_rows": 0,
                "all_order_value": 0.0,
                "submitted_order_value": 0.0,
                "all_expected_cost_total": 0.0,
                "all_expected_cost_bps_numerator": 0.0,
                "submitted_expected_cost_total": 0.0,
                "submitted_expected_cost_bps_numerator": 0.0,
                "fill_count": 0,
                "fill_notional": 0.0,
                "commission_total": 0.0,
                "slippage_cost_total": 0.0,
                "_slippage_samples": [],
                "_slippage_dev_samples": [],
                "_style_counts_all": {},
                "_style_counts_submitted": {},
            },
        )

    for row in execution_orders:
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        symbol = str(row.get("symbol") or "").upper().strip()
        if not portfolio_id or not symbol:
            continue
        broker_order_id = int(row.get("broker_order_id") or 0)
        if broker_order_id > 0:
            order_meta_by_broker[broker_order_id] = dict(row)
        status = str(row.get("status") or "").strip().upper()
        if status.startswith("BLOCKED"):
            continue
        session = _execution_session_profile_from_order(row)
        metrics = _planned_cost_metrics_from_order(row)
        bucket = _bucket(
            portfolio_id,
            str(row.get("market") or _market_from_portfolio_or_symbol(portfolio_id, symbol)),
            symbol,
            session["session_bucket"],
            session["session_label"],
        )
        submitted_like = (
            broker_order_id > 0
            or status in {"CREATED", "SUBMITTED", "PRESUBMITTED", "FILLED", "PARTIAL", "PARTIALLY_FILLED"}
            or status.startswith("ERROR_")
        )
        bucket["all_order_rows"] = int(bucket["all_order_rows"]) + 1
        bucket["all_order_value"] = float(bucket["all_order_value"]) + float(metrics["order_value"])
        bucket["all_expected_cost_total"] = float(bucket["all_expected_cost_total"]) + float(metrics["expected_cost_value"])
        bucket["all_expected_cost_bps_numerator"] = float(bucket["all_expected_cost_bps_numerator"]) + float(metrics["expected_cost_bps"]) * float(metrics["order_value"])
        style = str(session.get("execution_style") or metrics.get("execution_style") or "").strip()
        if style:
            style_counts_all = dict(bucket.get("_style_counts_all") or {})
            style_counts_all[style] = int(style_counts_all.get(style, 0)) + 1
            bucket["_style_counts_all"] = style_counts_all
        if submitted_like:
            bucket["submitted_order_rows"] = int(bucket["submitted_order_rows"]) + 1
            bucket["submitted_order_value"] = float(bucket["submitted_order_value"]) + float(metrics["order_value"])
            bucket["submitted_expected_cost_total"] = float(bucket["submitted_expected_cost_total"]) + float(metrics["expected_cost_value"])
            bucket["submitted_expected_cost_bps_numerator"] = float(bucket["submitted_expected_cost_bps_numerator"]) + float(metrics["expected_cost_bps"]) * float(metrics["order_value"])
            if style:
                style_counts_submitted = dict(bucket.get("_style_counts_submitted") or {})
                style_counts_submitted[style] = int(style_counts_submitted.get(style, 0)) + 1
                bucket["_style_counts_submitted"] = style_counts_submitted

    for row in fill_rows:
        broker_order_id = int(row.get("order_id") or 0)
        order_meta = dict(order_meta_by_broker.get(broker_order_id) or {})
        portfolio_id = str(order_meta.get("portfolio_id") or row.get("portfolio_id") or "").strip()
        symbol = str(order_meta.get("symbol") or row.get("symbol") or "").upper().strip()
        if not portfolio_id or not symbol:
            continue
        session = _execution_session_profile_from_order(order_meta)
        bucket = _bucket(
            portfolio_id,
            str(order_meta.get("market") or _market_from_portfolio_or_symbol(portfolio_id, symbol)),
            symbol,
            session["session_bucket"],
            session["session_label"],
        )
        fill_notional = abs(float(row.get("qty") or 0.0)) * abs(float(row.get("price") or 0.0))
        actual_slippage_bps = row.get("actual_slippage_bps")
        slippage_dev_bps = row.get("slippage_bps_deviation")
        commission = float(commission_by_exec.get(str(row.get("exec_id") or "").strip(), 0.0))
        bucket["fill_count"] = int(bucket["fill_count"]) + 1
        bucket["fill_notional"] = float(bucket["fill_notional"]) + float(fill_notional)
        bucket["commission_total"] = float(bucket["commission_total"]) + float(commission)
        if actual_slippage_bps not in (None, ""):
            slip = float(actual_slippage_bps or 0.0)
            bucket["_slippage_samples"].append(slip)
            bucket["slippage_cost_total"] = float(bucket["slippage_cost_total"]) + float(fill_notional) * slip / 10000.0
        if slippage_dev_bps not in (None, ""):
            bucket["_slippage_dev_samples"].append(float(slippage_dev_bps or 0.0))

    session_sort_order = {"OPEN": 0, "MIDDAY": 1, "CLOSE": 2, "UNKNOWN": 3}
    out: List[Dict[str, Any]] = []
    for (_, session_bucket, symbol), bucket in grouped.items():
        submitted_rows = int(bucket.get("submitted_order_rows", 0) or 0)
        submitted_value = float(bucket.get("submitted_order_value", 0.0) or 0.0)
        use_submitted = submitted_rows > 0 and submitted_value > 0.0
        basis_prefix = "submitted" if use_submitted else "all"
        value = float(bucket.get(f"{basis_prefix}_order_value", 0.0) or 0.0)
        numerator = float(bucket.get(f"{basis_prefix}_expected_cost_bps_numerator", 0.0) or 0.0)
        planned_execution_cost_total = float(bucket.get(f"{basis_prefix}_expected_cost_total", 0.0) or 0.0)
        execution_cost_total = float(bucket.get("commission_total", 0.0) or 0.0) + float(bucket.get("slippage_cost_total", 0.0) or 0.0)
        execution_cost_gap = float(execution_cost_total - planned_execution_cost_total)
        avg_expected_cost_bps = float(numerator / value) if value > 0.0 else 0.0
        avg_actual_slippage_bps = _avg_defined([float(v) for v in list(bucket.pop("_slippage_samples", []) or [])])
        avg_slippage_bps_deviation = _avg_defined([float(v) for v in list(bucket.pop("_slippage_dev_samples", []) or [])])
        style_counts = dict(bucket.get("_style_counts_submitted") or {}) if use_submitted else dict(bucket.get("_style_counts_all") or {})
        bps_gap = max(0.0, float(avg_actual_slippage_bps or 0.0) - float(avg_expected_cost_bps or 0.0))
        pressure_score = float(max(0.0, execution_cost_gap) + max(0.0, float(bucket.get("fill_notional", 0.0) or value)) * bps_gap / 10000.0)
        hotspot_action = "INVESTIGATE_EXECUTION"
        hotspot_reason = "该标的在当前时段的实际执行成本高于计划，优先复盘成交时机、参与率和拆单风格。"
        if execution_cost_gap <= max(2.0, planned_execution_cost_total * 0.12) and bps_gap <= 4.0:
            hotspot_action = "OBSERVE"
            hotspot_reason = "该标的在当前时段没有明显超成本，先继续观察样本。"
        out.append(
            {
                "portfolio_id": str(bucket.get("portfolio_id") or ""),
                "market": str(bucket.get("market") or ""),
                "symbol": symbol,
                "session_bucket": str(session_bucket),
                "session_label": str(bucket.get("session_label") or _SESSION_LABELS.get(session_bucket, "未知时段")),
                "planned_order_rows": int(bucket.get(f"{basis_prefix}_order_rows", 0) or 0),
                "submitted_order_rows": int(submitted_rows),
                "planned_order_value": float(value),
                "planned_execution_cost_total": float(planned_execution_cost_total),
                "execution_cost_total": float(execution_cost_total),
                "execution_cost_gap": float(execution_cost_gap),
                "avg_expected_cost_bps": float(avg_expected_cost_bps),
                "avg_actual_slippage_bps": avg_actual_slippage_bps,
                "avg_slippage_bps_deviation": avg_slippage_bps_deviation,
                "fill_count": int(bucket.get("fill_count", 0) or 0),
                "fill_notional": float(bucket.get("fill_notional", 0.0) or 0.0),
                "execution_style_breakdown": ",".join(f"{name}:{style_counts[name]}" for name in sorted(style_counts)),
                "pressure_score": float(pressure_score),
                "hotspot_action": hotspot_action,
                "hotspot_reason": hotspot_reason,
            }
        )
    out.sort(
        key=lambda row: (
            -float(row.get("pressure_score", 0.0) or 0.0),
            -float(row.get("execution_cost_gap", 0.0) or 0.0),
            str(row.get("portfolio_id") or ""),
            session_sort_order.get(str(row.get("session_bucket") or ""), 9),
            str(row.get("symbol") or ""),
        )
    )
    return out


def _build_execution_feedback_rows(
    attribution_rows: List[Dict[str, Any]],
    broker_summary_rows: List[Dict[str, Any]],
    execution_session_rows: List[Dict[str, Any]] | None = None,
    execution_hotspot_rows: List[Dict[str, Any]] | None = None,
    feedback_calibration_map: Dict[str, Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    broker_summary_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in broker_summary_rows
        if str(row.get("portfolio_id") or "").strip()
    }
    session_map: Dict[str, List[Dict[str, Any]]] = {}
    for raw in list(execution_session_rows or []):
        portfolio_id = str(raw.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        session_map.setdefault(portfolio_id, []).append(dict(raw))
    hotspot_map: Dict[str, List[Dict[str, Any]]] = {}
    for raw in list(execution_hotspot_rows or []):
        portfolio_id = str(raw.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        hotspot_map.setdefault(portfolio_id, []).append(dict(raw))
    out: List[Dict[str, Any]] = []
    for row in attribution_rows:
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        broker_summary = dict(broker_summary_map.get(portfolio_id) or {})
        plan_cost = float(row.get("planned_execution_cost_total", 0.0) or 0.0)
        actual_cost = float(row.get("execution_cost_total", 0.0) or 0.0)
        cost_gap = float(row.get("execution_cost_gap", actual_cost - plan_cost) or 0.0)
        expected_bps = float(row.get("avg_expected_cost_bps", 0.0) or 0.0)
        actual_bps = float(row.get("avg_actual_slippage_bps", 0.0) or 0.0)
        submitted_order_rows = int(broker_summary.get("submitted_order_rows", 0) or 0)
        error_order_rows = int(broker_summary.get("error_order_rows", 0) or 0)
        latest_gap_symbols = int(broker_summary.get("latest_gap_symbols", 0) or 0)
        if submitted_order_rows <= 0 and plan_cost <= 0.0 and actual_cost <= 0.0:
            continue
        control_context = _feedback_control_driver_context(
            strategy_delta=float(row.get("strategy_control_weight_delta", 0.0) or 0.0),
            risk_delta=float(row.get("risk_overlay_weight_delta", 0.0) or 0.0),
            execution_gate_weight=float(row.get("execution_gate_blocked_weight", 0.0) or 0.0),
            execution_gate_ratio=float(row.get("execution_gate_blocked_order_ratio", 0.0) or 0.0),
            execution_gate_value=float(row.get("execution_gate_blocked_order_value", 0.0) or 0.0),
        )
        control_driver = str(control_context.get("feedback_control_driver", "") or "")
        gate_weight = float(control_context.get("execution_gate_blocked_weight", 0.0) or 0.0)
        gate_ratio = float(control_context.get("execution_gate_blocked_order_ratio", 0.0) or 0.0)
        control_driver_reason = ""

        action = "HOLD"
        feedback_reason = "计划与实际执行成本目前大致一致，继续观察当前拆单与参与率。"
        adv_max_participation_delta = 0.0
        adv_split_trigger_delta = 0.0
        max_slices_delta = 0
        open_session_scale_delta = 0.0
        midday_session_scale_delta = 0.0
        close_session_scale_delta = 0.0

        gap_ratio = max(0.0, cost_gap / max(plan_cost, 1.0))
        bps_gap = max(0.0, actual_bps - expected_bps)
        severity = max(
            gap_ratio,
            bps_gap / 20.0,
            0.50 if error_order_rows > 0 else 0.0,
            0.35 if latest_gap_symbols > 0 else 0.0,
        )

        if cost_gap > max(10.0, plan_cost * 0.35) or actual_bps > expected_bps + 8.0 or error_order_rows > 0:
            action = "TIGHTEN"
            adv_max_participation_delta = -_clamp(0.005 + severity * 0.007, 0.005, 0.020)
            adv_split_trigger_delta = -_clamp(0.002 + severity * 0.003, 0.002, 0.010)
            max_slices_delta = int(min(2, max(1, round(1.0 + severity))))
            open_session_scale_delta = -_clamp(0.03 + severity * 0.04, 0.03, 0.10)
            midday_session_scale_delta = -_clamp(0.02 + severity * 0.03, 0.02, 0.08)
            close_session_scale_delta = -_clamp(0.03 + severity * 0.04, 0.03, 0.10)
            feedback_reason = "实际执行成本高于计划，下一轮收紧 ADV 参与率、提前触发拆单，并降低开盘/尾盘的参与强度。"
        elif (
            plan_cost > 0.0
            and cost_gap < -max(5.0, plan_cost * 0.20)
            and actual_bps + 5.0 < expected_bps
            and error_order_rows == 0
            and latest_gap_symbols == 0
        ):
            action = "RELAX"
            adv_max_participation_delta = _clamp(0.005 + min(0.010, abs(cost_gap) / max(plan_cost, 1.0) * 0.004), 0.005, 0.015)
            adv_split_trigger_delta = _clamp(0.002 + min(0.004, abs(cost_gap) / max(plan_cost, 1.0) * 0.002), 0.002, 0.006)
            max_slices_delta = -1 if submitted_order_rows > 0 else 0
            open_session_scale_delta = _clamp(0.02 + min(0.03, abs(cost_gap) / max(plan_cost, 1.0) * 0.02), 0.02, 0.05)
            midday_session_scale_delta = _clamp(0.01 + min(0.02, abs(cost_gap) / max(plan_cost, 1.0) * 0.01), 0.01, 0.03)
            close_session_scale_delta = _clamp(0.02 + min(0.03, abs(cost_gap) / max(plan_cost, 1.0) * 0.02), 0.02, 0.05)
            feedback_reason = "实际执行成本持续低于计划，可适度放宽参与率并减少过度拆单。"

        session_feedback_rows: List[Dict[str, Any]] = []
        session_scale_delta_map = {"OPEN": 0.0, "MIDDAY": 0.0, "CLOSE": 0.0}
        dominant_session_bucket = ""
        dominant_session_label = ""
        dominant_session_magnitude = -1.0
        dominant_hotspot_symbol = ""
        dominant_hotspot_session_label = ""
        hotspot_rows = list(hotspot_map.get(portfolio_id, []) or [])
        execution_penalties = _build_execution_hotspot_penalties(hotspot_rows)
        used_session_specific_scales = False
        for session_row in sorted(
            list(session_map.get(portfolio_id, []) or []),
            key=lambda item: {"OPEN": 0, "MIDDAY": 1, "CLOSE": 2, "UNKNOWN": 3}.get(str(item.get("session_bucket") or ""), 9),
        ):
            session_bucket = str(session_row.get("session_bucket") or "").upper().strip()
            if session_bucket not in {"OPEN", "MIDDAY", "CLOSE"}:
                continue
            session_label = str(session_row.get("session_label") or _SESSION_LABELS.get(session_bucket, session_bucket))
            session_plan_cost = float(session_row.get("planned_execution_cost_total", 0.0) or 0.0)
            session_actual_cost = float(session_row.get("execution_cost_total", 0.0) or 0.0)
            session_cost_gap = float(session_row.get("execution_cost_gap", session_actual_cost - session_plan_cost) or 0.0)
            session_expected_bps = float(session_row.get("avg_expected_cost_bps", 0.0) or 0.0)
            session_actual_bps = float(session_row.get("avg_actual_slippage_bps", 0.0) or 0.0)
            session_fill_count = int(session_row.get("fill_count", 0) or 0)
            session_submitted_rows = int(session_row.get("submitted_order_rows", 0) or 0)
            session_action = "HOLD"
            session_reason = f"{session_label}成本与滑点大致稳定，暂不单独调整该时段参与率。"
            session_scale_delta = 0.0
            if session_submitted_rows > 0 or session_fill_count > 0:
                session_gap_ratio = max(0.0, session_cost_gap / max(session_plan_cost, 1.0))
                session_bps_gap = max(0.0, session_actual_bps - session_expected_bps)
                session_severity = max(session_gap_ratio, session_bps_gap / 16.0)
                if session_cost_gap > max(3.0, session_plan_cost * 0.20) or session_actual_bps > session_expected_bps + 4.0:
                    session_action = "TIGHTEN"
                    session_scale_delta = -_clamp(0.015 + session_severity * 0.040, 0.015, 0.100)
                    session_reason = f"{session_label}的实际执行成本高于计划，下一轮应降低该时段参与率。"
                elif (
                    session_plan_cost > 0.0
                    and session_cost_gap < -max(2.0, session_plan_cost * 0.12)
                    and session_actual_bps + 3.0 < session_expected_bps
                ):
                    session_action = "RELAX"
                    session_scale_delta = _clamp(0.010 + min(0.030, abs(session_cost_gap) / max(session_plan_cost, 1.0) * 0.015), 0.010, 0.050)
                    session_reason = f"{session_label}的实际执行成本持续低于计划，可适度放宽该时段参与率。"
            if abs(session_scale_delta) > 1e-9:
                session_scale_delta_map[session_bucket] = float(session_scale_delta)
                used_session_specific_scales = True
            magnitude = abs(session_cost_gap) + abs(session_actual_bps - session_expected_bps) / 10.0
            if magnitude > dominant_session_magnitude:
                dominant_session_magnitude = magnitude
                dominant_session_bucket = session_bucket
                dominant_session_label = session_label
            session_feedback_rows.append(
                {
                    "session_bucket": session_bucket,
                    "session_label": session_label,
                    "session_action": session_action,
                    "planned_execution_cost_total": round(float(session_plan_cost), 6),
                    "execution_cost_total": round(float(session_actual_cost), 6),
                    "execution_cost_gap": round(float(session_cost_gap), 6),
                    "avg_expected_cost_bps": round(float(session_expected_bps), 6),
                    "avg_actual_slippage_bps": round(float(session_actual_bps), 6),
                    "submitted_order_rows": int(session_submitted_rows),
                    "fill_count": int(session_fill_count),
                    "scale_delta": round(float(session_scale_delta), 6),
                    "execution_style_breakdown": str(session_row.get("execution_style_breakdown", "") or ""),
                    "reason": session_reason,
                }
            )

        if used_session_specific_scales:
            open_session_scale_delta = float(session_scale_delta_map.get("OPEN", 0.0))
            midday_session_scale_delta = float(session_scale_delta_map.get("MIDDAY", 0.0))
            close_session_scale_delta = float(session_scale_delta_map.get("CLOSE", 0.0))
            if action == "HOLD":
                if any(str(item.get("session_action") or "") == "TIGHTEN" for item in session_feedback_rows):
                    action = "TIGHTEN"
                elif any(str(item.get("session_action") or "") == "RELAX" for item in session_feedback_rows):
                    action = "RELAX"
            if dominant_session_label:
                feedback_reason = f"总执行成本之外，{dominant_session_label}是本周最需要关注的执行时段；已优先按时段反馈调整下一轮参与率。"
        hotspot_rows.sort(
            key=lambda item: (
                -float(item.get("pressure_score", 0.0) or 0.0),
                -float(item.get("execution_cost_gap", 0.0) or 0.0),
                str(item.get("symbol") or ""),
            )
        )
        top_hotspots: List[Dict[str, Any]] = []
        for hotspot in hotspot_rows:
            if float(hotspot.get("pressure_score", 0.0) or 0.0) <= 0.0:
                continue
            top_hotspots.append(
                {
                    "symbol": str(hotspot.get("symbol") or ""),
                    "session_bucket": str(hotspot.get("session_bucket") or ""),
                    "session_label": str(hotspot.get("session_label") or ""),
                    "hotspot_action": str(hotspot.get("hotspot_action") or ""),
                    "planned_execution_cost_total": round(float(hotspot.get("planned_execution_cost_total", 0.0) or 0.0), 6),
                    "execution_cost_total": round(float(hotspot.get("execution_cost_total", 0.0) or 0.0), 6),
                    "execution_cost_gap": round(float(hotspot.get("execution_cost_gap", 0.0) or 0.0), 6),
                    "avg_expected_cost_bps": round(float(hotspot.get("avg_expected_cost_bps", 0.0) or 0.0), 6),
                    "avg_actual_slippage_bps": round(float(hotspot.get("avg_actual_slippage_bps", 0.0) or 0.0), 6),
                    "pressure_score": round(float(hotspot.get("pressure_score", 0.0) or 0.0), 6),
                    "execution_style_breakdown": str(hotspot.get("execution_style_breakdown", "") or ""),
                    "reason": str(hotspot.get("hotspot_reason", "") or ""),
                }
            )
            if len(top_hotspots) >= 6:
                break
        if top_hotspots:
            dominant_hotspot_symbol = str(top_hotspots[0].get("symbol") or "")
            dominant_hotspot_session_label = str(top_hotspots[0].get("session_label") or "")
            feedback_reason = f"{feedback_reason.rstrip('。')}。当前最需要排查的执行热点是 {dominant_hotspot_session_label}/{dominant_hotspot_symbol}。"
        if execution_penalties:
            penalty_symbols = ",".join(str(item.get("symbol") or "") for item in execution_penalties[:6])
            feedback_reason = f"{feedback_reason.rstrip('。')}。下一轮候选会对这些执行热点标的增加成本/执行惩罚: {penalty_symbols}。"

        gate_pressure_high = gate_ratio >= 0.35 or gate_weight >= 0.03
        if gate_pressure_high and control_driver == "EXECUTION" and action in {"HOLD", "RELAX"}:
            action = "HOLD"
            adv_max_participation_delta = 0.0
            adv_split_trigger_delta = 0.0
            max_slices_delta = 0
            open_session_scale_delta = 0.0
            midday_session_scale_delta = 0.0
            close_session_scale_delta = 0.0
            execution_penalties = []
            control_driver_reason = "本周更明显的问题是执行 gate 阻断，而不是成交成本；优先复核 opportunity/quality/risk/review gate，暂不直接调整 ADV/拆单参数。"
            feedback_reason = f"{control_driver_reason}（{str(control_context.get('feedback_control_split_text') or '')}）"
        elif gate_pressure_high and control_driver == "EXECUTION" and action == "TIGHTEN":
            control_driver_reason = (
                f"同时存在明显的执行 gate 阻断（{str(control_context.get('feedback_control_split_text') or '')}），"
                "执行参数收紧之外还应复核 gate 阈值。"
            )
            feedback_reason = f"{feedback_reason.rstrip('。')}。{control_driver_reason}"

        bps_gap_ratio = max(0.0, actual_bps - expected_bps) / 12.0
        gap_value_ratio = max(0.0, cost_gap) / max(plan_cost, 1.0)
        if action != "HOLD" and control_driver == "EXECUTION":
            gap_value_ratio = max(gap_value_ratio, min(1.0, gate_weight / 0.05))
        base_confidence = _feedback_confidence(
            sample_ratio=float((submitted_order_rows + latest_gap_symbols) / 5.0),
            magnitude_ratio=max(bps_gap_ratio, gap_value_ratio / 0.50),
            persistence_ratio=float(len(top_hotspots) / 3.0),
            structure_ratio=float(len(session_feedback_rows) / 3.0),
        ) if action != "HOLD" else 0.0
        calibration_info = _feedback_calibration_support(
            dict((feedback_calibration_map or {}).get(portfolio_id, {}) or {}),
            feedback_kind="execution",
            action=action,
        )
        confidence = _apply_outcome_calibration(base_confidence, float(calibration_info.get("score", 0.5) or 0.5))

        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(row.get("market") or ""),
                "feedback_scope": "paper_only",
                "execution_feedback_action": action,
                "execution_adv_max_participation_pct_delta": round(float(adv_max_participation_delta), 6),
                "execution_adv_split_trigger_pct_delta": round(float(adv_split_trigger_delta), 6),
                "execution_max_slices_per_symbol_delta": int(max_slices_delta),
                "execution_open_session_participation_scale_delta": round(float(open_session_scale_delta), 6),
                "execution_midday_session_participation_scale_delta": round(float(midday_session_scale_delta), 6),
                "execution_close_session_participation_scale_delta": round(float(close_session_scale_delta), 6),
                "planned_execution_cost_total": round(float(plan_cost), 6),
                "execution_cost_total": round(float(actual_cost), 6),
                "execution_cost_gap": round(float(cost_gap), 6),
                "avg_expected_cost_bps": round(float(expected_bps), 6),
                "avg_actual_slippage_bps": round(float(actual_bps), 6),
                "submitted_order_rows": int(submitted_order_rows),
                "error_order_rows": int(error_order_rows),
                "latest_gap_symbols": int(latest_gap_symbols),
                "execution_style_breakdown": str(row.get("execution_style_breakdown", "") or ""),
                "dominant_execution_session_bucket": dominant_session_bucket,
                "dominant_execution_session_label": dominant_session_label,
                "execution_session_feedback_json": json.dumps(session_feedback_rows, ensure_ascii=False),
                "dominant_execution_hotspot_symbol": dominant_hotspot_symbol,
                "dominant_execution_hotspot_session_label": dominant_hotspot_session_label,
                "execution_hotspots_json": json.dumps(top_hotspots, ensure_ascii=False),
                "execution_penalty_symbol_count": int(len(execution_penalties)),
                "execution_penalty_symbols": ",".join(str(item.get("symbol") or "") for item in execution_penalties[:12]),
                "execution_penalties_json": json.dumps(execution_penalties, ensure_ascii=False),
                "feedback_sample_count": int(submitted_order_rows + latest_gap_symbols),
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
                "strategy_control_weight_delta": float(control_context.get("strategy_control_weight_delta", 0.0) or 0.0),
                "risk_overlay_weight_delta": float(control_context.get("risk_overlay_weight_delta", 0.0) or 0.0),
                "execution_gate_blocked_weight": float(gate_weight),
                "execution_gate_blocked_order_ratio": float(gate_ratio),
                "execution_gate_blocked_order_value": float(control_context.get("execution_gate_blocked_order_value", 0.0) or 0.0),
            }
        )
    out.sort(
        key=lambda row: (
            0 if str(row.get("execution_feedback_action", "") or "") == "TIGHTEN" else 1 if str(row.get("execution_feedback_action", "") or "") == "RELAX" else 2,
            str(row.get("portfolio_id", "") or ""),
        )
    )
    return out


def _decision_evidence_context_maps(
    *,
    strategy_context_rows: List[Dict[str, Any]] | None = None,
    attribution_rows: List[Dict[str, Any]] | None = None,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    return {
        "strategy_context_map": _portfolio_row_map(strategy_context_rows),
        "attribution_map": _portfolio_row_map(attribution_rows),
    }


def _build_weekly_decision_evidence_row(
    row: Dict[str, Any],
    *,
    strategy_context: Dict[str, Any],
    attribution: Dict[str, Any],
) -> Dict[str, Any]:
    realized_edge_bps = row.get("realized_edge_bps")
    if realized_edge_bps in (None, ""):
        realized_edge_bps = row.get("execution_capture_bps")
    return {
        "portfolio_id": str(row.get("portfolio_id") or ""),
        "market": str(row.get("market") or ""),
        "run_id": str(row.get("run_id") or ""),
        "parent_order_key": str(row.get("parent_order_key") or ""),
        "symbol": str(row.get("symbol") or ""),
        "action": str(row.get("action") or ""),
        "decision_status": str(row.get("status_bucket") or ""),
        "candidate_snapshot_id": str(row.get("linked_snapshot_id") or ""),
        "candidate_stage": str(row.get("linked_snapshot_stage") or ""),
        "order_value": float(row.get("order_value", 0.0) or 0.0),
        "fill_notional": float(row.get("fill_notional", 0.0) or 0.0),
        "signal_score": float(row.get("score_before_cost", 0.0) or 0.0),
        "expected_edge_bps": float(row.get("expected_edge_bps", 0.0) or 0.0),
        "expected_cost_bps": float(row.get("expected_cost_bps", 0.0) or 0.0),
        "edge_gate_threshold_bps": float(row.get("edge_gate_threshold_bps", 0.0) or 0.0),
        "required_edge_gap_bps": float(row.get("required_edge_gap_bps", 0.0) or 0.0),
        "blocked_market_rule_order_count": int(row.get("blocked_market_rule_order_count", 0) or 0),
        "blocked_edge_order_count": int(row.get("blocked_edge_order_count", 0) or 0),
        "blocked_gate_order_count": int(row.get("blocked_gate_order_count", 0) or 0),
        "dynamic_liquidity_bucket": str(row.get("dynamic_liquidity_bucket") or ""),
        "dynamic_order_adv_pct": float(row.get("avg_dynamic_order_adv_pct", 0.0) or 0.0),
        "slice_count": int(row.get("slice_count", 1) or 1),
        "strategy_control_weight_delta": float(attribution.get("strategy_control_weight_delta", 0.0) or 0.0),
        "risk_overlay_weight_delta": float(attribution.get("risk_overlay_weight_delta", 0.0) or 0.0),
        "risk_market_profile_budget_weight_delta": float(
            attribution.get("risk_market_profile_budget_weight_delta", 0.0) or 0.0
        ),
        "risk_throttle_weight_delta": float(
            attribution.get("risk_throttle_weight_delta", 0.0) or 0.0
        ),
        "risk_recovery_weight_credit": float(
            attribution.get("risk_recovery_weight_credit", 0.0) or 0.0
        ),
        "execution_gate_blocked_weight": float(
            attribution.get("execution_gate_blocked_weight", 0.0) or 0.0
        ),
        "strategy_effective_controls_note": str(
            strategy_context.get("strategy_effective_controls_note") or ""
        ),
        "execution_gate_summary": str(strategy_context.get("execution_gate_summary") or ""),
        "realized_slippage_bps": row.get("realized_slippage_bps"),
        "realized_edge_bps": realized_edge_bps,
        "execution_capture_bps": row.get("execution_capture_bps"),
        "first_fill_delay_seconds": row.get("first_fill_delay_seconds"),
        "outcome_5d_bps": row.get("outcome_5d_future_return_bps"),
        "outcome_20d_bps": row.get("outcome_20d_future_return_bps"),
        "outcome_60d_bps": row.get("outcome_60d_future_return_bps"),
        "outcome_5d_realized_edge_bps": row.get("outcome_5d_realized_edge_bps"),
        "outcome_20d_realized_edge_bps": row.get("outcome_20d_realized_edge_bps"),
        "outcome_60d_realized_edge_bps": row.get("outcome_60d_realized_edge_bps"),
    }


def _build_weekly_decision_evidence_rows(
    execution_parent_rows: List[Dict[str, Any]],
    *,
    strategy_context_rows: List[Dict[str, Any]] | None = None,
    attribution_rows: List[Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    context_maps = _decision_evidence_context_maps(
        strategy_context_rows=strategy_context_rows,
        attribution_rows=attribution_rows,
    )
    strategy_context_map = dict(context_maps.get("strategy_context_map") or {})
    attribution_map = dict(context_maps.get("attribution_map") or {})
    out: List[Dict[str, Any]] = []
    for raw in list(execution_parent_rows or []):
        row = dict(raw or {})
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        strategy_context = dict(strategy_context_map.get(portfolio_id) or {})
        attribution = dict(attribution_map.get(portfolio_id) or {})
        out.append(
            _build_weekly_decision_evidence_row(
                row,
                strategy_context=strategy_context,
                attribution=attribution,
            )
        )
    out.sort(
        key=lambda item: (
            str(item.get("market") or ""),
            str(item.get("portfolio_id") or ""),
            str(item.get("parent_order_key") or ""),
        )
    )
    return out


def _weighted_avg_defined(
    rows: List[Dict[str, Any]],
    key: str,
    *,
    weight_key: str = "order_value",
) -> float | None:
    numerator = 0.0
    denominator = 0.0
    for item in list(rows or []):
        value = item.get(key)
        if value in (None, ""):
            continue
        weight = abs(_safe_float(item.get(weight_key), 0.0))
        if weight <= 0.0:
            weight = 1.0
        numerator += weight * _safe_float(value, 0.0)
        denominator += weight
    if denominator <= 0.0:
        return None
    return float(numerator / denominator)


def _primary_liquidity_bucket(rows: List[Dict[str, Any]]) -> str:
    liquidity_counts: Dict[str, float] = {}
    has_fill_weight = any(abs(_safe_float(item.get("fill_notional"), 0.0)) > 0.0 for item in list(rows or []))
    for item in list(rows or []):
        bucket = str(item.get("dynamic_liquidity_bucket") or "").strip().upper()
        if not bucket:
            continue
        weight = abs(_safe_float(item.get("fill_notional" if has_fill_weight else "order_value"), 0.0))
        liquidity_counts[bucket] = float(liquidity_counts.get(bucket, 0.0) or 0.0) + weight
    if not liquidity_counts:
        return ""
    return max(
        liquidity_counts.items(),
        key=lambda part: (float(part[1] or 0.0), str(part[0] or "")),
    )[0]


def _build_weekly_decision_evidence_summary_rows(
    decision_evidence_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in list(decision_evidence_rows or []):
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if portfolio_id:
            grouped.setdefault(portfolio_id, []).append(dict(row))

    out: List[Dict[str, Any]] = []
    for portfolio_id, rows in grouped.items():
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(rows[0].get("market") or ""),
                "decision_evidence_row_count": int(len(rows)),
                "decision_blocked_market_rule_order_count": int(
                    sum(int(item.get("blocked_market_rule_order_count", 0) or 0) for item in rows)
                ),
                "decision_blocked_edge_order_count": int(
                    sum(int(item.get("blocked_edge_order_count", 0) or 0) for item in rows)
                ),
                "decision_primary_liquidity_bucket": str(_primary_liquidity_bucket(rows)),
                "decision_avg_dynamic_order_adv_pct": _weighted_avg_defined(rows, "dynamic_order_adv_pct"),
                "decision_avg_slice_count": _weighted_avg_defined(rows, "slice_count"),
                "decision_avg_expected_edge_bps": _weighted_avg_defined(rows, "expected_edge_bps"),
                "decision_avg_expected_cost_bps": _weighted_avg_defined(rows, "expected_cost_bps"),
                "decision_avg_edge_gate_threshold_bps": _weighted_avg_defined(rows, "edge_gate_threshold_bps"),
                "decision_avg_realized_slippage_bps": _weighted_avg_defined(
                    [item for item in rows if item.get("realized_slippage_bps") not in (None, "")],
                    "realized_slippage_bps",
                    weight_key="fill_notional",
                ),
                "decision_avg_realized_edge_bps": _weighted_avg_defined(
                    [item for item in rows if item.get("realized_edge_bps") not in (None, "")],
                    "realized_edge_bps",
                    weight_key="fill_notional",
                ),
                "decision_avg_fill_delay_seconds": _weighted_avg_defined(
                    [item for item in rows if item.get("first_fill_delay_seconds") not in (None, "")],
                    "first_fill_delay_seconds",
                    weight_key="fill_notional",
                ),
                "decision_avg_outcome_5d_bps": _weighted_avg_defined(
                    [item for item in rows if item.get("outcome_5d_bps") not in (None, "")],
                    "outcome_5d_bps",
                ),
                "decision_avg_outcome_20d_bps": _weighted_avg_defined(
                    [item for item in rows if item.get("outcome_20d_bps") not in (None, "")],
                    "outcome_20d_bps",
                ),
                "decision_avg_outcome_60d_bps": _weighted_avg_defined(
                    [item for item in rows if item.get("outcome_60d_bps") not in (None, "")],
                    "outcome_60d_bps",
                ),
            }
        )
    out.sort(key=lambda item: (str(item.get("market") or ""), str(item.get("portfolio_id") or "")))
    return out


def _decision_summary_by_week(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    grouped_decision_history: Dict[str, List[Dict[str, Any]]] = {}
    for item in list(rows or []):
        week_key = str(item.get("week_label") or "").strip()
        if week_key:
            grouped_decision_history.setdefault(week_key, []).append(dict(item))
    decision_weekly_map: Dict[str, Dict[str, Any]] = {}
    for week_key, week_items in grouped_decision_history.items():
        summary_rows = _build_weekly_decision_evidence_summary_rows(week_items)
        if summary_rows:
            decision_weekly_map[week_key] = dict(summary_rows[0])
    return decision_weekly_map


def _build_weekly_decision_evidence_history_overview(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 6,
) -> List[Dict[str, Any]]:
    if not rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for raw in list(rows or []):
        portfolio_id = str(raw.get("portfolio_id") or "").strip()
        market = resolve_market_code(str(raw.get("market") or ""))
        key = (market, portfolio_id)
        if not market or not portfolio_id or key in seen:
            continue
        seen.add(key)
        history_rows = storage.get_recent_investment_weekly_decision_evidence_history(
            market,
            portfolio_id=portfolio_id,
            limit=max(20, int(limit) * 50),
        )
        if not history_rows:
            continue
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for item in list(history_rows or []):
            week_key = str(item.get("week_label") or "").strip()
            if week_key:
                grouped.setdefault(week_key, []).append(dict(item))
        weekly_rows: List[Dict[str, Any]] = []
        for week_key, week_items in grouped.items():
            summary_rows = _build_weekly_decision_evidence_summary_rows(week_items)
            if not summary_rows:
                continue
            summary_row = dict(summary_rows[0])
            summary_row["week_label"] = week_key
            summary_row["week_start"] = str((week_items[0] or {}).get("week_start") or "")
            weekly_rows.append(summary_row)
        weekly_rows.sort(
            key=lambda item: (
                str(item.get("week_start") or ""),
                str(item.get("week_label") or ""),
            ),
            reverse=True,
        )
        tracked_rows = weekly_rows[: max(2, int(limit))]
        if not tracked_rows:
            continue
        latest = dict(tracked_rows[0] or {})
        baseline = dict(tracked_rows[-1] or latest)
        liquidity_bucket_chain = " -> ".join(
            f"{str(item.get('week_label') or '')}:{str(item.get('decision_primary_liquidity_bucket') or '-')}"
            for item in reversed(tracked_rows)
        )
        realized_slippage_delta = float(latest.get("decision_avg_realized_slippage_bps", 0.0) or 0.0) - float(
            baseline.get("decision_avg_realized_slippage_bps", 0.0) or 0.0
        )
        realized_edge_delta = float(latest.get("decision_avg_realized_edge_bps", 0.0) or 0.0) - float(
            baseline.get("decision_avg_realized_edge_bps", 0.0) or 0.0
        )
        outcome_20d_delta = float(latest.get("decision_avg_outcome_20d_bps", 0.0) or 0.0) - float(
            baseline.get("decision_avg_outcome_20d_bps", 0.0) or 0.0
        )
        fill_delay_delta = float(_safe_float(latest.get("decision_avg_fill_delay_seconds"), 0.0)) - float(
            _safe_float(baseline.get("decision_avg_fill_delay_seconds"), 0.0)
        )
        blocked_edge_delta = float(latest.get("decision_blocked_edge_order_count", 0) or 0.0) - float(
            baseline.get("decision_blocked_edge_order_count", 0) or 0.0
        )
        blocked_market_rule_delta = float(
            latest.get("decision_blocked_market_rule_order_count", 0) or 0.0
        ) - float(baseline.get("decision_blocked_market_rule_order_count", 0) or 0.0)
        dynamic_adv_pct_delta = float(latest.get("decision_avg_dynamic_order_adv_pct", 0.0) or 0.0) - float(
            baseline.get("decision_avg_dynamic_order_adv_pct", 0.0) or 0.0
        )
        slice_count_delta = float(latest.get("decision_avg_slice_count", 0.0) or 0.0) - float(
            baseline.get("decision_avg_slice_count", 0.0) or 0.0
        )
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "weeks_tracked": int(len(tracked_rows)),
                "latest_week_label": str(latest.get("week_label") or ""),
                "baseline_week_label": str(baseline.get("week_label") or ""),
                "latest_primary_liquidity_bucket": str(latest.get("decision_primary_liquidity_bucket") or ""),
                "liquidity_bucket_chain": liquidity_bucket_chain,
                "latest_decision_evidence_row_count": int(latest.get("decision_evidence_row_count", 0) or 0),
                "latest_blocked_edge_order_count": int(latest.get("decision_blocked_edge_order_count", 0) or 0),
                "latest_blocked_market_rule_order_count": int(
                    latest.get("decision_blocked_market_rule_order_count", 0) or 0
                ),
                "latest_decision_avg_expected_edge_bps": float(
                    latest.get("decision_avg_expected_edge_bps", 0.0) or 0.0
                ),
                "baseline_decision_avg_expected_edge_bps": float(
                    baseline.get("decision_avg_expected_edge_bps", 0.0) or 0.0
                ),
                "latest_decision_avg_realized_slippage_bps": float(
                    latest.get("decision_avg_realized_slippage_bps", 0.0) or 0.0
                ),
                "baseline_decision_avg_realized_slippage_bps": float(
                    baseline.get("decision_avg_realized_slippage_bps", 0.0) or 0.0
                ),
                "decision_avg_realized_slippage_bps_delta": float(realized_slippage_delta),
                "decision_slippage_trend": _weekly_tuning_history_trend_label(
                    realized_slippage_delta,
                    threshold=3.0,
                    improving_if_negative=True,
                ),
                "latest_decision_avg_realized_edge_bps": float(
                    latest.get("decision_avg_realized_edge_bps", 0.0) or 0.0
                ),
                "baseline_decision_avg_realized_edge_bps": float(
                    baseline.get("decision_avg_realized_edge_bps", 0.0) or 0.0
                ),
                "decision_avg_realized_edge_bps_delta": float(realized_edge_delta),
                "decision_realized_edge_trend": _weekly_tuning_history_trend_label(
                    realized_edge_delta,
                    threshold=10.0,
                ),
                "latest_decision_avg_outcome_20d_bps": float(
                    latest.get("decision_avg_outcome_20d_bps", 0.0) or 0.0
                ),
                "baseline_decision_avg_outcome_20d_bps": float(
                    baseline.get("decision_avg_outcome_20d_bps", 0.0) or 0.0
                ),
                "decision_avg_outcome_20d_bps_delta": float(outcome_20d_delta),
                "decision_outcome_20d_trend": _weekly_tuning_history_trend_label(
                    outcome_20d_delta,
                    threshold=25.0,
                ),
                "latest_decision_avg_fill_delay_seconds": float(
                    latest.get("decision_avg_fill_delay_seconds", 0.0) or 0.0
                ),
                "baseline_decision_avg_fill_delay_seconds": float(
                    baseline.get("decision_avg_fill_delay_seconds", 0.0) or 0.0
                ),
                "decision_avg_fill_delay_seconds_delta": float(fill_delay_delta),
                "decision_fill_delay_trend": _weekly_tuning_history_trend_label(
                    fill_delay_delta,
                    threshold=30.0,
                    improving_if_negative=True,
                ),
                "decision_blocked_edge_order_count_delta": float(blocked_edge_delta),
                "decision_blocked_edge_trend": _weekly_tuning_history_trend_label(
                    blocked_edge_delta,
                    threshold=1.0,
                    improving_if_negative=True,
                ),
                "decision_blocked_market_rule_order_count_delta": float(blocked_market_rule_delta),
                "decision_market_rule_block_trend": _weekly_tuning_history_trend_label(
                    blocked_market_rule_delta,
                    threshold=1.0,
                    improving_if_negative=True,
                ),
                "decision_avg_dynamic_order_adv_pct_delta": float(dynamic_adv_pct_delta),
                "decision_avg_slice_count_delta": float(slice_count_delta),
            }
        )
    out.sort(key=lambda row: (str(row.get("market") or ""), str(row.get("portfolio_id") or "")))
    return out


def _recent_decision_history_rows(
    storage: Storage,
    market: str,
    portfolio_id: str,
    *,
    limit: int,
) -> List[Dict[str, Any]]:
    history_rows = storage.get_recent_investment_weekly_decision_evidence_history(
        market,
        portfolio_id=portfolio_id,
        limit=max(20, int(limit) * 50),
    )
    if not history_rows:
        return []
    weekly_order: List[str] = []
    for item in list(history_rows or []):
        week_key = str(item.get("week_label") or "").strip()
        if week_key and week_key not in weekly_order:
            weekly_order.append(week_key)
    allowed_weeks = set(weekly_order[: max(2, int(limit))])
    return [dict(item) for item in list(history_rows or []) if str(item.get("week_label") or "").strip() in allowed_weeks]


def _market_portfolio_keys(rows: List[Dict[str, Any]] | None) -> List[tuple[str, str]]:
    keys = {
        (resolve_market_code(str(raw.get("market") or "")), str(raw.get("portfolio_id") or "").strip())
        for raw in list(rows or [])
        if resolve_market_code(str(raw.get("market") or "")) and str(raw.get("portfolio_id") or "").strip()
    }
    return sorted(keys)


def _build_weekly_edge_calibration_row(
    market: str,
    portfolio_id: str,
    history_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    filled = [
        row for row in history_rows
        if str(row.get("decision_status") or "").strip().upper() == "FILLED"
    ]
    blocked_edge = [
        row for row in history_rows
        if int(row.get("blocked_edge_order_count", 0) or 0) > 0
        or str(row.get("decision_status") or "").strip().upper() == "BLOCKED_EDGE"
    ]
    blocked_market_rule = [
        row for row in history_rows
        if int(row.get("blocked_market_rule_order_count", 0) or 0) > 0
    ]
    filled_outcome_20d = _avg_defined([row.get("outcome_20d_bps") for row in filled if row.get("outcome_20d_bps") not in (None, "")])
    blocked_edge_outcome_20d = _avg_defined(
        [row.get("outcome_20d_bps") for row in blocked_edge if row.get("outcome_20d_bps") not in (None, "")]
    )
    blocked_market_rule_outcome_20d = _avg_defined(
        [row.get("outcome_20d_bps") for row in blocked_market_rule if row.get("outcome_20d_bps") not in (None, "")]
    )
    edge_gap = None
    if filled_outcome_20d is not None and blocked_edge_outcome_20d is not None:
        edge_gap = float(blocked_edge_outcome_20d - filled_outcome_20d)
    market_rule_gap = None
    if filled_outcome_20d is not None and blocked_market_rule_outcome_20d is not None:
        market_rule_gap = float(blocked_market_rule_outcome_20d - filled_outcome_20d)

    edge_quality = "OBSERVE"
    if edge_gap is not None:
        if edge_gap <= -25.0:
            edge_quality = "GATE_DISCIPLINE_GOOD"
        elif edge_gap >= 25.0:
            edge_quality = "GATE_TOO_TIGHT"
        else:
            edge_quality = "GATE_MIXED"
    market_rule_quality = "OBSERVE"
    if market_rule_gap is not None:
        if market_rule_gap <= -25.0:
            market_rule_quality = "RULE_FILTER_GOOD"
        elif market_rule_gap >= 25.0:
            market_rule_quality = "RULE_FILTER_TOO_TIGHT"
        else:
            market_rule_quality = "RULE_FILTER_MIXED"

    note = "继续观察 edge 与市场规则阻断的事后表现。"
    if edge_quality == "GATE_DISCIPLINE_GOOD":
        note = "被 edge gate 挡掉的单事后 outcome 明显弱于成交单，当前 gate 纪律有效。"
    elif edge_quality == "GATE_TOO_TIGHT":
        note = "被 edge gate 挡掉的单事后并不差，当前 edge floor/buffer 可能偏紧。"
    elif market_rule_quality == "RULE_FILTER_TOO_TIGHT":
        note = "市场规则阻断样本事后并不弱，需复核 board lot / research-only 等限制是否过保守。"

    return {
        "portfolio_id": portfolio_id,
        "market": market,
        "weeks_tracked": int(len({str(item.get('week_label') or '') for item in history_rows if str(item.get('week_label') or '').strip()})),
        "filled_sample_count": int(len(filled)),
        "blocked_edge_sample_count": int(len(blocked_edge)),
        "blocked_market_rule_sample_count": int(len(blocked_market_rule)),
        "filled_avg_outcome_20d_bps": filled_outcome_20d,
        "blocked_edge_avg_outcome_20d_bps": blocked_edge_outcome_20d,
        "blocked_market_rule_avg_outcome_20d_bps": blocked_market_rule_outcome_20d,
        "blocked_edge_vs_filled_outcome_20d_bps": edge_gap,
        "blocked_market_rule_vs_filled_outcome_20d_bps": market_rule_gap,
        "edge_gate_quality": edge_quality,
        "market_rule_quality": market_rule_quality,
        "edge_calibration_note": note,
    }


def _build_weekly_slicing_calibration_bucket_rows(
    market: str,
    portfolio_id: str,
    history_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    bucket_groups: Dict[str, List[Dict[str, Any]]] = {}
    for item in list(history_rows or []):
        bucket = str(item.get("dynamic_liquidity_bucket") or "").strip().upper()
        if bucket:
            bucket_groups.setdefault(bucket, []).append(dict(item))
    out: List[Dict[str, Any]] = []
    for bucket, bucket_rows in bucket_groups.items():
        filled_rows = [
            row for row in bucket_rows
            if str(row.get("decision_status") or "").strip().upper() == "FILLED"
        ]
        avg_adv_pct = _avg_defined([row.get("dynamic_order_adv_pct") for row in bucket_rows if row.get("dynamic_order_adv_pct") not in (None, "")])
        avg_slice_count = _avg_defined([row.get("slice_count") for row in bucket_rows if row.get("slice_count") not in (None, "")])
        avg_slippage = _avg_defined([row.get("realized_slippage_bps") for row in filled_rows if row.get("realized_slippage_bps") not in (None, "")])
        avg_fill_delay = _avg_defined([row.get("first_fill_delay_seconds") for row in filled_rows if row.get("first_fill_delay_seconds") not in (None, "")])
        avg_realized_edge = _avg_defined([row.get("realized_edge_bps") for row in filled_rows if row.get("realized_edge_bps") not in (None, "")])
        avg_outcome_20d = _avg_defined([row.get("outcome_20d_bps") for row in filled_rows if row.get("outcome_20d_bps") not in (None, "")])

        assessment = "BALANCED"
        note = "当前 bucket 的切片强度与成交质量大体匹配。"
        if (avg_slice_count or 0.0) >= 3.5 and (avg_slippage or 0.0) <= 8.0:
            assessment = "POSSIBLY_TOO_CONSERVATIVE"
            note = "切片次数偏多但滑点仍低，当前 bucket 可能过度保守。"
        elif (avg_slice_count or 0.0) <= 1.5 and (avg_slippage or 0.0) >= 18.0:
            assessment = "NEED_MORE_SLICING"
            note = "切片偏少且滑点偏高，当前 bucket 可能需要更积极拆单。"
        elif (avg_fill_delay or 0.0) >= 150.0 and (avg_slice_count or 0.0) >= 3.0:
            assessment = "DELAY_HEAVY"
            note = "成交等待偏长，当前切片节奏可能拖慢执行。"

        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "dynamic_liquidity_bucket": bucket,
                "sample_count": int(len(bucket_rows)),
                "filled_sample_count": int(len(filled_rows)),
                "avg_dynamic_order_adv_pct": avg_adv_pct,
                "avg_slice_count": avg_slice_count,
                "avg_realized_slippage_bps": avg_slippage,
                "avg_fill_delay_seconds": avg_fill_delay,
                "avg_realized_edge_bps": avg_realized_edge,
                "avg_outcome_20d_bps": avg_outcome_20d,
                "slicing_assessment": assessment,
                "slicing_calibration_note": note,
            }
        )
    return out


def _build_weekly_risk_calibration_row(
    storage: Storage,
    market: str,
    portfolio_id: str,
    *,
    limit: int = 6,
) -> Dict[str, Any] | None:
    tuning_rows = storage.get_recent_investment_weekly_tuning_history(
        market,
        portfolio_id=portfolio_id,
        limit=max(2, int(limit)),
    )
    if not tuning_rows:
        return None
    decision_history_rows = _recent_decision_history_rows(storage, market, portfolio_id, limit=limit)
    decision_weekly_map = _decision_summary_by_week(decision_history_rows)

    latest = dict(tuning_rows[0] or {})
    baseline = dict(tuning_rows[-1] or latest)
    latest_details = dict(latest.get("details_json") or {})
    baseline_details = dict(baseline.get("details_json") or {})
    latest_decision = dict(decision_weekly_map.get(str(latest.get("week_label") or ""), {}) or {})
    baseline_decision = dict(decision_weekly_map.get(str(baseline.get("week_label") or ""), {}) or {})

    latest_budget = float(latest_details.get("risk_market_profile_budget_weight_delta", 0.0) or 0.0)
    latest_throttle = float(latest_details.get("risk_throttle_weight_delta", 0.0) or 0.0)
    latest_recovery = float(latest_details.get("risk_recovery_weight_credit", 0.0) or 0.0)
    baseline_budget = float(baseline_details.get("risk_market_profile_budget_weight_delta", 0.0) or 0.0)
    baseline_throttle = float(baseline_details.get("risk_throttle_weight_delta", 0.0) or 0.0)
    baseline_recovery = float(baseline_details.get("risk_recovery_weight_credit", 0.0) or 0.0)
    outcome_20d_delta = float(latest_decision.get("decision_avg_outcome_20d_bps", 0.0) or 0.0) - float(
        baseline_decision.get("decision_avg_outcome_20d_bps", 0.0) or 0.0
    )
    realized_edge_delta = float(latest_decision.get("decision_avg_realized_edge_bps", 0.0) or 0.0) - float(
        baseline_decision.get("decision_avg_realized_edge_bps", 0.0) or 0.0
    )
    component_scores = {
        "BUDGET": abs(latest_budget),
        "THROTTLE": abs(latest_throttle),
        "RECOVERY": abs(latest_recovery),
    }
    dominant_component = max(component_scores.items(), key=lambda item: (float(item[1] or 0.0), str(item[0] or "")))[0]
    calibration_target = "OBSERVE"
    note = "当前风险预算、throttle 与 recovery 还需要继续观察。"
    if dominant_component == "BUDGET" and latest_budget > baseline_budget and outcome_20d_delta < -25.0:
        calibration_target = "BUDGET_TOO_TIGHT"
        note = "最近收益拖累更像来自 market-profile budget 收紧，优先复核 net/gross exposure budget。"
    elif dominant_component == "THROTTLE" and latest_throttle > baseline_throttle and outcome_20d_delta < -25.0:
        calibration_target = "THROTTLE_TOO_TIGHT"
        note = "最近收益拖累更像来自 throttle 层，优先复核相关性/流动性/集中度 throttle。"
    elif latest_recovery > baseline_recovery and outcome_20d_delta > 25.0 and realized_edge_delta > 10.0:
        calibration_target = "RECOVERY_HELPING"
        note = "recovery 近期在改善收益恢复，可继续保持温和回补节奏。"

    return {
        "portfolio_id": portfolio_id,
        "market": market,
        "latest_week_label": str(latest.get("week_label") or ""),
        "baseline_week_label": str(baseline.get("week_label") or ""),
        "latest_budget_weight_delta": latest_budget,
        "baseline_budget_weight_delta": baseline_budget,
        "latest_throttle_weight_delta": latest_throttle,
        "baseline_throttle_weight_delta": baseline_throttle,
        "latest_recovery_weight_credit": latest_recovery,
        "baseline_recovery_weight_credit": baseline_recovery,
        "latest_dominant_throttle_layer": str(latest_details.get("risk_dominant_throttle_layer") or ""),
        "latest_dominant_throttle_layer_label": str(latest_details.get("risk_dominant_throttle_layer_label") or ""),
        "decision_avg_outcome_20d_bps_delta": float(outcome_20d_delta),
        "decision_avg_realized_edge_bps_delta": float(realized_edge_delta),
        "risk_calibration_target": calibration_target,
        "risk_calibration_note": note,
    }


def _build_weekly_edge_calibration_rows(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 6,
) -> List[Dict[str, Any]]:
    if not rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    for market, portfolio_id in _market_portfolio_keys(rows):
        history_rows = _recent_decision_history_rows(storage, market, portfolio_id, limit=limit)
        if not history_rows:
            continue
        out.append(_build_weekly_edge_calibration_row(market, portfolio_id, history_rows))
    out.sort(key=lambda row: (str(row.get("market") or ""), str(row.get("portfolio_id") or "")))
    return out


def _build_weekly_slicing_calibration_rows(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 6,
) -> List[Dict[str, Any]]:
    if not rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    for market, portfolio_id in _market_portfolio_keys(rows):
        history_rows = _recent_decision_history_rows(storage, market, portfolio_id, limit=limit)
        out.extend(_build_weekly_slicing_calibration_bucket_rows(market, portfolio_id, history_rows))
    out.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
            str(row.get("dynamic_liquidity_bucket") or ""),
        )
    )
    return out


def _build_weekly_risk_calibration_rows(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 6,
) -> List[Dict[str, Any]]:
    if not rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    for market, portfolio_id in _market_portfolio_keys(rows):
        row = _build_weekly_risk_calibration_row(
            storage,
            market,
            portfolio_id,
            limit=limit,
        )
        if row:
            out.append(row)
    out.sort(key=lambda row: (str(row.get("market") or ""), str(row.get("portfolio_id") or "")))
    return out


def _market_profile_patch_conflict(raw: Dict[str, Any]) -> tuple[bool, str]:
    row = dict(raw or {})
    action = str(row.get("market_profile_tuning_action") or "").strip().upper()
    risk_action = str(row.get("risk_feedback_action") or "").strip().upper()
    execution_action = str(row.get("execution_feedback_action") or "").strip().upper()
    strategy_delta = float(row.get("strategy_control_weight_delta", 0.0) or 0.0)
    risk_delta = float(row.get("risk_overlay_weight_delta", 0.0) or 0.0)
    if action == "REVIEW_EXECUTION_GATE" and execution_action == "TIGHTEN":
        return True, "执行反馈仍建议收紧，不宜现在下调 edge gate。"
    if action == "REVIEW_REGIME_PLAN" and risk_action == "TIGHTEN" and risk_delta >= max(0.04, strategy_delta - 0.01):
        return True, "风险 overlay 仍在主导压仓，先不要放松 regime/plan 参数。"
    return False, ""


def _build_market_profile_patch_readiness(
    db_path: Path,
    rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    for raw in list(rows or []):
        row = dict(raw)
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        market = resolve_market_code(str(row.get("market") or ""))
        tuning_action = str(row.get("market_profile_tuning_action") or "").strip().upper()
        review_required = tuning_action in {"REVIEW_EXECUTION_GATE", "REVIEW_REGIME_PLAN"}
        if not portfolio_id or not market:
            continue
        history_rows = storage.get_recent_investment_market_profile_patch_history(
            market,
            portfolio_id=portfolio_id,
            limit=12,
        )
        history_rows = sorted(
            list(history_rows or []),
            key=lambda item: (str(item.get("week_start", "") or ""), str(item.get("ts", "") or "")),
            reverse=True,
        )
        same_action_weeks = 0
        for item in history_rows:
            if str(item.get("tuning_action") or "").strip().upper() != tuning_action:
                break
            same_action_weeks += 1
        action_chain = " -> ".join(
            f"{str(item.get('week_label', '') or '-')}:"
            f"{str(item.get('tuning_action', '') or '-')}"
            for item in reversed(history_rows[:max(1, same_action_weeks)])
        ) or "-"
        baseline_week = str(history_rows[same_action_weeks - 1].get("week_label") or "-") if same_action_weeks > 0 else "-"
        conflict_flag, conflict_reason = _market_profile_patch_conflict(row)
        ready_for_manual_apply = bool(review_required and same_action_weeks >= 2 and not conflict_flag)
        if not review_required:
            readiness_label = "NO_PATCH"
            readiness_summary = "当前还没有需要进入人工复核的 market profile patch。"
        elif conflict_flag:
            readiness_label = "BLOCKED_BY_CONFLICT"
            readiness_summary = (
                f"虽已连续 {max(1, same_action_weeks)} 周维持同方向，但当前与执行/风险反馈冲突；"
                f"{conflict_reason}"
            )
        elif ready_for_manual_apply:
            readiness_label = "READY_FOR_MANUAL_APPLY"
            readiness_summary = (
                f"已连续 {same_action_weeks} 周维持同方向，且当前无明显执行/风险冲突，"
                "可升级为人工应用候选。"
            )
        else:
            readiness_label = "OBSERVE_COHORT"
            readiness_summary = (
                f"当前仅连续 {max(1, same_action_weeks)} 周维持同方向，先继续观察到至少 2 周再决定是否人工应用。"
            )
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "adaptive_strategy_active_market_profile": str(row.get("adaptive_strategy_active_market_profile") or ""),
                "market_profile_tuning_action": tuning_action,
                "market_profile_tuning_target": str(row.get("market_profile_tuning_target") or ""),
                "market_profile_cohort_weeks": int(same_action_weeks),
                "market_profile_baseline_week": baseline_week,
                "market_profile_action_chain": action_chain,
                "market_profile_conflict_flag": int(conflict_flag),
                "market_profile_conflict_reason": conflict_reason,
                "market_profile_ready_for_manual_apply": int(ready_for_manual_apply),
                "market_profile_readiness_label": readiness_label,
                "market_profile_readiness_summary": readiness_summary,
            }
        )
    out.sort(
        key=lambda row: (
            0 if int(row.get("market_profile_ready_for_manual_apply", 0) or 0) == 1 else 1 if str(row.get("market_profile_readiness_label") or "") == "BLOCKED_BY_CONFLICT" else 2,
            -int(row.get("market_profile_cohort_weeks", 0) or 0),
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
        )
    )
    return out


def _portfolio_horizon_row_map(rows: List[Dict[str, Any]] | None) -> Dict[str, Dict[int, Dict[str, Any]]]:
    outcome_map: Dict[str, Dict[int, Dict[str, Any]]] = {}
    for raw in list(rows or []):
        portfolio_id = str(raw.get("portfolio_id") or "").strip()
        horizon_days = _safe_int(raw.get("horizon_days"), 0)
        if not portfolio_id or horizon_days <= 0:
            continue
        outcome_map.setdefault(portfolio_id, {})[horizon_days] = dict(raw)
    return outcome_map


def _portfolio_feedback_kind_map(
    rows: List[Dict[str, Any]] | None,
) -> Dict[tuple[str, str], Dict[str, Any]]:
    return {
        (str(row.get("portfolio_id") or "").strip(), str(row.get("feedback_kind") or "").strip().lower()): dict(row)
        for row in list(rows or [])
        if str(row.get("portfolio_id") or "").strip() and str(row.get("feedback_kind") or "").strip()
    }


def _build_weekly_tuning_dataset_lookup_maps(
    *,
    decision_evidence_rows: List[Dict[str, Any]] | None = None,
    strategy_context_rows: List[Dict[str, Any]] | None = None,
    attribution_rows: List[Dict[str, Any]] | None = None,
    outcome_spread_rows: List[Dict[str, Any]] | None = None,
    edge_realization_rows: List[Dict[str, Any]] | None = None,
    blocked_edge_rows: List[Dict[str, Any]] | None = None,
    risk_review_rows: List[Dict[str, Any]] | None = None,
    risk_feedback_rows: List[Dict[str, Any]] | None = None,
    execution_feedback_rows: List[Dict[str, Any]] | None = None,
    market_profile_tuning_rows: List[Dict[str, Any]] | None = None,
    feedback_calibration_rows: List[Dict[str, Any]] | None = None,
    feedback_automation_rows: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    return {
        "strategy_context_map": _portfolio_row_map(strategy_context_rows),
        "attribution_map": _portfolio_row_map(attribution_rows),
        "decision_evidence_summary_map": _portfolio_row_map(
            _build_weekly_decision_evidence_summary_rows(list(decision_evidence_rows or []))
        ),
        "outcome_spread_map": _portfolio_horizon_row_map(outcome_spread_rows),
        "edge_realization_map": _portfolio_row_map(edge_realization_rows),
        "blocked_edge_map": _portfolio_row_map(blocked_edge_rows),
        "risk_review_map": _portfolio_row_map(risk_review_rows),
        "risk_feedback_map": _portfolio_row_map(risk_feedback_rows),
        "execution_feedback_map": _portfolio_row_map(execution_feedback_rows),
        "tuning_map": _portfolio_row_map(market_profile_tuning_rows),
        "calibration_map": _portfolio_row_map(feedback_calibration_rows),
        "automation_map": _portfolio_feedback_kind_map(feedback_automation_rows),
    }


def _build_weekly_tuning_dataset_row(
    summary: Dict[str, Any],
    *,
    lookup_maps: Dict[str, Any],
    week_label: str = "",
    window_start: str = "",
    window_end: str = "",
) -> Dict[str, Any]:
    portfolio_id = str(summary.get("portfolio_id") or "").strip()
    strategy_context = dict(dict(lookup_maps.get("strategy_context_map") or {}).get(portfolio_id) or {})
    attribution = dict(dict(lookup_maps.get("attribution_map") or {}).get(portfolio_id) or {})
    decision_evidence = dict(dict(lookup_maps.get("decision_evidence_summary_map") or {}).get(portfolio_id) or {})
    outcome_spreads = dict(dict(lookup_maps.get("outcome_spread_map") or {}).get(portfolio_id) or {})
    edge_realization = dict(dict(lookup_maps.get("edge_realization_map") or {}).get(portfolio_id) or {})
    blocked_edge = dict(dict(lookup_maps.get("blocked_edge_map") or {}).get(portfolio_id) or {})
    risk_review = dict(dict(lookup_maps.get("risk_review_map") or {}).get(portfolio_id) or {})
    risk_feedback = dict(dict(lookup_maps.get("risk_feedback_map") or {}).get(portfolio_id) or {})
    execution_feedback = dict(dict(lookup_maps.get("execution_feedback_map") or {}).get(portfolio_id) or {})
    tuning = dict(dict(lookup_maps.get("tuning_map") or {}).get(portfolio_id) or {})
    calibration = dict(dict(lookup_maps.get("calibration_map") or {}).get(portfolio_id) or {})
    automation_map = dict(lookup_maps.get("automation_map") or {})
    shadow_automation = dict(automation_map.get((portfolio_id, "shadow")) or {})
    risk_automation = dict(automation_map.get((portfolio_id, "risk")) or {})
    execution_automation = dict(automation_map.get((portfolio_id, "execution")) or {})
    return {
        "week_label": str(week_label or ""),
        "window_start": str(window_start or ""),
        "window_end": str(window_end or ""),
        "portfolio_id": portfolio_id,
        "market": str(summary.get("market") or ""),
        "weekly_return": float(summary.get("weekly_return", 0.0) or 0.0),
        "max_drawdown": float(summary.get("max_drawdown", 0.0) or 0.0),
        "turnover": float(summary.get("turnover", 0.0) or 0.0),
        "latest_equity": float(summary.get("latest_equity", 0.0) or 0.0),
        "adaptive_strategy_active_market_profile": str(
            strategy_context.get("adaptive_strategy_active_market_profile")
            or tuning.get("adaptive_strategy_active_market_profile")
            or ""
        ),
        "adaptive_strategy_market_profile_note": str(
            strategy_context.get("adaptive_strategy_market_profile_note")
            or tuning.get("adaptive_strategy_market_profile_note")
            or ""
        ),
        "strategy_effective_controls_applied": int(
            bool(summary.get("strategy_effective_controls_applied", False))
        ),
        "strategy_effective_controls_note": str(
            strategy_context.get("strategy_effective_controls_note")
            or summary.get("strategy_effective_controls_note")
            or ""
        ),
        "execution_gate_summary": str(
            strategy_context.get("execution_gate_summary")
            or summary.get("execution_gate_summary")
            or ""
        ),
        "outcome_sample_count": int(calibration.get("outcome_sample_count", 0) or 0),
        "outcome_positive_rate": float(calibration.get("outcome_positive_rate", 0.0) or 0.0),
        "outcome_broken_rate": float(calibration.get("outcome_broken_rate", 0.0) or 0.0),
        "signal_quality_score": float(calibration.get("signal_quality_score", 0.0) or 0.0),
        "calibration_confidence": float(calibration.get("calibration_confidence", 0.0) or 0.0),
        "calibration_confidence_label": str(calibration.get("calibration_confidence_label") or ""),
        "latest_outcome_ts": str(calibration.get("latest_outcome_ts") or ""),
        "selection_scope_label": str(calibration.get("selection_scope_label") or ""),
        "selected_horizon_days": str(calibration.get("selected_horizon_days") or ""),
        "shadow_apply_mode": str(shadow_automation.get("calibration_apply_mode") or ""),
        "shadow_apply_mode_label": str(shadow_automation.get("calibration_apply_mode_label") or ""),
        "shadow_outcome_maturity_label": str(shadow_automation.get("outcome_maturity_label") or ""),
        "risk_feedback_action": str(risk_feedback.get("risk_feedback_action") or ""),
        "risk_feedback_confidence": float(risk_feedback.get("feedback_confidence", 0.0) or 0.0),
        "risk_feedback_confidence_label": str(risk_feedback.get("feedback_confidence_label") or ""),
        "risk_feedback_reason": str(risk_feedback.get("feedback_reason") or ""),
        "risk_apply_mode": str(risk_automation.get("calibration_apply_mode") or ""),
        "risk_apply_mode_label": str(risk_automation.get("calibration_apply_mode_label") or ""),
        "risk_outcome_maturity_label": str(risk_automation.get("outcome_maturity_label") or ""),
        "execution_feedback_action": str(execution_feedback.get("execution_feedback_action") or ""),
        "execution_feedback_confidence": float(execution_feedback.get("feedback_confidence", 0.0) or 0.0),
        "execution_feedback_confidence_label": str(execution_feedback.get("feedback_confidence_label") or ""),
        "execution_feedback_reason": str(execution_feedback.get("feedback_reason") or ""),
        "execution_apply_mode": str(execution_automation.get("calibration_apply_mode") or ""),
        "execution_apply_mode_label": str(execution_automation.get("calibration_apply_mode_label") or ""),
        "execution_outcome_maturity_label": str(execution_automation.get("outcome_maturity_label") or ""),
        "market_data_gate_status": str(execution_automation.get("market_data_gate_status") or ""),
        "market_data_gate_label": str(execution_automation.get("market_data_gate_label") or ""),
        "planned_execution_cost_total": float(attribution.get("planned_execution_cost_total", 0.0) or 0.0),
        "execution_cost_total": float(attribution.get("execution_cost_total", 0.0) or 0.0),
        "execution_cost_gap": float(attribution.get("execution_cost_gap", 0.0) or 0.0),
        "avg_expected_cost_bps": float(
            decision_evidence.get("decision_avg_expected_cost_bps", attribution.get("avg_expected_cost_bps", 0.0)) or 0.0
        ),
        "avg_actual_slippage_bps": float(
            decision_evidence.get("decision_avg_realized_slippage_bps", attribution.get("avg_actual_slippage_bps", 0.0)) or 0.0
        ),
        "avg_expected_edge_bps": float(
            decision_evidence.get("decision_avg_expected_edge_bps", edge_realization.get("avg_expected_edge_bps", 0.0)) or 0.0
        ),
        "avg_edge_gate_threshold_bps": float(
            decision_evidence.get("decision_avg_edge_gate_threshold_bps", edge_realization.get("avg_edge_gate_threshold_bps", 0.0)) or 0.0
        ),
        "avg_execution_capture_bps": float(edge_realization.get("avg_execution_capture_bps", 0.0) or 0.0),
        "avg_fill_delay_seconds": float(edge_realization.get("avg_fill_delay_seconds", 0.0) or 0.0),
        "median_fill_delay_seconds": float(edge_realization.get("median_fill_delay_seconds", 0.0) or 0.0),
        "matured_20d_avg_realized_edge_bps": float(
            decision_evidence.get("decision_avg_realized_edge_bps", edge_realization.get("matured_20d_avg_realized_edge_bps", 0.0)) or 0.0
        ),
        "decision_evidence_row_count": int(
            decision_evidence.get("decision_evidence_row_count", 0) or 0
        ),
        "decision_blocked_market_rule_order_count": int(
            decision_evidence.get("decision_blocked_market_rule_order_count", 0) or 0
        ),
        "decision_blocked_edge_order_count": int(
            decision_evidence.get("decision_blocked_edge_order_count", 0) or 0
        ),
        "decision_primary_liquidity_bucket": str(
            decision_evidence.get("decision_primary_liquidity_bucket") or ""
        ),
        "decision_avg_dynamic_order_adv_pct": float(
            decision_evidence.get("decision_avg_dynamic_order_adv_pct", 0.0) or 0.0
        ),
        "decision_avg_slice_count": float(
            decision_evidence.get("decision_avg_slice_count", 0.0) or 0.0
        ),
        "decision_avg_realized_edge_bps": float(
            decision_evidence.get("decision_avg_realized_edge_bps", 0.0) or 0.0
        ),
        "decision_avg_outcome_5d_bps": float(
            decision_evidence.get("decision_avg_outcome_5d_bps", 0.0) or 0.0
        ),
        "decision_avg_outcome_20d_bps": float(
            decision_evidence.get("decision_avg_outcome_20d_bps", 0.0) or 0.0
        ),
        "decision_avg_outcome_60d_bps": float(
            decision_evidence.get("decision_avg_outcome_60d_bps", 0.0) or 0.0
        ),
        "outcome_selected_spread_5d_bps": float(
            dict(outcome_spreads.get(5) or {}).get("selected_spread_vs_unselected_bps", 0.0) or 0.0
        ),
        "outcome_selected_spread_20d_bps": float(
            dict(outcome_spreads.get(20) or {}).get("selected_spread_vs_unselected_bps", 0.0) or 0.0
        ),
        "outcome_selected_spread_60d_bps": float(
            dict(outcome_spreads.get(60) or {}).get("selected_spread_vs_unselected_bps", 0.0) or 0.0
        ),
        "outcome_executed_vs_blocked_edge_spread_20d_bps": float(
            dict(outcome_spreads.get(20) or {}).get("executed_spread_vs_blocked_edge_bps", 0.0) or 0.0
        ),
        "dominant_execution_session_label": str(
            execution_feedback.get("dominant_execution_session_label") or ""
        ),
        "dominant_execution_hotspot_symbol": str(
            execution_feedback.get("dominant_execution_hotspot_symbol") or ""
        ),
        "execution_penalty_symbol_count": int(
            execution_feedback.get("execution_penalty_symbol_count", 0) or 0
        ),
        "strategy_control_weight_delta": float(attribution.get("strategy_control_weight_delta", 0.0) or 0.0),
        "risk_overlay_weight_delta": float(attribution.get("risk_overlay_weight_delta", 0.0) or 0.0),
        "risk_market_profile_budget_weight_delta": float(
            attribution.get("risk_market_profile_budget_weight_delta", 0.0) or 0.0
        ),
        "risk_throttle_weight_delta": float(
            attribution.get("risk_throttle_weight_delta", 0.0) or 0.0
        ),
        "risk_recovery_weight_credit": float(
            attribution.get("risk_recovery_weight_credit", 0.0) or 0.0
        ),
        "risk_layered_split_text": str(attribution.get("risk_layered_split_text") or ""),
        "risk_dominant_throttle_layer": str(attribution.get("risk_dominant_throttle_layer") or ""),
        "risk_dominant_throttle_layer_label": str(attribution.get("risk_dominant_throttle_layer_label") or ""),
        "execution_gate_blocked_order_count": int(
            attribution.get("execution_gate_blocked_order_count", 0) or 0
        ),
        "execution_gate_blocked_order_value": float(
            attribution.get("execution_gate_blocked_order_value", 0.0) or 0.0
        ),
        "execution_gate_blocked_order_ratio": float(
            attribution.get("execution_gate_blocked_order_ratio", 0.0) or 0.0
        ),
        "execution_gate_blocked_weight": float(
            attribution.get("execution_gate_blocked_weight", 0.0) or 0.0
        ),
        "blocked_edge_parent_count": int(blocked_edge.get("blocked_edge_parent_count", 0) or 0),
        "blocked_edge_order_value": float(blocked_edge.get("blocked_edge_order_value", 0.0) or 0.0),
        "blocked_expected_edge_value": float(blocked_edge.get("blocked_expected_edge_value", 0.0) or 0.0),
        "blocked_required_gap_value": float(blocked_edge.get("blocked_required_gap_value", 0.0) or 0.0),
        "blocked_20d_avg_counterfactual_edge_bps": float(
            blocked_edge.get("matured_20d_avg_counterfactual_edge_bps", 0.0) or 0.0
        ),
        "feedback_control_driver": str(execution_feedback.get("feedback_control_driver") or ""),
        "feedback_control_driver_label": str(
            execution_feedback.get("feedback_control_driver_label")
            or risk_feedback.get("feedback_control_driver_label")
            or ""
        ),
        "control_split_text": str(attribution.get("control_split_text") or ""),
        "dominant_driver": str(attribution.get("dominant_driver") or ""),
        "dominant_risk_driver": str(risk_review.get("dominant_risk_driver") or ""),
        "risk_latest_market_profile_budget_tightening": float(
            risk_review.get("latest_market_profile_budget_tightening", 0.0) or 0.0
        ),
        "risk_latest_throttle_tightening": float(
            risk_review.get("latest_throttle_tightening", 0.0) or 0.0
        ),
        "risk_latest_recovery_credit": float(
            risk_review.get("latest_recovery_credit", 0.0) or 0.0
        ),
        "risk_latest_dominant_throttle_layer": str(
            risk_review.get("latest_dominant_throttle_layer") or ""
        ),
        "risk_latest_dominant_throttle_layer_label": str(
            risk_review.get("latest_dominant_throttle_layer_label") or ""
        ),
        "risk_diagnosis": str(risk_review.get("risk_diagnosis") or ""),
        "market_profile_tuning_target": str(tuning.get("market_profile_tuning_target") or ""),
        "market_profile_tuning_bias": str(tuning.get("market_profile_tuning_bias") or ""),
        "market_profile_tuning_action": str(tuning.get("market_profile_tuning_action") or ""),
        "market_profile_tuning_note": str(tuning.get("market_profile_tuning_note") or ""),
        "market_profile_ready_for_manual_apply": int(
            summary.get("market_profile_ready_for_manual_apply", 0) or 0
        ),
        "market_profile_readiness_label": str(summary.get("market_profile_readiness_label") or ""),
        "market_profile_readiness_summary": str(summary.get("market_profile_readiness_summary") or ""),
        "market_profile_cohort_weeks": int(summary.get("market_profile_cohort_weeks", 0) or 0),
    }


def _build_weekly_tuning_dataset_rows(
    summary_rows: List[Dict[str, Any]],
    *,
    decision_evidence_rows: List[Dict[str, Any]] | None = None,
    strategy_context_rows: List[Dict[str, Any]] | None = None,
    attribution_rows: List[Dict[str, Any]] | None = None,
    outcome_spread_rows: List[Dict[str, Any]] | None = None,
    edge_realization_rows: List[Dict[str, Any]] | None = None,
    blocked_edge_rows: List[Dict[str, Any]] | None = None,
    risk_review_rows: List[Dict[str, Any]] | None = None,
    risk_feedback_rows: List[Dict[str, Any]] | None = None,
    execution_feedback_rows: List[Dict[str, Any]] | None = None,
    market_profile_tuning_rows: List[Dict[str, Any]] | None = None,
    feedback_calibration_rows: List[Dict[str, Any]] | None = None,
    feedback_automation_rows: List[Dict[str, Any]] | None = None,
    week_label: str = "",
    window_start: str = "",
    window_end: str = "",
) -> List[Dict[str, Any]]:
    lookup_maps = _build_weekly_tuning_dataset_lookup_maps(
        decision_evidence_rows=decision_evidence_rows,
        strategy_context_rows=strategy_context_rows,
        attribution_rows=attribution_rows,
        outcome_spread_rows=outcome_spread_rows,
        edge_realization_rows=edge_realization_rows,
        blocked_edge_rows=blocked_edge_rows,
        risk_review_rows=risk_review_rows,
        risk_feedback_rows=risk_feedback_rows,
        execution_feedback_rows=execution_feedback_rows,
        market_profile_tuning_rows=market_profile_tuning_rows,
        feedback_calibration_rows=feedback_calibration_rows,
        feedback_automation_rows=feedback_automation_rows,
    )

    rows: List[Dict[str, Any]] = []
    for raw in list(summary_rows or []):
        summary = dict(raw or {})
        portfolio_id = str(summary.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        rows.append(
            _build_weekly_tuning_dataset_row(
                summary,
                lookup_maps=lookup_maps,
                week_label=week_label,
                window_start=window_start,
                window_end=window_end,
            )
        )
    rows.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
        )
    )
    return rows


def _build_weekly_tuning_dataset_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    portfolio_count = int(len(rows))
    dominant_driver_counts = {"STRATEGY": 0, "RISK": 0, "EXECUTION": 0, "OTHER": 0}
    for row in list(rows or []):
        driver = str(row.get("dominant_driver") or "").strip().upper()
        if driver not in dominant_driver_counts:
            driver = "OTHER"
        dominant_driver_counts[driver] = int(dominant_driver_counts.get(driver, 0) or 0) + 1
    return {
        "portfolio_count": portfolio_count,
        "strategy_driver_count": int(dominant_driver_counts.get("STRATEGY", 0) or 0),
        "risk_driver_count": int(dominant_driver_counts.get("RISK", 0) or 0),
        "execution_driver_count": int(dominant_driver_counts.get("EXECUTION", 0) or 0),
        "market_profile_review_count": int(
            sum(
                1
                for row in list(rows or [])
                if str(row.get("market_profile_tuning_action") or "").startswith("REVIEW_")
            )
        ),
        "ready_for_manual_apply_count": int(
            sum(1 for row in list(rows or []) if int(row.get("market_profile_ready_for_manual_apply", 0) or 0) == 1)
        ),
        "execution_tighten_count": int(
            sum(1 for row in list(rows or []) if str(row.get("execution_feedback_action") or "") == "TIGHTEN")
        ),
        "risk_tighten_count": int(
            sum(1 for row in list(rows or []) if str(row.get("risk_feedback_action") or "") == "TIGHTEN")
        ),
        "avg_execution_cost_gap": float(_avg_defined([row.get("execution_cost_gap") for row in list(rows or [])]) or 0.0),
        "avg_execution_gate_blocked_weight": float(
            _avg_defined([row.get("execution_gate_blocked_weight") for row in list(rows or [])]) or 0.0
        ),
        "avg_outcome_sample_count": float(
            _avg_defined([row.get("outcome_sample_count") for row in list(rows or [])]) or 0.0
        ),
        "avg_signal_quality_score": float(
            _avg_defined([row.get("signal_quality_score") for row in list(rows or [])]) or 0.0
        ),
    }


def _build_weekly_portfolio_summary_rows(
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
    *,
    trade_rows: List[Dict[str, Any]],
    latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    sector_rows: List[Dict[str, Any]],
    change_rows: List[Dict[str, Any]],
    run_source_fn: Callable[[Dict[str, Any]], str],
    mean_fn: Callable[[List[float]], float],
    max_drawdown_fn: Callable[[List[float]], float],
    top_holdings_fn: Callable[[List[Dict[str, Any]], int], str],
    top_sector_fn: Callable[[List[Dict[str, Any]], str, int], str],
    summarize_changes_fn: Callable[[List[Dict[str, Any]], str], str],
    holdings_limit: int = 5,
    sector_limit: int = 3,
) -> List[Dict[str, Any]]:
    summary_rows: List[Dict[str, Any]] = []
    for portfolio_id, rows in runs_by_portfolio.items():
        first_row = rows[0]
        last_row = rows[-1]
        perf_rows = [r for r in rows if run_source_fn(r) != "broker_sync"]
        perf_first_row = perf_rows[0] if perf_rows else first_row
        perf_last_row = perf_rows[-1] if perf_rows else last_row
        equity_path = [float(r.get("equity_after") or 0.0) for r in perf_rows if r.get("equity_after") is not None]
        start_equity = float(perf_first_row.get("equity_before") or perf_first_row.get("equity_after") or 0.0)
        latest_equity = float(perf_last_row.get("equity_after") or 0.0)
        weekly_return = ((latest_equity / start_equity) - 1.0) if start_equity > 0 else 0.0
        portfolio_trades = [row for row in trade_rows if str(row.get("portfolio_id") or "") == portfolio_id]
        gross_buy_value = sum(
            abs(float(row.get("trade_value") or 0.0))
            for row in portfolio_trades
            if str(row.get("action") or "").upper() == "BUY"
        )
        gross_sell_value = sum(
            abs(float(row.get("trade_value") or 0.0))
            for row in portfolio_trades
            if str(row.get("action") or "").upper() == "SELL"
        )
        holdings = latest_rows_by_portfolio.get(portfolio_id, [])
        summary_rows.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(last_row.get("market") or ""),
                "runs_in_window": int(len(rows)),
                "executed_rebalances": int(sum(1 for r in rows if int(r.get("executed") or 0) == 1)),
                "trade_count": int(len(portfolio_trades)),
                "buy_count": int(sum(1 for row in portfolio_trades if str(row.get("action") or "").upper() == "BUY")),
                "sell_count": int(sum(1 for row in portfolio_trades if str(row.get("action") or "").upper() == "SELL")),
                "gross_buy_value": float(gross_buy_value),
                "gross_sell_value": float(gross_sell_value),
                "net_trade_value": float(gross_buy_value - gross_sell_value),
                "start_equity": float(start_equity),
                "latest_equity": float(last_row.get("equity_after") or latest_equity),
                "weekly_return": float(weekly_return),
                "avg_equity": float(mean_fn(equity_path)),
                "max_drawdown": float(max_drawdown_fn(equity_path)),
                "turnover": float((gross_buy_value + gross_sell_value) / max(1.0, mean_fn(equity_path))),
                "cash_after": float(last_row.get("cash_after") or 0.0),
                "holdings_count": int(len(holdings)),
                "top_holdings": top_holdings_fn(holdings, holdings_limit),
                "top_sectors": top_sector_fn(sector_rows, portfolio_id, sector_limit),
                "holdings_change_summary": summarize_changes_fn(change_rows, portfolio_id),
                "broker_sync_runs": int(sum(1 for r in rows if run_source_fn(r) == "broker_sync")),
            }
        )
    summary_rows.sort(key=lambda row: float(row.get("weekly_return", 0.0) or 0.0), reverse=True)
    return summary_rows


def _apply_market_profile_tuning_context(
    summary_rows: List[Dict[str, Any]],
    strategy_context_rows: List[Dict[str, Any]],
    market_profile_tuning_rows: List[Dict[str, Any]],
    market_profile_patch_readiness_rows: List[Dict[str, Any]],
) -> None:
    tuning_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in list(market_profile_tuning_rows or [])
        if str(row.get("portfolio_id") or "").strip()
    }
    readiness_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in list(market_profile_patch_readiness_rows or [])
        if str(row.get("portfolio_id") or "").strip()
    }

    def _apply(row: Dict[str, Any]) -> None:
        portfolio_id = str(row.get("portfolio_id") or "")
        tuning = dict(tuning_map.get(portfolio_id, {}) or {})
        readiness = dict(readiness_map.get(portfolio_id, {}) or {})
        row["market_profile_tuning_target"] = str(tuning.get("market_profile_tuning_target", "") or "")
        row["market_profile_tuning_target_label"] = str(tuning.get("market_profile_tuning_target_label", "") or "")
        row["market_profile_tuning_bias"] = str(tuning.get("market_profile_tuning_bias", "") or "")
        row["market_profile_tuning_bias_label"] = str(tuning.get("market_profile_tuning_bias_label", "") or "")
        row["market_profile_tuning_action"] = str(tuning.get("market_profile_tuning_action", "") or "")
        row["market_profile_tuning_note"] = str(tuning.get("market_profile_tuning_note", "") or "")
        row["market_profile_tuning_summary"] = str(tuning.get("market_profile_tuning_summary", "") or "")
        row["market_profile_cohort_weeks"] = int(readiness.get("market_profile_cohort_weeks", 0) or 0)
        row["market_profile_baseline_week"] = str(readiness.get("market_profile_baseline_week", "") or "")
        row["market_profile_action_chain"] = str(readiness.get("market_profile_action_chain", "") or "")
        row["market_profile_conflict_flag"] = int(readiness.get("market_profile_conflict_flag", 0) or 0)
        row["market_profile_conflict_reason"] = str(readiness.get("market_profile_conflict_reason", "") or "")
        row["market_profile_ready_for_manual_apply"] = int(readiness.get("market_profile_ready_for_manual_apply", 0) or 0)
        row["market_profile_readiness_label"] = str(readiness.get("market_profile_readiness_label", "") or "")
        row["market_profile_readiness_summary"] = str(readiness.get("market_profile_readiness_summary", "") or "")

    for row in list(summary_rows or []):
        _apply(row)
    for row in list(strategy_context_rows or []):
        _apply(row)


def _risk_overlay_from_history_row(row: Dict[str, Any]) -> Dict[str, Any]:
    if str(row.get("source_kind") or "").strip():
        stress_scenarios = _parse_json_dict(row.get("stress_scenarios_json"))
        details = _parse_json_dict(row.get("details"))
        risk_details = dict(details.get("risk_overlay") or {})
        normalized = {
            "dynamic_scale": row.get("dynamic_scale"),
            "dynamic_net_exposure": row.get("dynamic_net_exposure"),
            "dynamic_gross_exposure": row.get("dynamic_gross_exposure"),
            "dynamic_short_exposure": row.get("dynamic_short_exposure"),
            "applied_net_exposure": row.get("applied_net_exposure"),
            "applied_gross_exposure": row.get("applied_gross_exposure"),
            "avg_pair_correlation": row.get("avg_pair_correlation"),
            "final_avg_pair_correlation": row.get("avg_pair_correlation"),
            "max_pair_correlation": row.get("max_pair_correlation"),
            "final_max_pair_correlation": row.get("max_pair_correlation"),
            "top_sector_share": row.get("top_sector_share"),
            "stress_worst_loss": row.get("stress_worst_loss"),
            "final_stress_worst_loss": row.get("stress_worst_loss"),
            "stress_worst_scenario": row.get("stress_worst_scenario"),
            "final_stress_worst_scenario": row.get("stress_worst_scenario"),
            "stress_worst_scenario_label": row.get("stress_worst_scenario_label"),
            "final_stress_worst_scenario_label": row.get("stress_worst_scenario_label"),
            "notes": _parse_json_list(row.get("notes_json")),
            "correlation_reduced_symbols": _parse_json_list(row.get("correlation_reduced_symbols_json")),
            "stress_scenarios": stress_scenarios,
            "final_stress_scenarios": stress_scenarios,
        }
        for key, value in risk_details.items():
            if key not in normalized or normalized.get(key) in (None, "", [], {}):
                normalized[key] = value
        return normalized
    details = _parse_json_dict(row.get("details"))
    risk = dict(details.get("risk_overlay") or {})
    if not risk:
        summary = _parse_json_dict(details.get("summary"))
        if summary:
            risk = {
                "dynamic_scale": summary.get("risk_dynamic_scale"),
                "dynamic_net_exposure": summary.get("risk_dynamic_net_exposure"),
                "dynamic_gross_exposure": summary.get("risk_dynamic_gross_exposure"),
                "dynamic_short_exposure": summary.get("risk_dynamic_short_exposure"),
                "avg_pair_correlation": summary.get("risk_avg_pair_correlation"),
                "max_pair_correlation": summary.get("risk_max_pair_correlation"),
                "stress_worst_loss": summary.get("risk_stress_worst_loss"),
                "stress_worst_scenario_label": summary.get("risk_stress_worst_scenario_label"),
                "top_sector_share": summary.get("risk_top_sector_share"),
                "notes": summary.get("risk_notes"),
                "correlation_reduced_symbols": summary.get("risk_correlation_reduced_symbols"),
            }
    return risk


def _latest_risk_overlay(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    latest: Dict[str, Any] = {}
    latest_ts = ""
    for row in rows:
        risk = _risk_overlay_from_history_row(row)
        ts = str(row.get("ts") or "")
        if not risk or ts < latest_ts:
            continue
        latest = risk
        latest_ts = ts
    return latest


def _risk_driver_and_diagnosis(row: Dict[str, Any]) -> tuple[str, str]:
    avg_corr = float(row.get("latest_avg_pair_correlation", 0.0) or 0.0)
    worst_loss = float(row.get("latest_stress_worst_loss", 0.0) or 0.0)
    dynamic_net = float(row.get("latest_dynamic_net_exposure", 0.0) or 0.0)
    dynamic_gross = float(row.get("latest_dynamic_gross_exposure", 0.0) or 0.0)
    top_sector_share = float(row.get("latest_top_sector_share", 0.0) or 0.0)
    market_budget_tightening = float(row.get("latest_market_profile_budget_tightening", 0.0) or 0.0)
    throttle_tightening = float(row.get("latest_throttle_tightening", 0.0) or 0.0)
    recovery_credit = float(row.get("latest_recovery_credit", 0.0) or 0.0)
    throttle_layer = str(row.get("latest_dominant_throttle_layer", "") or "").strip().upper()
    throttle_layer_label = str(row.get("latest_dominant_throttle_layer_label", "") or "").strip()
    if market_budget_tightening >= max(throttle_tightening, 0.03):
        return "MARKET_PROFILE_BUDGET", "当前市场档案先收紧了基础风险预算，优先复核 market-profile exposure budget 是否仍匹配这类市场。"
    if throttle_layer:
        diagnosis = f"当前主导风险 throttle 为 {throttle_layer_label or throttle_layer}，优先复核这一层的风险阈值与持仓结构。"
        if recovery_credit > 1e-9:
            diagnosis += " 组合已经出现部分 recovery，但还未完全释放预算。"
        return throttle_layer, diagnosis
    if avg_corr >= 0.62 or top_sector_share >= 0.45:
        return "CORRELATION", "组合拥挤度偏高，优先增加跨行业/跨市场分散度，再考虑放宽仓位。"
    if worst_loss >= 0.085:
        return "STRESS", "最差 stress 场景压力偏大，优先收缩净/总敞口并复盘高波动标的。"
    if dynamic_net <= 0.70 or dynamic_gross <= 0.75:
        return "EXPOSURE_BUDGET", "组合风险预算仍偏紧，优先提升流动性与数据质量，再争取释放仓位。"
    return "NORMAL", "当前组合风险覆盖整体平稳，可以继续观察信号质量与资金利用率。"


def _strategy_effective_controls_note(*summaries: Dict[str, Any]) -> str:
    for summary in summaries:
        payload = dict(summary or {})
        note = str(
            payload.get("strategy_effective_controls_human_note")
            or payload.get("strategy_effective_controls_note")
            or ""
        ).strip()
        if note:
            return note
        controls = dict(payload.get("strategy_effective_controls") or {})
        note = adaptive_strategy_effective_controls_human_note(controls)
        if note:
            return note
    return ""


def _active_market_profile_note(*summaries: Dict[str, Any]) -> str:
    for summary in summaries:
        payload = dict(summary or {})
        note = str(payload.get("adaptive_strategy_active_market_note") or "").strip()
        if note:
            return note
    return ""


def _active_market_strategy_field(field_name: str, *summaries: Dict[str, Any]) -> str:
    for summary in summaries:
        payload = dict(summary or {})
        value = str(payload.get(field_name) or "").strip()
        if value:
            return value
    return ""


def _execution_gate_summary(execution_summary: Dict[str, Any]) -> str:
    execution_summary = dict(execution_summary or {})
    blocked_total = int(execution_summary.get("blocked_order_count", 0) or 0)
    if blocked_total <= 0:
        return ""
    labels = [
        ("边际收益", int(execution_summary.get("blocked_edge_order_count", 0) or 0)),
        ("流动性", int(execution_summary.get("blocked_liquidity_order_count", 0) or 0)),
        ("风险告警", int(execution_summary.get("blocked_risk_alert_order_count", 0) or 0)),
        ("人工复核", int(execution_summary.get("blocked_manual_review_order_count", 0) or 0)),
        ("机会过滤", int(execution_summary.get("blocked_opportunity_order_count", 0) or 0)),
        ("质量过滤", int(execution_summary.get("blocked_quality_order_count", 0) or 0)),
        ("热点惩罚", int(execution_summary.get("blocked_hotspot_penalty_order_count", 0) or 0)),
    ]
    detail = "，".join(f"{label} {count}" for label, count in labels if count > 0)
    if detail:
        return f"另外有 {blocked_total} 笔计划单因执行 gate 暂未下发（{detail}）。"
    return f"另外有 {blocked_total} 笔计划单因执行 gate 暂未下发。"


def _weekly_strategy_note(
    *,
    market_rules: Dict[str, Any],
    account_profile: Dict[str, Any],
    adaptive_strategy: Dict[str, Any],
    opportunity_summary: Dict[str, Any],
    market_sentiment: Dict[str, Any],
    strategy_effective_controls_note: str = "",
    execution_gate_summary: str = "",
) -> str:
    if bool(market_rules.get("research_only", False)):
        return "当前市场仍以研究为主，周度结论优先用于研究跟踪，不直接放大自动交易动作。"
    defensive_wait_count = int(opportunity_summary.get("adaptive_strategy_wait_count", 0) or 0)
    control_note = str(strategy_effective_controls_note or "").strip()
    gate_note = str(execution_gate_summary or "").strip()
    if control_note:
        parts = [control_note]
        if defensive_wait_count > 0:
            parts.append(f"同时有 {defensive_wait_count} 个新开仓机会因防守环境被降级为观察。")
        if gate_note:
            parts.append(gate_note)
        return " ".join(parts)
    if defensive_wait_count > 0:
        note = f"本周有 {defensive_wait_count} 个新开仓机会因防守环境被降级为观察，先不把回撤信号直接转成加仓动作。"
        if gate_note:
            return f"{note} {gate_note}"
        return note
    sentiment_label = str(market_sentiment.get("label", "") or "").strip().upper()
    if sentiment_label == "DEFENSIVE":
        note = "本周市场处于防守环境，周报应优先解释仓位保护、减速加仓和执行保守化。"
        if gate_note:
            return f"{note} {gate_note}"
        return note
    if bool(market_rules.get("small_account_rule_active", False)):
        preferred = "/".join(
            str(item).upper()
            for item in list(market_rules.get("small_account_preferred_asset_classes", []) or [])
            if str(item).strip()
        ) or "ETF"
        note = f"当前账户仍在小资金规则范围内，本周先按 {preferred} 优先级解释机会与执行，不扩展到低流动性单股。"
        if gate_note:
            return f"{note} {gate_note}"
        return note
    profile_label = str(account_profile.get("label", "") or account_profile.get("name", "") or "").strip()
    if profile_label:
        note = f"当前按 {profile_label} 档位运行，周报优先关注这档账户适配的仓位节奏、持仓数和执行密度。"
        if gate_note:
            return f"{note} {gate_note}"
        return note
    strategy_name = str(adaptive_strategy.get("name", "") or "ACM-RS").strip()
    note = f"当前按 {strategy_name} 自适应中频框架运行，周报优先复盘市场状态、执行成本和信号质量。"
    if gate_note:
        return f"{note} {gate_note}"
    return note


def _augment_summary_rows_with_strategy_context(
    summary_rows: List[Dict[str, Any]],
    *,
    broker_summary_rows: List[Dict[str, Any]],
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
    latest_report_dir_fn: Callable[[Dict[str, List[Dict[str, Any]]], str], str],
    load_market_sentiment_fn: Callable[[str], Dict[str, Any]],
    report_json_fn: Callable[[str, str], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    broker_summary_map = {str(row.get("portfolio_id") or ""): dict(row) for row in list(broker_summary_rows or [])}
    market_cache: Dict[str, Dict[str, Any]] = {}
    context_rows: List[Dict[str, Any]] = []
    for row in list(summary_rows or []):
        portfolio_id = str(row.get("portfolio_id") or "")
        market = resolve_market_code(str(row.get("market") or ""))
        report_dir = latest_report_dir_fn(runs_by_portfolio, portfolio_id)
        market_sentiment = load_market_sentiment_fn(report_dir)
        opportunity_summary = report_json_fn(report_dir, "investment_opportunity_summary.json")
        paper_summary = report_json_fn(report_dir, "investment_paper_summary.json")
        execution_summary = report_json_fn(report_dir, "investment_execution_summary.json")
        if market not in market_cache:
            runtime_paths = _runtime_config_paths_for_market(market)
            market_cache[market] = {
                "market_structure": load_market_structure(BASE_DIR, market, str(runtime_paths["market_structure"])),
                "account_profiles": load_account_profiles(BASE_DIR, str(runtime_paths["account_profile"])),
                "adaptive_strategy": load_adaptive_strategy(BASE_DIR, str(runtime_paths["adaptive_strategy"])),
            }
        cached = market_cache[market]
        broker_summary = dict(broker_summary_map.get(portfolio_id) or {})
        broker_equity = float(
            broker_summary.get("latest_broker_equity")
            or row.get("latest_equity")
            or row.get("start_equity")
            or 0.0
        )
        market_rules = market_structure_summary(cached["market_structure"], broker_equity=broker_equity)
        account_profile = (
            resolved_account_profile_summary(cached["account_profiles"], broker_equity=broker_equity)
            if broker_equity > 0.0
            else {}
        )
        adaptive_strategy = adaptive_strategy_context(cached["adaptive_strategy"])
        controls_note = _strategy_effective_controls_note(execution_summary, paper_summary)
        gate_summary = _execution_gate_summary(execution_summary)
        strategy_note = _weekly_strategy_note(
            market_rules=market_rules,
            account_profile=account_profile,
            adaptive_strategy=adaptive_strategy,
            opportunity_summary=opportunity_summary,
            market_sentiment=market_sentiment,
            strategy_effective_controls_note=controls_note,
            execution_gate_summary=gate_summary,
        )
        row["market_rules_summary"] = str(market_rules.get("summary_text", "") or "")
        row["account_profile_label"] = str(account_profile.get("label", "") or account_profile.get("name", "") or "")
        row["account_profile_summary"] = str(account_profile.get("summary", "") or "")
        row["adaptive_strategy_name"] = str(adaptive_strategy.get("name", "") or "")
        row["adaptive_strategy_summary"] = str(adaptive_strategy.get("summary_text", "") or "")
        row["adaptive_strategy_active_market_profile"] = _active_market_strategy_field(
            "adaptive_strategy_active_market_profile",
            execution_summary,
            paper_summary,
        )
        row["adaptive_strategy_active_market_plan_summary"] = _active_market_strategy_field(
            "adaptive_strategy_active_market_plan_summary",
            execution_summary,
            paper_summary,
        )
        row["adaptive_strategy_active_market_regime_summary"] = _active_market_strategy_field(
            "adaptive_strategy_active_market_regime_summary",
            execution_summary,
            paper_summary,
        )
        row["adaptive_strategy_active_market_execution_summary"] = _active_market_strategy_field(
            "adaptive_strategy_active_market_execution_summary",
            execution_summary,
            paper_summary,
        )
        row["adaptive_strategy_market_profile_note"] = _active_market_profile_note(execution_summary, paper_summary)
        row["strategy_effective_controls_applied"] = bool(
            execution_summary.get("strategy_effective_controls_applied")
            or paper_summary.get("strategy_effective_controls_applied")
        )
        row["strategy_effective_controls_note"] = controls_note
        row["execution_gate_summary"] = gate_summary
        row["execution_blocked_order_count"] = int(execution_summary.get("blocked_order_count", 0) or 0)
        row["weekly_strategy_note"] = strategy_note
        context_rows.append(
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "report_dir": report_dir,
                "market_rules_summary": row["market_rules_summary"],
                "account_profile_label": row["account_profile_label"],
                "account_profile_summary": row["account_profile_summary"],
                "adaptive_strategy_name": row["adaptive_strategy_name"],
                "adaptive_strategy_summary": row["adaptive_strategy_summary"],
                "adaptive_strategy_active_market_profile": row["adaptive_strategy_active_market_profile"],
                "adaptive_strategy_active_market_plan_summary": row["adaptive_strategy_active_market_plan_summary"],
                "adaptive_strategy_active_market_regime_summary": row["adaptive_strategy_active_market_regime_summary"],
                "adaptive_strategy_active_market_execution_summary": row["adaptive_strategy_active_market_execution_summary"],
                "adaptive_strategy_market_profile_note": row["adaptive_strategy_market_profile_note"],
                "strategy_effective_controls_applied": row["strategy_effective_controls_applied"],
                "strategy_effective_controls_note": row["strategy_effective_controls_note"],
                "execution_gate_summary": row["execution_gate_summary"],
                "execution_blocked_order_count": row["execution_blocked_order_count"],
                "weekly_strategy_note": row["weekly_strategy_note"],
                "market_sentiment_label": str(market_sentiment.get("label", "") or ""),
                "adaptive_strategy_wait_count": int(opportunity_summary.get("adaptive_strategy_wait_count", 0) or 0),
            }
        )
    return context_rows


def _sector_top_weight(rows: List[Dict[str, Any]], portfolio_id: str) -> tuple[str, float]:
    ordered = [row for row in rows if str(row.get("portfolio_id") or "") == portfolio_id]
    ordered.sort(key=lambda row: float(row.get("weight") or 0.0), reverse=True)
    if not ordered:
        return "", 0.0
    first = ordered[0]
    return str(first.get("sector") or ""), float(first.get("weight") or 0.0)


def _build_attribution_rows(
    summary_rows: List[Dict[str, Any]],
    *,
    sector_rows: List[Dict[str, Any]],
    latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    execution_effect_rows: List[Dict[str, Any]],
    planned_execution_cost_rows: List[Dict[str, Any]] | None = None,
    execution_gate_rows: List[Dict[str, Any]] | None = None,
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
    latest_report_dir_fn: Callable[[Dict[str, List[Dict[str, Any]]], str], str],
    load_market_sentiment_fn: Callable[[str], Dict[str, Any]],
    report_json_fn: Callable[[str, str], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    execution_map = {str(row.get("portfolio_id") or ""): dict(row) for row in execution_effect_rows}
    planned_execution_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in list(planned_execution_cost_rows or [])
    }
    execution_gate_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in list(execution_gate_rows or [])
    }
    out: List[Dict[str, Any]] = []
    neutral_exposure_ratio = 0.60
    for summary in summary_rows:
        portfolio_id = str(summary.get("portfolio_id") or "")
        latest_positions = list(latest_rows_by_portfolio.get(portfolio_id) or [])
        latest_equity = float(summary.get("latest_equity") or 0.0)
        holdings_value = float(sum(float(row.get("market_value") or 0.0) for row in latest_positions))
        invested_ratio = 0.0
        if latest_equity > 0.0:
            invested_ratio = max(0.0, min(1.5, holdings_value / latest_equity))
        elif float(summary.get("cash_after") or 0.0) > 0.0:
            invested_ratio = max(
                0.0,
                min(
                    1.5,
                    1.0 - float(summary.get("cash_after") or 0.0) / max(float(summary.get("start_equity") or 1.0), 1.0),
                ),
            )

        report_dir = latest_report_dir_fn(runs_by_portfolio, portfolio_id)
        market_sentiment = load_market_sentiment_fn(report_dir)
        paper_summary = report_json_fn(report_dir, "investment_paper_summary.json")
        execution_summary = report_json_fn(report_dir, "investment_execution_summary.json")
        market_proxy_return = float(market_sentiment.get("benchmark_ret5d", 0.0) or 0.0)
        market_contribution = market_proxy_return * neutral_exposure_ratio
        sizing_contribution = market_proxy_return * (invested_ratio - neutral_exposure_ratio)

        execution_effect = dict(execution_map.get(portfolio_id) or {})
        planned_effect = dict(planned_execution_map.get(portfolio_id) or {})
        gate_effect = dict(execution_gate_map.get(portfolio_id) or {})
        execution_cost_total = float(execution_effect.get("execution_cost_total", 0.0) or 0.0)
        planned_execution_cost_total = float(planned_effect.get("planned_execution_cost_total", 0.0) or 0.0)
        execution_contribution = -execution_cost_total / latest_equity if latest_equity > 0.0 else 0.0
        execution_cost_gap = float(execution_cost_total - planned_execution_cost_total)

        strategy_controls = dict(
            execution_summary.get("strategy_effective_controls")
            or paper_summary.get("strategy_effective_controls")
            or {}
        )
        strategy_base_target = float(
            strategy_controls.get(
                "base_effective_target_invested_weight",
                strategy_controls.get("base_target_invested_weight", 0.0),
            )
            or 0.0
        )
        strategy_effective_target = float(
            strategy_controls.get("effective_target_invested_weight", strategy_base_target) or strategy_base_target
        )
        strategy_control_weight_delta = max(0.0, strategy_base_target - strategy_effective_target)

        risk_source = dict(paper_summary or {})
        risk_source.update(dict(execution_summary or {}))
        risk_net_tightening = max(
            0.0,
            float(
                risk_source.get(
                    "risk_net_exposure_tightening",
                    max(
                        0.0,
                        float(risk_source.get("risk_base_net_exposure", 0.0) or 0.0)
                        - float(risk_source.get("risk_dynamic_net_exposure", 0.0) or 0.0),
                    ),
                )
                or 0.0
            ),
        )
        risk_gross_tightening = max(
            0.0,
            float(
                risk_source.get(
                    "risk_gross_exposure_tightening",
                    max(
                        0.0,
                        float(risk_source.get("risk_base_gross_exposure", 0.0) or 0.0)
                        - float(risk_source.get("risk_dynamic_gross_exposure", 0.0) or 0.0),
                    ),
                )
                or 0.0
            ),
        )
        risk_overlay_weight_delta = max(risk_net_tightening, risk_gross_tightening)
        risk_market_profile_budget_weight_delta = max(
            0.0,
            float(
                risk_source.get(
                    "risk_market_profile_budget_tightening",
                    max(
                        float(risk_source.get("risk_market_profile_budget_net_tightening", 0.0) or 0.0),
                        float(risk_source.get("risk_market_profile_budget_gross_tightening", 0.0) or 0.0),
                    ),
                )
                or 0.0
            ),
        )
        risk_throttle_weight_delta = max(
            0.0,
            float(
                risk_source.get(
                    "risk_throttle_weight_delta",
                    max(
                        float(risk_source.get("risk_throttle_net_tightening", 0.0) or 0.0),
                        float(risk_source.get("risk_throttle_gross_tightening", 0.0) or 0.0),
                    ),
                )
                or 0.0
            ),
        )
        risk_recovery_weight_credit = max(
            0.0,
            float(
                risk_source.get(
                    "risk_recovery_weight_credit",
                    max(
                        float(risk_source.get("risk_recovery_net_credit", 0.0) or 0.0),
                        float(risk_source.get("risk_recovery_gross_credit", 0.0) or 0.0),
                    ),
                )
                or 0.0
            ),
        )
        risk_layered_split_text = str(risk_source.get("risk_layered_throttle_text", "") or "")
        risk_dominant_throttle_layer = str(risk_source.get("risk_dominant_throttle_layer", "") or "")
        risk_dominant_throttle_layer_label = str(risk_source.get("risk_dominant_throttle_layer_label", "") or "")

        execution_gate_blocked_order_count = int(gate_effect.get("blocked_order_count", 0) or 0)
        execution_gate_blocked_order_value = float(gate_effect.get("blocked_order_value", 0.0) or 0.0)
        execution_gate_blocked_order_ratio = float(gate_effect.get("blocked_order_ratio", 0.0) or 0.0)
        execution_gate_blocked_weight = float(execution_gate_blocked_order_value / latest_equity) if latest_equity > 0.0 else 0.0
        control_split_text = _attribution_control_split_text(
            strategy_delta=strategy_control_weight_delta,
            risk_delta=risk_overlay_weight_delta,
            gate_weight=execution_gate_blocked_weight,
            gate_value=execution_gate_blocked_order_value,
            gate_ratio=execution_gate_blocked_order_ratio,
        )

        top_sector, top_sector_weight = _sector_top_weight(sector_rows, portfolio_id)
        residual_after_base = float(summary.get("weekly_return") or 0.0) - market_contribution - sizing_contribution - execution_contribution
        sector_strength = max(0.0, min(0.45, (top_sector_weight - 0.25) / 0.45))
        sector_contribution = residual_after_base * sector_strength
        selection_contribution = float(summary.get("weekly_return") or 0.0) - (
            market_contribution + sizing_contribution + execution_contribution + sector_contribution
        )

        contributions = {
            "selection": float(selection_contribution),
            "sizing": float(sizing_contribution),
            "sector": float(sector_contribution),
            "execution": float(execution_contribution),
            "market": float(market_contribution),
        }
        dominant_key = max(contributions, key=lambda key: abs(float(contributions[key])))
        diagnosis = {
            "selection": "收益主要由选股质量驱动，优先复盘信号与候选排序。",
            "sizing": "收益主要受仓位利用率影响，优先复盘资金闲置与加减仓节奏。",
            "sector": "收益主要受行业/主题倾斜影响，优先复盘行业暴露是否过强或过弱。",
            "execution": (
                "收益主要受执行损耗影响，优先复盘佣金、滑点和执行时机。"
                if execution_cost_gap <= 0.0
                else "收益主要受执行损耗影响，而且实际执行成本高于计划，优先复盘拆单节奏、时段风格和成交质量。"
            ),
            "market": "收益主要受市场方向影响，优先复盘 regime 与净敞口控制。",
        }[dominant_key]
        abs_total = sum(abs(float(value)) for value in contributions.values()) or max(abs(float(summary.get("weekly_return") or 0.0)), 1e-9)
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(summary.get("market") or ""),
                "attribution_mode": "proxy_v1",
                "weekly_return": float(summary.get("weekly_return") or 0.0),
                "market_proxy_return": float(market_proxy_return),
                "invested_ratio": float(invested_ratio),
                "selection_contribution": float(selection_contribution),
                "sizing_contribution": float(sizing_contribution),
                "sector_contribution": float(sector_contribution),
                "execution_contribution": float(execution_contribution),
                "market_contribution": float(market_contribution),
                "selection_share": float(abs(selection_contribution) / abs_total),
                "sizing_share": float(abs(sizing_contribution) / abs_total),
                "sector_share": float(abs(sector_contribution) / abs_total),
                "execution_share": float(abs(execution_contribution) / abs_total),
                "market_share": float(abs(market_contribution) / abs_total),
                "top_sector": top_sector,
                "top_sector_weight": float(top_sector_weight),
                "execution_cost_total": float(execution_cost_total),
                "planned_execution_cost_total": float(planned_execution_cost_total),
                "planned_spread_cost_total": float(planned_effect.get("planned_spread_cost_total", 0.0) or 0.0),
                "planned_slippage_cost_total": float(planned_effect.get("planned_slippage_cost_total", 0.0) or 0.0),
                "planned_commission_cost_total": float(planned_effect.get("planned_commission_cost_total", 0.0) or 0.0),
                "avg_expected_cost_bps": planned_effect.get("avg_expected_cost_bps"),
                "planned_cost_basis": str(planned_effect.get("planned_cost_basis", "") or ""),
                "execution_style_breakdown": str(planned_effect.get("execution_style_breakdown", "") or ""),
                "execution_cost_gap": float(execution_cost_gap),
                "commission_total": float(execution_effect.get("commission_total", 0.0) or 0.0),
                "slippage_cost_total": float(execution_effect.get("slippage_cost_total", 0.0) or 0.0),
                "avg_actual_slippage_bps": execution_effect.get("avg_actual_slippage_bps"),
                "strategy_control_weight_delta": float(strategy_control_weight_delta),
                "risk_overlay_weight_delta": float(risk_overlay_weight_delta),
                "risk_market_profile_budget_weight_delta": float(risk_market_profile_budget_weight_delta),
                "risk_throttle_weight_delta": float(risk_throttle_weight_delta),
                "risk_recovery_weight_credit": float(risk_recovery_weight_credit),
                "risk_layered_split_text": str(risk_layered_split_text),
                "risk_dominant_throttle_layer": str(risk_dominant_throttle_layer),
                "risk_dominant_throttle_layer_label": str(risk_dominant_throttle_layer_label),
                "execution_gate_blocked_order_count": int(execution_gate_blocked_order_count),
                "execution_gate_blocked_order_value": float(execution_gate_blocked_order_value),
                "execution_gate_blocked_order_ratio": float(execution_gate_blocked_order_ratio),
                "execution_gate_blocked_weight": float(execution_gate_blocked_weight),
                "control_split_text": control_split_text,
                "dominant_driver": dominant_key.upper(),
                "diagnosis": diagnosis,
            }
        )
    out.sort(key=lambda row: abs(float(row.get("weekly_return", 0.0) or 0.0)), reverse=True)
    return out


def _build_risk_review_rows(
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
    risk_history_by_portfolio: Dict[str, List[Dict[str, Any]]] | None = None,
    *,
    risk_overlay_from_history_row_fn: Callable[[Dict[str, Any]], Dict[str, Any]],
    latest_risk_overlay_fn: Callable[[List[Dict[str, Any]]], Dict[str, Any]],
    risk_driver_and_diagnosis_fn: Callable[[Dict[str, Any]], tuple[str, str]],
    mean_fn: Callable[[List[float]], float],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    all_portfolios = set(runs_by_portfolio)
    all_portfolios.update((risk_history_by_portfolio or {}).keys())
    for portfolio_id in sorted(all_portfolios):
        runs = list(runs_by_portfolio.get(portfolio_id, []) or [])
        history_rows = list((risk_history_by_portfolio or {}).get(portfolio_id, []) or [])
        source_rows = history_rows or runs
        overlays = [risk for risk in (risk_overlay_from_history_row_fn(row) for row in source_rows) if risk]
        if not overlays:
            continue
        latest = latest_risk_overlay_fn(source_rows)
        avg_dynamic_net = mean_fn([float(item.get("dynamic_net_exposure", 0.0) or 0.0) for item in overlays])
        avg_dynamic_gross = mean_fn([float(item.get("dynamic_gross_exposure", 0.0) or 0.0) for item in overlays])
        avg_avg_corr = mean_fn([float(item.get("avg_pair_correlation", 0.0) or 0.0) for item in overlays])
        avg_worst_loss = mean_fn([float(item.get("stress_worst_loss", 0.0) or 0.0) for item in overlays])
        latest_scenarios = dict(latest.get("final_stress_scenarios", {}) or latest.get("stress_scenarios", {}) or {})
        latest_row = (source_rows[-1] if source_rows else {})
        source_kinds = sorted(
            {
                str(row.get("source_kind") or "").strip().lower()
                for row in history_rows
                if str(row.get("source_kind") or "").strip()
            }
        )
        row = {
            "portfolio_id": portfolio_id,
            "market": str(latest_row.get("market") or (runs[-1] if runs else {}).get("market") or ""),
            "risk_overlay_runs": int(len(overlays)),
            "risk_history_source": "normalized_table" if history_rows else "legacy_run_details",
            "risk_history_sources": ",".join(source_kinds),
            "avg_dynamic_net_exposure": float(avg_dynamic_net),
            "avg_dynamic_gross_exposure": float(avg_dynamic_gross),
            "avg_pair_correlation": float(avg_avg_corr),
            "avg_stress_worst_loss": float(avg_worst_loss),
            "latest_dynamic_scale": float(latest.get("dynamic_scale", 1.0) or 1.0),
            "latest_dynamic_net_exposure": float(latest.get("dynamic_net_exposure", 0.0) or 0.0),
            "latest_dynamic_gross_exposure": float(latest.get("dynamic_gross_exposure", 0.0) or 0.0),
            "latest_dynamic_short_exposure": float(latest.get("dynamic_short_exposure", 0.0) or 0.0),
            "latest_market_profile_net_exposure_budget": float(latest.get("market_profile_net_exposure_budget", 0.0) or 0.0),
            "latest_market_profile_gross_exposure_budget": float(latest.get("market_profile_gross_exposure_budget", 0.0) or 0.0),
            "latest_market_profile_budget_tightening": float(
                max(
                    float(latest.get("market_profile_budget_tightening_net", 0.0) or 0.0),
                    float(latest.get("market_profile_budget_tightening_gross", 0.0) or 0.0),
                )
            ),
            "latest_throttle_tightening": float(
                max(
                    float(latest.get("throttle_net_tightening", 0.0) or 0.0),
                    float(latest.get("throttle_gross_tightening", 0.0) or 0.0),
                )
            ),
            "latest_recovery_credit": float(
                max(
                    float(latest.get("recovery_net_credit", 0.0) or 0.0),
                    float(latest.get("recovery_gross_credit", 0.0) or 0.0),
                )
            ),
            "latest_dominant_throttle_layer": str(latest.get("dominant_throttle_layer", "") or ""),
            "latest_dominant_throttle_layer_label": str(latest.get("dominant_throttle_layer_label", "") or ""),
            "latest_layered_throttle_text": str(latest.get("layered_throttle_text", "") or ""),
            "latest_recovery_active": int(bool(latest.get("recovery_active", False))),
            "latest_avg_pair_correlation": float(
                latest.get("final_avg_pair_correlation", latest.get("avg_pair_correlation", 0.0)) or 0.0
            ),
            "latest_max_pair_correlation": float(
                latest.get("final_max_pair_correlation", latest.get("max_pair_correlation", 0.0)) or 0.0
            ),
            "latest_top_sector_share": float(latest.get("top_sector_share", 0.0) or 0.0),
            "latest_stress_index_drop_loss": float(latest_scenarios.get("index_drop", {}).get("loss", 0.0) or 0.0),
            "latest_stress_volatility_spike_loss": float(latest_scenarios.get("volatility_spike", {}).get("loss", 0.0) or 0.0),
            "latest_stress_liquidity_shock_loss": float(latest_scenarios.get("liquidity_shock", {}).get("loss", 0.0) or 0.0),
            "latest_stress_worst_loss": float(
                latest.get("final_stress_worst_loss", latest.get("stress_worst_loss", 0.0)) or 0.0
            ),
            "latest_stress_worst_scenario": str(
                latest.get("final_stress_worst_scenario", latest.get("stress_worst_scenario", "")) or ""
            ),
            "latest_stress_worst_scenario_label": str(
                latest.get("final_stress_worst_scenario_label", latest.get("stress_worst_scenario_label", "")) or ""
            ),
            "risk_notes": " | ".join(str(x).strip() for x in list(latest.get("notes", []) or []) if str(x).strip()),
            "correlation_reduced_symbols": ",".join(list(latest.get("correlation_reduced_symbols", []) or [])[:12]),
        }
        dominant_driver, diagnosis = risk_driver_and_diagnosis_fn(row)
        row["dominant_risk_driver"] = dominant_driver
        row["risk_diagnosis"] = diagnosis
        rows.append(row)
    rows.sort(
        key=lambda row: (
            0 if str(row.get("dominant_risk_driver", "") or "") == "STRESS" else 1 if str(row.get("dominant_risk_driver", "") or "") == "CORRELATION" else 2,
            -float(row.get("latest_stress_worst_loss", 0.0) or 0.0),
            str(row.get("portfolio_id", "") or ""),
        )
    )
    return rows


def _build_market_profile_tuning_summary(
    strategy_context_rows: List[Dict[str, Any]],
    attribution_rows: List[Dict[str, Any]],
    risk_feedback_rows: List[Dict[str, Any]],
    execution_feedback_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    attribution_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in list(attribution_rows or [])
        if str(row.get("portfolio_id") or "").strip()
    }
    risk_feedback_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in list(risk_feedback_rows or [])
        if str(row.get("portfolio_id") or "").strip()
    }
    execution_feedback_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in list(execution_feedback_rows or [])
        if str(row.get("portfolio_id") or "").strip()
    }
    out: List[Dict[str, Any]] = []
    for raw in list(strategy_context_rows or []):
        row = dict(raw)
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        attribution = dict(attribution_map.get(portfolio_id) or {})
        risk_feedback = dict(risk_feedback_map.get(portfolio_id) or {})
        execution_feedback = dict(execution_feedback_map.get(portfolio_id) or {})
        strategy_delta = float(attribution.get("strategy_control_weight_delta", 0.0) or 0.0)
        risk_delta = float(attribution.get("risk_overlay_weight_delta", 0.0) or 0.0)
        gate_weight = float(attribution.get("execution_gate_blocked_weight", 0.0) or 0.0)
        gate_ratio = float(attribution.get("execution_gate_blocked_order_ratio", 0.0) or 0.0)
        blocked_edge_count = int(attribution.get("execution_gate_blocked_order_count", 0) or 0)
        split_text = str(attribution.get("control_split_text", "") or "").strip()
        risk_action = str(risk_feedback.get("risk_feedback_action", "") or "HOLD").upper()
        execution_action = str(execution_feedback.get("execution_feedback_action", "") or "HOLD").upper()

        tuning_target = "WATCH"
        tuning_target_label = "继续观察"
        tuning_bias = "NEUTRAL"
        tuning_bias_label = "暂无明显失配"
        tuning_action = "KEEP_BASELINE"
        note = "当前市场档案没有出现单一主导的失配信号，先继续观察。"

        if gate_weight >= max(0.02, strategy_delta + 0.01, risk_delta + 0.01) and blocked_edge_count > 0:
            tuning_target = "EXECUTION_GATE"
            tuning_target_label = "执行门槛"
            tuning_bias = "TOO_TIGHT"
            tuning_bias_label = "执行 gate 偏紧"
            tuning_action = "REVIEW_EXECUTION_GATE"
            note = (
                "本周更明显的阻断来自 execution edge gate，优先复核 "
                "min_expected_edge_bps / edge_cost_buffer_bps，而不是继续收紧执行节奏。"
            )
        elif strategy_delta >= max(0.05, risk_delta + 0.02, gate_weight + 0.02):
            tuning_target = "REGIME_PLAN"
            tuning_target_label = "Regime / 计划参数"
            tuning_bias = "TOO_TIGHT"
            tuning_bias_label = "策略参数偏紧"
            tuning_action = "REVIEW_REGIME_PLAN"
            note = (
                "本周压仓主要来自策略主动控仓，优先复核 risk_on / hard_risk_off、"
                "no_trade_band 和 turnover_penalty，而不是先改风险 overlay。"
            )
        elif risk_delta >= max(0.04, strategy_delta + 0.02, gate_weight + 0.02):
            tuning_target = "RISK_OVERLAY"
            tuning_target_label = "风险 Overlay"
            tuning_bias = "RISK_DRIVEN"
            tuning_bias_label = "风险层主导"
            tuning_action = "KEEP_RISK_OVERLAY"
            note = "本周压仓主要来自风险 overlay，先不要把问题误判成市场档案参数本身。"
        elif execution_action == "RELAX":
            tuning_target = "EXECUTION_TACTICS"
            tuning_target_label = "执行节奏"
            tuning_bias = "CAN_RELAX"
            tuning_bias_label = "执行节奏可放宽"
            tuning_action = "KEEP_EXECUTION_RELAX"
            note = "实际执行成本持续低于计划，可继续沿执行参与率/拆单参数做温和放宽。"
        elif risk_action == "RELAX":
            tuning_target = "RISK_BUDGET"
            tuning_target_label = "风险预算"
            tuning_bias = "CAN_RELAX"
            tuning_bias_label = "风险预算可放宽"
            tuning_action = "KEEP_RISK_RELAX"
            note = "组合风险预算相对保守，若后续样本持续稳定，可继续沿风险预算方向温和放宽。"

        if split_text:
            note = f"{note}（{split_text}）"

        summary_text = f"{tuning_target_label} / {tuning_bias_label}: {note}"
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(row.get("market") or ""),
                "adaptive_strategy_active_market_profile": str(row.get("adaptive_strategy_active_market_profile") or ""),
                "adaptive_strategy_active_market_plan_summary": str(row.get("adaptive_strategy_active_market_plan_summary") or ""),
                "adaptive_strategy_active_market_regime_summary": str(row.get("adaptive_strategy_active_market_regime_summary") or ""),
                "adaptive_strategy_active_market_execution_summary": str(row.get("adaptive_strategy_active_market_execution_summary") or ""),
                "adaptive_strategy_market_profile_note": str(row.get("adaptive_strategy_market_profile_note") or ""),
                "market_profile_tuning_target": tuning_target,
                "market_profile_tuning_target_label": tuning_target_label,
                "market_profile_tuning_bias": tuning_bias,
                "market_profile_tuning_bias_label": tuning_bias_label,
                "market_profile_tuning_action": tuning_action,
                "market_profile_tuning_note": note,
                "market_profile_tuning_summary": summary_text,
                "strategy_control_weight_delta": float(strategy_delta),
                "risk_overlay_weight_delta": float(risk_delta),
                "execution_gate_blocked_weight": float(gate_weight),
                "execution_gate_blocked_order_ratio": float(gate_ratio),
                "execution_gate_blocked_order_count": int(blocked_edge_count),
                "risk_feedback_action": risk_action,
                "execution_feedback_action": execution_action,
            }
        )
    out.sort(
        key=lambda row: (
            0 if str(row.get("market_profile_tuning_bias") or "") == "TOO_TIGHT" else 1 if str(row.get("market_profile_tuning_bias") or "") == "CAN_RELAX" else 2,
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
        )
    )
    return out


def _filter_execution_metric_rows(
    rows: List[Dict[str, Any]],
    *,
    since_ts: str,
    portfolio_filter: str,
    market_filter: str,
    market_from_portfolio_or_symbol_fn: Callable[[str, str], str],
) -> List[Dict[str, Any]]:
    filtered_rows: List[Dict[str, Any]] = []
    for row in list(rows or []):
        if str(row.get("system_kind") or "").strip() not in {"investment", ""}:
            continue
        ts = str(row.get("ts") or "")
        if ts and ts < since_ts:
            continue
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if portfolio_filter and portfolio_id != portfolio_filter:
            continue
        market_code = market_from_portfolio_or_symbol_fn(portfolio_id, str(row.get("symbol") or ""))
        if market_filter and market_code and market_code != market_filter:
            continue
        filtered_rows.append(dict(row))
    return filtered_rows


def _apply_execution_broker_summary_context(
    broker_summary_rows: List[Dict[str, Any]],
    *,
    execution_effect_rows: List[Dict[str, Any]],
    planned_execution_cost_rows: List[Dict[str, Any]],
    edge_realization_rows: List[Dict[str, Any]],
) -> None:
    execution_effect_map = {str(row.get("portfolio_id") or ""): dict(row) for row in list(execution_effect_rows or [])}
    planned_execution_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in list(planned_execution_cost_rows or [])
    }
    edge_realization_map = {str(row.get("portfolio_id") or ""): dict(row) for row in list(edge_realization_rows or [])}
    for row in list(broker_summary_rows or []):
        portfolio_id = str(row.get("portfolio_id") or "")
        effect = dict(execution_effect_map.get(portfolio_id) or {})
        planned = dict(planned_execution_map.get(portfolio_id) or {})
        realized = dict(edge_realization_map.get(portfolio_id) or {})
        row["fill_count"] = int(effect.get("fill_count", 0) or 0)
        row["fill_notional"] = float(effect.get("fill_notional", 0.0) or 0.0)
        row["commission_total"] = float(effect.get("commission_total", 0.0) or 0.0)
        row["slippage_cost_total"] = float(effect.get("slippage_cost_total", 0.0) or 0.0)
        row["execution_cost_total"] = float(effect.get("execution_cost_total", 0.0) or 0.0)
        row["avg_actual_slippage_bps"] = effect.get("avg_actual_slippage_bps")
        row["avg_fill_delay_seconds"] = realized.get("avg_fill_delay_seconds")
        row["avg_realized_total_cost_bps"] = realized.get("avg_realized_total_cost_bps")
        row["avg_execution_capture_bps"] = realized.get("avg_execution_capture_bps")
        row["planned_cost_basis"] = str(planned.get("planned_cost_basis", "") or "")
        row["planned_order_rows"] = int(planned.get("planned_order_rows", 0) or 0)
        row["planned_order_value"] = float(planned.get("planned_order_value", 0.0) or 0.0)
        row["planned_spread_cost_total"] = float(planned.get("planned_spread_cost_total", 0.0) or 0.0)
        row["planned_slippage_cost_total"] = float(planned.get("planned_slippage_cost_total", 0.0) or 0.0)
        row["planned_commission_cost_total"] = float(planned.get("planned_commission_cost_total", 0.0) or 0.0)
        row["planned_execution_cost_total"] = float(planned.get("planned_execution_cost_total", 0.0) or 0.0)
        row["avg_expected_cost_bps"] = planned.get("avg_expected_cost_bps")
        row["execution_style_breakdown"] = str(planned.get("execution_style_breakdown", "") or "")
        row["execution_cost_gap"] = float(row["execution_cost_total"] - row["planned_execution_cost_total"])


def _build_execution_analysis_bundle(
    *,
    fill_rows: List[Dict[str, Any]],
    commission_rows: List[Dict[str, Any]],
    execution_order_rows: List[Dict[str, Any]],
    execution_run_rows: List[Dict[str, Any]],
    snapshot_rows: List[Dict[str, Any]],
    outcome_rows: List[Dict[str, Any]],
    broker_summary_rows: List[Dict[str, Any]],
    since_ts: str,
    portfolio_filter: str,
    market_filter: str,
    market_from_portfolio_or_symbol_fn: Callable[[str, str], str],
) -> Dict[str, Any]:
    filtered_fill_rows = _filter_execution_metric_rows(
        fill_rows,
        since_ts=since_ts,
        portfolio_filter=portfolio_filter,
        market_filter=market_filter,
        market_from_portfolio_or_symbol_fn=market_from_portfolio_or_symbol_fn,
    )
    filtered_commission_rows = _filter_execution_metric_rows(
        commission_rows,
        since_ts=since_ts,
        portfolio_filter=portfolio_filter,
        market_filter=market_filter,
        market_from_portfolio_or_symbol_fn=market_from_portfolio_or_symbol_fn,
    )
    execution_effect_rows = _build_execution_effect_rows(filtered_fill_rows, filtered_commission_rows)
    planned_execution_cost_rows = _build_planned_execution_cost_rows(execution_order_rows)
    execution_gate_rows = _build_execution_gate_rows(execution_order_rows)
    linked_execution_order_rows = _link_execution_orders_to_candidate_snapshots(
        execution_order_rows,
        execution_run_rows,
        snapshot_rows,
    )
    execution_parent_rows = _build_execution_parent_rows(
        linked_execution_order_rows,
        filtered_fill_rows,
        filtered_commission_rows,
        outcome_rows,
    )
    outcome_spread_rows = _build_weekly_outcome_spread_rows(
        snapshot_rows,
        outcome_rows,
        execution_parent_rows,
    )
    edge_realization_rows = _build_weekly_edge_realization_rows(execution_parent_rows)
    blocked_edge_attribution_rows = _build_weekly_blocked_edge_attribution_rows(execution_parent_rows)
    execution_session_rows = _build_execution_session_rows(
        execution_order_rows,
        filtered_fill_rows,
        filtered_commission_rows,
    )
    execution_hotspot_rows = _build_execution_hotspot_rows(
        execution_order_rows,
        filtered_fill_rows,
        filtered_commission_rows,
    )
    _apply_execution_broker_summary_context(
        broker_summary_rows,
        execution_effect_rows=execution_effect_rows,
        planned_execution_cost_rows=planned_execution_cost_rows,
        edge_realization_rows=edge_realization_rows,
    )
    return {
        "filtered_fill_rows": filtered_fill_rows,
        "filtered_commission_rows": filtered_commission_rows,
        "execution_effect_rows": execution_effect_rows,
        "planned_execution_cost_rows": planned_execution_cost_rows,
        "execution_gate_rows": execution_gate_rows,
        "linked_execution_order_rows": linked_execution_order_rows,
        "execution_parent_rows": execution_parent_rows,
        "outcome_spread_rows": outcome_spread_rows,
        "edge_realization_rows": edge_realization_rows,
        "blocked_edge_attribution_rows": blocked_edge_attribution_rows,
        "execution_session_rows": execution_session_rows,
        "execution_hotspot_rows": execution_hotspot_rows,
    }


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


def _calibration_patch_field_meta(field: str) -> Dict[str, Any]:
    return {
        "min_expected_edge_bps": {"field_label": "min expected edge", "step": 2.0, "bounds": (4.0, 60.0), "precision": 1},
        "edge_cost_buffer_bps": {"field_label": "edge cost buffer", "step": 1.0, "bounds": (1.0, 20.0), "precision": 1},
        "risk_budget_net_exposure": {"field_label": "risk budget net exposure", "step": 0.03, "bounds": (0.20, 1.00), "precision": 2},
        "risk_budget_gross_exposure": {"field_label": "risk budget gross exposure", "step": 0.03, "bounds": (0.25, 1.20), "precision": 2},
        "risk_budget_short_exposure": {"field_label": "risk budget short exposure", "step": 0.02, "bounds": (0.00, 0.50), "precision": 2},
        "risk_recovery_max_bonus": {"field_label": "risk recovery max bonus", "step": 0.01, "bounds": (0.00, 0.15), "precision": 2},
        "adv_max_participation_pct": {"field_label": "ADV max participation", "step": 0.005, "bounds": (0.005, 0.20), "precision": 3},
        "adv_split_trigger_pct": {"field_label": "ADV split trigger", "step": 0.005, "bounds": (0.002, 0.10), "precision": 3},
        "limit_price_buffer_bps": {"field_label": "limit price buffer", "step": 2.0, "bounds": (2.0, 40.0), "precision": 1},
        "max_slices_per_symbol": {"field_label": "max slices per symbol", "step": 1.0, "bounds": (1.0, 8.0), "precision": 0},
        "correlation_soft_limit": {"field_label": "correlation soft limit", "step": 0.03, "bounds": (0.30, 0.90), "precision": 2},
        "stress_loss_soft_limit": {"field_label": "stress loss soft limit", "step": 0.01, "bounds": (0.03, 0.20), "precision": 3},
        "portfolio_liquidity_soft_floor": {"field_label": "portfolio liquidity soft floor", "step": 0.03, "bounds": (0.10, 0.80), "precision": 2},
        "portfolio_atr_soft_limit": {"field_label": "portfolio ATR soft limit", "step": 0.005, "bounds": (0.02, 0.15), "precision": 3},
        "sector_concentration_soft_limit": {"field_label": "sector concentration soft limit", "step": 0.03, "bounds": (0.20, 0.80), "precision": 2},
        "market_sentiment_soft_floor": {"field_label": "market sentiment soft floor", "step": 0.05, "bounds": (-0.60, 0.10), "precision": 2},
    }.get(str(field or "").strip(), {})


def _calibration_patch_priority(scope: str, field: str) -> tuple[int, str]:
    scope_code = str(scope or "").strip().upper()
    field_name = str(field or "").strip()
    rankings: Dict[str, Dict[str, tuple[int, str]]] = {
        "EXECUTION_GATE": {
            "edge_cost_buffer_bps": (1, "先改低风险 buffer"),
            "min_expected_edge_bps": (2, "再改主门槛"),
        },
        "SLICING_RELAX": {
            "adv_split_trigger_pct": (1, "先调 split trigger"),
            "adv_max_participation_pct": (2, "再调 ADV 上限"),
            "limit_price_buffer_bps": (3, "最后调 limit buffer"),
        },
        "SLICING_TIGHTEN": {
            "adv_split_trigger_pct": (1, "先调 split trigger"),
            "adv_max_participation_pct": (2, "再调 ADV 上限"),
            "max_slices_per_symbol": (3, "必要时再抬 slices 上限"),
            "limit_price_buffer_bps": (4, "最后调 limit buffer"),
        },
        "RISK_BUDGET": {
            "risk_budget_net_exposure": (1, "先改 net budget"),
            "risk_budget_gross_exposure": (2, "再改 gross budget"),
            "risk_budget_short_exposure": (3, "最后改 short budget"),
        },
        "RISK_THROTTLE": {
            "correlation_soft_limit": (1, "先改相关性阈值"),
            "stress_loss_soft_limit": (1, "先改 stress 阈值"),
            "portfolio_liquidity_soft_floor": (1, "先改流动性阈值"),
            "portfolio_atr_soft_limit": (1, "先改波动率阈值"),
            "sector_concentration_soft_limit": (1, "先改集中度阈值"),
            "market_sentiment_soft_floor": (1, "先改情绪阈值"),
        },
        "RISK_RECOVERY": {
            "risk_recovery_max_bonus": (1, "先改 recovery bonus"),
        },
    }
    return rankings.get(scope_code, {}).get(field_name, (9, "后续再评估"))


def _calibration_patch_value(field: str, current_value: Any, change_hint: str) -> Any:
    meta = _calibration_patch_field_meta(field)
    if not meta:
        return current_value
    try:
        current = float(current_value)
    except Exception:
        return current_value
    step = float(meta.get("step", 0.0) or 0.0)
    lower, upper = meta.get("bounds", (-1e9, 1e9))
    precision = int(meta.get("precision", 4) or 4)
    direction = str(change_hint or "").strip().upper()
    if direction in {"RELAX_LOWER", "LOWER", "REDUCE"}:
        proposed = current - step
    elif direction in {"INCREASE", "HIGHER", "TIGHTEN_HIGHER"}:
        proposed = current + step
    else:
        proposed = current
    proposed = _clamp(float(proposed), float(lower), float(upper))
    if precision <= 0:
        return int(round(proposed))
    return round(float(proposed), precision)


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


def _weekly_tuning_history_trend_label(
    delta: float,
    *,
    threshold: float,
    improving_if_negative: bool = False,
) -> str:
    value = float(delta or 0.0)
    if improving_if_negative:
        if value <= -abs(float(threshold or 0.0)):
            return "IMPROVING"
        if value >= abs(float(threshold or 0.0)):
            return "WORSENING"
        return "STABLE"
    if value >= abs(float(threshold or 0.0)):
        return "IMPROVING"
    if value <= -abs(float(threshold or 0.0)):
        return "WORSENING"
    return "STABLE"


def _build_weekly_tuning_history_overview(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 6,
) -> List[Dict[str, Any]]:
    if not rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    for raw in list(rows or []):
        row = dict(raw)
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        market = resolve_market_code(str(row.get("market") or ""))
        if not portfolio_id or not market:
            continue
        history_rows = storage.get_recent_investment_weekly_tuning_history(
            market,
            portfolio_id=portfolio_id,
            limit=max(2, int(limit)),
        )
        if not history_rows:
            continue
        latest = dict(history_rows[0] or {})
        baseline = dict(history_rows[-1] or latest)
        driver_chain = " -> ".join(
            f"{str(item.get('week_label') or '')}:{str(item.get('dominant_driver') or '-')}"
            for item in reversed(history_rows)
        )
        tuning_action_chain = " -> ".join(
            f"{str(item.get('week_label') or '')}:{str(item.get('market_profile_tuning_action') or '-')}"
            for item in reversed(history_rows)
        )
        signal_quality_delta = float(latest.get("signal_quality_score", 0.0) or 0.0) - float(
            baseline.get("signal_quality_score", 0.0) or 0.0
        )
        execution_cost_gap_delta = float(latest.get("execution_cost_gap", 0.0) or 0.0) - float(
            baseline.get("execution_cost_gap", 0.0) or 0.0
        )
        blocked_weight_delta = float(latest.get("execution_gate_blocked_weight", 0.0) or 0.0) - float(
            baseline.get("execution_gate_blocked_weight", 0.0) or 0.0
        )
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "weeks_tracked": int(len(history_rows)),
                "latest_week_label": str(latest.get("week_label") or ""),
                "baseline_week_label": str(baseline.get("week_label") or ""),
                "latest_dominant_driver": str(latest.get("dominant_driver") or ""),
                "latest_market_profile_tuning_action": str(latest.get("market_profile_tuning_action") or ""),
                "latest_market_profile_ready_for_manual_apply": int(
                    latest.get("market_profile_ready_for_manual_apply", 0) or 0
                ),
                "driver_chain": driver_chain,
                "tuning_action_chain": tuning_action_chain,
                "latest_signal_quality_score": float(latest.get("signal_quality_score", 0.0) or 0.0),
                "baseline_signal_quality_score": float(baseline.get("signal_quality_score", 0.0) or 0.0),
                "signal_quality_delta": float(signal_quality_delta),
                "signal_quality_trend": _weekly_tuning_history_trend_label(signal_quality_delta, threshold=0.05),
                "latest_execution_cost_gap": float(latest.get("execution_cost_gap", 0.0) or 0.0),
                "baseline_execution_cost_gap": float(baseline.get("execution_cost_gap", 0.0) or 0.0),
                "execution_cost_gap_delta": float(execution_cost_gap_delta),
                "execution_cost_gap_trend": _weekly_tuning_history_trend_label(
                    execution_cost_gap_delta,
                    threshold=5.0,
                    improving_if_negative=True,
                ),
                "latest_execution_gate_blocked_weight": float(
                    latest.get("execution_gate_blocked_weight", 0.0) or 0.0
                ),
                "baseline_execution_gate_blocked_weight": float(
                    baseline.get("execution_gate_blocked_weight", 0.0) or 0.0
                ),
                "execution_gate_blocked_weight_delta": float(blocked_weight_delta),
                "execution_gate_pressure_trend": _weekly_tuning_history_trend_label(
                    blocked_weight_delta,
                    threshold=0.01,
                    improving_if_negative=True,
                ),
            }
        )
    out.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
        )
    )
    return out


def _patch_review_kind_label(kind: str) -> str:
    raw = str(kind or "").strip().lower()
    if raw == "market_profile":
        return "市场档案"
    if raw == "calibration":
        return "校准补丁"
    return raw or "-"


def _patch_review_week_start_dt(text: str) -> datetime | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _build_weekly_patch_governance_summary_rows(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 24,
) -> List[Dict[str, Any]]:
    if not rows:
        return []
    storage = Storage(str(db_path))
    cycle_rows: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for raw in list(rows or []):
        portfolio_id = str(raw.get("portfolio_id") or "").strip()
        market = resolve_market_code(str(raw.get("market") or ""))
        key = (market, portfolio_id)
        if not market or not portfolio_id or key in seen:
            continue
        seen.add(key)
        history_rows = storage.get_recent_investment_patch_review_history(
            market,
            portfolio_id=portfolio_id,
            limit=max(20, int(limit) * 8),
        )
        if not history_rows:
            continue
        grouped: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
        for item in list(history_rows or []):
            row = dict(item)
            patch_kind = str(row.get("patch_kind") or "").strip().lower()
            if not patch_kind:
                continue
            feedback_signature = str(row.get("feedback_signature") or "").strip()
            if not feedback_signature:
                details_json = dict(row.get("details_json") or {})
                primary_item = dict(details_json.get("primary_item") or {})
                feedback_signature = (
                    f"{patch_kind}|"
                    f"{str(primary_item.get('config_path') or row.get('config_path') or '').strip()}|"
                    f"{str(row.get('week_label') or '').strip()}"
                )
            grouped.setdefault((patch_kind, feedback_signature), []).append(row)
        for (patch_kind, _feedback_signature), cycle_events in grouped.items():
            cycle_events.sort(
                key=lambda item: (
                    str(item.get("week_start") or ""),
                    str(item.get("ts") or ""),
                    int(item.get("id", 0) or 0),
                )
            )
            first = dict(cycle_events[0] or {})
            latest = dict(cycle_events[-1] or {})
            first_details = dict(first.get("details_json") or {})
            latest_details = dict(latest.get("details_json") or {})
            latest_primary_item = dict(latest_details.get("primary_item") or first_details.get("primary_item") or {})
            config_path = str(latest_primary_item.get("config_path") or latest.get("config_path") or "").strip()
            field = str(latest_primary_item.get("field") or "").strip()
            if not field and config_path:
                field = config_path.split(".")[-1]
            scope_label = str(
                latest_primary_item.get("scope_label")
                or latest_primary_item.get("scope")
                or latest.get("scope")
                or "-"
            )
            applied_row = next(
                (
                    dict(item)
                    for item in cycle_events
                    if str(item.get("review_status") or "").strip().upper() == "APPLIED"
                ),
                {},
            )
            start_week = _patch_review_week_start_dt(str(first.get("week_start") or ""))
            applied_week = _patch_review_week_start_dt(str(applied_row.get("week_start") or ""))
            review_to_apply_weeks = None
            if start_week is not None and applied_week is not None:
                review_to_apply_weeks = round(max(0.0, (applied_week - start_week).days / 7.0), 2)
            latest_status = str(latest.get("review_status") or "").strip().upper()
            cycle_rows.append(
                {
                    "market": market,
                    "portfolio_id": portfolio_id,
                    "patch_kind": patch_kind,
                    "patch_kind_label": _patch_review_kind_label(patch_kind),
                    "field": field or "-",
                    "scope_label": scope_label,
                    "latest_week_label": str(latest.get("week_label") or "-"),
                    "latest_ts": str(latest.get("ts") or ""),
                    "latest_status": latest_status,
                    "latest_status_label": str(latest.get("review_status_label") or latest_status or "-"),
                    "approved": any(
                        str(item.get("review_status") or "").strip().upper() == "APPROVED"
                        for item in cycle_events
                    ),
                    "rejected": any(
                        str(item.get("review_status") or "").strip().upper() == "REJECTED"
                        for item in cycle_events
                    ),
                    "applied": bool(applied_row),
                    "approved_not_applied": latest_status == "APPROVED" and not bool(applied_row),
                    "open_cycle": latest_status not in {"APPLIED", "REJECTED", "CLEAR"},
                    "review_to_apply_weeks": review_to_apply_weeks,
                }
            )
    grouped_rows: Dict[tuple[str, str, str, str], Dict[str, Any]] = {}
    for cycle in cycle_rows:
        key = (
            str(cycle.get("market") or ""),
            str(cycle.get("patch_kind") or ""),
            str(cycle.get("field") or ""),
            str(cycle.get("scope_label") or ""),
        )
        agg = grouped_rows.get(key)
        if agg is None:
            agg = {
                "market": str(cycle.get("market") or ""),
                "patch_kind_label": str(cycle.get("patch_kind_label") or "-"),
                "field": str(cycle.get("field") or "-"),
                "scope_label": str(cycle.get("scope_label") or "-"),
                "review_cycle_count": 0,
                "approved_count": 0,
                "rejected_count": 0,
                "applied_count": 0,
                "approved_not_applied_count": 0,
                "open_cycle_count": 0,
                "review_to_apply_weeks_values": [],
                "latest_ts": "",
                "latest_week_label": "-",
                "latest_status_label": "-",
                "examples": [],
            }
            grouped_rows[key] = agg
        agg["review_cycle_count"] += 1
        if bool(cycle.get("approved", False)):
            agg["approved_count"] += 1
        if bool(cycle.get("rejected", False)):
            agg["rejected_count"] += 1
        if bool(cycle.get("applied", False)):
            agg["applied_count"] += 1
        if bool(cycle.get("approved_not_applied", False)):
            agg["approved_not_applied_count"] += 1
        if bool(cycle.get("open_cycle", False)):
            agg["open_cycle_count"] += 1
        if cycle.get("review_to_apply_weeks") is not None:
            agg["review_to_apply_weeks_values"].append(float(cycle["review_to_apply_weeks"]))
        latest_ts = str(cycle.get("latest_ts") or "")
        if latest_ts >= str(agg.get("latest_ts") or ""):
            agg["latest_ts"] = latest_ts
            agg["latest_week_label"] = str(cycle.get("latest_week_label") or "-")
            agg["latest_status_label"] = str(cycle.get("latest_status_label") or "-")
        example = f"{str(cycle.get('portfolio_id') or '-') or '-'}:{str(cycle.get('latest_status_label') or '-')}"
        if example not in agg["examples"]:
            agg["examples"].append(example)
    out: List[Dict[str, Any]] = []
    for agg in grouped_rows.values():
        review_cycle_count = max(1, int(agg.get("review_cycle_count", 0) or 0))
        review_to_apply_values = list(agg.get("review_to_apply_weeks_values") or [])
        out.append(
            {
                "market": str(agg.get("market") or ""),
                "patch_kind_label": str(agg.get("patch_kind_label") or "-"),
                "field": str(agg.get("field") or "-"),
                "scope_label": str(agg.get("scope_label") or "-"),
                "review_cycle_count": review_cycle_count,
                "approved_count": int(agg.get("approved_count", 0) or 0),
                "rejected_count": int(agg.get("rejected_count", 0) or 0),
                "applied_count": int(agg.get("applied_count", 0) or 0),
                "approved_not_applied_count": int(agg.get("approved_not_applied_count", 0) or 0),
                "open_cycle_count": int(agg.get("open_cycle_count", 0) or 0),
                "approval_rate": round(float(agg.get("approved_count", 0) or 0) / review_cycle_count, 4),
                "rejection_rate": round(float(agg.get("rejected_count", 0) or 0) / review_cycle_count, 4),
                "apply_rate": round(float(agg.get("applied_count", 0) or 0) / review_cycle_count, 4),
                "avg_review_to_apply_weeks": (
                    round(sum(review_to_apply_values) / len(review_to_apply_values), 2)
                    if review_to_apply_values
                    else None
                ),
                "review_latency_basis": "review_to_apply",
                "latest_week_label": str(agg.get("latest_week_label") or "-"),
                "latest_status_label": str(agg.get("latest_status_label") or "-"),
                "examples": " / ".join(list(agg.get("examples") or [])[:3]) or "-",
            }
        )
    out.sort(
        key=lambda row: (
            -int(row.get("open_cycle_count", 0) or 0),
            -int(row.get("approved_not_applied_count", 0) or 0),
            -int(row.get("review_cycle_count", 0) or 0),
            str(row.get("market") or ""),
            str(row.get("patch_kind_label") or ""),
            str(row.get("field") or ""),
        )
    )
    return out[:24]


def _build_weekly_control_timeseries_rows(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 12,
) -> List[Dict[str, Any]]:
    if not rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for raw in list(rows or []):
        portfolio_id = str(raw.get("portfolio_id") or "").strip()
        market = resolve_market_code(str(raw.get("market") or ""))
        key = (market, portfolio_id)
        if not market or not portfolio_id or key in seen:
            continue
        seen.add(key)
        history_rows = storage.get_recent_investment_weekly_tuning_history(
            market,
            portfolio_id=portfolio_id,
            limit=max(2, int(limit)),
        )
        for item in reversed(list(history_rows or [])):
            strategy_delta = _safe_float(item.get("strategy_control_weight_delta"), 0.0)
            risk_delta = _safe_float(item.get("risk_overlay_weight_delta"), 0.0)
            execution_delta = _safe_float(item.get("execution_gate_blocked_weight"), 0.0)
            total_delta = float(strategy_delta + risk_delta + execution_delta)
            out.append(
                {
                    "portfolio_id": portfolio_id,
                    "market": market,
                    "week_label": str(item.get("week_label") or ""),
                    "week_start": str(item.get("week_start") or ""),
                    "weekly_return": _safe_float(item.get("weekly_return"), 0.0),
                    "signal_quality_score": _safe_float(item.get("signal_quality_score"), 0.0),
                    "execution_cost_gap": _safe_float(item.get("execution_cost_gap"), 0.0),
                    "strategy_control_weight_delta": float(strategy_delta),
                    "risk_overlay_weight_delta": float(risk_delta),
                    "execution_gate_blocked_weight": float(execution_delta),
                    "control_total_weight": float(total_delta),
                    "strategy_control_share": float(strategy_delta / total_delta) if total_delta > 0.0 else 0.0,
                    "risk_overlay_share": float(risk_delta / total_delta) if total_delta > 0.0 else 0.0,
                    "execution_gate_share": float(execution_delta / total_delta) if total_delta > 0.0 else 0.0,
                    "dominant_driver": str(item.get("dominant_driver") or ""),
                }
            )
    out.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
            str(row.get("week_start") or row.get("week_label") or ""),
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


def _execution_order_status_bucket(row: Dict[str, Any]) -> str:
    status = str(row.get("status") or "").strip().upper()
    broker_order_id = _safe_int(row.get("broker_order_id"), 0)
    if status == "BLOCKED_EDGE":
        return "BLOCKED_EDGE"
    if _is_execution_gate_status(status):
        return "BLOCKED_GATE"
    if broker_order_id > 0 or status in {
        "CREATED",
        "SUBMITTED",
        "PRESUBMITTED",
        "FILLED",
        "PARTIAL",
        "PARTIALLY_FILLED",
    } or status.startswith("ERROR_"):
        return "SUBMITTED"
    return "PLANNED"


def _order_edge_metrics(row: Dict[str, Any]) -> Dict[str, Any]:
    details = _parse_json_dict(row.get("details"))
    plan_row = dict(details.get("plan_row") or {}) if isinstance(details.get("plan_row"), dict) else {}

    def _pick(key: str, default: Any = 0.0) -> Any:
        direct = row.get(key)
        if direct not in (None, ""):
            return direct
        nested = details.get(key)
        if nested not in (None, ""):
            return nested
        plan_val = plan_row.get(key)
        if plan_val not in (None, ""):
            return plan_val
        return default

    return {
        "parent_order_key": str(_pick("parent_order_key", "") or ""),
        "score_before_cost": _safe_float(_pick("score_before_cost", _pick("score", 0.0)), 0.0),
        "expected_cost_bps": _safe_float(_pick("expected_cost_bps", 0.0), 0.0),
        "expected_edge_threshold": _safe_float(_pick("expected_edge_threshold", 0.0), 0.0),
        "expected_edge_score": _safe_float(_pick("expected_edge_score", 0.0), 0.0),
        "expected_edge_bps": _safe_float(_pick("expected_edge_bps", 0.0), 0.0),
        "edge_gate_threshold_bps": _safe_float(_pick("edge_gate_threshold_bps", 0.0), 0.0),
        "session_bucket": str(_pick("session_bucket", "") or ""),
        "session_label": str(_pick("session_label", "") or ""),
        "execution_style": str(_pick("execution_style", "") or ""),
    }


def _order_execution_microstructure(row: Dict[str, Any]) -> Dict[str, Any]:
    details = _parse_json_dict(row.get("details"))
    plan_row = dict(details.get("plan_row") or {}) if isinstance(details.get("plan_row"), dict) else {}

    def _pick(key: str, default: Any = "") -> Any:
        direct = row.get(key)
        if direct not in (None, ""):
            return direct
        nested = details.get(key)
        if nested not in (None, ""):
            return nested
        plan_val = plan_row.get(key)
        if plan_val not in (None, ""):
            return plan_val
        return default

    return {
        "dynamic_liquidity_bucket": str(_pick("dynamic_liquidity_bucket", "") or "").strip().upper(),
        "dynamic_order_adv_pct": _safe_float(_pick("dynamic_order_adv_pct", 0.0), 0.0),
        "slice_count": max(1, _safe_int(_pick("slice_count", 1), 1)),
        "market_rule_status": str(_pick("market_rule_status", "") or "").strip().upper(),
    }


def _enrich_snapshot_rows(snapshot_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    for raw in list(snapshot_rows or []):
        row = dict(raw)
        details = _parse_json_dict(row.get("details"))
        stage = str(row.get("stage") or details.get("stage") or "").strip().lower()
        row["details_json"] = details
        row["stage"] = stage
        row["analysis_run_id"] = str(row.get("analysis_run_id") or "").strip()
        row["report_dir"] = str(row.get("report_dir") or "").strip()
        row["stage_rank"] = _safe_int(details.get("stage_rank"), 0)
        row["stage1_rank"] = _safe_int(details.get("stage1_rank"), 0)
        row["expected_edge_threshold"] = _safe_float(
            row.get("expected_edge_threshold", details.get("expected_edge_threshold", 0.0)),
            0.0,
        )
        row["expected_edge_score"] = _safe_float(
            row.get("expected_edge_score", details.get("expected_edge_score", 0.0)),
            0.0,
        )
        row["expected_edge_bps"] = _safe_float(
            row.get("expected_edge_bps", details.get("expected_edge_bps", 0.0)),
            0.0,
        )
        enriched.append(row)
    return enriched


def _link_execution_orders_to_candidate_snapshots(
    execution_orders: List[Dict[str, Any]],
    execution_runs: List[Dict[str, Any]],
    snapshot_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    run_meta = {
        str(row.get("run_id") or ""): {
            "report_dir": str(row.get("report_dir") or "").strip(),
            "portfolio_id": str(row.get("portfolio_id") or "").strip(),
            "market": str(row.get("market") or "").strip(),
        }
        for row in list(execution_runs or [])
        if str(row.get("run_id") or "").strip()
    }
    enriched_snapshots = _enrich_snapshot_rows(snapshot_rows)
    snapshots_by_key: Dict[tuple[str, str, str, str], List[Dict[str, Any]]] = {}
    snapshots_by_symbol: Dict[tuple[str, str, str], List[Dict[str, Any]]] = {}
    for row in enriched_snapshots:
        report_dir = str(row.get("report_dir") or "").strip()
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        symbol = str(row.get("symbol") or "").upper().strip()
        stage = str(row.get("stage") or "").strip().lower()
        if not report_dir or not portfolio_id or not symbol:
            continue
        snapshots_by_key.setdefault((report_dir, portfolio_id, symbol, stage), []).append(row)
        snapshots_by_symbol.setdefault((report_dir, portfolio_id, symbol), []).append(row)
    for rows in snapshots_by_key.values():
        rows.sort(
            key=lambda item: (
                -_candidate_snapshot_stage_priority(str(item.get("stage") or "")),
                _safe_int(item.get("stage_rank"), 10**6),
                str(item.get("ts") or ""),
            )
        )
    for rows in snapshots_by_symbol.values():
        rows.sort(
            key=lambda item: (
                -_candidate_snapshot_stage_priority(str(item.get("stage") or "")),
                _safe_int(item.get("stage_rank"), 10**6),
                str(item.get("ts") or ""),
            )
        )

    linked: List[Dict[str, Any]] = []
    for raw in list(execution_orders or []):
        row = dict(raw)
        metrics = _order_edge_metrics(row)
        row.update(metrics)
        run_id = str(row.get("run_id") or "").strip()
        meta = dict(run_meta.get(run_id) or {})
        report_dir = str(meta.get("report_dir") or "").strip()
        portfolio_id = str(row.get("portfolio_id") or meta.get("portfolio_id") or "").strip()
        symbol = str(row.get("symbol") or "").upper().strip()
        linked_snapshot: Dict[str, Any] = {}
        if report_dir and portfolio_id and symbol:
            for stage in _preferred_snapshot_stages_for_order(row):
                candidates = snapshots_by_key.get((report_dir, portfolio_id, symbol, stage), [])
                if candidates:
                    linked_snapshot = dict(candidates[0])
                    break
            if not linked_snapshot:
                fallback_rows = snapshots_by_symbol.get((report_dir, portfolio_id, symbol), [])
                if fallback_rows:
                    linked_snapshot = dict(fallback_rows[0])
        row["linked_report_dir"] = report_dir
        row["linked_snapshot_id"] = str(linked_snapshot.get("snapshot_id") or "")
        row["linked_snapshot_stage"] = str(linked_snapshot.get("stage") or "")
        row["linked_snapshot_stage_rank"] = _safe_int(linked_snapshot.get("stage_rank"), 0)
        row["linked_snapshot_ts"] = str(linked_snapshot.get("ts") or linked_snapshot.get("snapshot_ts") or "")
        row["linked_analysis_run_id"] = str(linked_snapshot.get("analysis_run_id") or "")
        linked.append(row)
    return linked


def _build_execution_parent_rows(
    execution_orders: List[Dict[str, Any]],
    fill_rows: List[Dict[str, Any]],
    commission_rows: List[Dict[str, Any]],
    outcome_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    commission_by_exec: Dict[str, float] = {}
    for row in list(commission_rows or []):
        exec_id = str(row.get("exec_id") or "").strip()
        if exec_id:
            commission_by_exec[exec_id] = float(commission_by_exec.get(exec_id, 0.0) or 0.0) + _safe_float(row.get("value"), 0.0)

    fills_by_order: Dict[int, List[Dict[str, Any]]] = {}
    for raw in list(fill_rows or []):
        order_id = _safe_int(raw.get("order_id"), 0)
        if order_id <= 0:
            continue
        fills_by_order.setdefault(order_id, []).append(dict(raw))
    for rows in fills_by_order.values():
        rows.sort(key=lambda item: str(item.get("ts") or ""))

    outcomes_by_snapshot: Dict[str, Dict[int, Dict[str, Any]]] = {}
    for raw in list(outcome_rows or []):
        snapshot_id = str(raw.get("snapshot_id") or "").strip()
        horizon_days = _safe_int(raw.get("horizon_days"), 0)
        if not snapshot_id or horizon_days <= 0:
            continue
        outcomes_by_snapshot.setdefault(snapshot_id, {})[horizon_days] = dict(raw)

    grouped: Dict[tuple[str, str], Dict[str, Any]] = {}
    for idx, raw in enumerate(list(execution_orders or []), start=1):
        row = dict(raw)
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        parent_order_key = str(row.get("parent_order_key") or "").strip()
        if not parent_order_key:
            parent_order_key = f"{str(row.get('run_id') or '')}:{str(row.get('linked_snapshot_id') or '')}:{str(row.get('symbol') or '')}:{idx}"
        key = (portfolio_id, parent_order_key)
        bucket = grouped.setdefault(
            key,
            {
                "portfolio_id": portfolio_id,
                "market": str(row.get("market") or _market_from_portfolio_or_symbol(portfolio_id, str(row.get("symbol") or ""))),
                "run_id": str(row.get("run_id") or ""),
                "parent_order_key": parent_order_key,
                "symbol": str(row.get("symbol") or "").upper(),
                "action": str(row.get("action") or ""),
                "linked_snapshot_id": str(row.get("linked_snapshot_id") or ""),
                "linked_snapshot_stage": str(row.get("linked_snapshot_stage") or ""),
                "linked_analysis_run_id": str(row.get("linked_analysis_run_id") or ""),
                "linked_report_dir": str(row.get("linked_report_dir") or ""),
                "order_row_count": 0,
                "order_value": 0.0,
                "expected_edge_bps_numerator": 0.0,
                "expected_cost_bps_numerator": 0.0,
                "edge_gate_threshold_bps_numerator": 0.0,
                "score_before_cost_numerator": 0.0,
                "expected_edge_score_numerator": 0.0,
                "dynamic_order_adv_pct_numerator": 0.0,
                "submitted_ts": "",
                "slice_count_max": 1,
                "blocked_market_rule_order_count": 0,
                "blocked_edge_order_count": 0,
                "blocked_gate_order_count": 0,
                "statuses": set(),
                "broker_order_ids": set(),
                "dynamic_liquidity_bucket_value_map": {},
                "market_rule_statuses": set(),
            },
        )
        order_value = abs(_safe_float(row.get("order_value"), 0.0))
        status_bucket = _execution_order_status_bucket(row)
        micro = _order_execution_microstructure(row)
        bucket["order_row_count"] = int(bucket["order_row_count"]) + 1
        bucket["order_value"] = float(bucket["order_value"]) + order_value
        bucket["expected_edge_bps_numerator"] = float(bucket["expected_edge_bps_numerator"]) + order_value * _safe_float(row.get("expected_edge_bps"), 0.0)
        bucket["expected_cost_bps_numerator"] = float(bucket["expected_cost_bps_numerator"]) + order_value * _safe_float(row.get("expected_cost_bps"), 0.0)
        bucket["edge_gate_threshold_bps_numerator"] = float(bucket["edge_gate_threshold_bps_numerator"]) + order_value * _safe_float(row.get("edge_gate_threshold_bps"), 0.0)
        bucket["score_before_cost_numerator"] = float(bucket["score_before_cost_numerator"]) + order_value * _safe_float(row.get("score_before_cost"), 0.0)
        bucket["expected_edge_score_numerator"] = float(bucket["expected_edge_score_numerator"]) + order_value * _safe_float(row.get("expected_edge_score"), 0.0)
        bucket["dynamic_order_adv_pct_numerator"] = float(bucket["dynamic_order_adv_pct_numerator"]) + order_value * _safe_float(micro.get("dynamic_order_adv_pct"), 0.0)
        bucket["slice_count_max"] = max(int(bucket.get("slice_count_max", 1) or 1), int(micro.get("slice_count", 1) or 1))
        bucket["statuses"].add(status_bucket)
        if status_bucket == "BLOCKED_GATE":
            bucket["blocked_gate_order_count"] = int(bucket.get("blocked_gate_order_count", 0) or 0) + 1
        if str(row.get("status") or "").strip().upper() == "BLOCKED_MARKET_RULE":
            bucket["blocked_market_rule_order_count"] = int(bucket.get("blocked_market_rule_order_count", 0) or 0) + 1
        if status_bucket == "BLOCKED_EDGE":
            bucket["blocked_edge_order_count"] = int(bucket.get("blocked_edge_order_count", 0) or 0) + 1
        bucket_name = str(micro.get("dynamic_liquidity_bucket") or "").strip().upper()
        if bucket_name:
            bucket_value_map = dict(bucket.get("dynamic_liquidity_bucket_value_map") or {})
            bucket_value_map[bucket_name] = float(bucket_value_map.get(bucket_name, 0.0) or 0.0) + float(order_value)
            bucket["dynamic_liquidity_bucket_value_map"] = bucket_value_map
        market_rule_status = str(micro.get("market_rule_status") or "").strip().upper()
        if market_rule_status:
            cast_rule_statuses = set(bucket.get("market_rule_statuses") or set())
            cast_rule_statuses.add(market_rule_status)
            bucket["market_rule_statuses"] = cast_rule_statuses
        broker_order_id = _safe_int(row.get("broker_order_id"), 0)
        if broker_order_id > 0:
            cast_ids = set(bucket.get("broker_order_ids") or set())
            cast_ids.add(broker_order_id)
            bucket["broker_order_ids"] = cast_ids
        if status_bucket == "SUBMITTED":
            row_ts = str(row.get("ts") or "")
            current_ts = str(bucket.get("submitted_ts") or "")
            if row_ts and (not current_ts or row_ts < current_ts):
                bucket["submitted_ts"] = row_ts
        if not bucket.get("linked_snapshot_id") and str(row.get("linked_snapshot_id") or "").strip():
            bucket["linked_snapshot_id"] = str(row.get("linked_snapshot_id") or "")
            bucket["linked_snapshot_stage"] = str(row.get("linked_snapshot_stage") or "")
            bucket["linked_analysis_run_id"] = str(row.get("linked_analysis_run_id") or "")
            bucket["linked_report_dir"] = str(row.get("linked_report_dir") or "")

    parent_rows: List[Dict[str, Any]] = []
    for bucket in grouped.values():
        broker_order_ids = sorted(int(v) for v in list(bucket.get("broker_order_ids") or set()))
        order_fills: List[Dict[str, Any]] = []
        for broker_order_id in broker_order_ids:
            order_fills.extend(list(fills_by_order.get(broker_order_id, []) or []))
        order_fills.sort(key=lambda item: str(item.get("ts") or ""))
        fill_notional = 0.0
        slippage_cost_total = 0.0
        commission_total = 0.0
        slippage_samples: List[float] = []
        fill_delay_samples: List[float] = []
        for fill in order_fills:
            notional = abs(_safe_float(fill.get("qty"), 0.0)) * abs(_safe_float(fill.get("price"), 0.0))
            fill_notional += notional
            actual_slippage_bps = fill.get("actual_slippage_bps")
            if actual_slippage_bps not in (None, ""):
                slip = _safe_float(actual_slippage_bps, 0.0)
                slippage_samples.append(slip)
                slippage_cost_total += notional * slip / 10000.0
            commission_total += _safe_float(commission_by_exec.get(str(fill.get("exec_id") or "").strip()), 0.0)
            fill_delay = fill.get("fill_delay_seconds")
            if fill_delay not in (None, ""):
                fill_delay_samples.append(_safe_float(fill_delay, 0.0))
        execution_cost_total = float(slippage_cost_total + commission_total)
        realized_total_cost_bps = float(execution_cost_total / fill_notional * 10000.0) if fill_notional > 0.0 else None
        avg_actual_slippage_bps = float(slippage_cost_total / fill_notional * 10000.0) if fill_notional > 0.0 else None
        first_fill_ts = str(order_fills[0].get("ts") or "") if order_fills else ""
        last_fill_ts = str(order_fills[-1].get("ts") or "") if order_fills else ""
        first_fill_delay_seconds = (
            min(fill_delay_samples)
            if fill_delay_samples
            else _seconds_between(bucket.get("submitted_ts"), first_fill_ts)
        )
        statuses = set(bucket.get("statuses") or set())
        status_bucket = "PLANNED"
        if order_fills:
            status_bucket = "FILLED"
        elif "SUBMITTED" in statuses:
            status_bucket = "SUBMITTED"
        elif "BLOCKED_EDGE" in statuses:
            status_bucket = "BLOCKED_EDGE"
        elif "BLOCKED_GATE" in statuses:
            status_bucket = "BLOCKED_GATE"
        expected_edge_bps = (
            float(bucket.get("expected_edge_bps_numerator", 0.0) or 0.0) / float(bucket.get("order_value", 0.0) or 1.0)
            if float(bucket.get("order_value", 0.0) or 0.0) > 0.0 else 0.0
        )
        expected_cost_bps = (
            float(bucket.get("expected_cost_bps_numerator", 0.0) or 0.0) / float(bucket.get("order_value", 0.0) or 1.0)
            if float(bucket.get("order_value", 0.0) or 0.0) > 0.0 else 0.0
        )
        edge_gate_threshold_bps = (
            float(bucket.get("edge_gate_threshold_bps_numerator", 0.0) or 0.0) / float(bucket.get("order_value", 0.0) or 1.0)
            if float(bucket.get("order_value", 0.0) or 0.0) > 0.0 else 0.0
        )
        score_before_cost = (
            float(bucket.get("score_before_cost_numerator", 0.0) or 0.0) / float(bucket.get("order_value", 0.0) or 1.0)
            if float(bucket.get("order_value", 0.0) or 0.0) > 0.0 else 0.0
        )
        expected_edge_score = (
            float(bucket.get("expected_edge_score_numerator", 0.0) or 0.0) / float(bucket.get("order_value", 0.0) or 1.0)
            if float(bucket.get("order_value", 0.0) or 0.0) > 0.0 else 0.0
        )
        avg_dynamic_order_adv_pct = (
            float(bucket.get("dynamic_order_adv_pct_numerator", 0.0) or 0.0) / float(bucket.get("order_value", 0.0) or 1.0)
            if float(bucket.get("order_value", 0.0) or 0.0) > 0.0 else 0.0
        )
        liquidity_bucket_value_map = dict(bucket.get("dynamic_liquidity_bucket_value_map") or {})
        dominant_liquidity_bucket = ""
        if liquidity_bucket_value_map:
            dominant_liquidity_bucket = max(
                liquidity_bucket_value_map.items(),
                key=lambda item: (float(item[1] or 0.0), str(item[0] or "")),
            )[0]
        row = {
            "portfolio_id": str(bucket.get("portfolio_id") or ""),
            "market": str(bucket.get("market") or ""),
            "run_id": str(bucket.get("run_id") or ""),
            "parent_order_key": str(bucket.get("parent_order_key") or ""),
            "symbol": str(bucket.get("symbol") or ""),
            "action": str(bucket.get("action") or ""),
            "linked_snapshot_id": str(bucket.get("linked_snapshot_id") or ""),
            "linked_snapshot_stage": str(bucket.get("linked_snapshot_stage") or ""),
            "linked_analysis_run_id": str(bucket.get("linked_analysis_run_id") or ""),
            "linked_report_dir": str(bucket.get("linked_report_dir") or ""),
            "status_bucket": status_bucket,
            "order_row_count": int(bucket.get("order_row_count", 0) or 0),
            "order_value": float(bucket.get("order_value", 0.0) or 0.0),
            "score_before_cost": float(score_before_cost),
            "expected_edge_score": float(expected_edge_score),
            "expected_edge_bps": float(expected_edge_bps),
            "expected_cost_bps": float(expected_cost_bps),
            "edge_gate_threshold_bps": float(edge_gate_threshold_bps),
            "required_edge_gap_bps": max(0.0, float(edge_gate_threshold_bps) - float(expected_edge_bps)),
            "expected_edge_value": float(float(bucket.get("order_value", 0.0) or 0.0) * float(expected_edge_bps) / 10000.0),
            "blocked_market_rule_order_count": int(bucket.get("blocked_market_rule_order_count", 0) or 0),
            "blocked_edge_order_count": int(bucket.get("blocked_edge_order_count", 0) or 0),
            "blocked_gate_order_count": int(bucket.get("blocked_gate_order_count", 0) or 0),
            "dynamic_liquidity_bucket": str(dominant_liquidity_bucket),
            "avg_dynamic_order_adv_pct": float(avg_dynamic_order_adv_pct),
            "slice_count": int(bucket.get("slice_count_max", 1) or 1),
            "market_rule_statuses": ",".join(sorted(str(item) for item in list(bucket.get("market_rule_statuses") or set()) if str(item).strip())),
            "submitted_ts": str(bucket.get("submitted_ts") or ""),
            "fill_count": int(len(order_fills)),
            "fill_notional": float(fill_notional),
            "commission_total": float(commission_total),
            "slippage_cost_total": float(slippage_cost_total),
            "execution_cost_total": float(execution_cost_total),
            "avg_actual_slippage_bps": avg_actual_slippage_bps,
            "avg_realized_total_cost_bps": realized_total_cost_bps,
            "execution_capture_bps": (
                float(expected_edge_bps) - float(realized_total_cost_bps)
                if realized_total_cost_bps is not None
                else None
            ),
            "first_fill_ts": first_fill_ts,
            "last_fill_ts": last_fill_ts,
            "first_fill_delay_seconds": first_fill_delay_seconds,
            "median_fill_delay_seconds": _median(fill_delay_samples),
        }
        for horizon_days in (5, 20, 60):
            outcome = dict(outcomes_by_snapshot.get(str(bucket.get("linked_snapshot_id") or ""), {}).get(horizon_days) or {})
            future_return_bps = (
                float(_safe_float(outcome.get("future_return"), 0.0) * 10000.0)
                if outcome else None
            )
            row[f"outcome_{horizon_days}d_future_return_bps"] = future_return_bps
            row[f"outcome_{horizon_days}d_counterfactual_edge_bps"] = (
                future_return_bps - float(expected_cost_bps)
                if future_return_bps is not None
                else None
            )
            row[f"outcome_{horizon_days}d_realized_edge_bps"] = (
                future_return_bps - float(realized_total_cost_bps)
                if future_return_bps is not None and realized_total_cost_bps is not None
                else None
            )
        row["realized_slippage_bps"] = row.get("avg_actual_slippage_bps")
        row["realized_edge_bps"] = (
            row.get("outcome_20d_realized_edge_bps")
            if row.get("outcome_20d_realized_edge_bps") not in (None, "")
            else row.get("execution_capture_bps")
        )
        parent_rows.append(row)
    parent_rows.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
            str(row.get("parent_order_key") or ""),
        )
    )
    return parent_rows


def _avg_bps(rows: List[Dict[str, Any]], key: str) -> float | None:
    values = [row.get(key) for row in list(rows or []) if row.get(key) not in (None, "")]
    return _avg_defined(values)


def _build_weekly_outcome_spread_rows(
    snapshot_rows: List[Dict[str, Any]],
    outcome_rows: List[Dict[str, Any]],
    execution_parent_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    snapshots = {str(row.get("snapshot_id") or ""): row for row in _enrich_snapshot_rows(snapshot_rows) if str(row.get("snapshot_id") or "").strip()}
    status_by_snapshot: Dict[str, str] = {}
    precedence = {"FILLED": 4, "SUBMITTED": 3, "BLOCKED_EDGE": 2, "BLOCKED_GATE": 1, "PLANNED": 0}
    for row in list(execution_parent_rows or []):
        snapshot_id = str(row.get("linked_snapshot_id") or "").strip()
        status = str(row.get("status_bucket") or "").strip().upper() or "PLANNED"
        if not snapshot_id:
            continue
        current = str(status_by_snapshot.get(snapshot_id) or "").strip().upper()
        if precedence.get(status, -1) >= precedence.get(current, -1):
            status_by_snapshot[snapshot_id] = status

    deduped: Dict[tuple[str, str, str, str, int], Dict[str, Any]] = {}
    for raw in list(outcome_rows or []):
        snapshot_id = str(raw.get("snapshot_id") or "").strip()
        snapshot = dict(snapshots.get(snapshot_id) or {})
        if not snapshot:
            continue
        analysis_run_id = str(snapshot.get("analysis_run_id") or "").strip()
        symbol = str(raw.get("symbol") or snapshot.get("symbol") or "").upper().strip()
        direction = str(raw.get("direction") or snapshot.get("direction") or "LONG").upper().strip()
        horizon_days = _safe_int(raw.get("horizon_days"), 0)
        if not analysis_run_id or not symbol or horizon_days <= 0:
            continue
        enriched = dict(raw)
        enriched["analysis_run_id"] = analysis_run_id
        enriched["report_dir"] = str(snapshot.get("report_dir") or "")
        enriched["stage"] = str(snapshot.get("stage") or "")
        enriched["stage_rank"] = _safe_int(snapshot.get("stage_rank"), 0)
        enriched["stage1_rank"] = _safe_int(snapshot.get("stage1_rank"), 0)
        enriched["score"] = _safe_float(snapshot.get("score"), 0.0)
        enriched["score_before_cost"] = _safe_float(snapshot.get("score_before_cost"), _safe_float(snapshot.get("score"), 0.0))
        enriched["expected_cost_bps"] = _safe_float(snapshot.get("expected_cost_bps"), 0.0)
        enriched["expected_edge_bps"] = _safe_float(snapshot.get("expected_edge_bps"), 0.0)
        enriched["selected"] = int(_is_selected_snapshot_stage(str(snapshot.get("stage") or "")))
        enriched["execution_status"] = str(status_by_snapshot.get(snapshot_id) or "PLANNED")
        key = (
            str(raw.get("portfolio_id") or ""),
            analysis_run_id,
            symbol,
            direction,
            horizon_days,
        )
        current = dict(deduped.get(key) or {})
        if not current:
            deduped[key] = enriched
            continue
        current_priority = _candidate_snapshot_stage_priority(str(current.get("stage") or ""))
        next_priority = _candidate_snapshot_stage_priority(str(enriched.get("stage") or ""))
        if next_priority > current_priority:
            deduped[key] = enriched
            continue
        if next_priority == current_priority and _safe_int(enriched.get("stage_rank"), 10**6) < _safe_int(current.get("stage_rank"), 10**6):
            deduped[key] = enriched

    top_rank_cutoff: Dict[tuple[str, str], int] = {}
    grouped_selected: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for row in deduped.values():
        if int(row.get("selected", 0) or 0) != 1:
            continue
        key = (str(row.get("portfolio_id") or ""), str(row.get("analysis_run_id") or ""))
        grouped_selected.setdefault(key, []).append(row)
    for key, rows in grouped_selected.items():
        count = max(1, len(rows))
        top_rank_cutoff[key] = max(1, min(5, (count + 3) // 4))

    grouped: Dict[tuple[str, str, int], List[Dict[str, Any]]] = {}
    for row in deduped.values():
        key = (
            str(row.get("portfolio_id") or ""),
            str(row.get("market") or ""),
            _safe_int(row.get("horizon_days"), 0),
        )
        grouped.setdefault(key, []).append(row)

    def _avg_future(rows: List[Dict[str, Any]]) -> float | None:
        return _avg_defined([_safe_float(item.get("future_return"), 0.0) * 10000.0 for item in list(rows or [])])

    def _positive_rate(rows: List[Dict[str, Any]]) -> float | None:
        if not rows:
            return None
        positives = sum(1 for item in rows if _safe_float(item.get("future_return"), 0.0) > 0.0)
        return float(positives / len(rows))

    out: List[Dict[str, Any]] = []
    for (portfolio_id, market, horizon_days), rows in grouped.items():
        selected_rows = [row for row in rows if int(row.get("selected", 0) or 0) == 1]
        unselected_rows = [row for row in rows if int(row.get("selected", 0) or 0) != 1]
        top_ranked_rows = [
            row
            for row in selected_rows
            if _safe_int(row.get("stage_rank"), 0) > 0
            and _safe_int(row.get("stage_rank"), 0)
            <= int(top_rank_cutoff.get((portfolio_id, str(row.get("analysis_run_id") or "")), 1))
        ]
        executed_rows = [row for row in selected_rows if str(row.get("execution_status") or "") == "FILLED"]
        blocked_edge_rows = [row for row in selected_rows if str(row.get("execution_status") or "") == "BLOCKED_EDGE"]
        selected_avg = _avg_future(selected_rows)
        unselected_avg = _avg_future(unselected_rows)
        top_rank_avg = _avg_future(top_ranked_rows)
        executed_avg = _avg_future(executed_rows)
        blocked_edge_avg = _avg_future(blocked_edge_rows)
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "horizon_days": horizon_days,
                "universe_sample_count": int(len(rows)),
                "selected_sample_count": int(len(selected_rows)),
                "unselected_sample_count": int(len(unselected_rows)),
                "top_ranked_sample_count": int(len(top_ranked_rows)),
                "executed_sample_count": int(len(executed_rows)),
                "blocked_edge_sample_count": int(len(blocked_edge_rows)),
                "universe_avg_future_return_bps": _avg_future(rows),
                "selected_avg_future_return_bps": selected_avg,
                "unselected_avg_future_return_bps": unselected_avg,
                "selected_spread_vs_unselected_bps": (
                    float(selected_avg - unselected_avg)
                    if selected_avg is not None and unselected_avg is not None
                    else None
                ),
                "top_ranked_avg_future_return_bps": top_rank_avg,
                "top_ranked_spread_vs_unselected_bps": (
                    float(top_rank_avg - unselected_avg)
                    if top_rank_avg is not None and unselected_avg is not None
                    else None
                ),
                "executed_avg_future_return_bps": executed_avg,
                "blocked_edge_avg_future_return_bps": blocked_edge_avg,
                "executed_spread_vs_blocked_edge_bps": (
                    float(executed_avg - blocked_edge_avg)
                    if executed_avg is not None and blocked_edge_avg is not None
                    else None
                ),
                "selected_positive_rate": _positive_rate(selected_rows),
                "unselected_positive_rate": _positive_rate(unselected_rows),
                "executed_positive_rate": _positive_rate(executed_rows),
                "blocked_edge_positive_rate": _positive_rate(blocked_edge_rows),
            }
        )
    out.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
            _safe_int(row.get("horizon_days"), 0),
        )
    )
    return out


def _build_weekly_edge_realization_rows(execution_parent_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in list(execution_parent_rows or []):
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if portfolio_id:
            grouped.setdefault(portfolio_id, []).append(dict(row))
    out: List[Dict[str, Any]] = []
    for portfolio_id, rows in grouped.items():
        relevant = [
            row
            for row in rows
            if str(row.get("linked_snapshot_id") or "").strip()
            or _safe_float(row.get("expected_edge_bps"), 0.0) > 0.0
            or _safe_float(row.get("score_before_cost"), 0.0) != 0.0
        ]
        if not relevant:
            continue
        weighted_order_value = sum(abs(_safe_float(row.get("order_value"), 0.0)) for row in relevant)
        filled = [row for row in relevant if str(row.get("status_bucket") or "") == "FILLED"]
        fill_notional = sum(abs(_safe_float(row.get("fill_notional"), 0.0)) for row in filled)
        edge_blocked = [row for row in relevant if str(row.get("status_bucket") or "") == "BLOCKED_EDGE"]
        output = {
            "portfolio_id": portfolio_id,
            "market": str(relevant[0].get("market") or ""),
            "candidate_parent_count": int(len(relevant)),
            "filled_parent_count": int(len(filled)),
            "blocked_edge_parent_count": int(len(edge_blocked)),
            "linked_snapshot_count": int(sum(1 for row in relevant if str(row.get("linked_snapshot_id") or "").strip())),
            "avg_score_before_cost": (
                float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("score_before_cost"), 0.0) for row in relevant) / weighted_order_value)
                if weighted_order_value > 0.0 else None
            ),
            "avg_expected_edge_score": (
                float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("expected_edge_score"), 0.0) for row in relevant) / weighted_order_value)
                if weighted_order_value > 0.0 else None
            ),
            "avg_expected_edge_bps": (
                float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("expected_edge_bps"), 0.0) for row in relevant) / weighted_order_value)
                if weighted_order_value > 0.0 else None
            ),
            "avg_edge_gate_threshold_bps": (
                float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("edge_gate_threshold_bps"), 0.0) for row in relevant) / weighted_order_value)
                if weighted_order_value > 0.0 else None
            ),
            "avg_expected_cost_bps": (
                float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("expected_cost_bps"), 0.0) for row in relevant) / weighted_order_value)
                if weighted_order_value > 0.0 else None
            ),
            "avg_actual_slippage_bps": (
                float(sum(abs(_safe_float(row.get("fill_notional"), 0.0)) * _safe_float(row.get("avg_actual_slippage_bps"), 0.0) for row in filled) / fill_notional)
                if fill_notional > 0.0 else None
            ),
            "avg_realized_total_cost_bps": (
                float(sum(abs(_safe_float(row.get("fill_notional"), 0.0)) * _safe_float(row.get("avg_realized_total_cost_bps"), 0.0) for row in filled) / fill_notional)
                if fill_notional > 0.0 else None
            ),
            "avg_execution_capture_bps": (
                float(sum(abs(_safe_float(row.get("fill_notional"), 0.0)) * _safe_float(row.get("execution_capture_bps"), 0.0) for row in filled if row.get("execution_capture_bps") not in (None, "")) / fill_notional)
                if fill_notional > 0.0 else None
            ),
            "avg_fill_delay_seconds": _avg_defined([row.get("first_fill_delay_seconds") for row in filled if row.get("first_fill_delay_seconds") not in (None, "")]),
            "median_fill_delay_seconds": _median([row.get("first_fill_delay_seconds") for row in filled if row.get("first_fill_delay_seconds") not in (None, "")]),
        }
        for horizon_days in (5, 20, 60):
            future_key = f"outcome_{horizon_days}d_future_return_bps"
            edge_key = f"outcome_{horizon_days}d_realized_edge_bps"
            samples = [row for row in filled if row.get(future_key) not in (None, "")]
            output[f"matured_{horizon_days}d_sample_count"] = int(len(samples))
            output[f"matured_{horizon_days}d_avg_future_return_bps"] = _avg_bps(samples, future_key)
            output[f"matured_{horizon_days}d_avg_realized_edge_bps"] = _avg_bps(samples, edge_key)
        out.append(output)
    out.sort(key=lambda row: (str(row.get("market") or ""), str(row.get("portfolio_id") or "")))
    return out


def _build_weekly_blocked_edge_attribution_rows(execution_parent_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    total_value_by_portfolio: Dict[str, float] = {}
    for row in list(execution_parent_rows or []):
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        total_value_by_portfolio[portfolio_id] = float(total_value_by_portfolio.get(portfolio_id, 0.0) or 0.0) + abs(_safe_float(row.get("order_value"), 0.0))

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in list(execution_parent_rows or []):
        if str(row.get("status_bucket") or "") != "BLOCKED_EDGE":
            continue
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if portfolio_id:
            grouped.setdefault(portfolio_id, []).append(dict(row))

    out: List[Dict[str, Any]] = []
    for portfolio_id, rows in grouped.items():
        blocked_value = sum(abs(_safe_float(row.get("order_value"), 0.0)) for row in rows)
        total_value = float(total_value_by_portfolio.get(portfolio_id, 0.0) or 0.0)
        output = {
            "portfolio_id": portfolio_id,
            "market": str(rows[0].get("market") or ""),
            "blocked_edge_parent_count": int(len(rows)),
            "blocked_edge_order_value": float(blocked_value),
            "blocked_edge_weight": float(blocked_value / total_value) if total_value > 0.0 else 0.0,
            "avg_expected_edge_bps": (
                float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("expected_edge_bps"), 0.0) for row in rows) / blocked_value)
                if blocked_value > 0.0 else None
            ),
            "avg_edge_gate_threshold_bps": (
                float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("edge_gate_threshold_bps"), 0.0) for row in rows) / blocked_value)
                if blocked_value > 0.0 else None
            ),
            "avg_required_gap_bps": (
                float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("required_edge_gap_bps"), 0.0) for row in rows) / blocked_value)
                if blocked_value > 0.0 else None
            ),
            "blocked_expected_edge_value": float(sum(_safe_float(row.get("expected_edge_value"), 0.0) for row in rows)),
            "blocked_required_gap_value": float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("required_edge_gap_bps"), 0.0) / 10000.0 for row in rows)),
        }
        for horizon_days in (5, 20, 60):
            future_key = f"outcome_{horizon_days}d_future_return_bps"
            edge_key = f"outcome_{horizon_days}d_counterfactual_edge_bps"
            samples = [row for row in rows if row.get(future_key) not in (None, "")]
            output[f"matured_{horizon_days}d_sample_count"] = int(len(samples))
            output[f"matured_{horizon_days}d_avg_future_return_bps"] = _avg_bps(samples, future_key)
            output[f"matured_{horizon_days}d_avg_counterfactual_edge_bps"] = _avg_bps(samples, edge_key)
        out.append(output)
    out.sort(key=lambda row: (str(row.get("market") or ""), str(row.get("portfolio_id") or "")))
    return out
