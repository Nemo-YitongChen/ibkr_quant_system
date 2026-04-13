from __future__ import annotations

import argparse
import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from ..analysis.report import write_csv, write_json
from ..common.account_profile import load_account_profiles, resolved_account_profile_summary
from ..common.adaptive_strategy import (
    adaptive_strategy_context,
    adaptive_strategy_effective_controls_human_note,
    load_adaptive_strategy,
)
from ..common.cli import build_cli_parser, emit_cli_summary
from ..common.cli_contracts import ArtifactBundle, WeeklyReviewSummary
from ..common.market_structure import load_market_structure, market_structure_summary
from .review_weekly_io import (
    load_json_file as _load_json_file,
    load_yaml_file as _load_yaml_file,
    read_csv_rows as _read_csv,
    read_json as _read_json,
    read_yaml as _read_yaml,
)
from .review_weekly_markdown import write_weekly_review_markdown as _write_md
from .review_weekly_thresholds import (
    build_feedback_threshold_suggestion_rows as _build_feedback_threshold_suggestion_rows,
    build_feedback_threshold_tuning_summary as _build_feedback_threshold_tuning_summary,
    feedback_action_field as _feedback_action_field,
    feedback_automation_basis_label as _feedback_automation_basis_label,
    feedback_automation_mode_label as _feedback_automation_mode_label,
    feedback_automation_thresholds as _feedback_automation_thresholds,
    feedback_kind_label as _feedback_kind_label,
    feedback_maturity_label as _feedback_maturity_label,
    feedback_threshold_effect_bucket as _feedback_threshold_effect_bucket,
    load_feedback_threshold_overrides as _load_feedback_threshold_overrides,
)
from ..common.logger import get_logger
from ..common.markets import add_market_args, market_config_path, resolve_market_code
from ..common.runtime_paths import resolve_repo_path
from ..common.storage import Storage

log = get_logger("tools.review_investment_weekly")
BASE_DIR = Path(__file__).resolve().parents[2]
FEEDBACK_CALIBRATION_LOOKBACK_DAYS = 180


def build_parser() -> argparse.ArgumentParser:
    ap = build_cli_parser(
        description="Review weekly performance for investment paper portfolios.",
        command="ibkr-quant-weekly-review",
        examples=[
            "ibkr-quant-weekly-review --market HK --days 7",
            "ibkr-quant-weekly-review --market US --portfolio_id US:market_us --out_dir reports_investment_weekly_us",
        ],
        notes=[
            "Writes weekly_review.md, weekly_review_summary.json, and the weekly CSV breakdowns under --out_dir.",
        ],
    )
    add_market_args(ap)
    ap.add_argument("--db", default="audit.db", help="SQLite audit database used for weekly review inputs.")
    ap.add_argument("--out_dir", default="reports_investment_weekly", help="Directory for weekly review artifacts.")
    ap.add_argument("--labeling_dir", default="", help="Optional snapshot labeling output dir. Defaults to auto-detect.")
    ap.add_argument("--preflight_dir", default="reports_preflight", help="Optional preflight output dir for IBKR history probe summary.")
    ap.add_argument(
        "--feedback_thresholds_config",
        default="",
        help="Optional YAML of market-level AUTO_APPLY threshold overrides. Defaults to weekly_review out_dir override file.",
    )
    ap.add_argument("--days", type=int, default=7, help="Lookback window in days for weekly review inputs.")
    ap.add_argument("--portfolio_id", default="", help="Optional portfolio filter.")
    ap.add_argument("--include_legacy", action="store_true", default=False, help="Include legacy non-portfolio rows when present.")
    return ap


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _resolve_project_path(path_str: str) -> Path:
    return resolve_repo_path(BASE_DIR, path_str)


def _mean(values: List[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _max_drawdown(values: List[float]) -> float:
    peak = 0.0
    max_dd = 0.0
    for value in values:
        equity = float(value or 0.0)
        peak = max(peak, equity)
        if peak <= 0:
            continue
        dd = (equity / peak) - 1.0
        max_dd = min(max_dd, dd)
    return float(max_dd)


def _portfolio_key(row: Dict[str, Any]) -> str:
    portfolio_id = str(row.get("portfolio_id") or "").strip()
    return portfolio_id or f"LEGACY:{row.get('market', '')}"


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


def _run_source(row: Dict[str, Any]) -> str:
    details = _parse_json_dict(row.get("details"))
    return str(details.get("source") or "").strip().lower()



def _load_report_data_warning(report_dir: str) -> str:
    if not report_dir:
        return ""
    path = _resolve_project_path(report_dir) / "investment_report.md"
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
    universe_cfg = _load_yaml_file(_resolve_project_path(f"config/markets/{code.lower()}/universe.yaml"))
    ibkr_cfg = _load_yaml_file(market_config_path(BASE_DIR, code))
    investment_cfg_path = str(ibkr_cfg.get("investment_config", f"config/investment_{code.lower()}.yaml") or "")
    investment_cfg = _load_yaml_file(_resolve_project_path(investment_cfg_path))
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
        report_path = _resolve_project_path(report_dir) if report_dir else Path()
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



def _resolve_labeling_summary_dir(path_str: str, market_filter: str) -> Path | None:
    # 周报这里优先复用已经跑过的 snapshot labeling 结果。
    # 如果没有显式传路径，就在项目里常用的两个输出目录之间自动探测。
    raw_candidates = [str(path_str or "").strip()] if str(path_str or "").strip() else [
        "reports_investment_labeling",
        "reports_investment_labels",
    ]
    suffixes = []
    if market_filter:
        suffixes.append(market_filter.lower())
    suffixes.extend(["all", ""])
    for raw in raw_candidates:
        base = _resolve_project_path(raw)
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
    rows.sort(key=lambda row: (str(row.get("portfolio_id") or ""), str(row.get("ts") or ""), str(row.get("symbol") or "")), reverse=True)
    return rows


def _avg_defined(values: List[Any]) -> float | None:
    nums = [float(v) for v in values if v is not None]
    if not nums:
        return None
    return float(sum(nums) / len(nums))


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(value)))


def _feedback_confidence(
    sample_ratio: float,
    magnitude_ratio: float,
    persistence_ratio: float = 0.0,
    structure_ratio: float = 0.0,
) -> float:
    # 这里的 confidence 不是统计学置信区间，而是给 weekly feedback 自动应用用的“样本可靠度”代理分。
    # 先统一收敛到 0~1，便于 supervisor 后面按比例缩放调参增量。
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


def _feedback_confidence_label(value: float) -> str:
    value = _clamp(value, 0.0, 1.0)
    if value >= 0.75:
        return "HIGH"
    if value >= 0.45:
        return "MEDIUM"
    return "LOW"


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
    gap = _mean([item[1] for item in top]) - _mean([item[1] for item in bottom])
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
        avg_future_return = _mean([float(row.get("future_return") or 0.0) for row in selected_rows])
        avg_max_drawdown = _mean([float(row.get("max_drawdown") or 0.0) for row in selected_rows])
        avg_model_score = _mean(
            [
                float(dict(row.get("_details") or {}).get("model_recommendation_score", dict(row.get("_details") or {}).get("score", 0.0)) or 0.0)
                for row in selected_rows
            ]
        )
        avg_execution_score = _mean(
            [float(dict(row.get("_details") or {}).get("execution_score", 0.0) or 0.0) for row in selected_rows]
        )
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
    # calibration_score 以 0.5 为中性点：0.5 不改变原置信度，>0.5 适度放大，<0.5 适度收缩。
    multiplier = 0.70 + 0.60 * _clamp(calibration_score, 0.0, 1.0)
    return round(_clamp(base_confidence * multiplier, 0.0, 1.0), 6)



def _feedback_maturity_alert_bucket(row: Dict[str, Any], *, now_dt: datetime | None = None) -> str:
    # 这里复用 dashboard 上的成熟度口径，先把“当前是否接近自动应用”固化进周报历史。
    # 这样后面看趋势时，不用每次再从 apply_mode + maturity 临时反推。
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
            # 把每周自动化决策对应的效果快照一并写进历史，后面才能看 W+1/W+2/W+4 的真实变化。
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
                # details 里保留完整周报行，方便以后再扩趋势诊断而不用改 schema。
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
        # 把每周阈值建议完整落库，后面才能判断某个市场是在连续放宽、连续收紧，还是反复切换。
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


def _build_feedback_threshold_history_overview(
    db_path: Path,
    feedback_threshold_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not feedback_threshold_rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for raw in list(feedback_threshold_rows or []):
        row = dict(raw)
        market = resolve_market_code(str(row.get("market") or ""))
        feedback_kind = str(row.get("feedback_kind") or "").strip().lower()
        if not market or not feedback_kind:
            continue
        key = (market, feedback_kind)
        if key in seen:
            continue
        seen.add(key)
        history_rows = storage.get_recent_investment_feedback_threshold_history(
            market,
            feedback_kind=feedback_kind,
            limit=12,
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
        transition = "首次建议"
        if previous:
            previous_action = str(previous.get("suggestion_action") or "").strip().upper()
            transition = "动作变化" if previous_action != current_action else "持续试运行"
        action_chain = " -> ".join(
            f"{str(item.get('week_label', '') or '-')}:"
            f"{str(item.get('suggestion_action', '') or '-')}"
            for item in reversed(history_rows[:4])
        ) or "-"
        current_details = dict(current.get("details_json", {}) or {})
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


def _build_feedback_threshold_effect_overview(
    db_path: Path,
    feedback_threshold_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not feedback_threshold_rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for raw in list(feedback_threshold_rows or []):
        row = dict(raw)
        market = resolve_market_code(str(row.get("market") or ""))
        feedback_kind = str(row.get("feedback_kind") or "").strip().lower()
        if not market or not feedback_kind:
            continue
        key = (market, feedback_kind)
        if key in seen:
            continue
        seen.add(key)
        history_rows = storage.get_recent_investment_feedback_threshold_history(
            market,
            feedback_kind=feedback_kind,
            limit=12,
        )
        if not history_rows:
            continue
        history_rows = sorted(
            list(history_rows or []),
            key=lambda item: (str(item.get("week_start", "") or ""), str(item.get("ts", "") or "")),
            reverse=True,
        )
        current = dict(history_rows[0])
        current_action = str(current.get("suggestion_action") or "").strip().upper()
        same_action_weeks = 0
        for item in history_rows:
            if str(item.get("suggestion_action") or "").strip().upper() != current_action:
                break
            same_action_weeks += 1
        effect_label, effect_reason = _feedback_threshold_effect_label(
            current_action,
            str(current.get("summary_signal") or ""),
        )
        current_details = dict(current.get("details_json", {}) or {})
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
                "action_chain": " -> ".join(
                    f"{str(item.get('week_label', '') or '-')}:"
                    f"{str(item.get('suggestion_action', '') or '-')}"
                    for item in reversed(history_rows[:4])
                ) or "-",
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
        _feedback_threshold_effect_bucket(latest_effect),
        _feedback_threshold_effect_bucket(effect_w1),
        _feedback_threshold_effect_bucket(effect_w2),
        _feedback_threshold_effect_bucket(effect_w4),
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
    seen: set[tuple[str, str]] = set()
    for raw in list(feedback_threshold_rows or []):
        row = dict(raw)
        market = resolve_market_code(str(row.get("market") or ""))
        feedback_kind = str(row.get("feedback_kind") or "").strip().lower()
        if not market or not feedback_kind:
            continue
        key = (market, feedback_kind)
        if key in seen:
            continue
        seen.add(key)
        history_rows = storage.get_recent_investment_feedback_threshold_history(
            market,
            feedback_kind=feedback_kind,
            limit=12,
        )
        if not history_rows:
            continue
        history_rows = sorted(
            list(history_rows or []),
            key=lambda item: (str(item.get("week_start", "") or ""), str(item.get("ts", "") or "")),
            reverse=True,
        )
        current = dict(history_rows[0])
        current_action = str(current.get("suggestion_action") or "").strip().upper()
        same_action_weeks = 0
        for item in history_rows:
            if str(item.get("suggestion_action") or "").strip().upper() != current_action:
                break
            same_action_weeks += 1
        cohort_rows_asc = list(reversed(history_rows[:same_action_weeks]))
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
                    f"{str(item.get('week_label', '') or '-')}:"
                    f"{str(item.get('suggestion_action', '') or '-')}"
                    for item in cohort_rows_asc[:4]
                ) or "-",
            }
        )
    out.sort(
        key=lambda row: (
            0 if _feedback_threshold_effect_bucket(row.get("latest_effect")) == "恶化" else 1 if _feedback_threshold_effect_bucket(row.get("latest_effect")) == "观察中" else 2,
            -int(row.get("cohort_weeks", 0) or 0),
            str(row.get("market") or ""),
            str(row.get("feedback_kind_label") or ""),
        )
    )
    return out


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
        # 这里只提醒“刚进入”或“仍在早期”的阈值试运行市场，避免顶部提示长期堆积。
        out.append(
            {
                "market": str(row.get("market") or ""),
                "feedback_kind": str(row.get("feedback_kind") or ""),
                "feedback_kind_label": str(row.get("feedback_kind_label") or "-"),
                "cohort_label": str(row.get("cohort_label") or "-"),
                "baseline_week": str(row.get("baseline_week") or "-"),
                "cohort_weeks": cohort_weeks,
                "stage_label": stage_label,
                "action_label": action_label,
                "latest_effect": str(row.get("latest_effect") or "-"),
                "effect_w1": str(row.get("effect_w1") or "-"),
                "effect_w2": str(row.get("effect_w2") or "-"),
                "diagnosis": str(row.get("diagnosis") or "-"),
                "next_check": next_check,
            }
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


def _build_feedback_automation_effect_overview(
    db_path: Path,
    feedback_automation_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not feedback_automation_rows:
        return []
    storage = Storage(str(db_path))
    rows: List[Dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for raw in list(feedback_automation_rows or []):
        current_row = dict(raw)
        market = resolve_market_code(str(current_row.get("market") or ""))
        portfolio_id = str(current_row.get("portfolio_id") or "").strip()
        feedback_kind = str(current_row.get("feedback_kind") or "").strip().lower()
        if not market or not portfolio_id or not feedback_kind:
            continue
        key = (market, portfolio_id, feedback_kind)
        if key in seen:
            continue
        seen.add(key)
        history_rows = storage.get_recent_investment_feedback_automation_history(
            market,
            portfolio_id,
            feedback_kind=feedback_kind,
            limit=12,
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
        baseline_week = str(active_rows_asc[0].get("week_label", "") or "-") if active_rows_asc else "-"
        effect_label = "观察中"
        effect_metric = "-"
        details = dict(current.get("details_json", {}) or {})
        reason = str(details.get("automation_reason") or details.get("feedback_reason") or "-")
        driver = str(details.get("feedback_action") or current.get("feedback_action") or "-")
        if len(active_rows_asc) >= 2:
            baseline_snapshot = _feedback_effect_snapshot_from_history_row(active_rows_asc[0])
            latest_snapshot = _feedback_effect_snapshot_from_history_row(active_rows_asc[-1])
            compare_label, compare_metric = _feedback_effect_compare_snapshot(feedback_kind, baseline_snapshot, latest_snapshot)
            if compare_label != "-":
                effect_label = compare_label
                effect_metric = compare_metric
        rows.append(
            {
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
        )
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


def _build_feedback_effect_market_summary(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[str, str], Dict[str, Any]] = {}
    for raw in list(rows or []):
        row = dict(raw)
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

    out = list(grouped.values())
    for row in out:
        tracked = int(row.get("tracked_count", 0) or 0)
        row["avg_active_weeks"] = float(row.get("active_weeks_total", 0) or 0) / float(tracked or 1)
        if int(row.get("latest_deteriorated_count", 0) or 0) > 0:
            row["summary_signal"] = "需复核"
        elif int(row.get("latest_improved_count", 0) or 0) >= max(1, tracked // 2):
            row["summary_signal"] = "持续改善"
        elif int(row.get("latest_stable_count", 0) or 0) > 0:
            row["summary_signal"] = "稳定跟踪"
        else:
            row["summary_signal"] = "观察中"
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
    # 这里只统计“前向样本不足”的 skip，总结每个组合/周期还有多少 outcome 在路上，
    # 让第三阶段自动化不要只看已经成熟的样本，还能看到“还有多少马上会成熟”。
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
    maturity_label = _feedback_maturity_label(maturity_ratio, pending_skip_count, int(calibration_sample_count))
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


def _build_feedback_automation_row(
    feedback_kind: str,
    row: Dict[str, Any],
    maturity_map: Dict[tuple[str, str], Dict[str, Any]] | None = None,
    threshold_overrides: Dict[str, Dict[str, Dict[str, float]]] | None = None,
    market_data_gate_map: Dict[str, Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    kind = str(feedback_kind or "").strip().lower()
    action_field = _feedback_action_field(kind)
    action = str(row.get(action_field) or "").strip().upper()
    portfolio_id = str(row.get("portfolio_id") or "")
    base_confidence = float(row.get("feedback_base_confidence", 0.0) or 0.0)
    final_confidence = float(row.get("feedback_confidence", 0.0) or 0.0)
    calibration_score = float(row.get("feedback_calibration_score", 0.5) or 0.5)
    feedback_sample_count = int(row.get("feedback_sample_count", 0) or 0)
    calibration_sample_count = int(row.get("feedback_calibration_sample_count", 0) or 0)
    calibration_horizon_days = str(row.get("feedback_calibration_horizon_days") or "")
    thresholds = _feedback_automation_thresholds(
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

    maturity_note = ""
    if pending_skip_count > 0:
        remaining_min = int(maturity_info.get("min_remaining_forward_bars", 0) or 0)
        remaining_max = int(maturity_info.get("max_remaining_forward_bars", 0) or 0)
        ready_start = str(maturity_info.get("estimated_ready_start_ts") or "")
        ready_end = str(maturity_info.get("estimated_ready_end_ts") or "")
        maturity_note = (
            f"当前仍有 {pending_skip_count} 条 {calibration_horizon_days or '-'} 日 outcome 样本待成熟"
            f"（remaining={remaining_min}-{remaining_max}，ready={ready_start[:10] or '-'}->{ready_end[:10] or '-'}）。"
        )

    if not action or action in {"HOLD", "KEEP_OBSERVING", "NONE"}:
        apply_mode = "HOLD"
        basis = "NO_SIGNAL"
        reason = f"本周没有形成明确的{_feedback_kind_label(kind)}调参动作，先继续观察。"
    elif outcome_ready:
        # outcome 样本虽然已经“够数”，但如果待成熟样本仍然很多，就先降一级，
        # 避免在样本还快速变化的时候过早进入 AUTO_APPLY。
        if (
            final_confidence >= float(thresholds["auto_confidence"])
            and calibration_score >= float(thresholds["auto_calibration_score"])
            and feedback_sample_count >= int(thresholds["auto_feedback_samples"])
            and auto_maturity_ready
        ):
            apply_mode = "AUTO_APPLY"
            basis = "OUTCOME_CALIBRATED"
            reason = (
                f"{_feedback_kind_label(kind)}本周动作明确，且 outcome 校准样本已支持自动应用；"
                "paper 可自动落盘，live 保留人工确认。"
            )
        elif (
            final_confidence >= float(thresholds["suggest_confidence"])
            and (
                calibration_score >= float(thresholds["suggest_calibration_score"])
                or feedback_sample_count >= int(thresholds["suggest_feedback_samples"])
            )
            and suggest_maturity_ready
        ):
            apply_mode = "SUGGEST_ONLY"
            basis = "OUTCOME_CALIBRATED"
            reason = (
                f"{_feedback_kind_label(kind)}已有一定 outcome 支持，但强度还不够稳，"
                "建议先人工确认，不直接自动放大。"
            )
        else:
            apply_mode = "HOLD"
            basis = "OUTCOME_CALIBRATED"
            reason = (
                f"{_feedback_kind_label(kind)}虽然已有 outcome 校准样本，但本周置信度仍偏弱，"
                "先继续观察，避免过早自动改参数。"
            )
        if pending_skip_count > 0 and not auto_maturity_ready and apply_mode == "SUGGEST_ONLY":
            reason = f"{_feedback_kind_label(kind)}已有 outcome 支持，但样本仍在持续成熟中，建议先人工确认。{maturity_note}"
        elif pending_skip_count > 0 and not suggest_maturity_ready:
            apply_mode = "HOLD"
            reason = f"{_feedback_kind_label(kind)}仍有较多 outcome 样本待成熟，先继续观察，避免过早自动改参数。{maturity_note}"
    else:
        if (
            base_confidence >= float(thresholds["auto_base_confidence"])
            and feedback_sample_count >= int(thresholds["auto_feedback_samples"])
            and auto_maturity_ready
        ):
            apply_mode = "AUTO_APPLY"
            basis = "BASE_WEEKLY"
            reason = (
                f"{_feedback_kind_label(kind)}的周报样本已经足够强，虽然 outcome 样本还没完全成熟，"
                "paper 先自动应用，后续再用 outcome 继续校准。"
            )
        elif (
            base_confidence >= float(thresholds["suggest_base_confidence"])
            and feedback_sample_count >= int(thresholds["suggest_feedback_samples"])
            and suggest_maturity_ready
        ):
            apply_mode = "SUGGEST_ONLY"
            basis = "BASE_WEEKLY"
            reason = (
                f"{_feedback_kind_label(kind)}已有周报层面的调整依据，但 outcome 校准样本还不够，"
                "建议先人工确认。"
            )
        else:
            apply_mode = "HOLD"
            basis = "NO_SIGNAL"
            reason = (
                f"{_feedback_kind_label(kind)}当前样本仍偏少或置信度偏弱，"
                "暂时不建议自动改参数。"
            )
        if pending_skip_count > 0 and not auto_maturity_ready and apply_mode == "SUGGEST_ONLY":
            reason = f"{_feedback_kind_label(kind)}周报信号已经出现，但 outcome 样本还在成熟中，建议先人工确认。{maturity_note}"
        elif pending_skip_count > 0 and not suggest_maturity_ready:
            apply_mode = "HOLD"
            reason = f"{_feedback_kind_label(kind)}仍缺少足够成熟的 outcome 样本，先继续观察。{maturity_note}"

    market_data_gate = dict((market_data_gate_map or {}).get(portfolio_id, {}) or {})
    market_data_gate_status = str(market_data_gate.get("status_code") or "UNKNOWN").strip().upper() or "UNKNOWN"
    market_data_gate_label = str(market_data_gate.get("status_label") or "未检查").strip() or "未检查"
    market_data_gate_reason = str(market_data_gate.get("reason") or "").strip()
    if market_data_gate_status in {"ATTENTION", "RESEARCH_FALLBACK"}:
        # 这里把“市场数据本身还不稳”单独当成一层 gate：
        # 即使 weekly feedback 看起来够强，也先降一级，避免在 fallback/权限问题上继续自动放大。
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
        "portfolio_id": portfolio_id,
        "market": str(row.get("market") or ""),
        "feedback_kind": kind,
        "feedback_kind_label": _feedback_kind_label(kind),
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
        "calibration_apply_mode_label": _feedback_automation_mode_label(apply_mode),
        "calibration_basis": basis,
        "calibration_basis_label": _feedback_automation_basis_label(basis),
        "paper_auto_apply_enabled": int(apply_mode == "AUTO_APPLY"),
        "live_confirmation_required": int(apply_mode in {"AUTO_APPLY", "SUGGEST_ONLY"}),
        "automation_reason": reason,
        "feedback_reason": str(row.get("feedback_reason") or ""),
        "market_data_gate_status": market_data_gate_status,
        "market_data_gate_label": market_data_gate_label,
        "market_data_gate_reason": market_data_gate_reason,
        "market_data_probe_status_label": str(market_data_gate.get("probe_status_label") or ""),
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
    repeat_symbols = sum(1 for symbol in {str(row.get("symbol") or "") for row in rows} if sum(1 for r in rows if str(r.get("symbol") or "") == symbol) >= 2)
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
    out.sort(key=lambda row: (-int(row.get("repeat_count", 0) or 0), -float(row.get("score_penalty", 0.0) or 0.0), str(row.get("symbol") or "")))
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

        # 这里只生成“温和惩罚”，让下一轮排序和 execution gate 更保守，
        # 但不直接把热点标的从候选池里一刀切删除。
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

        base_confidence = _feedback_confidence(
            sample_ratio=float(len(rows) / 6.0),
            magnitude_ratio=max(avg_score_gap / 0.12 if avg_score_gap > 0.0 else 0.0, avg_prob_gap / 0.18 if avg_prob_gap > 0.0 else 0.0),
            persistence_ratio=float(repeat_count / 3.0),
            structure_ratio=float(avg_shadow_samples / 24.0),
        ) if action else 0.0
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


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (str(table),),
    ).fetchone()
    return bool(row)


def _build_position_snapshots(
    position_rows: List[Dict[str, Any]],
    *,
    asof_ts: str = "",
    strict_before: bool = False,
) -> Dict[str, List[Dict[str, Any]]]:
    latest_ts_by_portfolio: Dict[str, str] = {}
    latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]] = {}
    for row in position_rows:
        row_ts = str(row.get("ts") or "")
        if asof_ts:
            if strict_before and row_ts >= asof_ts:
                continue
            if (not strict_before) and row_ts > asof_ts:
                continue
        portfolio_id = _portfolio_key(row)
        latest_ts = latest_ts_by_portfolio.get(portfolio_id, "")
        if not latest_ts or row_ts > latest_ts:
            latest_ts_by_portfolio[portfolio_id] = row_ts
            latest_rows_by_portfolio[portfolio_id] = [dict(row)]
        elif row_ts == latest_ts:
            latest_rows_by_portfolio.setdefault(portfolio_id, []).append(dict(row))
    return latest_rows_by_portfolio


def _build_latest_run_positions(
    run_rows: List[Dict[str, Any]],
    position_rows: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    latest_run_id_by_portfolio: Dict[str, str] = {}
    latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]] = {}
    for row in run_rows:
        portfolio_id = _portfolio_key(row)
        latest_run_id_by_portfolio[portfolio_id] = str(row.get("run_id") or "")
        latest_rows_by_portfolio.setdefault(portfolio_id, [])
    rows_by_run: Dict[str, List[Dict[str, Any]]] = {}
    for row in position_rows:
        run_id = str(row.get("run_id") or "")
        rows_by_run.setdefault(run_id, []).append(dict(row))
    for portfolio_id, run_id in latest_run_id_by_portfolio.items():
        latest_rows_by_portfolio[portfolio_id] = list(rows_by_run.get(run_id, []))
    return latest_rows_by_portfolio


def _rows_to_symbol_map(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        out[symbol] = dict(row)
    return out


def _load_symbol_meta(report_dir: str) -> Dict[str, Dict[str, Any]]:
    meta: Dict[str, Dict[str, Any]] = {}
    if not report_dir:
        return meta
    report_path = Path(report_dir)
    fundamentals_path = report_path / "fundamentals.json"
    if fundamentals_path.exists():
        try:
            data = json.loads(fundamentals_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                for symbol, row in data.items():
                    meta[str(symbol).upper()] = dict(row or {})
        except Exception:
            pass
    for row in _read_csv(report_path / "investment_candidates.csv"):
        symbol = str(row.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        current = meta.setdefault(symbol, {})
        current.update(
            {
                "score": row.get("score"),
                "action": row.get("action"),
                "sector": row.get("sector") or current.get("sector") or "",
                "industry": row.get("industry") or current.get("industry") or "",
                "source": row.get("source") or current.get("source") or "",
            }
        )
    return meta


def _build_sector_rows(
    latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for portfolio_id, rows in latest_rows_by_portfolio.items():
        latest_runs = runs_by_portfolio.get(portfolio_id, [])
        report_dir = ""
        if latest_runs:
            report_dir = str(_parse_json_dict(latest_runs[-1].get("details")).get("report_dir") or latest_runs[-1].get("report_dir") or "")
        meta_by_symbol = _load_symbol_meta(report_dir)
        sector_agg: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            symbol = str(row.get("symbol") or "").upper().strip()
            weight = float(row.get("weight") or 0.0)
            market_value = float(row.get("market_value") or 0.0)
            meta = meta_by_symbol.get(symbol, {})
            sector = str(meta.get("sector") or "UNKNOWN").strip() or "UNKNOWN"
            bucket = sector_agg.setdefault(
                sector,
                {
                    "portfolio_id": portfolio_id,
                    "market": str(row.get("market") or ""),
                    "sector": sector,
                    "weight": 0.0,
                    "market_value": 0.0,
                    "symbol_count": 0,
                    "symbols": [],
                },
            )
            bucket["weight"] = float(bucket["weight"]) + weight
            bucket["market_value"] = float(bucket["market_value"]) + market_value
            bucket["symbol_count"] = int(bucket["symbol_count"]) + 1
            bucket["symbols"].append(symbol)
        for bucket in sector_agg.values():
            bucket["symbols"] = ",".join(sorted(bucket["symbols"]))
            out.append(bucket)
    out.sort(key=lambda row: (str(row.get("portfolio_id") or ""), -float(row.get("weight") or 0.0), str(row.get("sector") or "")))
    return out


def _build_holdings_change_rows(
    latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    baseline_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for portfolio_id in sorted(set(latest_rows_by_portfolio) | set(baseline_rows_by_portfolio)):
        latest_map = _rows_to_symbol_map(latest_rows_by_portfolio.get(portfolio_id, []))
        baseline_map = _rows_to_symbol_map(baseline_rows_by_portfolio.get(portfolio_id, []))
        symbols = sorted(set(latest_map) | set(baseline_map))
        for symbol in symbols:
            latest = latest_map.get(symbol, {})
            baseline = baseline_map.get(symbol, {})
            prev_qty = float(baseline.get("qty") or 0.0)
            latest_qty = float(latest.get("qty") or 0.0)
            prev_weight = float(baseline.get("weight") or 0.0)
            latest_weight = float(latest.get("weight") or 0.0)
            if prev_qty <= 0 and latest_qty > 0:
                change_type = "ADDED"
            elif prev_qty > 0 and latest_qty <= 0:
                change_type = "REMOVED"
            elif latest_qty > prev_qty:
                change_type = "INCREASED"
            elif latest_qty < prev_qty:
                change_type = "DECREASED"
            elif abs(latest_weight - prev_weight) > 1e-9:
                change_type = "WEIGHT_CHANGED"
            else:
                continue
            row = latest or baseline
            out.append(
                {
                    "portfolio_id": portfolio_id,
                    "market": str(row.get("market") or ""),
                    "symbol": symbol,
                    "change_type": change_type,
                    "prev_qty": prev_qty,
                    "latest_qty": latest_qty,
                    "delta_qty": latest_qty - prev_qty,
                    "prev_weight": prev_weight,
                    "latest_weight": latest_weight,
                    "delta_weight": latest_weight - prev_weight,
                }
            )
    out.sort(key=lambda row: (str(row.get("portfolio_id") or ""), str(row.get("change_type") or ""), str(row.get("symbol") or "")))
    return out


def _build_reason_summary(trade_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    agg: Dict[tuple[str, str, str], Dict[str, Any]] = {}
    for row in trade_rows:
        key = (
            str(row.get("portfolio_id") or ""),
            str(row.get("action") or ""),
            str(row.get("reason") or ""),
        )
        bucket = agg.setdefault(
            key,
            {
                "portfolio_id": key[0],
                "market": str(row.get("market") or ""),
                "action": key[1],
                "reason": key[2],
                "trade_count": 0,
                "trade_value": 0.0,
            },
        )
        bucket["trade_count"] = int(bucket["trade_count"]) + 1
        bucket["trade_value"] = float(bucket["trade_value"]) + abs(float(row.get("trade_value") or 0.0))
    rows = list(agg.values())
    rows.sort(key=lambda row: (str(row.get("portfolio_id") or ""), -float(row.get("trade_value") or 0.0), str(row.get("reason") or "")))
    return rows


def _build_equity_curve_rows(runs_by_portfolio: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for portfolio_id, runs in runs_by_portfolio.items():
        for run in runs:
            rows.append(
                {
                    "portfolio_id": portfolio_id,
                    "market": str(run.get("market") or ""),
                    "ts": str(run.get("ts") or ""),
                    "rebalance_due": int(run.get("rebalance_due") or 0),
                    "executed": int(run.get("executed") or 0),
                    "cash_before": float(run.get("cash_before") or 0.0),
                    "cash_after": float(run.get("cash_after") or 0.0),
                    "equity_before": float(run.get("equity_before") or 0.0),
                    "equity_after": float(run.get("equity_after") or 0.0),
                }
            )
    rows.sort(key=lambda row: (str(row.get("portfolio_id") or ""), str(row.get("ts") or "")))
    return rows


def _status_count_from_breakdown(text: str, status: str) -> int:
    wanted = str(status or "").strip().upper()
    if not wanted:
        return 0
    for part in str(text or "").split(","):
        name, _, value = part.partition(":")
        if str(name or "").strip().upper() != wanted:
            continue
        try:
            return int(float(value or 0))
        except Exception:
            return 0
    return 0


def _current_iso_week_label(now_dt: datetime) -> tuple[str, str]:
    iso_year, iso_week, iso_weekday = now_dt.isocalendar()
    week_start = (now_dt - timedelta(days=int(iso_weekday) - 1)).date().isoformat()
    return f"{iso_year}-W{iso_week:02d}", week_start


def _build_execution_summary_rows(
    execution_runs: List[Dict[str, Any]],
    execution_orders: List[Dict[str, Any]],
    fill_rows: List[Dict[str, Any]] | None = None,
    commission_rows: List[Dict[str, Any]] | None = None,
    *,
    week_label: str = "",
    week_start: str = "",
) -> List[Dict[str, Any]]:
    # 这里输出给 weekly review 和 dashboard 共用，所以同时保留旧字段别名和新字段。
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]] = {}
    for row in execution_runs:
        runs_by_portfolio.setdefault(_portfolio_key(row), []).append(row)

    fill_rows = list(fill_rows or [])
    commission_rows = list(commission_rows or [])
    commission_by_exec: Dict[str, float] = {}
    for row in commission_rows:
        exec_id = str(row.get("exec_id") or "").strip()
        if not exec_id:
            continue
        commission_by_exec[exec_id] = float(commission_by_exec.get(exec_id, 0.0)) + float(row.get("value") or 0.0)

    fills_by_run_order: Dict[tuple[str, int], List[Dict[str, Any]]] = {}
    fallback_fills_by_order: Dict[int, List[Dict[str, Any]]] = {}
    portfolio_fills: Dict[str, List[Dict[str, Any]]] = {}
    for row in fill_rows:
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if portfolio_id:
            portfolio_fills.setdefault(portfolio_id, []).append(row)
        order_id = int(float(row.get("order_id") or 0) or 0)
        if order_id <= 0:
            continue
        run_id = str(row.get("execution_run_id") or "").strip()
        fills_by_run_order.setdefault((run_id, order_id), []).append(row)
        fallback_fills_by_order.setdefault(order_id, []).append(row)

    out: List[Dict[str, Any]] = []
    for portfolio_id, rows in runs_by_portfolio.items():
        rows.sort(key=lambda row: str(row.get("ts") or ""))
        latest = rows[-1]
        details = _parse_json_dict(latest.get("details"))
        nested_summary = dict(details.get("summary") or {}) if isinstance(details.get("summary"), dict) else {}
        order_rows = [row for row in execution_orders if _portfolio_key(row) == portfolio_id]
        order_summary_rows: List[Dict[str, Any]] = []
        status_counts: Dict[str, int] = {}
        error_statuses: List[str] = []
        for row in order_rows:
            status = str(row.get("status") or "").strip().upper()
            if not status:
                continue
            status_counts[status] = int(status_counts.get(status, 0)) + 1
            if status.startswith("ERROR_") and status not in error_statuses:
                error_statuses.append(status)
            broker_order_id = int(float(row.get("broker_order_id") or 0) or 0)
            run_id = str(row.get("run_id") or row.get("execution_run_id") or "").strip()
            order_fills = list(fills_by_run_order.get((run_id, broker_order_id), []))
            if not order_fills and broker_order_id > 0:
                order_fills = [
                    fill for fill in fallback_fills_by_order.get(broker_order_id, [])
                    if not str(fill.get("execution_run_id") or "").strip()
                ]
            order_summary_rows.append(
                {
                    "status": status,
                    "broker_order_id": broker_order_id,
                    "has_fill_audit": int(bool(order_fills)),
                }
            )
        submitted_order_rows = int(sum(1 for row in order_summary_rows if int(row.get("broker_order_id") or 0) > 0))
        filled_order_rows = int(sum(1 for row in order_summary_rows if str(row.get("status") or "") == "FILLED"))
        filled_with_audit_rows = int(sum(1 for row in order_summary_rows if int(row.get("has_fill_audit") or 0) == 1))
        blocked_opportunity_rows = int(sum(1 for row in order_summary_rows if str(row.get("status") or "") == "BLOCKED_OPPORTUNITY"))
        portfolio_fill_rows = list(portfolio_fills.get(portfolio_id, []))
        slippage_values = [
            float(row.get("actual_slippage_bps") or 0.0)
            for row in portfolio_fill_rows
            if row.get("actual_slippage_bps") not in (None, "")
        ]
        realized_gross_pnl = float(sum(float(row.get("pnl") or 0.0) for row in portfolio_fill_rows))
        commission_total = float(sum(commission_by_exec.get(str(row.get("exec_id") or "").strip(), 0.0) for row in portfolio_fill_rows))
        fill_rate_status = (float(filled_order_rows) / float(submitted_order_rows)) if submitted_order_rows > 0 else None
        fill_rate_audit = (float(filled_with_audit_rows) / float(submitted_order_rows)) if submitted_order_rows > 0 else None
        out.append(
            {
                "week": str(week_label or ""),
                "week_start": str(week_start or ""),
                "portfolio_id": portfolio_id,
                "market": str(latest.get("market") or ""),
                "execution_run_rows": int(len(rows)),
                "execution_runs": int(len(rows)),
                "submitted_runs": int(sum(1 for row in rows if int(row.get("submitted") or 0) == 1)),
                "planned_order_rows": int(len(order_rows)),
                "execution_order_rows": int(len(order_rows)),
                "submitted_order_rows": submitted_order_rows,
                "filled_order_rows": filled_order_rows,
                "filled_with_audit_rows": filled_with_audit_rows,
                "blocked_opportunity_rows": blocked_opportunity_rows,
                "error_order_rows": int(sum(1 for row in order_summary_rows if str(row.get("status") or "").startswith("ERROR_"))),
                "fill_rows": int(len(portfolio_fill_rows)),
                "status_breakdown": ",".join(f"{status}:{status_counts[status]}" for status in sorted(status_counts)),
                "error_statuses": ",".join(sorted(error_statuses)),
                "planned_order_value": float(sum(abs(float(row.get("order_value") or 0.0)) for row in order_rows)),
                "commission_total": commission_total,
                "realized_gross_pnl": realized_gross_pnl,
                "realized_net_pnl": float(realized_gross_pnl - commission_total),
                "fill_rate_status": fill_rate_status,
                "fill_rate_audit": fill_rate_audit,
                "fill_rate": fill_rate_audit,
                "avg_actual_slippage_bps": _mean(slippage_values) if slippage_values else None,
                "latest_gap_symbols": int(nested_summary.get("gap_symbols", 0) or 0),
                "latest_gap_notional": float(nested_summary.get("gap_notional", 0.0) or 0.0),
                "latest_broker_equity": float(latest.get("broker_equity") or 0.0),
                "latest_broker_cash": float(latest.get("broker_cash") or 0.0),
            }
        )
    out.sort(key=lambda row: (str(row.get("portfolio_id") or ""),))
    return out


def _summarize_changes(change_rows: List[Dict[str, Any]], portfolio_id: str) -> str:
    marks = {"ADDED": "+", "REMOVED": "-", "INCREASED": "↑", "DECREASED": "↓", "WEIGHT_CHANGED": "~"}
    items = [
        f"{marks.get(str(row.get('change_type') or ''), '')}{row['symbol']}"
        for row in change_rows
        if str(row.get("portfolio_id") or "") == portfolio_id
    ]
    return ", ".join(items[:8])


def _top_holdings_text(rows: List[Dict[str, Any]], limit: int = 5) -> str:
    ordered = sorted(rows, key=lambda row: float(row.get("weight") or 0.0), reverse=True)
    return ",".join(
        f"{row['symbol']}:{float(row.get('weight', 0.0) or 0.0):.2f}"
        for row in ordered[:limit]
    )


def _top_sector_text(rows: List[Dict[str, Any]], portfolio_id: str, limit: int = 3) -> str:
    ordered = [row for row in rows if str(row.get("portfolio_id") or "") == portfolio_id]
    ordered.sort(key=lambda row: float(row.get("weight") or 0.0), reverse=True)
    return ",".join(
        f"{row['sector']}:{float(row.get('weight', 0.0) or 0.0):.2f}"
        for row in ordered[:limit]
    )


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
    return _load_json_file(_resolve_project_path(report_dir) / name)


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


def _runtime_config_paths_for_market(market: str) -> Dict[str, Path]:
    market_code = resolve_market_code(str(market or ""))
    ibkr_cfg = _load_yaml_file(market_config_path(BASE_DIR, market_code)) if market_code else {}
    return {
        "market_structure": _resolve_project_path(
            str(ibkr_cfg.get("market_structure_config", f"config/market_structure_{market_code.lower()}.yaml" if market_code else "config/market_structure.yaml"))
        ),
        "account_profile": _resolve_project_path(
            str(ibkr_cfg.get("account_profile_config", "config/account_profiles.yaml"))
        ),
        "adaptive_strategy": _resolve_project_path(
            str(ibkr_cfg.get("adaptive_strategy_config", "config/adaptive_strategy_framework.yaml"))
        ),
    }


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
        preferred = "/".join(str(item).upper() for item in list(market_rules.get("small_account_preferred_asset_classes", []) or []) if str(item).strip()) or "ETF"
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
) -> List[Dict[str, Any]]:
    broker_summary_map = {str(row.get("portfolio_id") or ""): dict(row) for row in list(broker_summary_rows or [])}
    market_cache: Dict[str, Dict[str, Any]] = {}
    context_rows: List[Dict[str, Any]] = []
    for row in list(summary_rows or []):
        portfolio_id = str(row.get("portfolio_id") or "")
        market = resolve_market_code(str(row.get("market") or ""))
        report_dir = _latest_report_dir(runs_by_portfolio, portfolio_id)
        market_sentiment = _load_market_sentiment(report_dir)
        opportunity_summary = _report_json(report_dir, "investment_opportunity_summary.json")
        paper_summary = _report_json(report_dir, "investment_paper_summary.json")
        execution_summary = _report_json(report_dir, "investment_execution_summary.json")
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
        account_profile = resolved_account_profile_summary(cached["account_profiles"], broker_equity=broker_equity) if broker_equity > 0.0 else {}
        adaptive_strategy = adaptive_strategy_context(cached["adaptive_strategy"])
        strategy_effective_controls_note = _strategy_effective_controls_note(execution_summary, paper_summary)
        execution_gate_summary = _execution_gate_summary(execution_summary)
        strategy_note = _weekly_strategy_note(
            market_rules=market_rules,
            account_profile=account_profile,
            adaptive_strategy=adaptive_strategy,
            opportunity_summary=opportunity_summary,
            market_sentiment=market_sentiment,
            strategy_effective_controls_note=strategy_effective_controls_note,
            execution_gate_summary=execution_gate_summary,
        )
        row["market_rules_summary"] = str(market_rules.get("summary_text", "") or "")
        row["account_profile_label"] = str(account_profile.get("label", "") or account_profile.get("name", "") or "")
        row["account_profile_summary"] = str(account_profile.get("summary", "") or "")
        row["adaptive_strategy_name"] = str(adaptive_strategy.get("name", "") or "")
        row["adaptive_strategy_summary"] = str(adaptive_strategy.get("summary_text", "") or "")
        row["strategy_effective_controls_applied"] = bool(
            execution_summary.get("strategy_effective_controls_applied")
            or paper_summary.get("strategy_effective_controls_applied")
        )
        row["strategy_effective_controls_note"] = strategy_effective_controls_note
        row["execution_gate_summary"] = execution_gate_summary
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


_SESSION_LABELS = {
    "OPEN": "开盘",
    "MIDDAY": "午盘",
    "CLOSE": "尾盘",
    "UNKNOWN": "未知时段",
}


def _execution_session_profile_from_order(row: Dict[str, Any]) -> Dict[str, str]:
    # 执行时段信息主要落在 details/plan_row 里；这里统一抽一遍，避免多个模块各写一套解析。
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


def _build_execution_session_rows(
    execution_orders: List[Dict[str, Any]],
    fill_rows: List[Dict[str, Any]],
    commission_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    # 这里把执行成本拆到 OPEN/MIDDAY/CLOSE 三段，方便判断到底是哪个时段的执行风格出了问题。
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
    # 这里继续向下钻到 symbol + session，专门找“哪个标的在什么时段最拖执行成本”。
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


def _build_planned_execution_cost_rows(execution_orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # 这里汇总“计划里的执行成本”，与 fills 侧的真实成本分开看，方便判断问题在信号还是执行。
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


def _build_attribution_rows(
    summary_rows: List[Dict[str, Any]],
    *,
    sector_rows: List[Dict[str, Any]],
    latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    execution_effect_rows: List[Dict[str, Any]],
    planned_execution_cost_rows: List[Dict[str, Any]] | None = None,
    execution_gate_rows: List[Dict[str, Any]] | None = None,
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    # 这是“策略升级用”的代理归因，不是严格的学术因子归因。
    # 目标是把周收益拆成几个最常见的调参方向，帮助判断该先调阈值、调仓位还是调执行。
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
            invested_ratio = max(0.0, min(1.5, 1.0 - float(summary.get("cash_after") or 0.0) / max(float(summary.get("start_equity") or 1.0), 1.0)))

        report_dir = _latest_report_dir(runs_by_portfolio, portfolio_id)
        market_sentiment = _load_market_sentiment(report_dir)
        paper_summary = _report_json(report_dir, "investment_paper_summary.json")
        execution_summary = _report_json(report_dir, "investment_execution_summary.json")
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

        execution_gate_blocked_order_count = int(gate_effect.get("blocked_order_count", 0) or 0)
        execution_gate_blocked_order_value = float(gate_effect.get("blocked_order_value", 0.0) or 0.0)
        execution_gate_blocked_order_ratio = float(gate_effect.get("blocked_order_ratio", 0.0) or 0.0)
        execution_gate_blocked_weight = (
            float(execution_gate_blocked_order_value / latest_equity)
            if latest_equity > 0.0
            else 0.0
        )
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


def _risk_overlay_from_history_row(row: Dict[str, Any]) -> Dict[str, Any]:
    # 新表优先读规范化字段；旧数据仍兼容从 details JSON 回退。
    if str(row.get("source_kind") or "").strip():
        stress_scenarios = _parse_json_dict(row.get("stress_scenarios_json"))
        return {
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
    if avg_corr >= 0.62 or top_sector_share >= 0.45:
        return "CORRELATION", "组合拥挤度偏高，优先增加跨行业/跨市场分散度，再考虑放宽仓位。"
    if worst_loss >= 0.085:
        return "STRESS", "最差 stress 场景压力偏大，优先收缩净/总敞口并复盘高波动标的。"
    if dynamic_net <= 0.70 or dynamic_gross <= 0.75:
        return "EXPOSURE_BUDGET", "组合风险预算仍偏紧，优先提升流动性与数据质量，再争取释放仓位。"
    return "NORMAL", "当前组合风险覆盖整体平稳，可以继续观察信号质量与资金利用率。"


def _build_risk_review_rows(
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
    risk_history_by_portfolio: Dict[str, List[Dict[str, Any]]] | None = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    all_portfolios = set(runs_by_portfolio)
    all_portfolios.update((risk_history_by_portfolio or {}).keys())
    for portfolio_id in sorted(all_portfolios):
        runs = list(runs_by_portfolio.get(portfolio_id, []) or [])
        history_rows = list((risk_history_by_portfolio or {}).get(portfolio_id, []) or [])
        source_rows = history_rows or runs
        overlays = [risk for risk in (_risk_overlay_from_history_row(row) for row in source_rows) if risk]
        if not overlays:
            continue
        latest = _latest_risk_overlay(source_rows)
        avg_dynamic_net = _mean([float(item.get("dynamic_net_exposure", 0.0) or 0.0) for item in overlays])
        avg_dynamic_gross = _mean([float(item.get("dynamic_gross_exposure", 0.0) or 0.0) for item in overlays])
        avg_avg_corr = _mean([float(item.get("avg_pair_correlation", 0.0) or 0.0) for item in overlays])
        avg_worst_loss = _mean([float(item.get("stress_worst_loss", 0.0) or 0.0) for item in overlays])
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
        dominant_driver, diagnosis = _risk_driver_and_diagnosis(row)
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


def _build_risk_feedback_rows(
    risk_review_rows: List[Dict[str, Any]],
    attribution_rows: List[Dict[str, Any]] | None = None,
    feedback_calibration_map: Dict[str, Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    # 这里把“风险复盘结论”转成下一轮 paper/execution 能直接消费的参数增减量。
    # 先只改组合预算相关参数，不去碰更深层的信号逻辑，避免闭环一下子过重。
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
            feedback_reason = (
                "组合风险预算偏紧，但相关性和 stress 仍在可接受范围，适度放宽预算以减少资金闲置。"
            )

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
                "本周更明显的压仓来自策略主动控仓，先复核 regime/target invested weight，"
                "暂不直接改风险预算。"
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
                f"本周主要压缩来自风险 overlay（{str(control_context.get('feedback_control_split_text') or '')}），"
                "继续沿风险预算方向调整更一致。"
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
        base_confidence = _feedback_confidence(
            sample_ratio=float(risk_overlay_runs / 4.0),
            magnitude_ratio=severity_ratio,
            persistence_ratio=float((1.0 - min(1.0, float(row.get("latest_dynamic_scale", 1.0) or 1.0))) / 0.30) if driver in {"CORRELATION", "STRESS"} else 0.0,
            structure_ratio=1.0 if driver in {"CORRELATION", "STRESS", "EXPOSURE_BUDGET"} else 0.0,
        ) if action != "HOLD" else 0.0
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
            0 if str(row.get("risk_feedback_action", "") or "") == "TIGHTEN" else 1 if str(row.get("risk_feedback_action", "") or "") == "RELAX" else 2,
            str(row.get("portfolio_id", "") or ""),
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
    # 这里把“计划成本 vs 实际执行成本”的偏差，转成下一轮 execution config 可直接使用的调参建议。
    # 第一版只收敛在参与率、拆单触发阈值、分片数和时段参与率，不自动碰更激进的价格缓冲。
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
            feedback_reason = (
                "实际执行成本高于计划，下一轮收紧 ADV 参与率、提前触发拆单，并降低开盘/尾盘的参与强度。"
            )
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

        # 有了时段级画像后，时段参与率不再一刀切；优先按 OPEN/MIDDAY/CLOSE 分段调，而不是全时段一起收紧。
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
                feedback_reason = (
                    f"总执行成本之外，{dominant_session_label}是本周最需要关注的执行时段；"
                    "已优先按时段反馈调整下一轮参与率。"
                )
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
            feedback_reason = (
                f"{feedback_reason.rstrip('。')}。当前最需要排查的执行热点是 "
                f"{dominant_hotspot_session_label}/{dominant_hotspot_symbol}。"
            )
        if execution_penalties:
            penalty_symbols = ",".join(str(item.get("symbol") or "") for item in execution_penalties[:6])
            feedback_reason = (
                f"{feedback_reason.rstrip('。')}。下一轮候选会对这些执行热点标的增加成本/执行惩罚: {penalty_symbols}。"
            )

        gate_pressure_high = gate_ratio >= 0.35 or gate_weight >= 0.03
        if (
            gate_pressure_high
            and control_driver == "EXECUTION"
            and action in {"HOLD", "RELAX"}
        ):
            action = "HOLD"
            adv_max_participation_delta = 0.0
            adv_split_trigger_delta = 0.0
            max_slices_delta = 0
            open_session_scale_delta = 0.0
            midday_session_scale_delta = 0.0
            close_session_scale_delta = 0.0
            execution_penalties = []
            control_driver_reason = (
                "本周更明显的问题是执行 gate 阻断，而不是成交成本；"
                "优先复核 opportunity/quality/risk/review gate，暂不直接调整 ADV/拆单参数。"
            )
            feedback_reason = (
                f"{control_driver_reason}（{str(control_context.get('feedback_control_split_text') or '')}）"
            )
        elif (
            gate_pressure_high
            and control_driver == "EXECUTION"
            and action == "TIGHTEN"
        ):
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


def _build_broker_summary_rows(
    execution_runs: List[Dict[str, Any]],
    execution_orders: List[Dict[str, Any]],
    broker_latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    fill_rows: List[Dict[str, Any]] | None = None,
    commission_rows: List[Dict[str, Any]] | None = None,
    *,
    week_label: str = "",
    week_start: str = "",
) -> List[Dict[str, Any]]:
    rows = _build_execution_summary_rows(
        execution_runs,
        execution_orders,
        fill_rows,
        commission_rows,
        week_label=week_label,
        week_start=week_start,
    )
    for row in rows:
        portfolio_id = str(row.get("portfolio_id") or "")
        holdings = broker_latest_rows_by_portfolio.get(portfolio_id, [])
        row["broker_holdings_count"] = int(len(holdings))
        row["broker_holdings_value"] = float(sum(float(h.get("market_value") or 0.0) for h in holdings))
        row["broker_top_holdings"] = _top_holdings_text(holdings)
    return rows


def _build_broker_local_diff_rows(
    local_latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    broker_latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for portfolio_id in sorted(set(local_latest_rows_by_portfolio) | set(broker_latest_rows_by_portfolio)):
        local_rows = local_latest_rows_by_portfolio.get(portfolio_id, [])
        broker_rows = broker_latest_rows_by_portfolio.get(portfolio_id, [])
        local_map = _rows_to_symbol_map(local_rows)
        broker_map = _rows_to_symbol_map(broker_rows)
        local_symbols = set(local_map)
        broker_symbols = set(broker_map)
        local_only = sorted(local_symbols - broker_symbols)
        broker_only = sorted(broker_symbols - local_symbols)
        common = sorted(local_symbols & broker_symbols)
        rows.append(
            {
                "portfolio_id": portfolio_id,
                "market": str((local_rows or broker_rows or [{}])[0].get("market") or ""),
                "local_holdings_count": int(len(local_symbols)),
                "broker_holdings_count": int(len(broker_symbols)),
                "common_symbol_count": int(len(common)),
                "local_only_count": int(len(local_only)),
                "broker_only_count": int(len(broker_only)),
                "local_only_symbols": ",".join(local_only),
                "broker_only_symbols": ",".join(broker_only),
            }
        )
    return rows



def _cli_summary_payload(summary: Dict[str, Any], out_dir: Path) -> tuple[Dict[str, Any], Dict[str, Path]]:
    summary_contract = WeeklyReviewSummary(
        market_filter=str(summary.get("market_filter") or "ALL"),
        portfolio_filter=str(summary.get("portfolio_filter") or "ALL"),
        portfolio_count=int(summary.get("portfolio_count") or 0),
        trade_count=int(summary.get("trade_count") or 0),
        execution_run_count=int(summary.get("execution_run_count") or 0),
        best_portfolio=str(summary.get("best_portfolio") or "-"),
        worst_portfolio=str(summary.get("worst_portfolio") or "-"),
    )
    artifacts = ArtifactBundle(
        summary_json=out_dir / "weekly_review_summary.json",
        summary_csv=out_dir / "weekly_portfolio_summary.csv",
        trade_log_csv=out_dir / "weekly_trade_log.csv",
        report_md=out_dir / "weekly_review.md",
    )
    return summary_contract.to_dict(), artifacts.to_dict()


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    db_path = _resolve_project_path(args.db)
    out_dir = _resolve_project_path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    thresholds_config_path = (
        _resolve_project_path(str(args.feedback_thresholds_config))
        if str(args.feedback_thresholds_config or "").strip()
        else (out_dir / "weekly_feedback_threshold_overrides.yaml")
    )
    feedback_threshold_overrides = _load_feedback_threshold_overrides(thresholds_config_path)

    market_filter = resolve_market_code(getattr(args, "market", ""))
    portfolio_filter = str(args.portfolio_id or "").strip()
    since_dt = datetime.now(timezone.utc) - timedelta(days=max(1, int(args.days)))
    since_ts = since_dt.isoformat()
    feedback_calibration_since_ts = (datetime.now(timezone.utc) - timedelta(days=FEEDBACK_CALIBRATION_LOOKBACK_DAYS)).isoformat()
    include_legacy = bool(args.include_legacy)
    labeling_dir = _resolve_labeling_summary_dir(str(args.labeling_dir or ""), market_filter)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        where = ["ts >= ?"]
        params: List[Any] = [since_ts]
        if market_filter:
            where.append("market = ?")
            params.append(market_filter)
        if portfolio_filter:
            where.append("portfolio_id = ?")
            params.append(portfolio_filter)
        elif not include_legacy:
            where.append("portfolio_id IS NOT NULL AND portfolio_id != ''")
        where_sql = " AND ".join(where)

        run_rows = [dict(row) for row in conn.execute(
            f"SELECT * FROM investment_runs WHERE {where_sql} ORDER BY ts ASC, id ASC",
            params,
        ).fetchall()]
        execution_run_rows = []
        if _table_exists(conn, "investment_execution_runs"):
            execution_run_rows = [dict(row) for row in conn.execute(
                f"SELECT * FROM investment_execution_runs WHERE {where_sql} ORDER BY ts ASC, id ASC",
                params,
            ).fetchall()]
        risk_history_rows = []
        if _table_exists(conn, "investment_risk_history"):
            risk_history_rows = [dict(row) for row in conn.execute(
                f"SELECT * FROM investment_risk_history WHERE {where_sql} ORDER BY ts ASC, id ASC",
                params,
            ).fetchall()]
        broker_position_rows = []
        if _table_exists(conn, "investment_broker_positions"):
            broker_position_rows = [dict(row) for row in conn.execute(
                f"SELECT * FROM investment_broker_positions WHERE {where_sql} ORDER BY ts ASC, id ASC",
                params,
            ).fetchall()]
        trade_rows = [dict(row) for row in conn.execute(
            f"SELECT * FROM investment_trades WHERE {where_sql} ORDER BY ts DESC, id DESC",
            params,
        ).fetchall()]
        execution_order_rows = []
        if _table_exists(conn, "investment_execution_orders"):
            execution_order_rows = [dict(row) for row in conn.execute(
                f"SELECT * FROM investment_execution_orders WHERE {where_sql} ORDER BY ts DESC, id DESC",
                params,
            ).fetchall()]
        fill_rows = []
        if _table_exists(conn, "fills"):
            fill_rows = [dict(row) for row in conn.execute(
                """
                SELECT ts, order_id, exec_id, symbol, qty, price, pnl, actual_slippage_bps, slippage_bps_deviation,
                       portfolio_id, system_kind, execution_run_id
                FROM fills
                ORDER BY ts DESC, id DESC
                """
            ).fetchall()]
        commission_rows = []
        if _table_exists(conn, "risk_events"):
            commission_rows = [dict(row) for row in conn.execute(
                """
                SELECT ts, kind, value, exec_id, symbol, portfolio_id, system_kind, execution_run_id
                FROM risk_events
                WHERE kind='COMMISSION'
                ORDER BY ts DESC, id DESC
                """
            ).fetchall()]

        pos_where = []
        pos_params: List[Any] = []
        if market_filter:
            pos_where.append("market = ?")
            pos_params.append(market_filter)
        if portfolio_filter:
            pos_where.append("portfolio_id = ?")
            pos_params.append(portfolio_filter)
        elif not include_legacy:
            pos_where.append("portfolio_id IS NOT NULL AND portfolio_id != ''")
        pos_sql = ("WHERE " + " AND ".join(pos_where)) if pos_where else ""
        position_rows = [dict(row) for row in conn.execute(
            f"SELECT * FROM investment_positions {pos_sql} ORDER BY ts ASC, id ASC",
            pos_params,
        ).fetchall()]
        outcome_rows = []
        if _table_exists(conn, "investment_candidate_outcomes"):
            outcome_where = ["outcome_ts >= ?"]
            outcome_params: List[Any] = [feedback_calibration_since_ts]
            if market_filter:
                outcome_where.append("market = ?")
                outcome_params.append(market_filter)
            if portfolio_filter:
                outcome_where.append("portfolio_id = ?")
                outcome_params.append(portfolio_filter)
            elif not include_legacy:
                outcome_where.append("portfolio_id IS NOT NULL AND portfolio_id != ''")
            outcome_sql = " AND ".join(outcome_where)
            outcome_rows = [dict(row) for row in conn.execute(
                f"""
                SELECT market, portfolio_id, symbol, horizon_days, snapshot_ts, outcome_ts, future_return, max_drawdown, max_runup, outcome_label, details
                FROM investment_candidate_outcomes
                WHERE {outcome_sql}
                ORDER BY outcome_ts DESC, id DESC
                """,
                outcome_params,
            ).fetchall()]
    finally:
        conn.close()

    labeling_summary = _read_json((labeling_dir / "investment_candidate_outcomes_summary.json")) if labeling_dir else {}
    labeling_skip_rows = _read_csv((labeling_dir / "investment_candidate_outcome_skip_summary.csv")) if labeling_dir else []
    if market_filter:
        labeling_skip_rows = [row for row in labeling_skip_rows if str(row.get("market") or "").upper() == market_filter]
    if portfolio_filter:
        labeling_skip_rows = [row for row in labeling_skip_rows if str(row.get("portfolio_id") or "") == portfolio_filter]

    runs_by_portfolio: Dict[str, List[Dict[str, Any]]] = {}
    for row in run_rows:
        runs_by_portfolio.setdefault(_portfolio_key(row), []).append(row)
    risk_history_by_portfolio: Dict[str, List[Dict[str, Any]]] = {}
    for row in risk_history_rows:
        risk_history_by_portfolio.setdefault(_portfolio_key(row), []).append(row)

    latest_rows_by_portfolio = _build_latest_run_positions(run_rows, position_rows)
    baseline_rows_by_portfolio = _build_position_snapshots(position_rows, asof_ts=since_ts, strict_before=True)
    broker_after_rows = [row for row in broker_position_rows if str(row.get("source") or "").strip().lower() == "after"]
    broker_latest_rows_by_portfolio = _build_latest_run_positions(execution_run_rows, broker_after_rows)
    sector_rows = _build_sector_rows(latest_rows_by_portfolio, runs_by_portfolio)
    change_rows = _build_holdings_change_rows(latest_rows_by_portfolio, baseline_rows_by_portfolio)
    reason_rows = _build_reason_summary(trade_rows)
    equity_curve_rows = _build_equity_curve_rows(runs_by_portfolio)
    review_now = datetime.now(timezone.utc)
    review_week_label, review_week_start = _current_iso_week_label(review_now)
    broker_summary_rows = _build_broker_summary_rows(
        execution_run_rows,
        execution_order_rows,
        broker_latest_rows_by_portfolio,
        fill_rows,
        commission_rows,
        week_label=review_week_label,
        week_start=review_week_start,
    )
    broker_diff_rows = _build_broker_local_diff_rows(latest_rows_by_portfolio, broker_latest_rows_by_portfolio)
    filtered_fill_rows = []
    for row in fill_rows:
        if str(row.get("system_kind") or "").strip() not in {"investment", ""}:
            continue
        ts = str(row.get("ts") or "")
        if ts and ts < since_ts:
            continue
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if portfolio_filter and portfolio_id != portfolio_filter:
            continue
        market_code = _market_from_portfolio_or_symbol(portfolio_id, str(row.get("symbol") or ""))
        if market_filter and market_code and market_code != market_filter:
            continue
        filtered_fill_rows.append(row)
    filtered_commission_rows = []
    for row in commission_rows:
        if str(row.get("system_kind") or "").strip() not in {"investment", ""}:
            continue
        ts = str(row.get("ts") or "")
        if ts and ts < since_ts:
            continue
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if portfolio_filter and portfolio_id != portfolio_filter:
            continue
        market_code = _market_from_portfolio_or_symbol(portfolio_id, str(row.get("symbol") or ""))
        if market_filter and market_code and market_code != market_filter:
            continue
        filtered_commission_rows.append(row)
    execution_effect_rows = _build_execution_effect_rows(filtered_fill_rows, filtered_commission_rows)
    planned_execution_cost_rows = _build_planned_execution_cost_rows(execution_order_rows)
    execution_gate_rows = _build_execution_gate_rows(execution_order_rows)
    execution_session_rows = _build_execution_session_rows(execution_order_rows, filtered_fill_rows, filtered_commission_rows)
    execution_hotspot_rows = _build_execution_hotspot_rows(execution_order_rows, filtered_fill_rows, filtered_commission_rows)
    execution_effect_map = {str(row.get("portfolio_id") or ""): dict(row) for row in execution_effect_rows}
    planned_execution_map = {str(row.get("portfolio_id") or ""): dict(row) for row in planned_execution_cost_rows}
    for row in broker_summary_rows:
        effect = dict(execution_effect_map.get(str(row.get("portfolio_id") or ""), {}) or {})
        planned = dict(planned_execution_map.get(str(row.get("portfolio_id") or ""), {}) or {})
        row["fill_count"] = int(effect.get("fill_count", 0) or 0)
        row["fill_notional"] = float(effect.get("fill_notional", 0.0) or 0.0)
        row["commission_total"] = float(effect.get("commission_total", 0.0) or 0.0)
        row["slippage_cost_total"] = float(effect.get("slippage_cost_total", 0.0) or 0.0)
        row["execution_cost_total"] = float(effect.get("execution_cost_total", 0.0) or 0.0)
        row["avg_actual_slippage_bps"] = effect.get("avg_actual_slippage_bps")
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
    shadow_review_order_rows = _build_shadow_review_order_rows(execution_order_rows)
    shadow_review_summary_rows = _build_shadow_review_summary_rows(shadow_review_order_rows)
    feedback_calibration_rows = _build_feedback_calibration_rows(outcome_rows)
    feedback_calibration_map: Dict[str, Dict[str, Any]] = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in feedback_calibration_rows
        if str(row.get("portfolio_id") or "").strip()
    }
    shadow_feedback_rows = _build_shadow_feedback_rows(
        shadow_review_order_rows,
        shadow_review_summary_rows,
        feedback_calibration_map=feedback_calibration_map,
    )
    risk_review_rows = _build_risk_review_rows(runs_by_portfolio, risk_history_by_portfolio)

    summary_rows: List[Dict[str, Any]] = []
    for portfolio_id, rows in runs_by_portfolio.items():
        first_row = rows[0]
        last_row = rows[-1]
        perf_rows = [r for r in rows if _run_source(r) != "broker_sync"]
        perf_first_row = perf_rows[0] if perf_rows else first_row
        perf_last_row = perf_rows[-1] if perf_rows else last_row
        equity_path = [float(r.get("equity_after") or 0.0) for r in perf_rows if r.get("equity_after") is not None]
        start_equity = float(perf_first_row.get("equity_before") or perf_first_row.get("equity_after") or 0.0)
        latest_equity = float(perf_last_row.get("equity_after") or 0.0)
        weekly_return = ((latest_equity / start_equity) - 1.0) if start_equity > 0 else 0.0
        portfolio_trades = [row for row in trade_rows if str(row.get("portfolio_id") or "") == portfolio_id]
        gross_buy_value = sum(abs(float(row.get("trade_value") or 0.0)) for row in portfolio_trades if str(row.get("action") or "").upper() == "BUY")
        gross_sell_value = sum(abs(float(row.get("trade_value") or 0.0)) for row in portfolio_trades if str(row.get("action") or "").upper() == "SELL")
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
                "avg_equity": float(_mean(equity_path)),
                "max_drawdown": float(_max_drawdown(equity_path)),
                "turnover": float((gross_buy_value + gross_sell_value) / max(1.0, _mean(equity_path))),
                "cash_after": float(last_row.get("cash_after") or 0.0),
                "holdings_count": int(len(holdings)),
                "top_holdings": _top_holdings_text(holdings),
                "top_sectors": _top_sector_text(sector_rows, portfolio_id),
                "holdings_change_summary": _summarize_changes(change_rows, portfolio_id),
                "broker_sync_runs": int(sum(1 for r in rows if _run_source(r) == "broker_sync")),
            }
        )

    summary_rows.sort(key=lambda row: float(row.get("weekly_return", 0.0) or 0.0), reverse=True)
    strategy_context_rows = _augment_summary_rows_with_strategy_context(
        summary_rows,
        broker_summary_rows=broker_summary_rows,
        runs_by_portfolio=runs_by_portfolio,
    )
    attribution_rows = _build_attribution_rows(
        summary_rows,
        sector_rows=sector_rows,
        latest_rows_by_portfolio=latest_rows_by_portfolio,
        execution_effect_rows=execution_effect_rows,
        planned_execution_cost_rows=planned_execution_cost_rows,
        execution_gate_rows=execution_gate_rows,
        runs_by_portfolio=runs_by_portfolio,
    )
    risk_feedback_rows = _build_risk_feedback_rows(
        risk_review_rows,
        attribution_rows=attribution_rows,
        feedback_calibration_map=feedback_calibration_map,
    )
    execution_feedback_rows = _build_execution_feedback_rows(
        attribution_rows,
        broker_summary_rows,
        execution_session_rows=execution_session_rows,
        execution_hotspot_rows=execution_hotspot_rows,
        feedback_calibration_map=feedback_calibration_map,
    )
    market_data_gate_map = _build_market_data_gate_map(
        runs_by_portfolio,
        preflight_dir=_resolve_project_path(str(args.preflight_dir or "reports_preflight")),
    )
    feedback_automation_rows = _build_feedback_automation_rows(
        shadow_feedback_rows,
        risk_feedback_rows,
        execution_feedback_rows,
        labeling_skip_rows=labeling_skip_rows,
        threshold_overrides=feedback_threshold_overrides,
        market_data_gate_map=market_data_gate_map,
    )
    window_label = f"{since_dt.date().isoformat()} -> {datetime.now(timezone.utc).date().isoformat()}"
    _persist_feedback_automation_history(
        db_path,
        feedback_automation_rows,
        week_label=review_week_label,
        week_start=review_week_start,
        window_start=since_ts,
        window_end=datetime.now(timezone.utc).isoformat(),
        feedback_calibration_map=feedback_calibration_map,
        risk_review_map={
            str(row.get("portfolio_id") or ""): dict(row)
            for row in risk_review_rows
            if str(row.get("portfolio_id") or "").strip()
        },
        execution_feedback_map={
            str(row.get("portfolio_id") or ""): dict(row)
            for row in execution_feedback_rows
            if str(row.get("portfolio_id") or "").strip()
        },
    )
    feedback_automation_effect_overview_rows = _build_feedback_automation_effect_overview(db_path, feedback_automation_rows)
    # 这里把自动应用后的真实效果按市场/反馈类型聚合成正式周报摘要，
    # 让后续调 AUTO_APPLY 阈值时直接看到“哪些市场在持续改善，哪些还要继续保守”。
    feedback_effect_market_summary_rows = _build_feedback_effect_market_summary(feedback_automation_effect_overview_rows)
    feedback_threshold_suggestion_rows = _build_feedback_threshold_suggestion_rows(
        feedback_effect_market_summary_rows,
        threshold_overrides=feedback_threshold_overrides,
    )
    _persist_feedback_threshold_history(
        db_path,
        feedback_threshold_suggestion_rows,
        week_label=review_week_label,
        week_start=review_week_start,
        window_start=since_ts,
        window_end=datetime.now(timezone.utc).isoformat(),
    )
    feedback_threshold_history_overview_rows = _build_feedback_threshold_history_overview(
        db_path,
        feedback_threshold_suggestion_rows,
    )
    feedback_threshold_effect_overview_rows = _build_feedback_threshold_effect_overview(
        db_path,
        feedback_threshold_suggestion_rows,
    )
    feedback_threshold_cohort_overview_rows = _build_feedback_threshold_cohort_overview(
        db_path,
        feedback_threshold_suggestion_rows,
    )
    feedback_threshold_trial_alert_rows = _build_feedback_threshold_trial_alert_overview(
        feedback_threshold_cohort_overview_rows,
    )
    feedback_threshold_tuning_rows = _build_feedback_threshold_tuning_summary(
        feedback_threshold_cohort_overview_rows,
    )

    avg_weekly_return = _mean([float(row.get("weekly_return") or 0.0) for row in summary_rows])
    avg_max_drawdown = _mean([float(row.get("max_drawdown") or 0.0) for row in summary_rows])
    buy_value_total = sum(float(row.get("gross_buy_value") or 0.0) for row in summary_rows)
    sell_value_total = sum(float(row.get("gross_sell_value") or 0.0) for row in summary_rows)
    best_portfolio = summary_rows[0]["portfolio_id"] if summary_rows else ""
    worst_portfolio = summary_rows[-1]["portfolio_id"] if summary_rows else ""

    write_csv(str(out_dir / "weekly_portfolio_summary.csv"), summary_rows)
    write_csv(str(out_dir / "weekly_trade_log.csv"), trade_rows)
    write_csv(str(out_dir / "weekly_holdings_change.csv"), change_rows)
    write_csv(str(out_dir / "weekly_sector_exposure.csv"), sector_rows)
    write_csv(str(out_dir / "weekly_reason_summary.csv"), reason_rows)
    write_csv(str(out_dir / "weekly_equity_curve.csv"), equity_curve_rows)
    write_csv(str(out_dir / "weekly_execution_summary.csv"), broker_summary_rows)
    write_csv(str(out_dir / "weekly_execution_orders.csv"), execution_order_rows)
    write_csv(str(out_dir / "weekly_shadow_review_orders.csv"), shadow_review_order_rows)
    write_csv(str(out_dir / "weekly_shadow_review_summary.csv"), shadow_review_summary_rows)
    write_csv(str(out_dir / "weekly_shadow_feedback_summary.csv"), shadow_feedback_rows)
    write_csv(str(out_dir / "weekly_feedback_calibration_summary.csv"), feedback_calibration_rows)
    write_csv(str(out_dir / "weekly_feedback_automation_summary.csv"), feedback_automation_rows)
    write_csv(str(out_dir / "weekly_feedback_automation_effect_overview.csv"), feedback_automation_effect_overview_rows)
    write_csv(str(out_dir / "weekly_feedback_effect_market_summary.csv"), feedback_effect_market_summary_rows)
    write_csv(str(out_dir / "weekly_feedback_threshold_suggestion_summary.csv"), feedback_threshold_suggestion_rows)
    write_csv(str(out_dir / "weekly_feedback_threshold_history_overview.csv"), feedback_threshold_history_overview_rows)
    write_csv(str(out_dir / "weekly_feedback_threshold_effect_overview.csv"), feedback_threshold_effect_overview_rows)
    write_csv(str(out_dir / "weekly_feedback_threshold_cohort_overview.csv"), feedback_threshold_cohort_overview_rows)
    write_csv(str(out_dir / "weekly_feedback_threshold_trial_alerts.csv"), feedback_threshold_trial_alert_rows)
    write_csv(str(out_dir / "weekly_feedback_threshold_tuning_summary.csv"), feedback_threshold_tuning_rows)
    write_csv(str(out_dir / "weekly_outcome_labeling_skip_summary.csv"), labeling_skip_rows)
    write_csv(str(out_dir / "weekly_execution_effects.csv"), execution_effect_rows)
    write_csv(str(out_dir / "weekly_planned_execution_costs.csv"), planned_execution_cost_rows)
    write_csv(str(out_dir / "weekly_execution_session_summary.csv"), execution_session_rows)
    write_csv(str(out_dir / "weekly_execution_hotspot_summary.csv"), execution_hotspot_rows)
    write_csv(str(out_dir / "weekly_attribution_summary.csv"), attribution_rows)
    write_csv(str(out_dir / "weekly_risk_review_summary.csv"), risk_review_rows)
    write_csv(str(out_dir / "weekly_risk_feedback_summary.csv"), risk_feedback_rows)
    write_csv(str(out_dir / "weekly_execution_feedback_summary.csv"), execution_feedback_rows)
    write_csv(str(out_dir / "weekly_broker_positions.csv"), [row for rows in broker_latest_rows_by_portfolio.values() for row in rows])
    write_csv(str(out_dir / "weekly_broker_comparison.csv"), broker_diff_rows)
    summary_payload = {
        "window_start": since_ts,
        "window_end": datetime.now(timezone.utc).isoformat(),
        "market_filter": market_filter or "ALL",
        "portfolio_filter": portfolio_filter or "ALL",
        "portfolio_count": len(summary_rows),
        "trade_count": len(trade_rows),
        "execution_run_count": len(execution_run_rows),
        "execution_order_count": len(execution_order_rows),
        "shadow_review_order_count": len(shadow_review_order_rows),
        "shadow_review_portfolio_count": len(shadow_review_summary_rows),
        "shadow_review_summary": shadow_review_summary_rows,
        "shadow_feedback_summary": shadow_feedback_rows,
        "feedback_calibration_summary": feedback_calibration_rows,
        "feedback_automation_summary": feedback_automation_rows,
        "feedback_automation_effect_overview": feedback_automation_effect_overview_rows,
        "feedback_effect_market_summary": feedback_effect_market_summary_rows,
        "feedback_threshold_suggestion_summary": feedback_threshold_suggestion_rows,
        "feedback_threshold_history_overview": feedback_threshold_history_overview_rows,
        "feedback_threshold_effect_overview": feedback_threshold_effect_overview_rows,
        "feedback_threshold_cohort_overview": feedback_threshold_cohort_overview_rows,
        "feedback_threshold_trial_alerts": feedback_threshold_trial_alert_rows,
        "feedback_threshold_tuning_summary": feedback_threshold_tuning_rows,
        "feedback_thresholds_config_path": str(thresholds_config_path),
        "labeling_summary": labeling_summary,
        "labeling_skip_summary": labeling_skip_rows,
        "execution_effect_summary": execution_effect_rows,
        "planned_execution_cost_summary": planned_execution_cost_rows,
        "execution_session_summary": execution_session_rows,
        "execution_hotspot_summary": execution_hotspot_rows,
        "attribution_summary": attribution_rows,
        "risk_review_summary": risk_review_rows,
        "risk_feedback_summary": risk_feedback_rows,
        "execution_feedback_summary": execution_feedback_rows,
        "broker_snapshot_portfolio_count": len(broker_latest_rows_by_portfolio),
        "avg_weekly_return": float(avg_weekly_return),
        "avg_max_drawdown": float(avg_max_drawdown),
        "gross_buy_value_total": float(buy_value_total),
        "gross_sell_value_total": float(sell_value_total),
        "best_portfolio": best_portfolio,
        "worst_portfolio": worst_portfolio,
        "portfolio_strategy_context": strategy_context_rows,
    }
    write_json(
        str(out_dir / "weekly_review_summary.json"),
        summary_payload,
    )
    _write_md(
        out_dir / "weekly_review.md",
        summary_rows,
        trade_rows,
        broker_summary_rows,
        broker_diff_rows,
        reason_rows,
        shadow_review_summary_rows,
        shadow_feedback_rows,
        feedback_calibration_rows,
        feedback_automation_rows,
        feedback_effect_market_summary_rows,
        feedback_threshold_suggestion_rows,
        feedback_threshold_history_overview_rows,
        feedback_threshold_effect_overview_rows,
        feedback_threshold_cohort_overview_rows,
        feedback_threshold_trial_alert_rows,
        feedback_threshold_tuning_rows,
        labeling_summary,
        labeling_skip_rows,
        attribution_rows,
        risk_review_rows,
        risk_feedback_rows,
        execution_session_rows,
        execution_hotspot_rows,
        execution_feedback_rows,
        window_label,
    )
    summary_fields, artifact_fields = _cli_summary_payload(summary_payload, out_dir)
    emit_cli_summary(
        command="ibkr-quant-weekly-review",
        headline="weekly investment review complete",
        summary=summary_fields,
        artifacts=artifact_fields,
    )
    log.info(
        "Wrote weekly investment review -> %s portfolios=%s trades=%s changes=%s sectors=%s",
        out_dir / "weekly_review.md",
        len(summary_rows),
        len(trade_rows),
        len(change_rows),
        len(sector_rows),
    )


if __name__ == "__main__":
    main()
