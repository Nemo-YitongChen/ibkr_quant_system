from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from ..common.markets import resolve_market_code
from .review_weekly_io import read_yaml


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def load_feedback_threshold_overrides(path: Path) -> Dict[str, Dict[str, Dict[str, float]]]:
    raw = read_yaml(path)
    markets_raw = raw.get("markets") if isinstance(raw, dict) else {}
    if not isinstance(markets_raw, dict):
        return {}
    out: Dict[str, Dict[str, Dict[str, float]]] = {}
    for market_key, market_value in markets_raw.items():
        market = resolve_market_code(str(market_key or ""))
        if not market or not isinstance(market_value, dict):
            continue
        kind_map: Dict[str, Dict[str, float]] = {}
        for feedback_kind, threshold_map in dict(market_value or {}).items():
            kind = str(feedback_kind or "").strip().lower()
            if not kind or not isinstance(threshold_map, dict):
                continue
            normalized: Dict[str, float] = {}
            for key, value in dict(threshold_map or {}).items():
                try:
                    normalized[str(key or "").strip()] = float(value)
                except Exception:
                    continue
            if normalized:
                kind_map[kind] = normalized
        if kind_map:
            out[market] = kind_map
    return out


def feedback_automation_thresholds(
    feedback_kind: str,
    *,
    market: str = "",
    threshold_overrides: Dict[str, Dict[str, Dict[str, float]]] | None = None,
) -> Dict[str, float]:
    kind = str(feedback_kind or "").strip().lower()
    if kind == "shadow":
        base = {
            "auto_confidence": 0.58,
            "suggest_confidence": 0.36,
            "auto_base_confidence": 0.72,
            "suggest_base_confidence": 0.46,
            "auto_feedback_samples": 2.0,
            "suggest_feedback_samples": 1.0,
            "ready_outcome_samples": 6.0,
            "auto_calibration_score": 0.55,
            "suggest_calibration_score": 0.42,
            "auto_maturity_ratio": 0.55,
            "suggest_maturity_ratio": 0.20,
        }
    elif kind == "risk":
        base = {
            "auto_confidence": 0.60,
            "suggest_confidence": 0.38,
            "auto_base_confidence": 0.74,
            "suggest_base_confidence": 0.48,
            "auto_feedback_samples": 2.0,
            "suggest_feedback_samples": 1.0,
            "ready_outcome_samples": 6.0,
            "auto_calibration_score": 0.54,
            "suggest_calibration_score": 0.42,
            "auto_maturity_ratio": 0.58,
            "suggest_maturity_ratio": 0.24,
        }
    else:
        base = {
            "auto_confidence": 0.60,
            "suggest_confidence": 0.40,
            "auto_base_confidence": 0.76,
            "suggest_base_confidence": 0.50,
            "auto_feedback_samples": 2.0,
            "suggest_feedback_samples": 1.0,
            "ready_outcome_samples": 6.0,
            "auto_calibration_score": 0.56,
            "suggest_calibration_score": 0.44,
            "auto_maturity_ratio": 0.60,
            "suggest_maturity_ratio": 0.24,
        }
    market_code = resolve_market_code(str(market or ""))
    overrides = dict(((threshold_overrides or {}).get(market_code, {}) or {}).get(kind, {}) or {})
    if overrides:
        base.update(overrides)
    return base


def feedback_kind_label(feedback_kind: str) -> str:
    kind = str(feedback_kind or "").strip().lower()
    if kind == "shadow":
        return "Shadow ML"
    if kind == "risk":
        return "风险预算"
    return "执行参数"


def feedback_automation_mode_label(mode: str) -> str:
    mode_code = str(mode or "").strip().upper()
    if mode_code == "AUTO_APPLY":
        return "自动应用"
    if mode_code == "SUGGEST_ONLY":
        return "建议确认"
    return "继续观察"


def feedback_automation_basis_label(basis: str) -> str:
    basis_code = str(basis or "").strip().upper()
    if basis_code == "OUTCOME_CALIBRATED":
        return "已有 outcome 校准"
    if basis_code == "BASE_WEEKLY":
        return "先按周报样本"
    if basis_code == "DATA_HEALTH_GATED":
        return "受市场数据 gate 保护"
    return "暂无自动应用依据"


def feedback_maturity_label(ratio: float, pending_count: int, sample_count: int) -> str:
    if sample_count <= 0 and pending_count <= 0:
        return "UNKNOWN"
    if pending_count <= 0 and sample_count > 0:
        return "MATURE"
    if ratio >= 0.60:
        return "LATE"
    if ratio >= 0.30:
        return "BUILDING"
    return "EARLY"

def feedback_action_field(feedback_kind: str) -> str:
    kind = str(feedback_kind or "").strip().lower()
    if kind == "shadow":
        return "shadow_review_action"
    if kind == "risk":
        return "risk_feedback_action"
    return "execution_feedback_action"


def feedback_threshold_effect_bucket(text: Any) -> str:
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


def _build_feedback_threshold_tuning_row(raw: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(raw)
    latest_bucket = feedback_threshold_effect_bucket(row.get("latest_effect"))
    w1_bucket = feedback_threshold_effect_bucket(row.get("effect_w1"))
    w2_bucket = feedback_threshold_effect_bucket(row.get("effect_w2"))
    w4_bucket = feedback_threshold_effect_bucket(row.get("effect_w4"))
    buckets = {latest_bucket, w1_bucket, w2_bucket, w4_bucket}
    cohort_action = str(row.get("cohort_action") or "").strip().upper()
    cohort_weeks = int(row.get("cohort_weeks", 0) or 0)

    suggestion_action = "WATCH_COHORT"
    suggestion_label = "继续观察"
    reason = "当前 cohort 还在积累样本，先继续观察。"

    if cohort_action == "RELAX_AUTO_APPLY":
        if "恶化" in buckets:
            suggestion_action = "REVERT_RELAX"
            suggestion_label = "收回放宽"
            reason = "放宽后的 cohort 已出现恶化，优先考虑收回这轮放宽。"
        elif cohort_weeks >= 2 and "改善" in buckets and "恶化" not in buckets:
            suggestion_action = "KEEP_RELAX"
            suggestion_label = "继续放宽试运行"
            reason = "放宽后的 cohort 已连续出现改善，可继续保留当前放宽。"
        elif cohort_weeks >= 2 and "稳定" in buckets and "恶化" not in buckets:
            suggestion_action = "SOFT_RELAX"
            suggestion_label = "温和保留放宽"
            reason = "放宽后总体稳定，但改善力度一般，建议先温和保留。"
    elif cohort_action == "TIGHTEN_AUTO_APPLY":
        if "恶化" in buckets:
            suggestion_action = "REVIEW_TIGHTEN"
            suggestion_label = "继续复核收紧"
            reason = "收紧后仍有恶化信号，需要继续复核这组阈值。"
        elif cohort_weeks >= 2 and ("稳定" in buckets or "改善" in buckets):
            suggestion_action = "KEEP_TIGHTEN"
            suggestion_label = "保持收紧"
            reason = "收紧后的 cohort 已趋稳，建议继续保持当前保守阈值。"

    return {
        "market": str(row.get("market") or ""),
        "feedback_kind": str(row.get("feedback_kind") or ""),
        "feedback_kind_label": str(row.get("feedback_kind_label") or "-"),
        "cohort_label": str(row.get("cohort_label") or "-"),
        "baseline_week": str(row.get("baseline_week") or "-"),
        "cohort_weeks": cohort_weeks,
        "latest_effect": str(row.get("latest_effect") or "-"),
        "effect_w1": str(row.get("effect_w1") or "-"),
        "effect_w2": str(row.get("effect_w2") or "-"),
        "effect_w4": str(row.get("effect_w4") or "-"),
        "suggestion_action": suggestion_action,
        "suggestion_label": suggestion_label,
        "diagnosis": str(row.get("diagnosis") or "-"),
        "reason": reason,
    }


def build_feedback_threshold_tuning_summary(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for raw in list(rows or []):
        out.append(_build_feedback_threshold_tuning_row(dict(raw)))
    out.sort(
        key=lambda row: (
            0 if str(row.get("suggestion_action") or "") in {"REVERT_RELAX", "REVIEW_TIGHTEN"} else 1 if str(row.get("suggestion_action") or "") in {"KEEP_RELAX", "KEEP_TIGHTEN"} else 2,
            -int(row.get("cohort_weeks", 0) or 0),
            str(row.get("market") or ""),
            str(row.get("feedback_kind_label") or ""),
        )
    )
    return out


def _feedback_threshold_suggestion_decision(
    row: Dict[str, Any],
) -> Dict[str, Any]:
    tracked_count = int(row.get("tracked_count", 0) or 0)
    avg_active_weeks = float(row.get("avg_active_weeks", 0.0) or 0.0)
    latest_up = int(row.get("latest_improved_count", 0) or 0)
    latest_down = int(row.get("latest_deteriorated_count", 0) or 0)
    milestone_up = int(row.get("w1_improved_count", 0) or 0) + int(row.get("w2_improved_count", 0) or 0) + int(
        row.get("w4_improved_count", 0) or 0
    )
    milestone_down = int(row.get("w1_deteriorated_count", 0) or 0) + int(
        row.get("w2_deteriorated_count", 0) or 0
    ) + int(row.get("w4_deteriorated_count", 0) or 0)

    action = "KEEP_BASE"
    suggestion_label = "维持基线"
    confidence_delta = 0.0
    base_confidence_delta = 0.0
    calibration_delta = 0.0
    maturity_delta = 0.0
    reason = "自动应用样本还不足以支持调阈值。"

    if latest_down > 0 or milestone_down > 0:
        action = "TIGHTEN_AUTO_APPLY"
        suggestion_label = "继续保守"
        confidence_delta = 0.04
        base_confidence_delta = 0.04
        calibration_delta = 0.03
        maturity_delta = 0.05
        reason = "自动应用后已出现恶化样本，建议抬高 AUTO_APPLY 门槛。"
    elif latest_up > 0 and milestone_up > 0 and tracked_count >= 1 and avg_active_weeks >= 2.0:
        action = "RELAX_AUTO_APPLY"
        suggestion_label = "可适度放宽"
        confidence_delta = -0.03
        base_confidence_delta = -0.03
        calibration_delta = -0.02
        maturity_delta = -0.05
        reason = "自动应用后已出现连续改善，可适度放宽 AUTO_APPLY 门槛。"
    elif str(row.get("summary_signal", "") or "") == "稳定跟踪":
        action = "KEEP_BASE"
        suggestion_label = "继续跟踪"
        reason = "自动应用后整体稳定，但改善力度还不足以放宽阈值。"
    elif str(row.get("summary_signal", "") or "") == "观察中":
        action = "KEEP_CONSERVATIVE"
        suggestion_label = "继续观察"
        reason = "自动应用样本仍偏少，先保持保守阈值。"

    return {
        "tracked_count": tracked_count,
        "avg_active_weeks": avg_active_weeks,
        "action": action,
        "suggestion_label": suggestion_label,
        "confidence_delta": confidence_delta,
        "base_confidence_delta": base_confidence_delta,
        "calibration_delta": calibration_delta,
        "maturity_delta": maturity_delta,
        "reason": reason,
    }


def _build_feedback_threshold_suggestion_row(
    raw: Dict[str, Any],
    threshold_overrides: Dict[str, Dict[str, Dict[str, float]]] | None = None,
) -> Dict[str, Any]:
    row = dict(raw)
    feedback_kind = str(row.get("feedback_kind", "") or "").strip().lower()
    base = feedback_automation_thresholds(
        feedback_kind,
        market=str(row.get("market") or ""),
        threshold_overrides=threshold_overrides,
    )
    decision = _feedback_threshold_suggestion_decision(row)
    return {
        "market": str(row.get("market", "") or "-"),
        "feedback_kind": feedback_kind,
        "feedback_kind_label": str(row.get("feedback_kind_label", "") or feedback_kind),
        "summary_signal": str(row.get("summary_signal", "") or "-"),
        "suggestion_action": str(decision.get("action") or ""),
        "suggestion_label": str(decision.get("suggestion_label") or ""),
        "tracked_count": int(decision.get("tracked_count", 0) or 0),
        "avg_active_weeks": float(decision.get("avg_active_weeks", 0.0) or 0.0),
        "base_auto_confidence": float(base.get("auto_confidence", 0.0) or 0.0),
        "suggested_auto_confidence": round(
            _clamp(float(base.get("auto_confidence", 0.0) or 0.0) + float(decision.get("confidence_delta", 0.0) or 0.0), 0.0, 1.0),
            6,
        ),
        "base_auto_base_confidence": float(base.get("auto_base_confidence", 0.0) or 0.0),
        "suggested_auto_base_confidence": round(
            _clamp(
                float(base.get("auto_base_confidence", 0.0) or 0.0)
                + float(decision.get("base_confidence_delta", 0.0) or 0.0),
                0.0,
                1.0,
            ),
            6,
        ),
        "base_auto_calibration_score": float(base.get("auto_calibration_score", 0.0) or 0.0),
        "suggested_auto_calibration_score": round(
            _clamp(
                float(base.get("auto_calibration_score", 0.0) or 0.0)
                + float(decision.get("calibration_delta", 0.0) or 0.0),
                0.0,
                1.0,
            ),
            6,
        ),
        "base_auto_maturity_ratio": float(base.get("auto_maturity_ratio", 0.0) or 0.0),
        "suggested_auto_maturity_ratio": round(
            _clamp(
                float(base.get("auto_maturity_ratio", 0.0) or 0.0)
                + float(decision.get("maturity_delta", 0.0) or 0.0),
                0.0,
                1.0,
            ),
            6,
        ),
        "reason": str(decision.get("reason") or ""),
        "examples": str(row.get("top_portfolios_text", "") or "-"),
    }


def build_feedback_threshold_suggestion_rows(
    rows: List[Dict[str, Any]],
    threshold_overrides: Dict[str, Dict[str, Dict[str, float]]] | None = None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for raw in list(rows or []):
        row = dict(raw)
        feedback_kind = str(row.get("feedback_kind", "") or "").strip().lower()
        if not feedback_kind:
            continue
        out.append(_build_feedback_threshold_suggestion_row(row, threshold_overrides))
    out.sort(
        key=lambda row: (
            0 if str(row.get("suggestion_action") or "") == "TIGHTEN_AUTO_APPLY" else 1 if str(row.get("suggestion_action") or "") == "RELAX_AUTO_APPLY" else 2,
            str(row.get("market") or ""),
            str(row.get("feedback_kind_label") or ""),
        )
    )
    return out
