from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from ..common.markets import market_config_path, resolve_market_code
from ..common.runtime_paths import resolve_repo_path
from .review_weekly_io import load_yaml_file as _load_yaml_file

BASE_DIR = Path(__file__).resolve().parents[2]


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


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


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def _median(values: List[Any]) -> float | None:
    nums = sorted(float(v) for v in values if v is not None)
    if not nums:
        return None
    mid = len(nums) // 2
    if len(nums) % 2 == 1:
        return float(nums[mid])
    return float((nums[mid - 1] + nums[mid]) / 2.0)


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
