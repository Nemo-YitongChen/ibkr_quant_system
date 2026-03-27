from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import math
from typing import Any, Dict, List

try:
    import numpy as np
except Exception:  # pragma: no cover - graceful fallback when numpy is unavailable
    np = None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(value)))


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(str(value))
    except Exception:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


@dataclass
class InvestmentShadowModelConfig:
    enabled: bool = True
    horizon_days: int = 20
    min_samples: int = 40
    max_training_rows: int = 4000
    lookback_days: int = 540
    ridge_lambda: float = 0.75
    positive_return_threshold: float = 0.0
    return_scale: float = 0.12
    score_return_weight: float = 0.55
    score_prob_weight: float = 0.45
    stage_values: tuple[str, ...] = ("final", "deep", "broad")

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "InvestmentShadowModelConfig":
        raw = dict(raw or {})
        if "stage_values" in raw:
            stage_values = raw.get("stage_values")
            if isinstance(stage_values, str):
                raw["stage_values"] = tuple(
                    str(part).strip().lower() for part in stage_values.split(",") if str(part).strip()
                ) or cls.stage_values
            elif isinstance(stage_values, (list, tuple)):
                raw["stage_values"] = tuple(
                    str(part).strip().lower() for part in stage_values if str(part).strip()
                ) or cls.stage_values
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


_FEATURE_NAMES = [
    "score",
    "model_recommendation_score",
    "execution_score",
    "analyst_recommendation_score",
    "market_sentiment_score",
    "data_quality_score",
    "source_coverage",
    "missing_ratio",
    "expected_cost_bps",
    "liquidity_score",
    "avg_daily_dollar_volume",
    "weekly_feedback_score_penalty",
    "weekly_feedback_execution_penalty",
    "microstructure_score",
    "micro_breakout_5m",
    "micro_reversal_5m",
    "micro_volume_burst_5m",
    "returns_ewma_vol_20d",
    "returns_downside_vol_20d",
    "execution_ready",
    "action_accumulate",
    "action_hold",
    "action_watch",
    "tier_final",
    "tier_deep",
    "tier_broad",
]


def _details_map(row: Dict[str, Any]) -> Dict[str, Any]:
    details = row.get("details_json")
    if isinstance(details, dict):
        return details
    raw = row.get("details")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except Exception:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _row_value(row: Dict[str, Any], key: str, default: float = 0.0) -> float:
    if key in row and row.get(key) is not None:
        return _safe_float(row.get(key), default)
    return _safe_float(_details_map(row).get(key), default)


def _feature_row(row: Dict[str, Any]) -> List[float]:
    action = str(row.get("action", "") or "").strip().upper()
    scan_tier = str(row.get("scan_tier", row.get("stage", "")) or "").strip().lower()
    return [
        _row_value(row, "score"),
        _row_value(row, "model_recommendation_score", _row_value(row, "score")),
        _row_value(row, "execution_score"),
        _row_value(row, "analyst_recommendation_score", _row_value(row, "recommendation_score")),
        _row_value(row, "market_sentiment_score"),
        _row_value(row, "data_quality_score", 1.0),
        _row_value(row, "source_coverage", 1.0),
        _row_value(row, "missing_ratio"),
        _row_value(row, "expected_cost_bps"),
        _row_value(row, "liquidity_score"),
        _row_value(row, "avg_daily_dollar_volume"),
        _row_value(row, "weekly_feedback_score_penalty"),
        _row_value(row, "weekly_feedback_execution_penalty"),
        _row_value(row, "microstructure_score"),
        _row_value(row, "micro_breakout_5m"),
        _row_value(row, "micro_reversal_5m"),
        _row_value(row, "micro_volume_burst_5m"),
        _row_value(row, "returns_ewma_vol_20d"),
        _row_value(row, "returns_downside_vol_20d"),
        float(bool(_safe_int(row.get("execution_ready", _details_map(row).get("execution_ready", 0)), 0))),
        1.0 if action == "ACCUMULATE" else 0.0,
        1.0 if action == "HOLD" else 0.0,
        1.0 if action == "WATCH" else 0.0,
        1.0 if scan_tier == "final" else 0.0,
        1.0 if scan_tier in {"deep", "deep_pool"} else 0.0,
        1.0 if scan_tier == "broad" else 0.0,
    ]


def _fit_ridge(design, target, ridge_lambda: float):
    assert np is not None
    gram = design.T @ design
    reg = np.eye(design.shape[1], dtype=float) * float(max(0.0, ridge_lambda))
    reg[0, 0] = 0.0
    return np.linalg.solve(gram + reg, design.T @ target)


def _sigmoid(value: float) -> float:
    clamped = _clamp(value, -30.0, 30.0)
    return 1.0 / (1.0 + math.exp(-clamped))


def _top_feature_weights(feature_names: List[str], beta, limit: int = 5) -> List[Dict[str, Any]]:
    if np is None:
        return []
    pairs = [
        {"feature": name, "weight": float(weight)}
        for name, weight in zip(feature_names, list(beta[1:]))
    ]
    pairs.sort(key=lambda row: abs(float(row.get("weight", 0.0) or 0.0)), reverse=True)
    return pairs[: max(1, int(limit))]


def train_investment_shadow_model(
    training_rows: List[Dict[str, Any]],
    *,
    cfg: InvestmentShadowModelConfig | None = None,
) -> Dict[str, Any]:
    cfg = cfg or InvestmentShadowModelConfig()
    if not bool(cfg.enabled):
        return {"enabled": False, "reason": "disabled"}
    if np is None:
        return {"enabled": False, "reason": "numpy_unavailable"}

    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(cfg.lookback_days)))
    filtered: List[Dict[str, Any]] = []
    for row in list(training_rows or []):
        outcome_ts = _parse_ts(row.get("outcome_ts"))
        if outcome_ts is not None and outcome_ts < cutoff:
            continue
        if row.get("future_return") is None:
            continue
        filtered.append(dict(row))
    if len(filtered) < int(cfg.min_samples):
        return {
            "enabled": False,
            "reason": "insufficient_samples",
            "training_samples": int(len(filtered)),
            "required_samples": int(cfg.min_samples),
            "horizon_days": int(cfg.horizon_days),
        }

    feature_matrix = np.array([_feature_row(row) for row in filtered], dtype=float)
    feature_means = feature_matrix.mean(axis=0)
    feature_stds = feature_matrix.std(axis=0)
    feature_stds = np.where(feature_stds < 1e-6, 1.0, feature_stds)
    standardized = (feature_matrix - feature_means) / feature_stds
    design = np.concatenate([np.ones((standardized.shape[0], 1), dtype=float), standardized], axis=1)
    y_return = np.array([_safe_float(row.get("future_return")) for row in filtered], dtype=float)
    y_positive = np.array(
        [1.0 if _safe_float(row.get("future_return")) > float(cfg.positive_return_threshold) else 0.0 for row in filtered],
        dtype=float,
    )

    beta_return = _fit_ridge(design, y_return, cfg.ridge_lambda)
    beta_prob = _fit_ridge(design, y_positive, cfg.ridge_lambda)
    pred_return = design @ beta_return
    pred_prob = np.array([_sigmoid(value) for value in list(design @ beta_prob)], dtype=float)

    directional_hits = np.mean(
        np.array(
            [
                1.0 if ((pred > float(cfg.positive_return_threshold)) == (actual > float(cfg.positive_return_threshold))) else 0.0
                for pred, actual in zip(list(pred_return), list(y_return))
            ],
            dtype=float,
        )
    )
    mae = np.mean(np.abs(pred_return - y_return))

    return {
        "enabled": True,
        "reason": "trained",
        "model_version": "ridge_v2",
        "feature_count": int(len(_FEATURE_NAMES)),
        "feature_names": list(_FEATURE_NAMES),
        "feature_means": [float(x) for x in list(feature_means)],
        "feature_stds": [float(x) for x in list(feature_stds)],
        "beta_return": [float(x) for x in list(beta_return)],
        "beta_prob": [float(x) for x in list(beta_prob)],
        "training_samples": int(len(filtered)),
        "required_samples": int(cfg.min_samples),
        "horizon_days": int(cfg.horizon_days),
        "lookback_days": int(cfg.lookback_days),
        "stages": [str(item) for item in list(cfg.stage_values)],
        "avg_future_return": float(np.mean(y_return)),
        "positive_rate": float(np.mean(y_positive)),
        "train_mae": float(mae),
        "train_directional_accuracy": float(directional_hits),
        "top_return_weights": _top_feature_weights(list(_FEATURE_NAMES), beta_return),
        "top_probability_weights": _top_feature_weights(list(_FEATURE_NAMES), beta_prob),
    }


def apply_investment_shadow_model(
    rows: List[Dict[str, Any]],
    *,
    model: Dict[str, Any],
    cfg: InvestmentShadowModelConfig | None = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    cfg = cfg or InvestmentShadowModelConfig()
    enabled = bool(model.get("enabled", False))
    if np is None or not enabled:
        summary = {
            "enabled": False,
            "reason": str(model.get("reason", "unavailable") or "unavailable"),
            "model_version": str(model.get("model_version", "ridge_v2") or "ridge_v2"),
            "training_samples": int(model.get("training_samples", 0) or 0),
            "horizon_days": int(model.get("horizon_days", cfg.horizon_days) or cfg.horizon_days),
            "avg_shadow_ml_score": 0.0,
            "avg_shadow_ml_return": 0.0,
            "avg_shadow_ml_positive_prob": 0.0,
        }
        out = []
        for row in list(rows or []):
            enriched = dict(row)
            enriched.update(
                {
                    "shadow_ml_enabled": 0,
                    "shadow_ml_score": 0.0,
                    "shadow_ml_return": 0.0,
                    "shadow_ml_positive_prob": 0.0,
                    "shadow_ml_horizon_days": int(summary["horizon_days"]),
                    "shadow_ml_training_samples": int(summary["training_samples"]),
                    "shadow_ml_reason": str(summary["reason"]),
                }
            )
            out.append(enriched)
        return out, summary

    feature_means = np.array(list(model.get("feature_means", []) or []), dtype=float)
    feature_stds = np.array(list(model.get("feature_stds", []) or []), dtype=float)
    beta_return = np.array(list(model.get("beta_return", []) or []), dtype=float)
    beta_prob = np.array(list(model.get("beta_prob", []) or []), dtype=float)

    out: List[Dict[str, Any]] = []
    score_values: List[float] = []
    return_values: List[float] = []
    prob_values: List[float] = []
    for row in list(rows or []):
        raw_features = np.array(_feature_row(row), dtype=float)
        standardized = (raw_features - feature_means) / feature_stds
        design = np.concatenate([np.ones(1, dtype=float), standardized], axis=0)
        predicted_return = float(design @ beta_return)
        positive_prob = float(_sigmoid(float(design @ beta_prob)))
        return_component = _clamp(predicted_return / max(float(cfg.return_scale), 1e-6), -1.0, 1.0)
        prob_component = _clamp((positive_prob - 0.5) * 2.0, -1.0, 1.0)
        shadow_score = _clamp(
            float(cfg.score_return_weight) * return_component + float(cfg.score_prob_weight) * prob_component,
            -1.0,
            1.0,
        )
        enriched = dict(row)
        enriched.update(
            {
                "shadow_ml_enabled": 1,
                "shadow_ml_score": float(shadow_score),
                "shadow_ml_return": float(predicted_return),
                "shadow_ml_positive_prob": float(positive_prob),
                "shadow_ml_horizon_days": int(model.get("horizon_days", cfg.horizon_days) or cfg.horizon_days),
                "shadow_ml_training_samples": int(model.get("training_samples", 0) or 0),
                "shadow_ml_reason": str(model.get("reason", "trained") or "trained"),
            }
        )
        out.append(enriched)
        score_values.append(float(shadow_score))
        return_values.append(float(predicted_return))
        prob_values.append(float(positive_prob))

    count = max(1, len(out))
    summary = {
        "enabled": True,
        "reason": str(model.get("reason", "trained") or "trained"),
        "model_version": str(model.get("model_version", "ridge_v2") or "ridge_v2"),
        "training_samples": int(model.get("training_samples", 0) or 0),
        "horizon_days": int(model.get("horizon_days", cfg.horizon_days) or cfg.horizon_days),
        "avg_shadow_ml_score": float(sum(score_values) / count),
        "avg_shadow_ml_return": float(sum(return_values) / count),
        "avg_shadow_ml_positive_prob": float(sum(prob_values) / count),
        "positive_rate": float(model.get("positive_rate", 0.0) or 0.0),
        "train_directional_accuracy": float(model.get("train_directional_accuracy", 0.0) or 0.0),
        "top_return_weights": list(model.get("top_return_weights", []) or []),
        "top_probability_weights": list(model.get("top_probability_weights", []) or []),
    }
    return out, summary
