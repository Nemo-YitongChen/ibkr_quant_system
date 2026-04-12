from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, List

import yaml


@dataclass
class StrategyMetaConfig:
    name: str = "ACM-RS"
    display_name: str = "Adaptive Cross-Market Relative Strength"
    objective: str = ""
    primary_alpha: str = ""
    secondary_alpha: str = ""
    implementation_bias: str = ""

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "StrategyMetaConfig":
        raw = dict(raw or {})
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


@dataclass
class RegimeRuleConfig:
    long_ma_window: int = 120
    short_ma_window: int = 20
    near_long_ma_band_pct: float = 0.03
    high_vol_multiple_vs_long: float = 1.2
    long_vol_window: int = 120
    short_vol_window: int = 20

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "RegimeRuleConfig":
        raw = dict(raw or {})
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


@dataclass
class RelativeStrengthRuleConfig:
    lookback_long: int = 126
    lookback_mid: int = 63
    volatility_window: int = 20
    long_weight: float = 0.50
    mid_weight: float = 0.30
    volatility_penalty_weight: float = 0.20
    price_filter_ma_window: int = 60
    liquidity_window: int = 60
    liquidity_percentile_floor: float = 0.80

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "RelativeStrengthRuleConfig":
        raw = dict(raw or {})
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


@dataclass
class PullbackRuleConfig:
    trend_ma_window: int = 120
    long_strength_lookback: int = 63
    short_pullback_lookback: int = 5
    long_strength_top_pct: float = 0.40
    short_pullback_bottom_pct: float = 0.20
    max_volatility_percentile: float = 0.80
    size_scale_vs_trend: float = 0.50

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "PullbackRuleConfig":
        raw = dict(raw or {})
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


@dataclass
class DefensiveProfileConfig:
    small_max_gross: float = 0.20
    medium_max_gross: float = 0.30
    large_max_gross: float = 0.40
    raise_entry_threshold_pct: float = 0.20

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "DefensiveProfileConfig":
        raw = dict(raw or {})
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


@dataclass
class ExecutionRhythmConfig:
    signal_frequency: str = "daily_close"
    rebalance_frequency: str = "weekly"
    max_rebalances_per_week_high_vol: int = 2
    entry_delay_min_minutes: int = 15
    entry_delay_max_minutes: int = 30

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "ExecutionRhythmConfig":
        raw = dict(raw or {})
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


@dataclass
class RolloutStage:
    name: str = ""
    scope: str = ""
    notes: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "RolloutStage":
        raw = dict(raw or {})
        return cls(
            name=str(raw.get("name") or "").strip(),
            scope=str(raw.get("scope") or "").strip(),
            notes=[str(item).strip() for item in list(raw.get("notes", []) or []) if str(item).strip()],
        )


@dataclass
class AdaptiveStrategyConfig:
    meta: StrategyMetaConfig = field(default_factory=StrategyMetaConfig)
    regime: RegimeRuleConfig = field(default_factory=RegimeRuleConfig)
    relative_strength: RelativeStrengthRuleConfig = field(default_factory=RelativeStrengthRuleConfig)
    pullback: PullbackRuleConfig = field(default_factory=PullbackRuleConfig)
    defensive: DefensiveProfileConfig = field(default_factory=DefensiveProfileConfig)
    execution: ExecutionRhythmConfig = field(default_factory=ExecutionRhythmConfig)
    rollout: List[RolloutStage] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "AdaptiveStrategyConfig":
        raw = dict(raw or {})
        nested = raw.get("adaptive_strategy")
        if isinstance(nested, dict):
            raw = dict(nested)
        return cls(
            meta=StrategyMetaConfig.from_dict(raw.get("meta")),
            regime=RegimeRuleConfig.from_dict(raw.get("regime")),
            relative_strength=RelativeStrengthRuleConfig.from_dict(raw.get("relative_strength")),
            pullback=PullbackRuleConfig.from_dict(raw.get("pullback")),
            defensive=DefensiveProfileConfig.from_dict(raw.get("defensive")),
            execution=ExecutionRhythmConfig.from_dict(raw.get("execution")),
            rollout=[RolloutStage.from_dict(item) for item in list(raw.get("rollout", []) or []) if isinstance(item, dict)],
            notes=[str(item).strip() for item in list(raw.get("notes", []) or []) if str(item).strip()],
        )


DEFENSIVE_REGIME_STATES = {"RISK_OFF", "HARD_RISK_OFF", "DEFENSIVE"}


def adaptive_strategy_config_path(base_dir: Path, explicit_path: str | None = None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if path.is_absolute():
            return path
        for candidate in (base_dir / path, base_dir / "config" / path, Path.cwd() / path, Path.cwd() / "config" / path):
            if candidate.exists():
                return candidate.resolve()
        return (base_dir / path).resolve()
    return (base_dir / "config" / "adaptive_strategy_framework.yaml").resolve()


def load_adaptive_strategy(base_dir: Path, explicit_path: str | None = None) -> AdaptiveStrategyConfig:
    path = adaptive_strategy_config_path(base_dir, explicit_path)
    if not path.exists():
        return AdaptiveStrategyConfig()
    with path.open("r", encoding="utf-8") as f:
        payload = yaml.safe_load(f) or {}
    return AdaptiveStrategyConfig.from_dict(payload)


def adaptive_strategy_context(cfg: AdaptiveStrategyConfig) -> Dict[str, Any]:
    rollout = [
        {"name": stage.name, "scope": stage.scope, "notes": list(stage.notes or [])}
        for stage in list(cfg.rollout or [])
    ]
    rs = cfg.relative_strength
    regime = cfg.regime
    execution = cfg.execution
    pullback = cfg.pullback
    defensive = cfg.defensive
    summary_parts = [
        f"{cfg.meta.name}",
        f"RS={rs.lookback_long}/{rs.lookback_mid}/{rs.volatility_window}",
        f"rebalance={execution.rebalance_frequency}",
        f"entry_delay={execution.entry_delay_min_minutes}-{execution.entry_delay_max_minutes}m",
    ]
    return {
        "name": cfg.meta.name,
        "display_name": cfg.meta.display_name,
        "objective": cfg.meta.objective,
        "primary_alpha": cfg.meta.primary_alpha,
        "secondary_alpha": cfg.meta.secondary_alpha,
        "implementation_bias": cfg.meta.implementation_bias,
        "regime": {
            "long_ma_window": int(regime.long_ma_window),
            "short_ma_window": int(regime.short_ma_window),
            "near_long_ma_band_pct": float(regime.near_long_ma_band_pct),
            "high_vol_multiple_vs_long": float(regime.high_vol_multiple_vs_long),
            "long_vol_window": int(regime.long_vol_window),
            "short_vol_window": int(regime.short_vol_window),
        },
        "relative_strength": {
            "lookback_long": int(rs.lookback_long),
            "lookback_mid": int(rs.lookback_mid),
            "volatility_window": int(rs.volatility_window),
            "long_weight": float(rs.long_weight),
            "mid_weight": float(rs.mid_weight),
            "volatility_penalty_weight": float(rs.volatility_penalty_weight),
            "price_filter_ma_window": int(rs.price_filter_ma_window),
            "liquidity_window": int(rs.liquidity_window),
            "liquidity_percentile_floor": float(rs.liquidity_percentile_floor),
        },
        "pullback": {
            "trend_ma_window": int(pullback.trend_ma_window),
            "long_strength_lookback": int(pullback.long_strength_lookback),
            "short_pullback_lookback": int(pullback.short_pullback_lookback),
            "long_strength_top_pct": float(pullback.long_strength_top_pct),
            "short_pullback_bottom_pct": float(pullback.short_pullback_bottom_pct),
            "max_volatility_percentile": float(pullback.max_volatility_percentile),
            "size_scale_vs_trend": float(pullback.size_scale_vs_trend),
        },
        "defensive": {
            "small_max_gross": float(defensive.small_max_gross),
            "medium_max_gross": float(defensive.medium_max_gross),
            "large_max_gross": float(defensive.large_max_gross),
            "raise_entry_threshold_pct": float(defensive.raise_entry_threshold_pct),
        },
        "execution": {
            "signal_frequency": execution.signal_frequency,
            "rebalance_frequency": execution.rebalance_frequency,
            "max_rebalances_per_week_high_vol": int(execution.max_rebalances_per_week_high_vol),
            "entry_delay_min_minutes": int(execution.entry_delay_min_minutes),
            "entry_delay_max_minutes": int(execution.entry_delay_max_minutes),
        },
        "rollout": rollout,
        "notes": list(cfg.notes or []),
        "summary_text": " | ".join(summary_parts),
    }


def align_opportunity_config_with_adaptive_strategy(opportunity_cfg: Any, cfg: AdaptiveStrategyConfig | None) -> Any:
    if opportunity_cfg is None or cfg is None:
        return opportunity_cfg
    aligned_ma_slow = max(
        int(getattr(opportunity_cfg, "ma_slow_days", 0) or 0),
        int(cfg.pullback.trend_ma_window or 0),
    )
    if aligned_ma_slow == int(getattr(opportunity_cfg, "ma_slow_days", 0) or 0):
        return opportunity_cfg
    return replace(opportunity_cfg, ma_slow_days=aligned_ma_slow)


def _row_regime_state(row: Dict[str, Any]) -> str:
    direct = str(row.get("regime_state", "") or "").strip().upper()
    if direct:
        return direct
    decision = dict(row.get("signal_decision", {}) or {})
    regime_state = dict(decision.get("regime_state", {}) or {})
    nested = str(regime_state.get("state", "") or "").strip().upper()
    if nested:
        return nested
    sentiment = str(row.get("market_sentiment", "") or "").strip().upper()
    return sentiment


def is_defensive_regime_state(state: str) -> bool:
    return str(state or "").strip().upper() in DEFENSIVE_REGIME_STATES


def _defensive_reason_text(cfg: AdaptiveStrategyConfig, state: str) -> str:
    threshold_raise_pct = float(cfg.defensive.raise_entry_threshold_pct or 0.0) * 100.0
    return (
        f"当前环境处于 {state} 防守阶段，先不新增进场；"
        f"按 ACM-RS 需要把入场阈值提高约 {threshold_raise_pct:.0f}% 并压低组合总仓位。"
    )


def apply_adaptive_defensive_rank_cap(
    ranked_rows: List[Dict[str, Any]],
    cfg: AdaptiveStrategyConfig | None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if cfg is None:
        return list(ranked_rows or []), {"enabled": False, "defensive_cap_count": 0, "top_defensive_symbols": []}

    adjusted: List[Dict[str, Any]] = []
    capped_symbols: List[str] = []
    for base_row in list(ranked_rows or []):
        row = dict(base_row)
        row.setdefault("adaptive_strategy_status", "CLEAR")
        row.setdefault("adaptive_strategy_reason", "")
        regime_state = _row_regime_state(row)
        action = str(row.get("action", "WATCH") or "WATCH").upper()
        if action == "ACCUMULATE" and is_defensive_regime_state(regime_state):
            row["adaptive_strategy_status"] = "DEFENSIVE_REGIME_CAP"
            row["adaptive_strategy_reason"] = _defensive_reason_text(cfg, regime_state)
            row["adaptive_strategy_original_action"] = action
            row["action"] = "WATCH"
            row["execution_ready"] = 0

            decision = dict(row.get("signal_decision", {}) or {})
            blocked = [str(item).strip() for item in list(decision.get("gates_blocked", []) or []) if str(item).strip()]
            reasons = [str(item).strip() for item in list(decision.get("reasons", []) or []) if str(item).strip()]
            context = dict(decision.get("context", {}) or {})
            if "adaptive_defensive_regime" not in blocked:
                blocked.append("adaptive_defensive_regime")
            reason_text = (
                f"adaptive strategy defensive cap applied: regime_state={regime_state} "
                f"threshold_raise_pct={float(cfg.defensive.raise_entry_threshold_pct or 0.0):.2f}"
            )
            if reason_text not in reasons:
                reasons.append(reason_text)
            context.update(
                {
                    "adaptive_strategy_applied": True,
                    "adaptive_strategy_status": "DEFENSIVE_REGIME_CAP",
                    "adaptive_strategy_regime_state": regime_state,
                    "adaptive_strategy_raise_entry_threshold_pct": float(cfg.defensive.raise_entry_threshold_pct or 0.0),
                }
            )
            decision["action"] = "WATCH"
            decision["gates_blocked"] = blocked
            decision["reasons"] = reasons
            decision["context"] = context
            row["signal_decision"] = decision
            row["signal_decision_json"] = json.dumps(decision, ensure_ascii=False)
            capped_symbols.append(str(row.get("symbol") or "").upper())
        adjusted.append(row)

    adjusted.sort(
        key=lambda row: (
            {"ACCUMULATE": 3, "HOLD": 2, "WATCH": 1, "REDUCE": 0}.get(str(row.get("action", "WATCH")).upper(), 1),
            float(row.get("score", 0.0) or 0.0),
        ),
        reverse=True,
    )
    return adjusted, {
        "enabled": True,
        "defensive_cap_count": int(len(capped_symbols)),
        "top_defensive_symbols": sorted({sym for sym in capped_symbols if sym})[:10],
    }


def apply_adaptive_defensive_opportunity_policy(
    rows: List[Dict[str, Any]],
    cfg: AdaptiveStrategyConfig | None,
) -> List[Dict[str, Any]]:
    if cfg is None:
        return list(rows or [])

    gated_statuses = {"ENTRY_NOW", "ADD_ON_PULLBACK", "NEAR_ENTRY"}
    adjusted: List[Dict[str, Any]] = []
    for base_row in list(rows or []):
        row = dict(base_row)
        row.setdefault("adaptive_strategy_status", "CLEAR")
        row.setdefault("adaptive_strategy_reason", "")
        regime_state = _row_regime_state(row)
        entry_status = str(row.get("entry_status", "") or "").upper()
        if entry_status in gated_statuses and is_defensive_regime_state(regime_state):
            row["adaptive_strategy_status"] = "DEFENSIVE_REGIME_CAP"
            row["adaptive_strategy_reason"] = _defensive_reason_text(cfg, regime_state)
            row["adaptive_strategy_original_entry_status"] = entry_status
            row["entry_status"] = "WAIT_DEFENSIVE_REGIME"
            row["entry_reason"] = row["adaptive_strategy_reason"]
        adjusted.append(row)
    return adjusted
