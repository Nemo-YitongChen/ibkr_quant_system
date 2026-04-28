from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, List

from .config_layers import load_layered_config


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
class MarketProfileConfig:
    label: str = ""
    staged_entry_parts: int | None = None
    accumulate_pullback_pct: float | None = None
    rebalance_window_days: int | None = None
    trim_fraction: float | None = None
    turnover_penalty_scale: float | None = None
    no_trade_band_pct: float | None = None
    regime_vol_elevated: float | None = None
    regime_vol_extreme: float | None = None
    regime_drawdown_warn: float | None = None
    regime_drawdown_stop: float | None = None
    regime_risk_on_threshold: float | None = None
    regime_hard_risk_off_threshold: float | None = None
    min_expected_edge_bps: float | None = None
    edge_cost_buffer_bps: float | None = None
    risk_budget_net_exposure: float | None = None
    risk_budget_gross_exposure: float | None = None
    risk_budget_short_exposure: float | None = None
    risk_recovery_max_bonus: float | None = None

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "MarketProfileConfig":
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
    market_profiles: Dict[str, MarketProfileConfig] = field(default_factory=dict)
    rollout: List[RolloutStage] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    config_sources: List[str] = field(default_factory=list)

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
            market_profiles={
                str(name).upper().strip(): MarketProfileConfig.from_dict(item)
                for name, item in dict(raw.get("market_profiles", {}) or {}).items()
                if str(name).strip() and isinstance(item, dict)
            },
            rollout=[RolloutStage.from_dict(item) for item in list(raw.get("rollout", []) or []) if isinstance(item, dict)],
            notes=[str(item).strip() for item in list(raw.get("notes", []) or []) if str(item).strip()],
        )


DEFENSIVE_REGIME_STATES = {"RISK_OFF", "HARD_RISK_OFF", "DEFENSIVE"}
DEFAULT_SMALL_ACCOUNT_MAX_EQUITY = 25_000.0
DEFAULT_MEDIUM_ACCOUNT_MAX_EQUITY = 150_000.0


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
    loaded = load_layered_config(base_dir, str(path))
    cfg = AdaptiveStrategyConfig.from_dict(loaded.payload)
    cfg.config_sources = list(loaded.sources)
    return cfg


def _load_json_dict(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


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
    market_profiles = {
        code: {
            "label": str(profile.label or ""),
            "staged_entry_parts": profile.staged_entry_parts,
            "accumulate_pullback_pct": profile.accumulate_pullback_pct,
            "rebalance_window_days": profile.rebalance_window_days,
            "trim_fraction": profile.trim_fraction,
            "turnover_penalty_scale": profile.turnover_penalty_scale,
            "no_trade_band_pct": profile.no_trade_band_pct,
            "regime_vol_elevated": profile.regime_vol_elevated,
            "regime_vol_extreme": profile.regime_vol_extreme,
            "regime_drawdown_warn": profile.regime_drawdown_warn,
            "regime_drawdown_stop": profile.regime_drawdown_stop,
            "regime_risk_on_threshold": profile.regime_risk_on_threshold,
            "regime_hard_risk_off_threshold": profile.regime_hard_risk_off_threshold,
            "min_expected_edge_bps": profile.min_expected_edge_bps,
            "edge_cost_buffer_bps": profile.edge_cost_buffer_bps,
            "risk_budget_net_exposure": profile.risk_budget_net_exposure,
            "risk_budget_gross_exposure": profile.risk_budget_gross_exposure,
            "risk_budget_short_exposure": profile.risk_budget_short_exposure,
            "risk_recovery_max_bonus": profile.risk_recovery_max_bonus,
        }
        for code, profile in dict(cfg.market_profiles or {}).items()
    }
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
        "market_profiles": market_profiles,
        "rollout": rollout,
        "notes": list(cfg.notes or []),
        "config_sources": list(cfg.config_sources or []),
        "summary_text": " | ".join(summary_parts),
    }


def load_report_adaptive_strategy_payload(
    report_dir: Path,
    *,
    base_dir: Path | None = None,
    explicit_config_path: str | None = None,
) -> Dict[str, Any]:
    payload = _load_json_dict(Path(report_dir) / "investment_adaptive_strategy_summary.json")
    adaptive = dict(payload.get("adaptive_strategy") or {})
    if not adaptive and base_dir is not None:
        adaptive = adaptive_strategy_context(load_adaptive_strategy(base_dir, explicit_config_path))
    summary = dict(payload.get("summary") or {})
    return {
        "adaptive_strategy": adaptive,
        "summary": summary,
        "active_market_plan": dict(payload.get("active_market_plan") or {}),
        "active_market_regime": dict(payload.get("active_market_regime") or {}),
        "active_market_execution": dict(payload.get("active_market_execution") or {}),
        "active_market_risk": dict(payload.get("active_market_risk") or {}),
    }


def adaptive_strategy_runtime_note(summary: Dict[str, Any] | None) -> str:
    summary = dict(summary or {})
    if not summary:
        return ""
    defensive_caps = int(summary.get("defensive_cap_count", 0) or 0)
    top_symbols = [
        str(symbol).upper().strip()
        for symbol in list(summary.get("top_defensive_symbols", []) or [])
        if str(symbol).strip()
    ]
    if not bool(summary.get("enabled", False)):
        return "enabled=false"
    if defensive_caps <= 0:
        return "enabled=true defensive_caps=0"
    if top_symbols:
        return f"enabled=true defensive_caps={defensive_caps} symbols={','.join(top_symbols[:5])}"
    return f"enabled=true defensive_caps={defensive_caps}"


def adaptive_strategy_active_market_human_note(payload: Dict[str, Any] | None) -> str:
    payload = dict(payload or {})
    active_market_plan = dict(payload.get("active_market_plan") or {})
    active_market_regime = dict(payload.get("active_market_regime") or {})
    active_market_execution = dict(payload.get("active_market_execution") or {})
    active_market_risk = dict(payload.get("active_market_risk") or {})
    profile_label = str(
        active_market_plan.get("profile_label")
        or active_market_regime.get("profile_label")
        or active_market_execution.get("profile_label")
        or active_market_risk.get("profile_label")
        or active_market_plan.get("profile_key")
        or active_market_regime.get("profile_key")
        or active_market_execution.get("profile_key")
        or active_market_risk.get("profile_key")
        or ""
    ).strip()
    if not profile_label:
        return ""
    parts = [f"当前使用 {profile_label} 市场档案"]
    plan_summary = str(active_market_plan.get("summary_text") or "").strip()
    regime_summary = str(active_market_regime.get("summary_text") or "").strip()
    execution_summary = str(active_market_execution.get("summary_text") or "").strip()
    risk_summary = str(active_market_risk.get("summary_text") or "").strip()
    if plan_summary:
        parts.append(f"计划={plan_summary}")
    if regime_summary:
        parts.append(f"regime={regime_summary}")
    if execution_summary:
        parts.append(f"执行={execution_summary}")
    if risk_summary:
        parts.append(f"风险={risk_summary}")
    return "；".join(parts) + "。"


def _market_profile_key(market: str) -> str:
    return str(market or "").strip().upper() or "DEFAULT"


def adaptive_strategy_market_profile(
    cfg: AdaptiveStrategyConfig | None,
    market: str,
) -> tuple[str, MarketProfileConfig]:
    if cfg is None:
        return "", MarketProfileConfig()
    profiles = dict(cfg.market_profiles or {})
    market_key = _market_profile_key(market)
    if market_key in profiles:
        return market_key, profiles[market_key]
    if "DEFAULT" in profiles:
        return "DEFAULT", profiles["DEFAULT"]
    return market_key, MarketProfileConfig()


def adaptive_strategy_market_plan_overrides(
    cfg: AdaptiveStrategyConfig | None,
    market: str,
) -> Dict[str, Any]:
    profile_key, profile = adaptive_strategy_market_profile(cfg, market)
    overrides: Dict[str, Any] = {}
    for field_name in (
        "staged_entry_parts",
        "accumulate_pullback_pct",
        "rebalance_window_days",
        "trim_fraction",
        "turnover_penalty_scale",
        "no_trade_band_pct",
    ):
        value = getattr(profile, field_name, None)
        if value is not None:
            overrides[field_name] = value
    summary_bits = []
    if "staged_entry_parts" in overrides:
        summary_bits.append(f"staged={int(overrides['staged_entry_parts'])}x")
    if "accumulate_pullback_pct" in overrides:
        summary_bits.append(f"pullback={float(overrides['accumulate_pullback_pct']) * 100.0:.1f}%")
    if "rebalance_window_days" in overrides:
        summary_bits.append(f"rebalance_window={int(overrides['rebalance_window_days'])}d")
    if "turnover_penalty_scale" in overrides:
        summary_bits.append(f"turnover_penalty={float(overrides['turnover_penalty_scale']):.2f}")
    if "no_trade_band_pct" in overrides:
        summary_bits.append(f"no_trade_band={float(overrides['no_trade_band_pct']) * 100.0:.1f}%")
    return {
        "market": _market_profile_key(market),
        "profile_key": profile_key,
        "profile_label": str(profile.label or profile_key or ""),
        "overrides": overrides,
        "summary_text": " | ".join(summary_bits),
    }


def adaptive_strategy_market_regime_overrides(
    cfg: AdaptiveStrategyConfig | None,
    market: str,
) -> Dict[str, Any]:
    profile_key, profile = adaptive_strategy_market_profile(cfg, market)
    overrides: Dict[str, Any] = {}
    field_map = {
        "regime_vol_elevated": "vol_elevated",
        "regime_vol_extreme": "vol_extreme",
        "regime_drawdown_warn": "drawdown_warn",
        "regime_drawdown_stop": "drawdown_stop",
        "regime_risk_on_threshold": "risk_on_threshold",
        "regime_hard_risk_off_threshold": "hard_risk_off_threshold",
    }
    for profile_field, target_field in field_map.items():
        value = getattr(profile, profile_field, None)
        if value is not None:
            overrides[target_field] = value
    summary_bits = []
    if "vol_elevated" in overrides or "vol_extreme" in overrides:
        summary_bits.append(
            "vol="
            f"{float(overrides.get('vol_elevated', 0.0)) * 100.0:.2f}%/"
            f"{float(overrides.get('vol_extreme', 0.0)) * 100.0:.2f}%"
        )
    if "drawdown_warn" in overrides or "drawdown_stop" in overrides:
        summary_bits.append(
            "drawdown="
            f"{float(overrides.get('drawdown_warn', 0.0)) * 100.0:.1f}%/"
            f"{float(overrides.get('drawdown_stop', 0.0)) * 100.0:.1f}%"
        )
    if "risk_on_threshold" in overrides:
        summary_bits.append(f"risk_on={float(overrides['risk_on_threshold']):.2f}")
    if "hard_risk_off_threshold" in overrides:
        summary_bits.append(f"hard_off={float(overrides['hard_risk_off_threshold']):.2f}")
    return {
        "market": _market_profile_key(market),
        "profile_key": profile_key,
        "profile_label": str(profile.label or profile_key or ""),
        "overrides": overrides,
        "summary_text": " | ".join(summary_bits),
    }


def adaptive_strategy_market_execution_overrides(
    cfg: AdaptiveStrategyConfig | None,
    market: str,
) -> Dict[str, Any]:
    profile_key, profile = adaptive_strategy_market_profile(cfg, market)
    overrides: Dict[str, Any] = {}
    for field_name in ("min_expected_edge_bps", "edge_cost_buffer_bps"):
        value = getattr(profile, field_name, None)
        if value is not None:
            overrides[field_name] = value
    summary_bits = []
    if "min_expected_edge_bps" in overrides:
        summary_bits.append(f"min_edge={float(overrides['min_expected_edge_bps']):.1f}bps")
    if "edge_cost_buffer_bps" in overrides:
        summary_bits.append(f"edge_buffer={float(overrides['edge_cost_buffer_bps']):.1f}bps")
    return {
        "market": _market_profile_key(market),
        "profile_key": profile_key,
        "profile_label": str(profile.label or profile_key or ""),
        "overrides": overrides,
        "summary_text": " | ".join(summary_bits),
    }


def adaptive_strategy_market_risk_overrides(
    cfg: AdaptiveStrategyConfig | None,
    market: str,
) -> Dict[str, Any]:
    profile_key, profile = adaptive_strategy_market_profile(cfg, market)
    overrides: Dict[str, Any] = {}
    field_map = {
        "risk_budget_net_exposure": "market_profile_net_exposure_budget",
        "risk_budget_gross_exposure": "market_profile_gross_exposure_budget",
        "risk_budget_short_exposure": "market_profile_short_exposure_budget",
        "risk_recovery_max_bonus": "dynamic_recovery_max_bonus",
    }
    for profile_field, target_field in field_map.items():
        value = getattr(profile, profile_field, None)
        if value is not None:
            overrides[target_field] = value
    summary_bits = []
    if "market_profile_net_exposure_budget" in overrides:
        summary_bits.append(f"net_budget={float(overrides['market_profile_net_exposure_budget']):.2f}")
    if "market_profile_gross_exposure_budget" in overrides:
        summary_bits.append(f"gross_budget={float(overrides['market_profile_gross_exposure_budget']):.2f}")
    if "market_profile_short_exposure_budget" in overrides:
        summary_bits.append(f"short_budget={float(overrides['market_profile_short_exposure_budget']):.2f}")
    if "dynamic_recovery_max_bonus" in overrides:
        summary_bits.append(f"recovery_bonus={float(overrides['dynamic_recovery_max_bonus']):.2f}")
    return {
        "market": _market_profile_key(market),
        "profile_key": profile_key,
        "profile_label": str(profile.label or profile_key or ""),
        "overrides": overrides,
        "summary_text": " | ".join(summary_bits),
    }


def _apply_dataclass_overrides(cfg: Any, overrides: Dict[str, Any] | None) -> Any:
    if cfg is None:
        return cfg
    updates = {
        key: value
        for key, value in dict(overrides or {}).items()
        if hasattr(cfg, key)
    }
    return replace(cfg, **updates) if updates else cfg


def apply_adaptive_strategy_plan_overrides(
    cfg: Any,
    adaptive_cfg: AdaptiveStrategyConfig | None,
    *,
    market: str,
) -> Any:
    overrides = dict(adaptive_strategy_market_plan_overrides(adaptive_cfg, market).get("overrides") or {})
    return _apply_dataclass_overrides(cfg, overrides)


def apply_adaptive_strategy_regime_overrides(
    cfg: Any,
    adaptive_cfg: AdaptiveStrategyConfig | None,
    *,
    market: str,
) -> Any:
    overrides = dict(adaptive_strategy_market_regime_overrides(adaptive_cfg, market).get("overrides") or {})
    return _apply_dataclass_overrides(cfg, overrides)


def apply_adaptive_strategy_risk_overrides(
    cfg: Any,
    adaptive_cfg: AdaptiveStrategyConfig | None,
    *,
    market: str,
) -> Any:
    overrides = dict(adaptive_strategy_market_risk_overrides(adaptive_cfg, market).get("overrides") or {})
    return _apply_dataclass_overrides(cfg, overrides)


def apply_active_market_execution_overrides(
    cfg: Any,
    payload: Dict[str, Any] | None,
) -> Any:
    payload = dict(payload or {})
    overrides = dict(dict(payload.get("active_market_execution") or {}).get("overrides") or {})
    return _apply_dataclass_overrides(cfg, overrides)


def apply_active_market_risk_overrides(
    cfg: Any,
    payload: Dict[str, Any] | None,
) -> Any:
    payload = dict(payload or {})
    overrides = dict(dict(payload.get("active_market_risk") or {}).get("overrides") or {})
    return _apply_dataclass_overrides(cfg, overrides)


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def adaptive_strategy_runtime_is_defensive(summary: Dict[str, Any] | None) -> bool:
    summary = dict(summary or {})
    if not bool(summary.get("enabled", False)):
        return False
    if bool(summary.get("defensive_regime_detected", False)):
        return True
    if int(summary.get("defensive_cap_count", 0) or 0) > 0:
        return True
    guard_status = str(summary.get("guard_status", "") or "").strip().upper()
    if guard_status == "DEFENSIVE_REGIME":
        return True
    active_states = [
        str(state).strip().upper()
        for state in list(summary.get("active_regime_states", []) or [])
        if str(state).strip()
    ]
    return any(state in DEFENSIVE_REGIME_STATES for state in active_states)


def adaptive_strategy_account_size_bucket(portfolio_equity: float) -> str:
    equity = max(0.0, _coerce_float(portfolio_equity, 0.0))
    if equity < DEFAULT_SMALL_ACCOUNT_MAX_EQUITY:
        return "small"
    if equity < DEFAULT_MEDIUM_ACCOUNT_MAX_EQUITY:
        return "medium"
    return "large"


def _adaptive_strategy_defensive_cap(adaptive: Dict[str, Any], *, portfolio_equity: float) -> tuple[str, float]:
    defensive = dict(adaptive.get("defensive", {}) or {})
    bucket = adaptive_strategy_account_size_bucket(portfolio_equity)
    field_name = {
        "small": "small_max_gross",
        "medium": "medium_max_gross",
        "large": "large_max_gross",
    }.get(bucket, "medium_max_gross")
    default_value = {
        "small": 0.20,
        "medium": 0.30,
        "large": 0.40,
    }.get(bucket, 0.30)
    return bucket, max(0.0, min(1.0, _coerce_float(defensive.get(field_name), default_value)))


def adaptive_strategy_effective_controls(
    payload: Dict[str, Any] | None,
    *,
    portfolio_equity: float,
    base_target_invested_weight: float,
    base_account_allocation_pct: float | None = None,
    base_max_order_value_pct: float | None = None,
) -> Dict[str, Any]:
    payload = dict(payload or {})
    adaptive = dict(payload.get("adaptive_strategy") or {})
    runtime_summary = dict(payload.get("summary") or payload.get("adaptive_strategy_runtime_summary") or {})
    defensive_mode = adaptive_strategy_runtime_is_defensive(runtime_summary)
    bucket, defensive_cap = _adaptive_strategy_defensive_cap(adaptive, portfolio_equity=portfolio_equity)
    base_target_weight = max(0.0, _coerce_float(base_target_invested_weight, 0.0))
    base_alloc = None if base_account_allocation_pct is None else max(0.0, min(1.0, _coerce_float(base_account_allocation_pct, 0.0)))
    base_order_cap = None if base_max_order_value_pct is None else max(0.0, min(1.0, _coerce_float(base_max_order_value_pct, 0.0)))
    base_effective_target = float(base_target_weight if base_alloc is None else base_target_weight * base_alloc)
    effective_target = float(base_effective_target)
    effective_alloc = base_alloc
    effective_order_cap = base_order_cap
    applied = False
    reason = "strategy_not_enabled"

    if adaptive or runtime_summary:
        reason = "defensive_not_triggered"

    if defensive_mode and base_target_weight > 0.0 and defensive_cap > 0.0:
        effective_target = min(base_effective_target, defensive_cap)
        if base_alloc is None:
            applied = effective_target + 1e-9 < base_effective_target
            reason = "adaptive_defensive_target_cap" if applied else "adaptive_defensive_within_cap"
        else:
            alloc_cap = min(1.0, defensive_cap / base_target_weight) if base_target_weight > 0.0 else base_alloc
            effective_alloc = min(base_alloc, alloc_cap)
            alloc_scale = 1.0 if base_alloc <= 0.0 else min(1.0, effective_alloc / base_alloc)
            if base_order_cap is not None:
                effective_order_cap = min(base_order_cap, base_order_cap * alloc_scale)
            applied = (
                effective_target + 1e-9 < base_effective_target
                or abs((effective_alloc or 0.0) - base_alloc) > 1e-9
                or abs((effective_order_cap or 0.0) - (base_order_cap or 0.0)) > 1e-9
            )
            reason = "adaptive_defensive_execution_cap" if applied else "adaptive_defensive_within_cap"

    parts: List[str] = []
    if defensive_mode:
        parts.append(f"adaptive defensive cap ({bucket})")
        if base_alloc is None:
            parts.append(f"target {base_effective_target:.1%}->{effective_target:.1%}")
        else:
            parts.append(f"effective target {base_effective_target:.1%}->{effective_target:.1%}")
            parts.append(f"alloc {(base_alloc or 0.0):.1%}->{(effective_alloc or 0.0):.1%}")
            if base_order_cap is not None and effective_order_cap is not None:
                parts.append(f"max_order {base_order_cap:.1%}->{effective_order_cap:.1%}")
    elif adaptive or runtime_summary:
        parts.append("adaptive strategy active")
        parts.append(f"target {base_effective_target:.1%}")

    return {
        "enabled": bool(adaptive or runtime_summary),
        "defensive_mode": bool(defensive_mode),
        "applied": bool(applied),
        "reason": reason,
        "account_size_bucket": bucket,
        "target_invested_weight_cap": float(defensive_cap),
        "base_target_invested_weight": float(base_target_weight),
        "base_effective_target_invested_weight": float(base_effective_target),
        "effective_target_invested_weight": float(effective_target),
        "base_account_allocation_pct": None if base_alloc is None else float(base_alloc),
        "effective_account_allocation_pct": None if effective_alloc is None else float(effective_alloc),
        "base_max_order_value_pct": None if base_order_cap is None else float(base_order_cap),
        "effective_max_order_value_pct": None if effective_order_cap is None else float(effective_order_cap),
        "defensive_cap_count": int(runtime_summary.get("defensive_cap_count", 0) or 0),
        "top_defensive_symbols": [
            str(symbol).upper().strip()
            for symbol in list(runtime_summary.get("top_defensive_symbols", []) or [])
            if str(symbol).strip()
        ],
        "summary_text": " | ".join(parts),
    }


def adaptive_strategy_effective_control_fields(controls: Dict[str, Any] | None) -> Dict[str, Any]:
    controls = dict(controls or {})
    return {
        "strategy_effective_controls": controls,
        "strategy_effective_controls_applied": bool(controls.get("applied", False)),
        "strategy_effective_controls_note": str(controls.get("summary_text", "") or ""),
        "strategy_effective_controls_human_note": adaptive_strategy_effective_controls_human_note(controls),
    }


def apply_adaptive_strategy_weight_cap(target_weights: Dict[str, Any], controls: Dict[str, Any] | None) -> Dict[str, float]:
    weights = {str(symbol).upper(): _coerce_float(weight, 0.0) for symbol, weight in dict(target_weights or {}).items()}
    controls = dict(controls or {})
    base_target = max(0.0, _coerce_float(controls.get("base_target_invested_weight"), 0.0))
    effective_target = max(0.0, _coerce_float(controls.get("effective_target_invested_weight"), base_target))
    if base_target <= 0.0 or effective_target >= base_target - 1e-9:
        return weights
    scale = max(0.0, min(1.0, effective_target / base_target))
    return {
        symbol: float(weight * scale)
        for symbol, weight in weights.items()
        if abs(float(weight * scale)) > 1e-9
    }


def apply_adaptive_strategy_execution_controls(cfg: Any, controls: Dict[str, Any] | None) -> Any:
    if cfg is None:
        return cfg
    controls = dict(controls or {})
    updates: Dict[str, Any] = {}
    effective_alloc = controls.get("effective_account_allocation_pct")
    effective_order_cap = controls.get("effective_max_order_value_pct")
    if effective_alloc is not None and hasattr(cfg, "account_allocation_pct"):
        next_alloc = max(0.0, min(1.0, _coerce_float(effective_alloc, getattr(cfg, "account_allocation_pct", 0.0))))
        if abs(next_alloc - _coerce_float(getattr(cfg, "account_allocation_pct", 0.0), 0.0)) > 1e-9:
            updates["account_allocation_pct"] = next_alloc
    if effective_order_cap is not None and hasattr(cfg, "max_order_value_pct"):
        next_order_cap = max(0.0, min(1.0, _coerce_float(effective_order_cap, getattr(cfg, "max_order_value_pct", 0.0))))
        if abs(next_order_cap - _coerce_float(getattr(cfg, "max_order_value_pct", 0.0), 0.0)) > 1e-9:
            updates["max_order_value_pct"] = next_order_cap
    return replace(cfg, **updates) if updates else cfg


def adaptive_strategy_effective_controls_human_note(controls: Dict[str, Any] | None) -> str:
    controls = dict(controls or {})
    if not bool(controls.get("enabled", False)):
        return ""
    if not bool(controls.get("defensive_mode", False)):
        return ""
    bucket_map = {
        "small": "小资金",
        "medium": "中等资金",
        "large": "大资金",
    }
    bucket_text = bucket_map.get(str(controls.get("account_size_bucket") or "").strip().lower(), "当前账户")
    base_effective_target = max(0.0, _coerce_float(controls.get("base_effective_target_invested_weight"), 0.0))
    effective_target = max(0.0, _coerce_float(controls.get("effective_target_invested_weight"), 0.0))
    base_alloc = controls.get("base_account_allocation_pct")
    effective_alloc = controls.get("effective_account_allocation_pct")
    base_order_cap = controls.get("base_max_order_value_pct")
    effective_order_cap = controls.get("effective_max_order_value_pct")
    if base_alloc is None or effective_alloc is None:
        return f"策略主动转入防守，按 {bucket_text} 上限把目标仓位从 {base_effective_target:.0%} 收到 {effective_target:.0%}。"
    sentence = (
        f"策略主动转入防守，按 {bucket_text} 上限把有效目标仓位从 {base_effective_target:.0%} 收到 {effective_target:.0%}，"
        f"账户分配从 {max(0.0, _coerce_float(base_alloc, 0.0)):.0%} 收到 {max(0.0, _coerce_float(effective_alloc, 0.0)):.0%}"
    )
    if base_order_cap is not None and effective_order_cap is not None:
        sentence += (
            f"，单笔订单上限从 {max(0.0, _coerce_float(base_order_cap, 0.0)):.0%} "
            f"收到 {max(0.0, _coerce_float(effective_order_cap, 0.0)):.0%}"
        )
    return sentence + "。"


def adaptive_strategy_summary_fields(payload: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = dict(payload or {})
    adaptive = dict(payload.get("adaptive_strategy") or {})
    runtime_summary = dict(payload.get("summary") or payload.get("adaptive_strategy_runtime_summary") or {})
    active_market_plan = dict(payload.get("active_market_plan") or {})
    active_market_regime = dict(payload.get("active_market_regime") or {})
    active_market_execution = dict(payload.get("active_market_execution") or {})
    active_market_risk = dict(payload.get("active_market_risk") or {})
    active_market_note = adaptive_strategy_active_market_human_note(payload)
    active_regime_states = [
        str(state).strip().upper()
        for state in list(runtime_summary.get("active_regime_states", []) or [])
        if str(state).strip()
    ]
    top_symbols = [
        str(symbol).upper().strip()
        for symbol in list(runtime_summary.get("top_defensive_symbols", []) or [])
        if str(symbol).strip()
    ]
    return {
        "adaptive_strategy": adaptive,
        "adaptive_strategy_runtime_summary": runtime_summary,
        "adaptive_strategy_name": str(adaptive.get("name", "") or ""),
        "adaptive_strategy_display_name": str(adaptive.get("display_name", "") or ""),
        "adaptive_strategy_summary": str(adaptive.get("summary_text", "") or ""),
        "adaptive_strategy_runtime_enabled": bool(runtime_summary.get("enabled", False)),
        "adaptive_strategy_defensive_caps": int(runtime_summary.get("defensive_cap_count", 0) or 0),
        "adaptive_strategy_defensive_regime": adaptive_strategy_runtime_is_defensive(runtime_summary),
        "adaptive_strategy_active_regime_states": active_regime_states,
        "adaptive_strategy_top_defensive_symbols": top_symbols,
        "adaptive_strategy_runtime_note": adaptive_strategy_runtime_note(runtime_summary),
        "adaptive_strategy_active_market_plan": active_market_plan,
        "adaptive_strategy_active_market_regime": active_market_regime,
        "adaptive_strategy_active_market_execution": active_market_execution,
        "adaptive_strategy_active_market_risk": active_market_risk,
        "adaptive_strategy_active_market_profile": str(
            active_market_plan.get("profile_key")
            or active_market_regime.get("profile_key")
            or active_market_execution.get("profile_key")
            or active_market_risk.get("profile_key")
            or ""
        ),
        "adaptive_strategy_active_market_plan_summary": str(active_market_plan.get("summary_text", "") or ""),
        "adaptive_strategy_active_market_regime_summary": str(active_market_regime.get("summary_text", "") or ""),
        "adaptive_strategy_active_market_execution_summary": str(active_market_execution.get("summary_text", "") or ""),
        "adaptive_strategy_active_market_risk_summary": str(active_market_risk.get("summary_text", "") or ""),
        "adaptive_strategy_active_market_note": active_market_note,
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
        return list(ranked_rows or []), {
            "enabled": False,
            "defensive_cap_count": 0,
            "top_defensive_symbols": [],
            "defensive_regime_detected": False,
            "active_regime_states": [],
        }

    adjusted: List[Dict[str, Any]] = []
    capped_symbols: List[str] = []
    defensive_states: List[str] = []
    for base_row in list(ranked_rows or []):
        row = dict(base_row)
        row.setdefault("adaptive_strategy_status", "CLEAR")
        row.setdefault("adaptive_strategy_reason", "")
        regime_state = _row_regime_state(row)
        if is_defensive_regime_state(regime_state):
            defensive_states.append(regime_state)
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
        "defensive_regime_detected": bool(defensive_states),
        "active_regime_states": sorted({state for state in defensive_states if state})[:10],
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
