from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List

import yaml

from .markets import resolve_market_code


@dataclass
class MarketCostRules:
    broker_commission_bps: float = 0.0
    stamp_duty_bps_per_side: float = 0.0
    trading_fee_bps_per_side: float = 0.0
    sfc_levy_bps_per_side: float = 0.0
    afrc_levy_bps_per_side: float = 0.0
    settlement_fee_bps_per_side: float = 0.0
    notes: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "MarketCostRules":
        raw = dict(raw or {})
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})

    def total_one_side_bps(self) -> float:
        return float(
            float(self.broker_commission_bps)
            + float(self.stamp_duty_bps_per_side)
            + float(self.trading_fee_bps_per_side)
            + float(self.sfc_levy_bps_per_side)
            + float(self.afrc_levy_bps_per_side)
            + float(self.settlement_fee_bps_per_side)
        )


@dataclass
class MarketOrderRules:
    buy_lot_multiple: int = 1
    day_turnaround_allowed: bool = True
    odd_lot_auto_match: bool = True
    odd_lot_discount_risk: bool = False
    price_limit_pct: float = 0.0
    notes: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "MarketOrderRules":
        raw = dict(raw or {})
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


@dataclass
class MarketAccountRules:
    standard_settlement_cycle: str = ""
    pdt_margin_equity_min: float = 0.0
    prefer_etf_only_below_equity: float = 0.0
    small_account_equity_label: str = ""
    notes: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "MarketAccountRules":
        raw = dict(raw or {})
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


@dataclass
class MarketPortfolioPreferences:
    preferred_instruments: List[str] = field(default_factory=list)
    small_account_preferred_asset_classes: List[str] = field(default_factory=list)
    recommended_signal_frequency: str = ""
    recommended_rebalance_frequency: str = ""
    max_rebalances_per_week: int = 1
    notes: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "MarketPortfolioPreferences":
        raw = dict(raw or {})
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


@dataclass
class MarketStructureConfig:
    market: str = ""
    market_scope: str = ""
    benchmark_symbol: str = ""
    research_only: bool = False
    strategy_bias: str = ""
    costs: MarketCostRules = field(default_factory=MarketCostRules)
    order_rules: MarketOrderRules = field(default_factory=MarketOrderRules)
    account_rules: MarketAccountRules = field(default_factory=MarketAccountRules)
    portfolio_preferences: MarketPortfolioPreferences = field(default_factory=MarketPortfolioPreferences)
    notes: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "MarketStructureConfig":
        raw = dict(raw or {})
        return cls(
            market=resolve_market_code(str(raw.get("market") or "")),
            market_scope=str(raw.get("market_scope") or ""),
            benchmark_symbol=str(raw.get("benchmark_symbol") or ""),
            research_only=bool(raw.get("research_only", False)),
            strategy_bias=str(raw.get("strategy_bias") or ""),
            costs=MarketCostRules.from_dict(raw.get("costs")),
            order_rules=MarketOrderRules.from_dict(raw.get("order_rules")),
            account_rules=MarketAccountRules.from_dict(raw.get("account_rules")),
            portfolio_preferences=MarketPortfolioPreferences.from_dict(raw.get("portfolio_preferences")),
            notes=[str(item).strip() for item in list(raw.get("notes", []) or []) if str(item).strip()],
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def small_account_requires_etf_first(self, equity: float) -> bool:
        threshold = float(self.account_rules.prefer_etf_only_below_equity or 0.0)
        return threshold > 0.0 and float(equity or 0.0) < threshold


def market_structure_summary(structure: MarketStructureConfig, *, broker_equity: float | None = None) -> Dict[str, Any]:
    threshold = float(structure.account_rules.prefer_etf_only_below_equity or 0.0)
    equity_value = float(broker_equity or 0.0)
    preferred_instruments = [str(item).strip() for item in list(structure.portfolio_preferences.preferred_instruments or []) if str(item).strip()]
    preferred_asset_classes = [
        str(item).strip().lower()
        for item in list(structure.portfolio_preferences.small_account_preferred_asset_classes or [])
        if str(item).strip()
    ]
    settlement_cycle = str(structure.account_rules.standard_settlement_cycle or "N/A")
    summary_parts = [
        f"settlement={settlement_cycle}",
        f"buy_lot={max(1, int(structure.order_rules.buy_lot_multiple or 1))}",
    ]
    if threshold > 0.0:
        rule_text = f"ETF-first below {threshold:.2f}"
        if equity_value > 0.0:
            rule_text = f"{rule_text} (equity={equity_value:.2f})"
        summary_parts.append(rule_text)
    if preferred_instruments:
        summary_parts.append(f"preferred={'/'.join(preferred_instruments)}")
    return {
        "market": str(structure.market or ""),
        "market_scope": str(structure.market_scope or ""),
        "research_only": bool(structure.research_only),
        "benchmark_symbol": str(structure.benchmark_symbol or ""),
        "strategy_bias": str(structure.strategy_bias or ""),
        "settlement_cycle": settlement_cycle,
        "buy_lot_multiple": max(1, int(structure.order_rules.buy_lot_multiple or 1)),
        "day_turnaround_allowed": bool(structure.order_rules.day_turnaround_allowed),
        "odd_lot_auto_match": bool(structure.order_rules.odd_lot_auto_match),
        "odd_lot_discount_risk": bool(structure.order_rules.odd_lot_discount_risk),
        "price_limit_pct": float(structure.order_rules.price_limit_pct or 0.0),
        "fee_floor_one_side_bps": float(structure.costs.total_one_side_bps()),
        "small_account_threshold": threshold,
        "small_account_rule_active": structure.small_account_requires_etf_first(equity_value),
        "small_account_preferred_asset_classes": preferred_asset_classes,
        "preferred_instruments": preferred_instruments,
        "signal_frequency": str(structure.portfolio_preferences.recommended_signal_frequency or ""),
        "rebalance_frequency": str(structure.portfolio_preferences.recommended_rebalance_frequency or ""),
        "max_rebalances_per_week": int(structure.portfolio_preferences.max_rebalances_per_week or 0),
        "notes": list(structure.notes or []),
        "summary_text": " | ".join(summary_parts),
    }


def market_structure_config_path(base_dir: Path, market: str | None, explicit_path: str | None = None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if path.is_absolute():
            return path
        for candidate in (base_dir / path, base_dir / "config" / path, Path.cwd() / path, Path.cwd() / "config" / path):
            if candidate.exists():
                return candidate.resolve()
        return (base_dir / path).resolve()

    code = resolve_market_code(market)
    if not code:
        return (base_dir / "config" / "market_structure.yaml").resolve()
    return (base_dir / "config" / f"market_structure_{code.lower()}.yaml").resolve()


def load_market_structure(base_dir: Path, market: str | None, explicit_path: str | None = None) -> MarketStructureConfig:
    code = resolve_market_code(market)
    default_path = (base_dir / "config" / "market_structure.yaml").resolve()
    payload: Dict[str, Any] = {}
    if default_path.exists():
        with default_path.open("r", encoding="utf-8") as f:
            payload = yaml.safe_load(f) or {}
    path = market_structure_config_path(base_dir, code, explicit_path)
    if path.exists() and path != default_path:
        with path.open("r", encoding="utf-8") as f:
            override = yaml.safe_load(f) or {}
        payload.update(override)
    nested = payload.get("market_structure")
    if isinstance(nested, dict):
        payload = dict(nested)
    if "market" not in payload and code:
        payload["market"] = code
    return MarketStructureConfig.from_dict(payload)
