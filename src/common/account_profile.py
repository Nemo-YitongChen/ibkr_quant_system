from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, List

import yaml

from ..portfolio.investment_allocator import InvestmentExecutionConfig


@dataclass
class AccountProfileExecutionOverrides:
    min_trade_value: float | None = None
    max_order_value_pct: float | None = None
    max_orders_per_run: int | None = None
    account_allocation_pct: float | None = None

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "AccountProfileExecutionOverrides":
        raw = dict(raw or {})
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})

    def to_override_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for field_name in self.__dataclass_fields__:
            value = getattr(self, field_name)
            if value is not None:
                out[field_name] = value
        return out


@dataclass
class AccountProfile:
    name: str
    label: str = ""
    min_equity: float = 0.0
    max_equity: float = 0.0
    summary: str = ""
    preferred_instruments: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    execution: AccountProfileExecutionOverrides = field(default_factory=AccountProfileExecutionOverrides)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "AccountProfile":
        raw = dict(raw or {})
        return cls(
            name=str(raw.get("name") or "").strip(),
            label=str(raw.get("label") or "").strip(),
            min_equity=float(raw.get("min_equity") or 0.0),
            max_equity=float(raw.get("max_equity") or 0.0),
            summary=str(raw.get("summary") or "").strip(),
            preferred_instruments=[str(item).strip() for item in list(raw.get("preferred_instruments", []) or []) if str(item).strip()],
            notes=[str(item).strip() for item in list(raw.get("notes", []) or []) if str(item).strip()],
            execution=AccountProfileExecutionOverrides.from_dict(raw.get("execution")),
        )

    def matches(self, equity: float) -> bool:
        equity_value = float(equity or 0.0)
        lower_ok = equity_value >= float(self.min_equity or 0.0)
        upper = float(self.max_equity or 0.0)
        upper_ok = upper <= 0.0 or equity_value < upper
        return lower_ok and upper_ok

    @property
    def display_label(self) -> str:
        return self.label or self.name or "default"

    def equity_band_label(self) -> str:
        min_equity = float(self.min_equity or 0.0)
        max_equity = float(self.max_equity or 0.0)
        if min_equity > 0.0 and max_equity > 0.0:
            return f"{min_equity:,.0f} - {max_equity:,.0f}"
        if max_equity > 0.0:
            return f"< {max_equity:,.0f}"
        if min_equity > 0.0:
            return f">= {min_equity:,.0f}"
        return "all"


@dataclass
class AccountProfilesConfig:
    default_profile: str = ""
    profiles: List[AccountProfile] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "AccountProfilesConfig":
        raw = dict(raw or {})
        nested = raw.get("account_profiles")
        if isinstance(nested, dict):
            raw = dict(nested)
        profiles = [AccountProfile.from_dict(item) for item in list(raw.get("profiles", []) or []) if isinstance(item, dict)]
        profiles = [profile for profile in profiles if profile.name]
        return cls(
            default_profile=str(raw.get("default_profile") or "").strip(),
            profiles=profiles,
        )

    def resolve(self, equity: float) -> AccountProfile | None:
        if not self.profiles:
            return None
        for profile in self.profiles:
            if profile.matches(equity):
                return profile
        default_name = str(self.default_profile or "").strip().lower()
        if default_name:
            for profile in self.profiles:
                if profile.name.lower() == default_name:
                    return profile
        ordered = sorted(self.profiles, key=lambda profile: float(profile.min_equity or 0.0))
        equity_value = float(equity or 0.0)
        eligible = [profile for profile in ordered if equity_value >= float(profile.min_equity or 0.0)]
        return eligible[-1] if eligible else ordered[0]


def account_profiles_config_path(base_dir: Path, explicit_path: str | None = None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if path.is_absolute():
            return path
        for candidate in (base_dir / path, base_dir / "config" / path, Path.cwd() / path, Path.cwd() / "config" / path):
            if candidate.exists():
                return candidate.resolve()
        return (base_dir / path).resolve()
    return (base_dir / "config" / "account_profiles.yaml").resolve()


def load_account_profiles(base_dir: Path, explicit_path: str | None = None) -> AccountProfilesConfig:
    path = account_profiles_config_path(base_dir, explicit_path)
    if not path.exists():
        return AccountProfilesConfig()
    with path.open("r", encoding="utf-8") as f:
        payload = yaml.safe_load(f) or {}
    return AccountProfilesConfig.from_dict(payload)


def apply_account_profile(
    cfg: InvestmentExecutionConfig,
    profiles: AccountProfilesConfig | None,
    *,
    broker_equity: float,
) -> tuple[InvestmentExecutionConfig, Dict[str, Any]]:
    profile = (profiles or AccountProfilesConfig()).resolve(broker_equity)
    if profile is None:
        return cfg, {}
    overrides = profile.execution.to_override_dict()
    effective_cfg = replace(cfg, **overrides) if overrides else cfg
    return effective_cfg, account_profile_summary(profile, broker_equity=broker_equity)


def resolved_account_profile_summary(
    profiles: AccountProfilesConfig | None,
    *,
    broker_equity: float,
) -> Dict[str, Any]:
    return account_profile_summary((profiles or AccountProfilesConfig()).resolve(broker_equity), broker_equity=broker_equity)


def account_profile_summary(profile: AccountProfile | None, *, broker_equity: float) -> Dict[str, Any]:
    if profile is None:
        return {}
    overrides = profile.execution.to_override_dict()
    preferred = [str(item).strip() for item in list(profile.preferred_instruments or []) if str(item).strip()]
    summary_parts = [
        f"profile={profile.display_label}",
        f"band={profile.equity_band_label()}",
        f"equity={float(broker_equity or 0.0):,.2f}",
    ]
    if overrides:
        if "min_trade_value" in overrides:
            summary_parts.append(f"min_trade={float(overrides['min_trade_value']):,.0f}")
        if "max_orders_per_run" in overrides:
            summary_parts.append(f"max_orders={int(overrides['max_orders_per_run'])}")
        if "account_allocation_pct" in overrides:
            summary_parts.append(f"alloc={float(overrides['account_allocation_pct']) * 100.0:.0f}%")
    return {
        "name": profile.name,
        "label": profile.display_label,
        "broker_equity": float(broker_equity or 0.0),
        "min_equity": float(profile.min_equity or 0.0),
        "max_equity": float(profile.max_equity or 0.0),
        "equity_band": profile.equity_band_label(),
        "summary": str(profile.summary or ""),
        "preferred_instruments": preferred,
        "notes": list(profile.notes or []),
        "execution_overrides": overrides,
        "summary_text": " | ".join(summary_parts),
    }
