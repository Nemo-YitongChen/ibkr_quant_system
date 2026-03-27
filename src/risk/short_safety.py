from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

import yaml


MARKET_TIME_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "US": {"market_timezone": "America/New_York", "market_open_hhmm": "09:30", "market_close_hhmm": "16:00"},
    "HK": {"market_timezone": "Asia/Hong_Kong", "market_open_hhmm": "09:30", "market_close_hhmm": "16:00"},
}


def _parse_hhmm(s: str) -> tuple[int, int]:
    hh, mm = str(s or "00:00").split(":", 1)
    return int(hh), int(mm)


def _join_reasons(xs: Iterable[str]) -> str:
    return ",".join(str(x) for x in xs if str(x).strip())


def load_symbol_float_map(path: Path, source_label: str, value_keys: Iterable[str]) -> tuple[dict[str, float], dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(str(path))

    value_keys = [str(x) for x in value_keys]
    values: dict[str, float] = {}
    sources: dict[str, str] = {}
    suffix = path.suffix.lower()

    if suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not isinstance(row, dict):
                    continue
                sym = str(row.get("symbol", "") or "").upper().strip()
                if not sym:
                    continue
                source = str(row.get("source", source_label) or source_label).strip() or source_label
                raw_val = None
                for key in value_keys:
                    if row.get(key) not in (None, ""):
                        raw_val = row.get(key)
                        break
                if raw_val in (None, ""):
                    sources[sym] = source if str(source).lower().startswith("unknown:") else f"unknown:{source}"
                    continue
                values[sym] = float(raw_val)
                sources[sym] = source
        return values, sources

    if suffix == ".json":
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f) or {}
    elif suffix in (".yaml", ".yml"):
        with path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    else:
        raise ValueError(f"unsupported file format: {path.suffix}")

    for key in value_keys:
        if isinstance(raw, dict) and isinstance(raw.get(key), dict):
            raw = raw.get(key, {})
            break

    if not isinstance(raw, dict):
        raise ValueError("file must resolve to a symbol->value mapping")

    for sym, val in raw.items():
        if val in (None, ""):
            continue
        key = str(sym).upper().strip()
        if not key:
            continue
        values[key] = float(val)
        sources[key] = source_label

    return values, sources


def load_short_safety_rule_file(path: Path, source_label: str) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(str(path))

    rows: List[Dict[str, Any]] = []
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as f:
            rows = [dict(r) for r in csv.DictReader(f) if isinstance(r, dict)]
    elif suffix == ".json":
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f) or []
        if isinstance(raw, dict) and isinstance(raw.get("rules"), list):
            rows = [dict(x) for x in raw.get("rules", []) if isinstance(x, dict)]
        elif isinstance(raw, list):
            rows = [dict(x) for x in raw if isinstance(x, dict)]
    elif suffix in (".yaml", ".yml"):
        with path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or []
        if isinstance(raw, dict) and isinstance(raw.get("rules"), list):
            rows = [dict(x) for x in raw.get("rules", []) if isinstance(x, dict)]
        elif isinstance(raw, list):
            rows = [dict(x) for x in raw if isinstance(x, dict)]
    else:
        raise ValueError(f"unsupported file format: {path.suffix}")

    out: Dict[str, Dict[str, Any]] = {
        "locate_status": {},
        "ssr_status": {},
        "spread_bps": {},
        "has_uptick_data": {},
        "sources": {},
    }
    for row in rows:
        sym = str(row.get("symbol", "") or "").upper().strip()
        if not sym:
            continue
        out["sources"][sym] = str(row.get("source", source_label) or source_label)
        if row.get("locate_status") not in (None, ""):
            out["locate_status"][sym] = str(row.get("locate_status")).upper()
        if row.get("ssr_status") not in (None, ""):
            out["ssr_status"][sym] = str(row.get("ssr_status")).upper()
        if row.get("spread_bps") not in (None, ""):
            out["spread_bps"][sym] = float(row.get("spread_bps"))
        if row.get("has_uptick_data") not in (None, ""):
            raw = str(row.get("has_uptick_data")).strip().lower()
            out["has_uptick_data"][sym] = raw in ("1", "true", "yes", "y")
    return out


@dataclass
class ShortSafetyConfig:
    shadow_mode: bool = False
    require_locate: bool = True
    require_ssr_state: bool = True
    require_borrow_data: bool = True
    require_spread_data: bool = True
    require_uptick_data_when_ssr: bool = True
    min_avg_bar_volume: float = 5_000.0
    max_spread_bps: float = 20.0
    borrow_fee_warn_bps: float = 80.0
    max_short_borrow_fee_bps: float = 150.0
    market_timezone: str = "America/New_York"
    market_open_hhmm: str = "09:30"
    market_close_hhmm: str = "16:00"
    open_block_minutes: int = 10
    close_block_minutes: int = 5
    block_event_risks: List[str] = field(default_factory=lambda: ["HIGH", "BLOCK"])
    locate_status: Dict[str, str] = field(default_factory=dict)
    ssr_status: Dict[str, str] = field(default_factory=dict)
    spread_bps: Dict[str, float] = field(default_factory=dict)
    has_uptick_data: Dict[str, bool] = field(default_factory=dict)
    sources: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None, *, market: str = "") -> "ShortSafetyConfig":
        raw = dict(raw or {})
        defaults = MARKET_TIME_DEFAULTS.get(str(market or "").upper(), {})
        for key, val in defaults.items():
            raw.setdefault(key, val)

        out: Dict[str, Any] = {}
        for key in cls.__dataclass_fields__:
            if key not in raw:
                continue
            if key in ("locate_status", "ssr_status"):
                out[key] = {str(sym).upper(): str(val).upper() for sym, val in dict(raw[key]).items()}
            elif key == "spread_bps":
                out[key] = {str(sym).upper(): float(val) for sym, val in dict(raw[key]).items()}
            elif key == "has_uptick_data":
                out[key] = {str(sym).upper(): bool(val) for sym, val in dict(raw[key]).items()}
            elif key == "sources":
                out[key] = {str(sym).upper(): str(val) for sym, val in dict(raw[key]).items()}
            elif key == "block_event_risks":
                out[key] = [str(x).upper() for x in raw[key]]
            else:
                out[key] = raw[key]
        return cls(**out)


@dataclass
class ShortSafetyDecision:
    allowed: bool
    tradable_status: str
    blocked_reasons: List[str] = field(default_factory=list)
    qty_multiplier: float = 1.0
    locate_status: str = ""
    ssr_status: str = ""
    spread_bps: Optional[float] = None
    borrow_fee_bps: float = 0.0
    event_risk: str = ""
    event_risk_reason: str = ""

    def blocked_reason_text(self) -> str:
        return _join_reasons(self.blocked_reasons)


class ShortSafetyGate:
    def __init__(self, cfg: ShortSafetyConfig | None = None, context: Any = None):
        self.cfg = cfg or ShortSafetyConfig()
        self.context = context

    def _context_event_risk(self, symbol: str) -> str:
        if self.context is not None and hasattr(self.context, "event_risk_for"):
            try:
                return str(self.context.event_risk_for(symbol) or "NONE").upper()
            except Exception:
                return "NONE"
        return "NONE"

    def _context_event_reason(self, symbol: str) -> str:
        if self.context is not None and hasattr(self.context, "event_risk_reason_for"):
            try:
                return str(self.context.event_risk_reason_for(symbol) or "")
            except Exception:
                return ""
        return ""

    def _context_borrow_fee(self, symbol: str) -> float:
        if self.context is not None and hasattr(self.context, "short_borrow_fee_bps_for"):
            try:
                return float(self.context.short_borrow_fee_bps_for(symbol) or 0.0)
            except Exception:
                return 0.0
        return 0.0

    def _context_borrow_source(self, symbol: str) -> str:
        if self.context is not None and hasattr(self.context, "short_borrow_source_for"):
            try:
                return str(self.context.short_borrow_source_for(symbol) or "")
            except Exception:
                return ""
        return ""

    def _daily_gate_allows_short(self) -> bool:
        if self.context is not None and hasattr(self.context, "can_trade_short"):
            try:
                return bool(self.context.can_trade_short())
            except Exception:
                return False
        return True

    def _timing_reasons(self, now: datetime) -> List[str]:
        try:
            tz = ZoneInfo(str(self.cfg.market_timezone))
        except Exception:
            tz = ZoneInfo("UTC")
        local_now = now.astimezone(tz)
        oh, om = _parse_hhmm(self.cfg.market_open_hhmm)
        ch, cm = _parse_hhmm(self.cfg.market_close_hhmm)
        open_dt = local_now.replace(hour=oh, minute=om, second=0, microsecond=0)
        close_dt = local_now.replace(hour=ch, minute=cm, second=0, microsecond=0)
        reasons: List[str] = []
        if 0 <= (local_now - open_dt).total_seconds() < int(self.cfg.open_block_minutes) * 60:
            reasons.append("open_window_block")
        if 0 <= (close_dt - local_now).total_seconds() < int(self.cfg.close_block_minutes) * 60:
            reasons.append("close_window_block")
        return reasons

    def evaluate(
        self,
        symbol: str,
        *,
        now: Optional[datetime] = None,
        avg_bar_volume: float = 0.0,
        action: str = "SELL",
        enforce_timing: bool = True,
        event_risk: Optional[str] = None,
        event_risk_reason: Optional[str] = None,
        short_borrow_fee_bps: Optional[float] = None,
        short_borrow_source: Optional[str] = None,
        locate_status: Optional[str] = None,
        ssr_status: Optional[str] = None,
        spread_bps: Optional[float] = None,
        has_uptick_data: Optional[bool] = None,
    ) -> ShortSafetyDecision:
        action = str(action or "").upper()
        if action != "SELL":
            return ShortSafetyDecision(allowed=True, tradable_status="NOT_SHORT")

        now = now or datetime.now(ZoneInfo("UTC"))
        symbol = str(symbol).upper()
        blocked: List[str] = []
        qty_multiplier = 1.0

        if not self._daily_gate_allows_short():
            blocked.append("daily_short_gate_blocked")

        locate = str(locate_status or self.cfg.locate_status.get(symbol, "UNKNOWN")).upper()
        ssr = str(ssr_status or self.cfg.ssr_status.get(symbol, "UNKNOWN")).upper()
        spread = spread_bps if spread_bps is not None else self.cfg.spread_bps.get(symbol)
        uptick = bool(has_uptick_data if has_uptick_data is not None else self.cfg.has_uptick_data.get(symbol, False))

        ev_risk = str(event_risk or self._context_event_risk(symbol) or "NONE").upper()
        ev_reason = str(event_risk_reason or self._context_event_reason(symbol) or "")
        borrow_fee = float(short_borrow_fee_bps if short_borrow_fee_bps is not None else self._context_borrow_fee(symbol))
        borrow_source = str(short_borrow_source or self._context_borrow_source(symbol) or "")
        borrow_source_norm = borrow_source.strip().lower()

        if self.cfg.require_locate and locate not in ("AVAILABLE", "LOCATED"):
            blocked.append("locate_unavailable" if locate in ("UNAVAILABLE", "BLOCKED") else "locate_unknown")

        if self.cfg.require_borrow_data and (borrow_source_norm in ("", "default") or borrow_source_norm.startswith("unknown:")):
            blocked.append("borrow_data_unknown")
        elif borrow_fee > float(self.cfg.max_short_borrow_fee_bps):
            blocked.append("borrow_fee_too_high")
        elif borrow_fee >= float(self.cfg.borrow_fee_warn_bps):
            qty_multiplier = min(qty_multiplier, 0.5)

        if self.cfg.require_ssr_state and ssr == "UNKNOWN":
            blocked.append("ssr_unknown")
        elif ssr in ("ON", "ACTIVE", "SSR") and self.cfg.require_uptick_data_when_ssr and not uptick:
            blocked.append("ssr_uptick_restricted")

        if ev_risk in {str(x).upper() for x in self.cfg.block_event_risks}:
            blocked.append("event_window_block")

        if enforce_timing:
            blocked.extend(self._timing_reasons(now))

        if float(avg_bar_volume or 0.0) < float(self.cfg.min_avg_bar_volume):
            blocked.append("liquidity_below_min")

        if self.cfg.require_spread_data and spread is None:
            blocked.append("spread_unknown")
        elif spread is not None and float(spread) > float(self.cfg.max_spread_bps):
            blocked.append("spread_too_wide")
        elif spread is not None and float(spread) > (0.75 * float(self.cfg.max_spread_bps)):
            qty_multiplier = min(qty_multiplier, 0.5)

        status = "BLOCKED" if blocked else ("REDUCED" if qty_multiplier < 0.999 else "ALLOWED")
        return ShortSafetyDecision(
            allowed=not blocked,
            tradable_status=status,
            blocked_reasons=blocked,
            qty_multiplier=float(qty_multiplier),
            locate_status=locate,
            ssr_status=ssr,
            spread_bps=float(spread) if spread is not None else None,
            borrow_fee_bps=float(borrow_fee),
            event_risk=ev_risk,
            event_risk_reason=ev_reason,
        )
