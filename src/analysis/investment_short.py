from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .features import FeatureConfig, compute_features_for_symbol
from .scoring import ReportScoringConfig, overlay_symbol
from ..offhours.compute_short import compute_engine_signal_for_symbol
from ..risk.model import TradeRiskConfig
from ..risk.short_safety import ShortSafetyConfig, ShortSafetyGate, load_short_safety_rule_file, load_symbol_float_map
from ..strategies import StrategyConfig


def _resolve_project_path(base_dir: Path, path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    for candidate in (base_dir / path, base_dir / "config" / path, Path.cwd() / path, Path.cwd() / "config" / path):
        if candidate.exists():
            return candidate.resolve()
    return (base_dir / path).resolve()


def _extract_vix(bundle: Dict[str, Any]) -> float:
    try:
        return float(bundle.get("markets", {}).get("tickers", {}).get("^VIX", {}).get("close", 0.0) or 0.0)
    except Exception:
        return 0.0


def _macro_high_risk(bundle: Dict[str, Any]) -> bool:
    events = bundle.get("macro_events") or bundle.get("bundle", {}).get("macro_events") or []
    for event in events:
        importance = str(event.get("importance", "")).lower()
        if importance in {"high", "3"}:
            return True
    return False


def _earnings_map(bundle: Dict[str, Any]) -> Dict[str, bool]:
    out: Dict[str, bool] = {}
    earnings = bundle.get("earnings") or bundle.get("bundle", {}).get("earnings") or {}
    if not isinstance(earnings, dict):
        return out
    for symbol, info in earnings.items():
        try:
            out[str(symbol).upper()] = bool(info.get("in_14d", info.get("in_window", False)))
        except Exception:
            out[str(symbol).upper()] = False
    return out


def _event_risk_for_symbol(bundle: Dict[str, Any], symbol: str, macro_high_risk: bool) -> Tuple[str, str]:
    sym = str(symbol).upper()
    reasons: List[str] = []
    earnings = bundle.get("earnings") or bundle.get("bundle", {}).get("earnings") or {}
    info = earnings.get(sym, {}) if isinstance(earnings, dict) else {}
    if bool(info.get("in_14d", info.get("in_window", False))):
        reasons.append(f"earnings:{str(info.get('next_earnings_date') or 'window').strip()}")
    if macro_high_risk:
        reasons.append("macro_calendar_high")
    return ("HIGH" if reasons else "NONE", ",".join(reasons))


class _ShortReportGate:
    def __init__(
        self,
        *,
        event_risk_by_symbol: Dict[str, str],
        event_reason_by_symbol: Dict[str, str],
        borrow_fee_bps_by_symbol: Dict[str, float],
        borrow_source_by_symbol: Dict[str, str],
    ):
        self._event_risk = {str(k).upper(): str(v).upper() for k, v in event_risk_by_symbol.items()}
        self._event_reason = {str(k).upper(): str(v) for k, v in event_reason_by_symbol.items()}
        self._borrow_fee = {str(k).upper(): float(v) for k, v in borrow_fee_bps_by_symbol.items()}
        self._borrow_source = {str(k).upper(): str(v) for k, v in borrow_source_by_symbol.items()}

    def can_trade_short(self) -> bool:
        return True

    def event_risk_for(self, symbol: str) -> str:
        return str(self._event_risk.get(str(symbol).upper(), "NONE"))

    def event_risk_reason_for(self, symbol: str) -> str:
        return str(self._event_reason.get(str(symbol).upper(), ""))

    def short_borrow_fee_bps_for(self, symbol: str) -> float:
        return float(self._borrow_fee.get(str(symbol).upper(), 0.0))

    def short_borrow_source_for(self, symbol: str) -> str:
        return str(self._borrow_source.get(str(symbol).upper(), "default"))


@dataclass
class InvestmentShortBookConfig:
    enabled: bool = False
    universe_limit: int = 32
    top_n: int = 4
    accumulate_threshold: float = 0.55
    hold_threshold: float = 0.30
    auto_execution_markets: Tuple[str, ...] = ("US", "HK")
    reduced_execution_mult: float = 0.75

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None, *, market: str = "") -> "InvestmentShortBookConfig":
        raw = dict(raw or {})
        if "auto_execution_markets" in raw:
            raw["auto_execution_markets"] = tuple(str(x).upper() for x in list(raw.get("auto_execution_markets") or []) if str(x).strip())
        if "enabled" not in raw:
            raw["enabled"] = str(market or "").upper() in {"US", "HK", "ASX"}
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


def _load_short_safety_context(
    *,
    base_dir: Path,
    market: str,
    risk_cfg: Dict[str, Any],
) -> Tuple[ShortSafetyGate, Dict[str, float], Dict[str, str]]:
    risk_context = dict(risk_cfg.get("risk_context") or {})
    borrow_fee_bps = {
        str(sym).upper(): float(val)
        for sym, val in dict(risk_context.get("short_borrow_fee_bps", {}) or {}).items()
    }
    borrow_fee_sources = {sym: "config" for sym in borrow_fee_bps}
    short_borrow_fee_file = str(risk_context.get("short_borrow_fee_file", "") or "").strip()
    if short_borrow_fee_file:
        try:
            file_values, file_sources = load_symbol_float_map(
                _resolve_project_path(base_dir, short_borrow_fee_file),
                source_label=f"file:{Path(short_borrow_fee_file).name}",
                value_keys=("borrow_fee_bps", "short_borrow_fee_bps", "fee_bps", "value"),
            )
            borrow_fee_bps.update(file_values)
            for symbol, source in file_sources.items():
                borrow_fee_sources[str(symbol).upper()] = str(source)
        except Exception:
            pass

    short_safety_raw = dict(risk_cfg.get("short_safety") or {})
    short_safety_file = str(short_safety_raw.get("short_safety_file", "") or "").strip()
    if short_safety_file:
        try:
            rule_payload = load_short_safety_rule_file(
                _resolve_project_path(base_dir, short_safety_file),
                source_label=f"file:{Path(short_safety_file).name}",
            )
            for key, values in rule_payload.items():
                merged = dict(short_safety_raw.get(key, {}) or {})
                merged.update(values if isinstance(values, dict) else {})
                short_safety_raw[key] = merged
        except Exception:
            pass

    return (
        ShortSafetyGate(ShortSafetyConfig.from_dict(short_safety_raw, market=market)),
        borrow_fee_bps,
        borrow_fee_sources,
    )


def build_short_book_candidates(
    symbols: List[str],
    *,
    market: str,
    base_dir: Path,
    data_adapter: Any,
    bundle: Dict[str, Any],
    strategy_cfg: Dict[str, Any],
    report_cfg: Dict[str, Any],
    risk_cfg: Dict[str, Any],
    adapted_regime_cfg: Any,
    short_book_cfg: InvestmentShortBookConfig,
) -> List[Dict[str, Any]]:
    if not bool(short_book_cfg.enabled):
        return []

    market_code = str(market or "").upper()
    selected_symbols = [str(symbol).upper() for symbol in list(symbols or []) if str(symbol).strip()]
    selected_symbols = selected_symbols[: max(0, int(short_book_cfg.universe_limit))]
    if not selected_symbols:
        return []

    short_safety_gate, borrow_fee_bps, borrow_fee_sources = _load_short_safety_context(
        base_dir=base_dir,
        market=market_code,
        risk_cfg=risk_cfg,
    )
    feature_cfg = FeatureConfig.from_dict(report_cfg.get("features"))
    scoring_cfg = ReportScoringConfig.from_dict(report_cfg.get("scoring"))
    vix = _extract_vix(bundle)
    macro_high = _macro_high_risk(bundle)
    earnings_map = _earnings_map(bundle)
    event_risk_by_symbol: Dict[str, str] = {}
    event_reason_by_symbol: Dict[str, str] = {}
    for symbol in selected_symbols:
        event_risk, event_reason = _event_risk_for_symbol(bundle, symbol, macro_high)
        event_risk_by_symbol[symbol] = event_risk
        event_reason_by_symbol[symbol] = event_reason

    report_gate = _ShortReportGate(
        event_risk_by_symbol=event_risk_by_symbol,
        event_reason_by_symbol=event_reason_by_symbol,
        borrow_fee_bps_by_symbol=borrow_fee_bps,
        borrow_source_by_symbol=borrow_fee_sources,
    )
    strat_cfg = StrategyConfig(
        take_profit_pct=float(strategy_cfg.get("orders", {}).get("default_take_profit_pct", 0.004)),
        stop_loss_pct=float(strategy_cfg.get("orders", {}).get("default_stop_loss_pct", 0.006)),
        mid=adapted_regime_cfg,
        risk=TradeRiskConfig.from_dict(risk_cfg.get("trade_risk")),
    )

    features: List[Dict[str, Any]] = []
    for symbol in selected_symbols:
        try:
            feat = compute_features_for_symbol(data_adapter, symbol, cfg=feature_cfg, regime_cfg=adapted_regime_cfg)
            if feat:
                features.append(feat)
        except Exception:
            continue

    feat_map = {str(row["symbol"]).upper(): row for row in features}
    engine_rows: List[Dict[str, Any]] = []
    for symbol in feat_map:
        try:
            signal_row = compute_engine_signal_for_symbol(
                symbol=symbol,
                md=data_adapter,
                cfg=strat_cfg,
                gate=report_gate,
            )
            if signal_row:
                engine_rows.append(signal_row)
        except Exception:
            continue

    ranked: List[Dict[str, Any]] = []
    for signal_row in engine_rows:
        symbol = str(signal_row.get("symbol") or "").upper()
        if str(signal_row.get("direction") or "").upper() != "SHORT":
            continue
        feat = feat_map.get(symbol)
        if feat is None:
            continue

        decision = short_safety_gate.evaluate(
            symbol,
            avg_bar_volume=float(signal_row.get("avg_bar_volume", feat.get("short_vol", 0.0)) or 0.0),
            action="SELL",
            enforce_timing=False,
            event_risk=str(signal_row.get("event_risk", event_risk_by_symbol.get(symbol, "NONE")) or "NONE"),
            event_risk_reason=str(signal_row.get("event_risk_reason", event_reason_by_symbol.get(symbol, "")) or ""),
            short_borrow_fee_bps=float(borrow_fee_bps.get(symbol, signal_row.get("short_borrow_fee_bps", 0.0)) or 0.0),
            short_borrow_source=str(borrow_fee_sources.get(symbol, signal_row.get("short_borrow_source", "default")) or "default"),
        )
        tradable_status = str(decision.tradable_status or "")
        blocked_reason = str(decision.blocked_reason_text() or "")
        overlay = overlay_symbol(
            feat,
            vix=vix,
            earnings_in_14d=bool(earnings_map.get(symbol, False)),
            macro_high_risk=macro_high,
            tradable_status=tradable_status,
            blocked_reason=blocked_reason,
            short_borrow_fee_bps=float(borrow_fee_bps.get(symbol, signal_row.get("short_borrow_fee_bps", 0.0)) or 0.0),
            cfg=scoring_cfg,
        )
        score = (
            float(scoring_cfg.engine_score_weight) * float(signal_row.get("engine_score", 0.0) or 0.0)
            + float(scoring_cfg.overlay_score_weight) * float(overlay.get("overlay_score", 0.0) or 0.0)
        )
        score = max(0.0, float(score))
        stability = float(signal_row.get("stability", 0.0) or 0.0)
        execution_score = max(
            0.0,
            min(
                1.0,
                0.55 * float(signal_row.get("engine_score", 0.0) or 0.0)
                + 0.20 * stability
                + 0.15 * float(signal_row.get("mid_scale", 0.0) or 0.0)
                + 0.10 * max(0.0, float(overlay.get("overlay_score", 0.0) or 0.0)),
            ),
        )
        action = "WATCH"
        if tradable_status.upper() == "BLOCKED" or blocked_reason:
            action = "WATCH"
        elif score >= float(short_book_cfg.accumulate_threshold) and bool(signal_row.get("should_trade", False)):
            action = "ACCUMULATE"
        elif score >= float(short_book_cfg.hold_threshold):
            action = "HOLD"
        short_execution_allowed = bool(market_code in set(short_book_cfg.auto_execution_markets))
        if tradable_status.upper() == "REDUCED":
            execution_score *= float(short_book_cfg.reduced_execution_mult)
        execution_ready = bool(
            short_execution_allowed
            and action in {"ACCUMULATE", "HOLD"}
            and bool(signal_row.get("risk_allowed", True))
            and tradable_status.upper() in {"AVAILABLE", "REDUCED"}
            and score >= float(short_book_cfg.hold_threshold)
        )
        ranked.append(
            {
                "symbol": symbol,
                "market": market_code,
                "direction": "SHORT",
                "action": action,
                "score": float(score),
                "model_recommendation_score": float(score),
                "execution_score": float(execution_score),
                "execution_ready": int(bool(execution_ready)),
                "short_execution_allowed": int(bool(short_execution_allowed)),
                "engine_score": float(signal_row.get("engine_score", 0.0) or 0.0),
                "stability": float(stability),
                "signal_strength": float(signal_row.get("signal_strength", 0.0) or 0.0),
                "signal_value": float(signal_row.get("signal_value", 0.0) or 0.0),
                "short_sig": float(signal_row.get("short_sig", 0.0) or 0.0),
                "total_sig": float(signal_row.get("total_sig", 0.0) or 0.0),
                "mid_scale": float(signal_row.get("mid_scale", feat.get("mid_scale", 0.5)) or 0.5),
                "trend_vs_ma200": float(-max(0.0, feat.get("trend", 0.0) or 0.0)),
                "mdd_1y": 0.0,
                "rebalance_flag": 0,
                "regime_state": str(signal_row.get("regime_state", feat.get("regime_state", "")) or ""),
                "regime_reason": str(signal_row.get("regime_reason", feat.get("regime_reason", "")) or ""),
                "regime_composite": float(feat.get("regime_composite", 0.0) or 0.0),
                "risk_on": bool(signal_row.get("risk_on", feat.get("risk_on", True))),
                "last_close": float(feat.get("last", signal_row.get("entry_price", 0.0)) or 0.0),
                "entry_price": float(signal_row.get("entry_price", feat.get("last", 0.0)) or 0.0),
                "tradable_status": tradable_status,
                "blocked_reason": blocked_reason,
                "risk_allowed": int(bool(signal_row.get("risk_allowed", True))),
                "event_risk": str(signal_row.get("event_risk", event_risk_by_symbol.get(symbol, "")) or ""),
                "event_risk_reason": str(signal_row.get("event_risk_reason", event_reason_by_symbol.get(symbol, "")) or ""),
                "earnings_in_14d": int(bool(earnings_map.get(symbol, False))),
                "short_borrow_fee_bps": float(borrow_fee_bps.get(symbol, signal_row.get("short_borrow_fee_bps", 0.0)) or 0.0),
                "short_borrow_source": str(borrow_fee_sources.get(symbol, signal_row.get("short_borrow_source", "default")) or "default"),
                "channel": str(signal_row.get("channel", "") or ""),
                "scan_tier": "short_book",
                "market_sentiment_score": 0.0,
                "alpha": float(overlay.get("overlay_alpha", 0.0) or 0.0),
                "risk": float(overlay.get("overlay_risk", 0.0) or 0.0),
                "reason": str(signal_row.get("reason", "") or ""),
            }
        )

    ranked.sort(
        key=lambda row: (
            int(bool(row.get("execution_ready", False))),
            float(row.get("score", 0.0) or 0.0),
            float(row.get("stability", 0.0) or 0.0),
        ),
        reverse=True,
    )
    return ranked[: max(0, int(short_book_cfg.top_n))]
