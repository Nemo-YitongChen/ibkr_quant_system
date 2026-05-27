from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Sequence


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _symbol(value: Any) -> str:
    return str(value or "").strip().upper()


@dataclass(frozen=True)
class WatchlistExpansionPolicy:
    max_symbols_per_market: int = 25
    min_score: float = 0.45
    min_data_quality_score: float = 0.65
    min_liquidity_score: float = 0.45
    max_expected_cost_bps: float = 45.0
    min_expected_edge_bps: float = 0.0
    min_whole_share_edge_margin_bps: float = 0.0
    max_last_close: float = 0.0
    require_execution_ready: bool = True
    require_whole_share_tradability: bool = True
    allowed_actions: Sequence[str] = field(default_factory=lambda: ("ACCUMULATE", "HOLD"))
    preferred_asset_classes: Sequence[str] = field(default_factory=lambda: ("etf", "equity"))

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> "WatchlistExpansionPolicy":
        source = dict(raw or {})
        allowed_actions = source.get("allowed_actions")
        if isinstance(allowed_actions, str):
            allowed_actions = [item.strip() for item in allowed_actions.split(",")]
        preferred_asset_classes = source.get("preferred_asset_classes")
        if isinstance(preferred_asset_classes, str):
            preferred_asset_classes = [item.strip() for item in preferred_asset_classes.split(",")]
        return cls(
            max_symbols_per_market=max(1, int(_float(source.get("max_symbols_per_market"), 25))),
            min_score=_float(source.get("min_score"), 0.45),
            min_data_quality_score=_float(source.get("min_data_quality_score"), 0.65),
            min_liquidity_score=_float(source.get("min_liquidity_score"), 0.45),
            max_expected_cost_bps=_float(source.get("max_expected_cost_bps"), 45.0),
            min_expected_edge_bps=_float(source.get("min_expected_edge_bps"), 0.0),
            min_whole_share_edge_margin_bps=_float(source.get("min_whole_share_edge_margin_bps"), 0.0),
            max_last_close=_float(source.get("max_last_close"), 0.0),
            require_execution_ready=bool(source.get("require_execution_ready", True)),
            require_whole_share_tradability=bool(source.get("require_whole_share_tradability", True)),
            allowed_actions=tuple(str(item).strip().upper() for item in list(allowed_actions or ("ACCUMULATE", "HOLD")) if str(item).strip()),
            preferred_asset_classes=tuple(
                str(item).strip().lower()
                for item in list(preferred_asset_classes or ("etf", "equity"))
                if str(item).strip()
            ),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_symbols_per_market": int(self.max_symbols_per_market),
            "min_score": float(self.min_score),
            "min_data_quality_score": float(self.min_data_quality_score),
            "min_liquidity_score": float(self.min_liquidity_score),
            "max_expected_cost_bps": float(self.max_expected_cost_bps),
            "min_expected_edge_bps": float(self.min_expected_edge_bps),
            "min_whole_share_edge_margin_bps": float(self.min_whole_share_edge_margin_bps),
            "max_last_close": float(self.max_last_close),
            "require_execution_ready": bool(self.require_execution_ready),
            "require_whole_share_tradability": bool(self.require_whole_share_tradability),
            "allowed_actions": list(self.allowed_actions),
            "preferred_asset_classes": list(self.preferred_asset_classes),
        }

    def with_overrides(self, raw: Mapping[str, Any] | None) -> "WatchlistExpansionPolicy":
        if not raw:
            return self
        payload = self.to_dict()
        payload.update({key: value for key, value in dict(raw or {}).items() if value is not None})
        return WatchlistExpansionPolicy.from_mapping(payload)


def _expected_edge_bps(row: Mapping[str, Any]) -> float:
    direct = _float(row.get("expected_edge_bps"), 0.0)
    if direct > 0.0:
        return direct
    whole_share = _float(row.get("whole_share_expected_edge_bps"), 0.0)
    if whole_share > 0.0:
        return whole_share
    score_before_cost = _float(row.get("score_before_cost"), 0.0)
    threshold = _float(row.get("accumulate_threshold"), _float(row.get("hold_threshold"), 0.0))
    if score_before_cost > threshold:
        return (score_before_cost - threshold) * 140.0
    return 0.0


def _selection_reasons(row: Mapping[str, Any], policy: WatchlistExpansionPolicy) -> List[str]:
    reasons: List[str] = []
    action = str(row.get("action") or "").strip().upper()
    asset_class = str(row.get("asset_class") or "").strip().lower()
    expected_cost_bps = _float(row.get("expected_cost_bps"), 0.0)
    expected_edge_bps = _expected_edge_bps(row)
    edge_margin_bps = _float(row.get("whole_share_edge_margin_bps"), expected_edge_bps - expected_cost_bps)
    if not _symbol(row.get("symbol")):
        reasons.append("missing_symbol")
    if policy.allowed_actions and action not in set(policy.allowed_actions):
        reasons.append("action_not_allowed")
    if policy.require_execution_ready and not _boolish(row.get("execution_ready")):
        reasons.append("execution_not_ready")
    if asset_class and policy.preferred_asset_classes and asset_class not in set(policy.preferred_asset_classes):
        reasons.append("asset_class_not_preferred")
    if _float(row.get("score"), 0.0) < float(policy.min_score):
        reasons.append("score_below_min")
    if _float(row.get("data_quality_score"), 0.0) < float(policy.min_data_quality_score):
        reasons.append("data_quality_below_min")
    if _float(row.get("liquidity_score"), 0.0) < float(policy.min_liquidity_score):
        reasons.append("liquidity_below_min")
    if expected_cost_bps > float(policy.max_expected_cost_bps):
        reasons.append("expected_cost_above_max")
    if expected_edge_bps < float(policy.min_expected_edge_bps):
        reasons.append("expected_edge_below_min")
    if edge_margin_bps < float(policy.min_whole_share_edge_margin_bps):
        reasons.append("whole_share_edge_margin_below_min")
    last_close = _float(row.get("last_close"), 0.0)
    if float(policy.max_last_close) > 0.0 and last_close > float(policy.max_last_close):
        reasons.append("last_close_above_account_cap")
    if (
        policy.require_whole_share_tradability
        and str(row.get("whole_share_tradability_reason") or "").strip().upper() != "PASS"
    ):
        reasons.append("whole_share_not_tradable")
    return reasons


def _asset_class_rank(asset_class: Any, policy: WatchlistExpansionPolicy) -> int:
    normalized = str(asset_class or "").strip().lower()
    preferred = [str(item).strip().lower() for item in list(policy.preferred_asset_classes or [])]
    if normalized in preferred:
        return preferred.index(normalized)
    return len(preferred) + 1


def build_watchlist_expansion_rows(
    candidate_rows: Iterable[Mapping[str, Any]],
    *,
    market: str,
    base_symbols: Iterable[str] = (),
    policy: WatchlistExpansionPolicy | None = None,
) -> List[Dict[str, Any]]:
    effective_policy = policy or WatchlistExpansionPolicy()
    base = {_symbol(symbol) for symbol in list(base_symbols or []) if _symbol(symbol)}
    latest_by_symbol: Dict[str, Dict[str, Any]] = {}
    for raw in list(candidate_rows or []):
        row = dict(raw or {})
        symbol = _symbol(row.get("symbol"))
        if not symbol or symbol == "SYMBOL":
            continue
        current = latest_by_symbol.get(symbol)
        if current is None or _float(row.get("score"), 0.0) > _float(current.get("score"), 0.0):
            latest_by_symbol[symbol] = row

    rows: List[Dict[str, Any]] = []
    for symbol, row in latest_by_symbol.items():
        reasons = _selection_reasons(row, effective_policy)
        expected_edge_bps = _expected_edge_bps(row)
        expected_cost_bps = _float(row.get("expected_cost_bps"), 0.0)
        edge_margin_bps = _float(row.get("whole_share_edge_margin_bps"), expected_edge_bps - expected_cost_bps)
        rows.append(
            {
                "symbol": symbol,
                "market": str(market or row.get("market") or "").strip().upper(),
                "selected": not reasons,
                "selection_status": "SELECTED" if not reasons else "REJECTED",
                "selection_reason": "PASS" if not reasons else ",".join(reasons),
                "already_in_base_watchlist": symbol in base,
                "action": str(row.get("action") or "").strip().upper(),
                "asset_class": str(row.get("asset_class") or "").strip().lower(),
                "score": round(_float(row.get("score"), 0.0), 6),
                "expected_edge_bps": round(expected_edge_bps, 6),
                "expected_cost_bps": round(expected_cost_bps, 6),
                "whole_share_edge_margin_bps": round(edge_margin_bps, 6),
                "max_last_close": round(float(effective_policy.max_last_close), 6),
                "whole_share_tradability_reason": str(row.get("whole_share_tradability_reason") or "").strip().upper(),
                "data_quality_score": round(_float(row.get("data_quality_score"), 0.0), 6),
                "liquidity_score": round(_float(row.get("liquidity_score"), 0.0), 6),
                "last_close": round(_float(row.get("last_close"), 0.0), 6),
                "source": str(row.get("source") or "").strip(),
            }
        )
    rows.sort(
        key=lambda item: (
            0 if bool(item.get("selected")) else 1,
            _asset_class_rank(item.get("asset_class"), effective_policy),
            -float(item.get("score", 0.0) or 0.0),
            -float(item.get("whole_share_edge_margin_bps", 0.0) or 0.0),
            str(item.get("symbol") or ""),
        )
    )
    selected_seen: set[str] = set()
    selected_count = 0
    limited_rows: List[Dict[str, Any]] = []
    for row in rows:
        if not bool(row.get("selected")):
            limited_rows.append(row)
            continue
        symbol = str(row.get("symbol") or "")
        if selected_count >= int(effective_policy.max_symbols_per_market):
            downgraded = dict(row)
            downgraded["selected"] = False
            downgraded["selection_status"] = "REJECTED"
            downgraded["selection_reason"] = "market_symbol_limit_reached"
            limited_rows.append(downgraded)
            continue
        if symbol in selected_seen:
            continue
        selected_seen.add(symbol)
        selected_count += 1
        limited_rows.append(row)
    return limited_rows


def selected_watchlist_symbols(rows: Iterable[Mapping[str, Any]]) -> List[str]:
    symbols: List[str] = []
    seen: set[str] = set()
    for row in list(rows or []):
        if not bool(row.get("selected")):
            continue
        symbol = _symbol(row.get("symbol"))
        if symbol and symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)
    return symbols
