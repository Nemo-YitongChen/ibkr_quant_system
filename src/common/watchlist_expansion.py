from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Sequence


def _float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        return float(default)
    return parsed if math.isfinite(parsed) else float(default)


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


def _selection_status(row: Mapping[str, Any]) -> str:
    status = str(row.get("selection_status") or "").strip().upper()
    if status:
        return status
    return "SELECTED" if _boolish(row.get("selected")) else "REJECTED"


def _selection_reason_parts(row: Mapping[str, Any]) -> List[str]:
    reasons: List[str] = []
    for reason in str(row.get("selection_reason") or "").split(","):
        normalized = reason.strip()
        if normalized and normalized.upper() != "PASS":
            reasons.append(normalized)
    return reasons


def _clean_asset_class(value: Any) -> str:
    return str(value or "").strip().lower()


def _preferred_asset_classes(policy: WatchlistExpansionPolicy | Mapping[str, Any] | None) -> List[str]:
    if isinstance(policy, WatchlistExpansionPolicy):
        raw = policy.preferred_asset_classes
    elif isinstance(policy, Mapping):
        raw = policy.get("preferred_asset_classes")
    else:
        raw = ()
    if isinstance(raw, str):
        raw = [item.strip() for item in raw.split(",")]
    return [str(item).strip().lower() for item in list(raw or []) if str(item).strip()]


def _asset_class_summary(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = {}
    for row in list(rows or []):
        asset_class = _clean_asset_class(row.get("asset_class")) or "unknown"
        counts[asset_class] = int(counts.get(asset_class, 0)) + 1
    return [
        {"asset_class": asset_class, "count": count}
        for asset_class, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def selection_reason_summary(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = {}
    for row in list(rows or []):
        if _selection_status(row) == "SELECTED":
            continue
        for reason in _selection_reason_parts(row):
            counts[reason] = int(counts.get(reason, 0)) + 1
    return [
        {"reason": reason, "count": count}
        for reason, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _recommendation_for_reason(reason: str) -> Dict[str, str]:
    normalized = str(reason or "").strip()
    if normalized == "expected_cost_above_max":
        return {
            "action": "calibrate_cost_or_expand_lower_cost_etfs",
            "note": "Review fee/spread assumptions and expand lower-cost ETF candidates; do not relax submit quality gates.",
        }
    if normalized == "whole_share_not_tradable":
        return {
            "action": "expand_whole_share_tradable_etfs",
            "note": "Prefer ETF symbols that fit current max order value, lot rules, and whole-share paper submit constraints.",
        }
    if normalized == "last_close_above_account_cap":
        return {
            "action": "prefer_lower_price_candidates",
            "note": "For the current small-account profile, add lower-price whole-share candidates before considering higher-priced stocks.",
        }
    if normalized == "liquidity_below_min":
        return {
            "action": "expand_liquid_etf_universe",
            "note": "Look for more liquid ETFs or wait for stronger liquidity evidence before increasing order frequency.",
        }
    if normalized == "data_quality_below_min":
        return {
            "action": "refresh_data_quality",
            "note": "Refresh candidate data quality before adding this market to the automatic submit frontier.",
        }
    if normalized in {"score_below_min", "expected_edge_below_min", "whole_share_edge_margin_below_min"}:
        return {
            "action": "improve_signal_edge_before_expansion",
            "note": "Keep collecting evidence and improve expected-edge calibration before expanding this market.",
        }
    if normalized == "execution_not_ready":
        return {
            "action": "refresh_execution_artifacts",
            "note": "Regenerate report and no-submit execution artifacts before ranking this market for expansion.",
        }
    if normalized == "asset_class_not_preferred":
        return {
            "action": "keep_etf_first_profile",
            "note": "For small accounts, keep ETF-first expansion unless evidence supports widening to individual equities.",
        }
    if normalized == "action_not_allowed":
        return {
            "action": "ignore_non_growth_actions",
            "note": "Do not expand watchlists with REDUCE/WATCH rows for growth-oriented paper submit.",
        }
    return {
        "action": "monitor_watchlist_expansion",
        "note": "Review rejected candidates and keep the existing risk, cost, and quality gates intact.",
    }


def _expansion_target(top_reason: str, *, preferred_asset_class_gap: bool) -> str:
    if preferred_asset_class_gap:
        return "seed_preferred_asset_class_candidates"
    if top_reason == "expected_cost_above_max":
        return "lower_cost_whole_share_etf_candidates"
    if top_reason == "whole_share_not_tradable":
        return "lower_price_whole_share_candidates"
    if top_reason == "last_close_above_account_cap":
        return "lower_price_candidates"
    if top_reason == "liquidity_below_min":
        return "higher_liquidity_candidates"
    if top_reason in {"data_quality_below_min", "execution_not_ready"}:
        return "refresh_candidate_evidence"
    return "collect_more_candidate_evidence"


def _near_miss_candidates(rows: Iterable[Mapping[str, Any]], *, limit: int = 5) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for row in list(rows or []):
        if _selection_status(row) == "SELECTED":
            continue
        reasons = _selection_reason_parts(row)
        if not reasons:
            continue
        candidates.append(
            {
                "symbol": _symbol(row.get("symbol")),
                "asset_class": _clean_asset_class(row.get("asset_class")) or "unknown",
                "selection_reason": ",".join(reasons),
                "blocking_reason_count": len(reasons),
                "score": round(_float(row.get("score"), 0.0), 6),
                "expected_edge_bps": round(_float(row.get("expected_edge_bps"), 0.0), 6),
                "expected_cost_bps": round(_float(row.get("expected_cost_bps"), 0.0), 6),
                "whole_share_edge_margin_bps": round(_float(row.get("whole_share_edge_margin_bps"), 0.0), 6),
                "last_close": round(_float(row.get("last_close"), 0.0), 6),
            }
        )
    candidates.sort(
        key=lambda row: (
            int(row.get("blocking_reason_count", 0) or 0),
            -float(row.get("score", 0.0) or 0.0),
            -float(row.get("whole_share_edge_margin_bps", 0.0) or 0.0),
            float(row.get("expected_cost_bps", 0.0) or 0.0),
            str(row.get("symbol") or ""),
        )
    )
    return candidates[: max(0, int(limit))]


def _seed_proposal_action(expansion_target: str) -> str:
    if expansion_target == "seed_preferred_asset_class_candidates":
        return "create_or_refresh_preferred_asset_seed_watchlist"
    if expansion_target == "lower_cost_whole_share_etf_candidates":
        return "add_lower_cost_whole_share_etf_candidates"
    if expansion_target == "lower_price_whole_share_candidates":
        return "add_lower_price_whole_share_candidates"
    if expansion_target == "higher_liquidity_candidates":
        return "add_higher_liquidity_candidates"
    if expansion_target == "refresh_candidate_evidence":
        return "refresh_candidate_report_evidence"
    return "review_candidate_seed_source"


def watchlist_seed_source_candidates(
    seed_source_registry: Mapping[str, Any] | None,
    *,
    market: str,
    preferred_asset_classes: Sequence[str],
) -> List[Dict[str, Any]]:
    registry = dict(seed_source_registry or {})
    markets = registry.get("markets")
    market_rows = dict(markets or {}).get(str(market or "").strip().upper()) if isinstance(markets, Mapping) else {}
    candidates = dict(market_rows or {}).get("candidates") if isinstance(market_rows, Mapping) else []
    preferred = {str(item).strip().lower() for item in list(preferred_asset_classes or []) if str(item).strip()}
    normalized: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for raw in list(candidates or []):
        if not isinstance(raw, Mapping):
            continue
        row = dict(raw)
        symbol = _symbol(row.get("symbol"))
        asset_class = _clean_asset_class(row.get("asset_class"))
        if not symbol or symbol in seen:
            continue
        if preferred and asset_class not in preferred:
            continue
        seen.add(symbol)
        normalized.append(
            {
                "symbol": symbol,
                "exchange_ticker": str(row.get("exchange_ticker") or "").strip(),
                "asset_class": asset_class or "unknown",
                "product_name": str(row.get("product_name") or "").strip(),
                "source_name": str(row.get("source_name") or "").strip(),
                "source_url": str(row.get("source_url") or "").strip(),
                "source_verified_at": str(row.get("source_verified_at") or "").strip(),
                "broker_mapping_status": str(row.get("broker_mapping_status") or "TO_VERIFY").strip().upper(),
                "reference_price": _float(row.get("reference_price"), 0.0),
                "reference_price_currency": str(row.get("reference_price_currency") or "").strip().upper(),
                "reference_price_at": str(row.get("reference_price_at") or "").strip(),
                "rationale": str(row.get("rationale") or "").strip(),
            }
        )
    return normalized


def _source_age_days(value: Any, *, now: datetime) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0.0, (now.astimezone(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds() / 86400.0)


def _latest_candidate_rows_by_symbol(rows: Iterable[Mapping[str, Any]]) -> Dict[tuple[str, str], Dict[str, Any]]:
    latest: Dict[tuple[str, str], Dict[str, Any]] = {}
    for raw in list(rows or []):
        row = dict(raw or {})
        symbol = _symbol(row.get("symbol"))
        market = str(row.get("market") or "").strip().upper()
        if not symbol:
            continue
        key = (market, symbol)
        current = latest.get(key)
        if current is None or _float(row.get("score"), 0.0) > _float(current.get("score"), 0.0):
            latest[key] = row
    return latest


def build_watchlist_seed_promotion_review(
    seed_intake_plan: Iterable[Mapping[str, Any]],
    candidate_rows: Iterable[Mapping[str, Any]],
    *,
    policy: WatchlistExpansionPolicy | Mapping[str, Any] | None = None,
    now: datetime | None = None,
    max_source_age_days: float = 90.0,
) -> List[Dict[str, Any]]:
    """Evaluate whether review seeds have enough evidence for manual promotion."""
    effective_policy = (
        policy
        if isinstance(policy, WatchlistExpansionPolicy)
        else WatchlistExpansionPolicy.from_mapping(policy if isinstance(policy, Mapping) else None)
    )
    now_dt = now or datetime.now(timezone.utc)
    candidate_by_key = _latest_candidate_rows_by_symbol(candidate_rows)
    mapping_ready = {"MAPPED", "PASS", "QUALIFIED", "RESOLVED", "VERIFIED"}
    reviews: List[Dict[str, Any]] = []
    for intake_raw in list(seed_intake_plan or []):
        intake = dict(intake_raw or {})
        market = str(intake.get("market") or "").strip().upper()
        for source_raw in list(intake.get("source_candidates") or []):
            if not isinstance(source_raw, Mapping):
                continue
            source = dict(source_raw)
            symbol = _symbol(source.get("symbol"))
            if not market or not symbol:
                continue
            source_age_days = _source_age_days(source.get("source_verified_at"), now=now_dt)
            source_fresh = source_age_days is not None and source_age_days <= max(0.0, float(max_source_age_days))
            broker_mapping_status = str(source.get("broker_mapping_status") or "TO_VERIFY").strip().upper()
            candidate = dict(candidate_by_key.get((market, symbol)) or candidate_by_key.get(("", symbol)) or {})
            evaluation = dict(candidate)
            if candidate:
                evaluation["action"] = str(
                    candidate.get("review_seed_original_action") or candidate.get("action") or ""
                ).strip().upper()
                evaluation["execution_ready"] = candidate.get(
                    "review_seed_original_execution_ready",
                    candidate.get("execution_ready"),
                )
            quality_reasons = _selection_reasons(evaluation, effective_policy) if candidate else []
            if not source_fresh:
                status = "SOURCE_REFRESH_REQUIRED"
                next_action = "refresh_official_source_evidence"
            elif not candidate:
                status = "CANDIDATE_REPORT_REQUIRED"
                next_action = "run_candidate_report_for_seed"
            elif quality_reasons:
                status = "QUALITY_REJECTED"
                next_action = "keep_review_only_and_recheck_after_new_evidence"
            elif broker_mapping_status not in mapping_ready:
                status = "BROKER_MAPPING_REQUIRED"
                next_action = "verify_ibkr_contract_mapping_and_market_rules"
            else:
                status = "PROMOTION_REVIEW_READY"
                next_action = "manual_review_before_symbol_master_promotion"
            reviews.append(
                {
                    "market": market,
                    "symbol": symbol,
                    "asset_class": str(source.get("asset_class") or "").strip().lower(),
                    "promotion_status": status,
                    "next_action": next_action,
                    "source_verified_at": str(source.get("source_verified_at") or ""),
                    "source_age_days": round(source_age_days, 3) if source_age_days is not None else None,
                    "source_fresh": bool(source_fresh),
                    "broker_mapping_status": broker_mapping_status,
                    "candidate_evidence_present": bool(candidate),
                    "candidate_original_action": str(evaluation.get("action") or ""),
                    "candidate_original_execution_ready": int(_boolish(evaluation.get("execution_ready"))),
                    "quality_reasons": quality_reasons,
                    "score": round(_float(candidate.get("score"), 0.0), 6),
                    "expected_edge_bps": round(_expected_edge_bps(candidate), 6),
                    "expected_cost_bps": round(_float(candidate.get("expected_cost_bps"), 0.0), 6),
                    "whole_share_edge_margin_bps": round(
                        _float(candidate.get("whole_share_edge_margin_bps"), 0.0),
                        6,
                    ),
                    "whole_share_tradability_reason": str(
                        candidate.get("whole_share_tradability_reason") or ""
                    ).strip().upper(),
                    "reference_price": round(_float(source.get("reference_price"), 0.0), 6),
                    "reference_price_currency": str(source.get("reference_price_currency") or ""),
                    "reference_price_at": str(source.get("reference_price_at") or ""),
                    "auto_apply": False,
                    "does_not_change_symbol_master": True,
                    "submit_gate_policy": "do_not_relax_submit_gates",
                }
            )
    reviews.sort(
        key=lambda row: (
            0 if str(row.get("promotion_status") or "") == "PROMOTION_REVIEW_READY" else 1,
            str(row.get("market") or ""),
            str(row.get("symbol") or ""),
        )
    )
    return reviews


def build_watchlist_seed_proposals(market_recommendations: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    proposals: List[Dict[str, Any]] = []
    for row in list(market_recommendations or []):
        market = str(row.get("market") or "").strip().upper()
        if not market:
            continue
        expansion_target = str(row.get("expansion_target") or "").strip()
        near_miss_symbols = [
            _symbol(item.get("symbol"))
            for item in list(row.get("near_miss_candidates") or [])
            if isinstance(item, Mapping) and _symbol(item.get("symbol"))
        ]
        proposals.append(
            {
                "market": market,
                "proposal_status": "MANUAL_REVIEW_REQUIRED",
                "proposal_action": _seed_proposal_action(expansion_target),
                "expansion_target": expansion_target,
                "linked_recommendation_action": str(row.get("recommendation_action") or ""),
                "top_reject_reason": str(row.get("top_reject_reason") or ""),
                "preferred_asset_class_gap": bool(row.get("preferred_asset_class_gap")),
                "preferred_asset_classes": list(row.get("preferred_asset_classes") or []),
                "near_miss_symbols": near_miss_symbols[:5],
                "acceptance_rule": (
                    "Add or tag seed symbols only after they are verified as IBKR-tradable, match the account profile, "
                    "and pass whole-share, cost, liquidity, data-quality, and expected-edge gates in the next candidate report."
                ),
                "submit_gate_policy": "do_not_relax_submit_gates",
                "auto_apply": False,
            }
        )
    proposals.sort(
        key=lambda row: (
            0 if bool(row.get("preferred_asset_class_gap")) else 1,
            str(row.get("market") or ""),
        )
    )
    return proposals


def build_watchlist_seed_intake_plan(
    market_recommendations: Iterable[Mapping[str, Any]],
    *,
    seed_source_registry: Mapping[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Build a review-only intake queue for candidate supply expansion."""
    intake_rows: List[Dict[str, Any]] = []
    for row in list(market_recommendations or []):
        market = str(row.get("market") or "").strip().upper()
        if not market:
            continue
        preferred_assets = _preferred_asset_classes(row)
        near_miss = [dict(item) for item in list(row.get("near_miss_candidates") or []) if isinstance(item, Mapping)]
        evidence_symbols = [_symbol(item.get("symbol")) for item in near_miss if _symbol(item.get("symbol"))]
        preferred_near_miss_symbols = [
            _symbol(item.get("symbol"))
            for item in near_miss
            if _symbol(item.get("symbol")) and _clean_asset_class(item.get("asset_class")) in set(preferred_assets)
        ]
        source_candidates = watchlist_seed_source_candidates(
            seed_source_registry,
            market=market,
            preferred_asset_classes=preferred_assets,
        )
        source_candidate_symbols = [str(item.get("symbol") or "") for item in source_candidates]
        preferred_gap = bool(row.get("preferred_asset_class_gap"))
        if source_candidate_symbols:
            intake_status = "MANUAL_REVIEW_REQUIRED"
            candidate_symbols = source_candidate_symbols
            next_action = "verify_seed_source_candidates_in_candidate_report"
        elif preferred_gap and not preferred_near_miss_symbols:
            intake_status = "NEEDS_EXTERNAL_PREFERRED_ASSET_SOURCE"
            candidate_symbols: List[str] = []
            next_action = "source_verified_low_cost_preferred_asset_candidates"
        else:
            intake_status = "MANUAL_REVIEW_REQUIRED"
            candidate_symbols = preferred_near_miss_symbols or evidence_symbols[:5]
            next_action = "review_seed_candidates_against_acceptance_rule"
        path_market = market.lower()
        intake_rows.append(
            {
                "market": market,
                "intake_status": intake_status,
                "priority": 20 if preferred_gap else 40,
                "proposal_action": _seed_proposal_action(str(row.get("expansion_target") or "")),
                "expansion_target": str(row.get("expansion_target") or ""),
                "top_reject_reason": str(row.get("top_reject_reason") or ""),
                "preferred_asset_class_gap": preferred_gap,
                "preferred_asset_classes": preferred_assets,
                "proposed_watchlist_path": f"config/watchlists/seed_review/{path_market}_preferred_asset_seed_review.yaml",
                "candidate_symbols": candidate_symbols[:10],
                "source_candidates": source_candidates[:10],
                "evidence_symbols": evidence_symbols[:10],
                "candidate_count": len(candidate_symbols[:10]),
                "source_candidate_count": len(source_candidates[:10]),
                "evidence_symbol_count": len(evidence_symbols[:10]),
                "next_action": next_action,
                "acceptance_rule": (
                    "Only promote seed candidates into symbol master after the next candidate report verifies "
                    "IBKR tradability, account-profile fit, whole-share support, expected cost, liquidity, "
                    "data quality, expected edge, and submit quality."
                ),
                "submit_gate_policy": "do_not_relax_submit_gates",
                "auto_apply": False,
                "does_not_change_symbol_master": True,
            }
        )
    intake_rows.sort(
        key=lambda item: (
            int(item.get("priority", 999) or 999),
            str(item.get("market") or ""),
        )
    )
    return intake_rows


def build_account_growth_tier_plan(
    account_profile: Mapping[str, Any] | None,
    *,
    market_recommendations: Iterable[Mapping[str, Any]] = (),
    seed_intake_plan: Iterable[Mapping[str, Any]] = (),
) -> Dict[str, Any]:
    """Summarize the account-size-specific expansion path without changing gates."""
    profile = dict(account_profile or {})
    profile_name = str(profile.get("name") or "").strip().lower() or "unknown"
    equity = _float(profile.get("broker_equity", profile.get("account_equity")), 0.0)
    execution = dict(profile.get("execution_overrides") or {})
    max_orders_per_run = int(_float(execution.get("max_orders_per_run"), 1))
    max_order_value_pct = _float(execution.get("max_order_value_pct"), 0.0)
    max_order_value = equity * max_order_value_pct if equity > 0.0 and max_order_value_pct > 0.0 else 0.0
    min_trade_value = _float(execution.get("min_trade_value"), 0.0)
    preferred_instruments = [
        str(item).strip()
        for item in list(profile.get("preferred_instruments") or [])
        if str(item).strip()
    ]
    recommendation_rows = [dict(row) for row in list(market_recommendations or []) if isinstance(row, Mapping)]
    intake_rows = [dict(row) for row in list(seed_intake_plan or []) if isinstance(row, Mapping)]
    seed_source_count = sum(int(row.get("source_candidate_count", 0) or 0) for row in intake_rows)
    seed_source_markets = [
        str(row.get("market") or "")
        for row in intake_rows
        if int(row.get("source_candidate_count", 0) or 0) > 0 and str(row.get("market") or "").strip()
    ]
    external_source_markets = [
        str(row.get("market") or "")
        for row in intake_rows
        if str(row.get("intake_status") or "") == "NEEDS_EXTERNAL_PREFERRED_ASSET_SOURCE"
        and str(row.get("market") or "").strip()
    ]
    if profile_name == "small":
        primary_action = "verify_one_share_etf_paper_frontier"
        expansion_mode = "whole_share_tradable_etf_first"
        submit_frequency_mode = "single_small_limit_order_until_fill_quality_passes"
        next_equity_milestone = float(profile.get("max_equity", 25000.0) or 25000.0)
    elif profile_name == "medium":
        primary_action = "scale_to_low_turnover_etf_and_large_cap_basket"
        expansion_mode = "etf_plus_liquid_large_cap"
        submit_frequency_mode = "multi_symbol_limited_paper_batches_after_post_cost_edge"
        next_equity_milestone = float(profile.get("max_equity", 150000.0) or 150000.0)
    elif profile_name == "large":
        primary_action = "scale_market_profile_budgeted_baskets"
        expansion_mode = "diversified_budgeted_basket"
        submit_frequency_mode = "budgeted_sliced_batches_with_weekly_attribution"
        next_equity_milestone = 0.0
    else:
        primary_action = "resolve_account_profile_before_frequency_increase"
        expansion_mode = "unknown"
        submit_frequency_mode = "review_only"
        next_equity_milestone = 0.0
    if external_source_markets:
        primary_action = "source_verified_preferred_assets_before_frequency_increase"
    elif seed_source_count > 0 and profile_name == "small":
        primary_action = "verify_seed_etfs_in_candidate_report_before_submit"
    return {
        "profile": profile_name,
        "label": str(profile.get("label") or profile_name),
        "equity": round(equity, 6),
        "equity_band": str(profile.get("equity_band") or ""),
        "next_equity_milestone": round(next_equity_milestone, 6),
        "preferred_instruments": preferred_instruments,
        "max_orders_per_run": max_orders_per_run,
        "min_trade_value": round(min_trade_value, 6),
        "max_order_value": round(max_order_value, 6),
        "primary_action": primary_action,
        "expansion_mode": expansion_mode,
        "submit_frequency_mode": submit_frequency_mode,
        "seed_source_candidate_count": seed_source_count,
        "seed_source_markets": seed_source_markets,
        "external_source_markets": external_source_markets,
        "zero_selected_market_count": len(recommendation_rows),
        "quality_gate_policy": "do_not_relax_submit_gates",
        "read_only": True,
        "summary_text": (
            f"profile={profile_name or '-'} equity={equity:.2f} "
            f"mode={expansion_mode} primary_action={primary_action}"
        ),
    }


def summarize_watchlist_expansion(
    rows: Iterable[Mapping[str, Any]],
    *,
    market_rows: Iterable[Mapping[str, Any]] = (),
    policy: WatchlistExpansionPolicy | Mapping[str, Any] | None = None,
    seed_source_registry: Mapping[str, Any] | None = None,
    account_profile: Mapping[str, Any] | None = None,
    seed_candidate_rows: Iterable[Mapping[str, Any]] = (),
    now: datetime | None = None,
) -> Dict[str, Any]:
    clean_rows = [dict(row or {}) for row in list(rows or [])]
    clean_market_rows = [dict(row or {}) for row in list(market_rows or [])]
    preferred_assets = _preferred_asset_classes(policy)
    selected_rows = [row for row in clean_rows if _selection_status(row) == "SELECTED"]
    reason_rows = selection_reason_summary(clean_rows)
    market_recommendations: List[Dict[str, Any]] = []
    for market_row in clean_market_rows:
        market = str(market_row.get("market") or "").strip().upper()
        if not market:
            continue
        candidate_count = int(_float(market_row.get("candidate_row_count"), 0.0))
        selected_count = int(_float(market_row.get("selected_count"), 0.0))
        if candidate_count <= 0 or selected_count > 0:
            continue
        scoped_rows = [
            row
            for row in clean_rows
            if str(row.get("market") or "").strip().upper() == market
        ]
        scoped_reasons = selection_reason_summary(scoped_rows)
        top_reason = str(scoped_reasons[0].get("reason") or "") if scoped_reasons else ""
        asset_summary = _asset_class_summary(scoped_rows)
        preferred_count = sum(
            1
            for row in scoped_rows
            if _clean_asset_class(row.get("asset_class")) in set(preferred_assets)
        )
        preferred_gap = bool(preferred_assets) and preferred_count <= 0
        recommendation = _recommendation_for_reason(top_reason)
        market_recommendations.append(
            {
                "market": market,
                "candidate_row_count": candidate_count,
                "selected_count": selected_count,
                "top_reject_reason": top_reason,
                "top_reject_count": int(scoped_reasons[0].get("count", 0) or 0) if scoped_reasons else 0,
                "asset_class_summary": asset_summary,
                "preferred_asset_classes": preferred_assets,
                "preferred_asset_class_count": preferred_count,
                "preferred_asset_class_gap": preferred_gap,
                "expansion_target": _expansion_target(top_reason, preferred_asset_class_gap=preferred_gap),
                "near_miss_candidates": _near_miss_candidates(scoped_rows),
                "recommendation_action": recommendation["action"],
                "recommendation_note": recommendation["note"],
                "do_not_relax_submit_gates": True,
            }
        )
    market_recommendations.sort(
        key=lambda row: (
            -int(row.get("top_reject_count", 0) or 0),
            str(row.get("market") or ""),
        )
    )
    primary = dict(market_recommendations[0]) if market_recommendations else {}
    seed_proposals = build_watchlist_seed_proposals(market_recommendations)
    seed_intake_plan = build_watchlist_seed_intake_plan(
        market_recommendations,
        seed_source_registry=seed_source_registry,
    )
    seed_promotion_review = build_watchlist_seed_promotion_review(
        seed_intake_plan,
        list(seed_candidate_rows or []) or clean_rows,
        policy=policy,
        now=now,
    )
    promotion_by_market: Dict[str, List[Dict[str, Any]]] = {}
    for review in seed_promotion_review:
        promotion_by_market.setdefault(str(review.get("market") or ""), []).append(review)
    seed_intake_plan = [
        {
            **row,
            "promotion_review": promotion_by_market.get(str(row.get("market") or ""), []),
            "promotion_review_count": len(promotion_by_market.get(str(row.get("market") or ""), [])),
            "promotion_ready_count": sum(
                1
                for review in promotion_by_market.get(str(row.get("market") or ""), [])
                if str(review.get("promotion_status") or "") == "PROMOTION_REVIEW_READY"
            ),
        }
        for row in seed_intake_plan
    ]
    account_growth_tier_plan = build_account_growth_tier_plan(
        account_profile,
        market_recommendations=market_recommendations,
        seed_intake_plan=seed_intake_plan,
    )
    return {
        "candidate_row_count": len(clean_rows),
        "selected_count": len(selected_rows),
        "rejected_count": len(clean_rows) - len(selected_rows),
        "zero_selected_market_count": len(market_recommendations),
        "reason_summary": reason_rows,
        "market_recommendations": market_recommendations,
        "seed_proposals": seed_proposals,
        "seed_proposal_count": len(seed_proposals),
        "manual_seed_proposal_count": sum(1 for row in seed_proposals if bool(row.get("auto_apply")) is False),
        "seed_intake_plan": seed_intake_plan,
        "seed_intake_plan_count": len(seed_intake_plan),
        "seed_intake_manual_review_count": sum(
            1 for row in seed_intake_plan if str(row.get("intake_status") or "") == "MANUAL_REVIEW_REQUIRED"
        ),
        "seed_intake_external_source_count": sum(
            1
            for row in seed_intake_plan
            if str(row.get("intake_status") or "") == "NEEDS_EXTERNAL_PREFERRED_ASSET_SOURCE"
        ),
        "seed_source_candidate_count": sum(int(row.get("source_candidate_count", 0) or 0) for row in seed_intake_plan),
        "seed_source_market_count": sum(1 for row in seed_intake_plan if int(row.get("source_candidate_count", 0) or 0) > 0),
        "seed_promotion_review": seed_promotion_review,
        "seed_promotion_review_count": len(seed_promotion_review),
        "seed_promotion_ready_count": sum(
            1
            for row in seed_promotion_review
            if str(row.get("promotion_status") or "") == "PROMOTION_REVIEW_READY"
        ),
        "seed_promotion_mapping_required_count": sum(
            1
            for row in seed_promotion_review
            if str(row.get("promotion_status") or "") == "BROKER_MAPPING_REQUIRED"
        ),
        "seed_promotion_candidate_report_required_count": sum(
            1
            for row in seed_promotion_review
            if str(row.get("promotion_status") or "") == "CANDIDATE_REPORT_REQUIRED"
        ),
        "seed_promotion_source_refresh_required_count": sum(
            1
            for row in seed_promotion_review
            if str(row.get("promotion_status") or "") == "SOURCE_REFRESH_REQUIRED"
        ),
        "seed_promotion_quality_rejected_count": sum(
            1
            for row in seed_promotion_review
            if str(row.get("promotion_status") or "") == "QUALITY_REJECTED"
        ),
        "primary_recommendation_market": str(primary.get("market") or ""),
        "primary_recommendation_reason": str(primary.get("top_reject_reason") or ""),
        "primary_recommendation_action": str(primary.get("recommendation_action") or ""),
        "primary_recommendation_note": str(primary.get("recommendation_note") or ""),
        "account_growth_tier_plan": account_growth_tier_plan,
    }


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
