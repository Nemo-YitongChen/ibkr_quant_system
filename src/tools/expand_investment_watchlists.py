from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

import yaml

from ..common.account_profile import AccountProfile, load_account_profiles
from ..common.markets import load_market_universe_config, resolve_market_code
from ..common.runtime_paths import resolve_repo_path
from ..common.watchlist_expansion import (
    WatchlistExpansionPolicy,
    build_watchlist_expansion_rows,
    selected_watchlist_symbols,
    summarize_watchlist_expansion,
)
from ..offhours.candidates import load_watchlist_symbols

BASE_DIR = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build quality-filtered auto-expanded watchlists from latest investment candidate reports.",
    )
    parser.add_argument("--config", default="config/supervisor.yaml", help="Supervisor config path.")
    parser.add_argument(
        "--runtime_root",
        default="runtime_data/paper_investment_only_duq152001",
        help="Runtime artifact root.",
    )
    parser.add_argument(
        "--out_dir",
        default="config/watchlists/auto_expanded",
        help="Directory for generated watchlist YAML files.",
    )
    parser.add_argument(
        "--analysis_dir",
        default="reports_supervisor/watchlist_expansion",
        help="Directory for CSV/JSON selection diagnostics.",
    )
    parser.add_argument("--max_symbols_per_market", type=int, default=None)
    parser.add_argument("--min_score", type=float, default=None)
    parser.add_argument("--min_data_quality_score", type=float, default=None)
    parser.add_argument("--min_liquidity_score", type=float, default=None)
    parser.add_argument("--max_expected_cost_bps", type=float, default=None)
    parser.add_argument("--min_expected_edge_bps", type=float, default=None)
    parser.add_argument("--min_whole_share_edge_margin_bps", type=float, default=None)
    parser.add_argument("--max_last_close", type=float, default=None)
    parser.add_argument("--account_profile_config", default="config/account_profiles.yaml")
    parser.add_argument("--seed_source_registry", default="config/watchlist_seed_sources.yaml")
    parser.add_argument("--account_equity", type=float, default=0.0)
    parser.add_argument("--account_profile", default="")
    parser.add_argument("--allow_non_whole_share", action="store_true")
    parser.add_argument("--include_cn", action="store_true")
    return parser.parse_args()


def _resolve(path: str | Path) -> Path:
    return resolve_repo_path(BASE_DIR, str(path))


def _display_path(path: str | Path) -> str:
    resolved = Path(path).resolve()
    try:
        return resolved.relative_to(BASE_DIR).as_posix()
    except ValueError:
        return str(resolved)


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return dict(data) if isinstance(data, Mapping) else {}


def _read_csv(path: Path | None) -> List[Dict[str, Any]]:
    if path is None or not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle) if str(row.get("symbol") or "").strip().upper() != "SYMBOL"]


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    clean = [dict(row) for row in list(rows or [])]
    if not clean:
        path.write_text("", encoding="utf-8")
        return
    fields: List[str] = []
    for row in clean:
        for key in row.keys():
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(clean)


def _write_seed_intake_review_files(path: Path, rows: Iterable[Mapping[str, Any]], *, generated_at: str) -> None:
    clean = [dict(row) for row in list(rows or [])]
    review_dir = path / "seed_review"
    review_dir.mkdir(parents=True, exist_ok=True)
    for row in clean:
        market = str(row.get("market") or "").strip().lower()
        if not market:
            continue
        payload = {
            "version": 1,
            "name": f"{market}_preferred_asset_seed_review",
            "generated_at": generated_at,
            "market": str(row.get("market") or "").strip().upper(),
            "review_only": True,
            "intake_status": str(row.get("intake_status") or ""),
            "proposal_action": str(row.get("proposal_action") or ""),
            "expansion_target": str(row.get("expansion_target") or ""),
            "top_reject_reason": str(row.get("top_reject_reason") or ""),
            "preferred_asset_class_gap": bool(row.get("preferred_asset_class_gap")),
            "preferred_asset_classes": list(row.get("preferred_asset_classes") or []),
            "symbols": list(row.get("candidate_symbols") or []),
            "source_candidates": list(row.get("source_candidates") or []),
            "evidence_symbols": list(row.get("evidence_symbols") or []),
            "acceptance_rule": str(row.get("acceptance_rule") or ""),
            "submit_gate_policy": str(row.get("submit_gate_policy") or "do_not_relax_submit_gates"),
            "auto_apply": False,
            "does_not_change_symbol_master": True,
            "notes": [
                "Review-only seed intake artifact.",
                "Do not wire this file into symbol_master_watchlists until the next candidate report passes all gates.",
            ],
        }
        with (review_dir / f"{market}_preferred_asset_seed_review.yaml").open("w", encoding="utf-8") as handle:
            yaml.safe_dump(payload, handle, sort_keys=False, allow_unicode=True)


def _slugify_report_name(name: str) -> str:
    text = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(name or "").strip())
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_") or "default"


def _candidate_report_dirs(
    *,
    runtime_root: Path,
    market: str,
    out_dir: str,
    watchlist_yaml: str,
) -> List[Path]:
    stem = _slugify_report_name(Path(str(watchlist_yaml or "")).stem)
    raw_out = str(out_dir or "reports_investment").strip() or "reports_investment"
    market_root = f"reports_investment_{str(market or '').lower()}"
    candidates = [
        _resolve(raw_out) / stem,
        runtime_root / raw_out / stem,
        _resolve(market_root) / stem,
        runtime_root / market_root / stem,
    ]
    seen: set[str] = set()
    out: List[Path] = []
    for path in candidates:
        key = str(path.resolve() if path.exists() else path)
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return out


def _latest_candidate_csv(*, runtime_root: Path, market: str, out_dir: str, watchlist_yaml: str) -> Path | None:
    paths = [
        path / "investment_candidates.csv"
        for path in _candidate_report_dirs(
            runtime_root=runtime_root,
            market=market,
            out_dir=out_dir,
            watchlist_yaml=watchlist_yaml,
        )
        if (path / "investment_candidates.csv").exists()
    ]
    if not paths:
        return None
    return max(paths, key=lambda path: path.stat().st_mtime)


def _watchlist_symbols(path_str: str) -> List[str]:
    if not str(path_str or "").strip():
        return []
    path = _resolve(path_str)
    return [str(symbol).upper() for symbol in load_watchlist_symbols(str(path))]


def _base_symbols_for_market(supervisor_reports: List[Mapping[str, Any]], market: str) -> List[str]:
    symbols: List[str] = []
    universe = load_market_universe_config(BASE_DIR, market)
    for key in ("seed_watchlist_yaml", "report_watchlist_yaml"):
        symbols.extend(_watchlist_symbols(str(universe.get(key) or "")))
    for watchlist in list(universe.get("symbol_master_watchlists") or []):
        symbols.extend(_watchlist_symbols(str(watchlist or "")))
    for report in supervisor_reports:
        symbols.extend(_watchlist_symbols(str(report.get("watchlist_yaml") or "")))
    seen: set[str] = set()
    out: List[str] = []
    for symbol in symbols:
        normalized = str(symbol or "").strip().upper()
        if normalized and normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out


def _reports_by_market(supervisor_config: Mapping[str, Any], *, include_cn: bool) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for market_cfg_raw in list(supervisor_config.get("markets") or []):
        market_cfg = dict(market_cfg_raw or {})
        market = resolve_market_code(str(market_cfg.get("market") or market_cfg.get("name") or ""))
        if not market or (market == "CN" and not include_cn):
            continue
        for report_raw in list(market_cfg.get("reports") or []):
            report = dict(report_raw or {})
            if str(report.get("kind", "investment") or "investment").strip().lower() != "investment":
                continue
            if bool(report.get("research_only", False)):
                continue
            if not str(report.get("watchlist_yaml") or "").strip():
                continue
            grouped.setdefault(market, []).append(report)
    return grouped


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _resolve_profile(args: argparse.Namespace) -> AccountProfile | None:
    profiles = load_account_profiles(BASE_DIR, str(args.account_profile_config or "config/account_profiles.yaml"))
    profile_name = str(args.account_profile or "").strip().lower()
    if profile_name:
        for profile in profiles.profiles:
            if profile.name.lower() == profile_name:
                return profile
    if _safe_float(args.account_equity, 0.0) > 0.0:
        return profiles.resolve(_safe_float(args.account_equity, 0.0))
    return None


def _policy_from_args(args: argparse.Namespace, profile: AccountProfile | None) -> WatchlistExpansionPolicy:
    policy = WatchlistExpansionPolicy()
    if profile is not None:
        profile_overrides = dict(profile.watchlist_expansion or {})
        max_last_close_pct = _safe_float(profile_overrides.pop("max_last_close_pct_of_equity", 0.0), 0.0)
        if max_last_close_pct > 0.0 and _safe_float(args.account_equity, 0.0) > 0.0:
            profile_overrides["max_last_close"] = round(_safe_float(args.account_equity, 0.0) * max_last_close_pct, 6)
        policy = policy.with_overrides(profile_overrides)

    cli_overrides: Dict[str, Any] = {}
    for key in (
        "max_symbols_per_market",
        "min_score",
        "min_data_quality_score",
        "min_liquidity_score",
        "max_expected_cost_bps",
        "min_expected_edge_bps",
        "min_whole_share_edge_margin_bps",
        "max_last_close",
    ):
        value = getattr(args, key)
        if value is not None:
            cli_overrides[key] = value
    cli_overrides["require_whole_share_tradability"] = not bool(args.allow_non_whole_share)
    return policy.with_overrides(cli_overrides)


def _profile_payload(profile: AccountProfile | None, *, account_equity: float) -> Dict[str, Any]:
    if profile is None:
        return {
            "name": "",
            "label": "",
            "equity_band": "",
            "account_equity": float(account_equity or 0.0),
        }
    return {
        "name": str(profile.name or ""),
        "label": str(profile.display_label or ""),
        "equity_band": str(profile.equity_band_label() or ""),
        "account_equity": float(account_equity or 0.0),
    }


def main() -> None:
    args = parse_args()
    config_path = _resolve(args.config)
    runtime_root = _resolve(args.runtime_root)
    out_dir = _resolve(args.out_dir)
    analysis_dir = _resolve(args.analysis_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    analysis_dir.mkdir(parents=True, exist_ok=True)

    supervisor_config = _load_yaml(config_path)
    reports_by_market = _reports_by_market(supervisor_config, include_cn=bool(args.include_cn))
    profile = _resolve_profile(args)
    account_equity = _safe_float(args.account_equity, 0.0)
    policy = _policy_from_args(args, profile)
    seed_source_registry_path = _resolve(str(args.seed_source_registry or "config/watchlist_seed_sources.yaml"))
    seed_source_registry = _load_yaml(seed_source_registry_path)
    account_profile_payload = _profile_payload(profile, account_equity=account_equity)
    generated_at = datetime.now(timezone.utc).isoformat()
    all_rows: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []

    for market, reports in sorted(reports_by_market.items()):
        candidate_rows: List[Dict[str, Any]] = []
        source_files: List[str] = []
        for report in reports:
            path = _latest_candidate_csv(
                runtime_root=runtime_root,
                market=market,
                out_dir=str(report.get("out_dir") or "reports_investment"),
                watchlist_yaml=str(report.get("watchlist_yaml") or ""),
            )
            if path is None:
                continue
            source_files.append(_display_path(path))
            candidate_rows.extend(_read_csv(path))

        base_symbols = _base_symbols_for_market(reports, market)
        rows = build_watchlist_expansion_rows(
            candidate_rows,
            market=market,
            base_symbols=base_symbols,
            policy=policy,
        )
        symbols = selected_watchlist_symbols(rows)
        watchlist_payload = {
            "version": 1,
            "name": f"auto_expanded_{market.lower()}_quality_growth",
            "generated_at": generated_at,
            "market": market,
            "account_profile": account_profile_payload,
            "selection_policy": policy.to_dict(),
            "source_candidate_files": source_files,
            "selected_count": len(symbols),
            "symbols": symbols,
            "notes": [
                "Generated from local investment candidate artifacts.",
                "Selection is paper-oriented and keeps whole-share tradability, cost, data-quality, and liquidity filters.",
            ],
        }
        watchlist_path = out_dir / f"{market.lower()}_quality_growth.yaml"
        with watchlist_path.open("w", encoding="utf-8") as handle:
            yaml.safe_dump(watchlist_payload, handle, sort_keys=False, allow_unicode=True)

        for row in rows:
            all_rows.append(
                {
                    **row,
                    "watchlist_path": _display_path(watchlist_path),
                    "account_profile": str(account_profile_payload.get("name") or ""),
                    "account_equity": float(account_profile_payload.get("account_equity", 0.0) or 0.0),
                }
            )
        summary_rows.append(
            {
                "market": market,
                "candidate_row_count": len(candidate_rows),
                "selected_count": len(symbols),
                "selected_symbols": ",".join(symbols),
                "watchlist_path": _display_path(watchlist_path),
                "source_candidate_file_count": len(source_files),
                "account_profile": str(account_profile_payload.get("name") or ""),
                "account_equity": float(account_profile_payload.get("account_equity", 0.0) or 0.0),
            }
        )

    _write_csv(analysis_dir / "watchlist_expansion_candidates.csv", all_rows)
    _write_csv(analysis_dir / "watchlist_expansion_summary.csv", summary_rows)
    expansion_summary = summarize_watchlist_expansion(
        all_rows,
        market_rows=summary_rows,
        policy=policy,
        seed_source_registry=seed_source_registry,
    )
    _write_seed_intake_review_files(
        analysis_dir,
        expansion_summary.get("seed_intake_plan", []),
        generated_at=generated_at,
    )
    (analysis_dir / "watchlist_expansion_summary.json").write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "config_path": str(config_path),
                "runtime_root": str(runtime_root),
                "seed_source_registry_path": str(seed_source_registry_path),
                "seed_source_registry_version": seed_source_registry.get("version"),
                "account_profile": account_profile_payload,
                "policy": policy.to_dict(),
                "markets": summary_rows,
                **expansion_summary,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"watchlist_expansion markets={len(summary_rows)} selected={sum(int(row['selected_count']) for row in summary_rows)}")


if __name__ == "__main__":
    main()
