from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

import yaml

from ..common.markets import load_market_universe_config, resolve_market_code
from ..common.runtime_paths import resolve_repo_path
from ..common.watchlist_expansion import (
    WatchlistExpansionPolicy,
    build_watchlist_expansion_rows,
    selected_watchlist_symbols,
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
    parser.add_argument("--max_symbols_per_market", type=int, default=25)
    parser.add_argument("--min_score", type=float, default=0.45)
    parser.add_argument("--min_data_quality_score", type=float, default=0.65)
    parser.add_argument("--min_liquidity_score", type=float, default=0.45)
    parser.add_argument("--max_expected_cost_bps", type=float, default=45.0)
    parser.add_argument("--min_expected_edge_bps", type=float, default=0.0)
    parser.add_argument("--min_whole_share_edge_margin_bps", type=float, default=0.0)
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


def _policy_from_args(args: argparse.Namespace) -> WatchlistExpansionPolicy:
    return WatchlistExpansionPolicy(
        max_symbols_per_market=int(args.max_symbols_per_market),
        min_score=float(args.min_score),
        min_data_quality_score=float(args.min_data_quality_score),
        min_liquidity_score=float(args.min_liquidity_score),
        max_expected_cost_bps=float(args.max_expected_cost_bps),
        min_expected_edge_bps=float(args.min_expected_edge_bps),
        min_whole_share_edge_margin_bps=float(args.min_whole_share_edge_margin_bps),
        require_whole_share_tradability=not bool(args.allow_non_whole_share),
    )


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
    policy = _policy_from_args(args)
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
            "selection_policy": policy.__dict__,
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
            all_rows.append({**row, "watchlist_path": _display_path(watchlist_path)})
        summary_rows.append(
            {
                "market": market,
                "candidate_row_count": len(candidate_rows),
                "selected_count": len(symbols),
                "selected_symbols": ",".join(symbols),
                "watchlist_path": _display_path(watchlist_path),
                "source_candidate_file_count": len(source_files),
            }
        )

    _write_csv(analysis_dir / "watchlist_expansion_candidates.csv", all_rows)
    _write_csv(analysis_dir / "watchlist_expansion_summary.csv", summary_rows)
    (analysis_dir / "watchlist_expansion_summary.json").write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "config_path": str(config_path),
                "runtime_root": str(runtime_root),
                "policy": policy.__dict__,
                "markets": summary_rows,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"watchlist_expansion markets={len(summary_rows)} selected={sum(int(row['selected_count']) for row in summary_rows)}")


if __name__ == "__main__":
    main()
