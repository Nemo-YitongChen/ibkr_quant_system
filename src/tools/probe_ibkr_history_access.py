from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import yaml
from ib_insync import IB  # type: ignore

from ..common.markets import market_config_path, resolve_market_code
from ..common.runtime_paths import resolve_repo_path
from ..ibkr.contracts import make_stock_contract

BASE_DIR = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Probe read-only IBKR historical daily access for configured markets.")
    ap.add_argument("--config", default="config/supervisor.yaml", help="Supervisor config path.")
    ap.add_argument("--markets", default="", help="Optional comma-separated market filter, e.g. XETRA,CN.")
    ap.add_argument("--symbols_per_market", type=int, default=2, help="How many watchlist symbols to sample per market.")
    ap.add_argument("--out_dir", default="reports_preflight", help="Directory to write probe summary files.")
    return ap.parse_args()


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _load_watchlist_symbols(path: Path) -> List[str]:
    payload = _load_yaml(path)
    values = list(payload.get("symbols", []) or [])
    out: List[str] = []
    for raw in values:
        text = str(raw or "").strip().upper()
        if text and text not in out:
            out.append(text)
    return out


def _report_items(cfg: Dict[str, Any], market_filter: set[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for market_cfg in list(cfg.get("markets", []) or []):
        market_cfg_dict = dict(market_cfg or {})
        market = resolve_market_code(str(market_cfg_dict.get("market") or market_cfg_dict.get("name") or "").strip())
        if not market or (market_filter and market not in market_filter):
            continue
        for item in list(market_cfg_dict.get("reports", []) or []):
            row = dict(item or {})
            row["market"] = market
            row["market_ibkr_config"] = str(market_cfg_dict.get("ibkr_config") or "").strip()
            out.append(row)
    return out


def _resolve_ibkr_config_path(item: Dict[str, Any]) -> Path:
    market = resolve_market_code(str(item.get("market") or "").strip())
    override = str(item.get("ibkr_config") or item.get("market_ibkr_config") or "").strip() or None
    return Path(market_config_path(BASE_DIR, market, override)).resolve()


def _collect_sample_targets(cfg: Dict[str, Any], *, market_filter: set[str], symbols_per_market: int) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for item in _report_items(cfg, market_filter):
        market = str(item.get("market") or "").strip().upper()
        if not market or market in grouped:
            continue
        watchlist_yaml = str(item.get("watchlist_yaml") or "").strip()
        watchlist_path = resolve_repo_path(BASE_DIR, watchlist_yaml) if watchlist_yaml else Path()
        symbols = _load_watchlist_symbols(watchlist_path)[: max(1, int(symbols_per_market))]
        grouped[market] = {
            "market": market,
            "watchlist_path": str(watchlist_path) if watchlist_yaml else "",
            "ibkr_config_path": str(_resolve_ibkr_config_path(item)),
            "symbols": symbols,
        }
    return [grouped[key] for key in sorted(grouped)]


def _match_recent_errors(errors: List[Dict[str, Any]], *, since_index: int) -> List[Dict[str, Any]]:
    return [dict(row) for row in list(errors[since_index:] or [])]


def _classify_probe_result(
    *,
    contract_details_count: int,
    history_bar_count: int,
    error_rows: List[Dict[str, Any]],
) -> tuple[str, str]:
    codes = {int(row.get("code", 0) or 0) for row in error_rows}
    if contract_details_count <= 0:
        if 200 in codes:
            return "NO_SECURITY_DEF", "合约定义不存在，优先检查市场映射、交易所代码或该标的是否可在当前市场接入。"
        return "NO_CONTRACT", "未拿到合约详情，优先检查 symbol 映射与合约路由。"
    if history_bar_count > 0:
        return "OK", "合约与历史日线都可正常获取。"
    if 162 in codes:
        return "NO_MARKET_DATA_PERMISSION", "合约能解析，但历史权限不足；更像订阅/权限问题而不是代码映射问题。"
    if 200 in codes:
        return "NO_SECURITY_DEF", "历史请求阶段返回 no security definition，需继续检查该标的的合约路由。"
    if error_rows:
        return "HISTORY_ERROR", "历史请求返回了错误事件，建议查看 code/message 继续排查。"
    return "EMPTY_HISTORY", "没有拿到错误事件，但历史结果为空，需继续检查交易时段或数据服务可用性。"


def _render_markdown(summary: Dict[str, Any]) -> str:
    lines = [
        "# IBKR History Probe",
        "",
        f"- generated_at: {summary.get('generated_at', '')}",
        f"- config: {summary.get('config_path', '')}",
        "",
        "## Market Summary",
        "",
    ]
    market_rows = list(summary.get("market_summary", []) or [])
    if not market_rows:
        lines.append("- 无数据")
    else:
        for row in market_rows:
            lines.append(
                "- "
                f"{row.get('market', '')}: sampled={row.get('sample_count', 0)} "
                f"ok={row.get('ok_count', 0)} perm={row.get('permission_count', 0)} "
                f"contract={row.get('contract_count', 0)} empty={row.get('empty_count', 0)} "
                f"status={row.get('status_label', '')} "
                f"diagnosis={row.get('diagnosis', '')}"
            )
    lines.extend(["", "## Symbol Details", ""])
    symbol_rows = list(summary.get("symbol_rows", []) or [])
    if not symbol_rows:
        lines.append("- 无数据")
    else:
        for row in symbol_rows:
            lines.append(
                "- "
                f"{row.get('market', '')}:{row.get('symbol', '')} "
                f"status={row.get('status_label', '')} "
                f"contract_details={row.get('contract_details_count', 0)} "
                f"bars={row.get('history_bar_count', 0)} "
                f"errors={row.get('error_codes', '') or '-'} "
                f"diagnosis={row.get('diagnosis', '')}"
            )
    return "\n".join(lines) + "\n"


def run_probe(config_path: str, *, markets: str = "", symbols_per_market: int = 2, out_dir: str = "reports_preflight") -> Dict[str, Any]:
    config_resolved = resolve_repo_path(BASE_DIR, config_path)
    cfg = _load_yaml(config_resolved)
    market_filter = {
        resolve_market_code(part.strip())
        for part in str(markets or "").split(",")
        if str(part).strip()
    }
    targets = _collect_sample_targets(cfg, market_filter=market_filter, symbols_per_market=symbols_per_market)
    all_rows: List[Dict[str, Any]] = []

    for target in targets:
        ibkr_cfg = _load_yaml(Path(str(target.get("ibkr_config_path") or "")))
        host = str(ibkr_cfg.get("host", "127.0.0.1") or "127.0.0.1")
        port = int(ibkr_cfg.get("port", 4002) or 4002)
        client_id = int(ibkr_cfg.get("client_id", 1) or 1) + 7000
        ib = IB()
        error_rows: List[Dict[str, Any]] = []

        def on_error(req_id: int, code: int, msg: str, contract: Any) -> None:
            error_rows.append(
                {
                    "req_id": int(req_id or 0),
                    "code": int(code or 0),
                    "message": str(msg or "").strip(),
                    "contract": str(contract or ""),
                }
            )

        try:
            ib.errorEvent += on_error
            ib.connect(host, port, clientId=client_id, readonly=True, timeout=10)
            for symbol in list(target.get("symbols", []) or []):
                start_index = len(error_rows)
                contract = make_stock_contract(str(symbol))
                contract_details = ib.reqContractDetails(contract)
                contract_error_rows = _match_recent_errors(error_rows, since_index=start_index)
                resolved_contract = contract_details[0].contract if contract_details else None

                hist_start_index = len(error_rows)
                bars = []
                if resolved_contract is not None:
                    bars = ib.reqHistoricalData(
                        contract=resolved_contract,
                        endDateTime="",
                        durationStr="1 Y",
                        barSizeSetting="1 day",
                        whatToShow="TRADES",
                        useRTH=False,
                        formatDate=2,
                        timeout=15,
                    )
                history_error_rows = _match_recent_errors(error_rows, since_index=hist_start_index)
                merged_errors = contract_error_rows + history_error_rows
                status_label, diagnosis = _classify_probe_result(
                    contract_details_count=len(contract_details or []),
                    history_bar_count=len(bars or []),
                    error_rows=merged_errors,
                )
                all_rows.append(
                    {
                        "market": str(target.get("market") or ""),
                        "symbol": str(symbol or ""),
                        "watchlist_path": str(target.get("watchlist_path") or ""),
                        "ibkr_config_path": str(target.get("ibkr_config_path") or ""),
                        "status_label": status_label,
                        "diagnosis": diagnosis,
                        "contract_details_count": int(len(contract_details or [])),
                        "history_bar_count": int(len(bars or [])),
                        "resolved_contract": str(resolved_contract or ""),
                        "error_codes": ",".join(str(row.get("code", "")) for row in merged_errors if str(row.get("code", "")).strip()),
                        "error_messages": " | ".join(str(row.get("message", "")).strip() for row in merged_errors if str(row.get("message", "")).strip()),
                    }
                )
        finally:
            try:
                ib.disconnect()
            except Exception:
                pass

    market_summary: List[Dict[str, Any]] = []
    for market in sorted({str(row.get("market") or "") for row in all_rows if str(row.get("market") or "").strip()}):
        rows = [row for row in all_rows if str(row.get("market") or "") == market]
        ok_count = sum(1 for row in rows if str(row.get("status_label") or "") == "OK")
        permission_count = sum(1 for row in rows if str(row.get("status_label") or "") == "NO_MARKET_DATA_PERMISSION")
        contract_count = sum(1 for row in rows if str(row.get("status_label") or "") in {"NO_SECURITY_DEF", "NO_CONTRACT"})
        empty_count = sum(1 for row in rows if str(row.get("status_label") or "") in {"EMPTY_HISTORY", "HISTORY_ERROR"})
        if permission_count > 0:
            status_label = "权限待补"
            diagnosis = "至少一个样本合约能解析，但历史权限不足，优先检查该市场订阅/权限。"
        elif contract_count > 0:
            status_label = "合约待修"
            diagnosis = "至少一个样本没有通过合约定义，优先检查 symbol 映射或交易所代码。"
        elif ok_count == len(rows):
            status_label = "正常"
            diagnosis = "抽样 symbol 的合约和历史都正常。"
        else:
            status_label = "待观察"
            diagnosis = "当前没有明确权限错误，但仍有空历史或其它错误，需要继续观察。"
        market_summary.append(
            {
                "market": market,
                "sample_count": len(rows),
                "ok_count": ok_count,
                "permission_count": permission_count,
                "contract_count": contract_count,
                "empty_count": empty_count,
                "status_label": status_label,
                "diagnosis": diagnosis,
                "symbols": ",".join(str(row.get("symbol") or "") for row in rows),
            }
        )

    summary = {
        "generated_at": datetime.now().isoformat(),
        "config_path": str(config_resolved),
        "market_summary": market_summary,
        "symbol_rows": all_rows,
    }
    out_root = resolve_repo_path(BASE_DIR, out_dir)
    out_root.mkdir(parents=True, exist_ok=True)
    (out_root / "ibkr_history_probe_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_root / "ibkr_history_probe_report.md").write_text(_render_markdown(summary), encoding="utf-8")
    return summary


def main() -> None:
    args = parse_args()
    summary = run_probe(
        args.config,
        markets=args.markets,
        symbols_per_market=int(args.symbols_per_market),
        out_dir=args.out_dir,
    )
    print(f"probe_json={resolve_repo_path(BASE_DIR, args.out_dir) / 'ibkr_history_probe_summary.json'}")
    print(f"probe_markets={len(list(summary.get('market_summary', []) or []))}")


if __name__ == "__main__":
    main()
