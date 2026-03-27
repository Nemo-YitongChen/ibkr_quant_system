from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

from ib_insync import ScannerSubscription  # type: ignore

from ..common.logger import get_logger
from ..common.markets import add_market_args, market_config_path, resolve_market_code
from ..ibkr.universe import scanner_location_codes_from_config
from ..offhours.ib_setup import connect_ib, set_delayed_frozen

log = get_logger("tools.probe_market_scanner")
BASE_DIR = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Probe IBKR scanner location/codes for one market.")
    add_market_args(ap)
    ap.add_argument("--ibkr_config", default="", help="Path to the IBKR runtime config yaml.")
    ap.add_argument(
        "--candidates_config",
        default="config/reference/scanner_probe_candidates.yaml",
        help="YAML with per-market scanner candidate location codes.",
    )
    ap.add_argument("--location_codes", default="", help="Comma-separated location codes to test.")
    ap.add_argument("--scanner_codes", default="", help="Comma-separated scanner codes to test.")
    ap.add_argument("--instrument", default="", help="Override scanner instrument such as STK.")
    ap.add_argument("--limit", type=int, default=8)
    ap.add_argument("--request_timeout_sec", type=float, default=8.0)
    ap.add_argument("--out_dir", default="reports_scanner_probe")
    return ap.parse_args()


def _resolve_project_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    for candidate in (BASE_DIR / path, BASE_DIR / "config" / path, Path.cwd() / path, Path.cwd() / "config" / path):
        if candidate.exists():
            return candidate.resolve()
    return (BASE_DIR / path).resolve()


def _load_yaml(path_str: str) -> Dict[str, Any]:
    import yaml

    with _resolve_project_path(path_str).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _parse_csv(raw_value: str) -> List[str]:
    return [str(part).strip() for part in str(raw_value or "").split(",") if str(part).strip()]


def _load_probe_candidates(path_str: str, market: str) -> Dict[str, Any]:
    cfg = _load_yaml(path_str) if str(path_str or "").strip() else {}
    markets = dict(cfg.get("markets") or {})
    return dict(markets.get(str(market or "").upper(), {}) or {})


def _probe_once(ib: Any, *, instrument: str, location_code: str, scanner_code: str, limit: int) -> Dict[str, Any]:
    sub = ScannerSubscription()
    sub.instrument = str(instrument or "STK")
    sub.locationCode = str(location_code or "")
    sub.scanCode = str(scanner_code or "")
    sub.numberOfRows = int(max(1, limit))

    try:
        # Use the blocking API so we don't read an empty live subscription before rows arrive.
        data = ib.reqScannerData(sub, [], [])
        rows: List[Dict[str, Any]] = []
        for item in list(data or [])[: max(1, int(limit))]:
            contract_details = getattr(item, "contractDetails", None)
            contract = getattr(contract_details, "contract", None) if contract_details else None
            if contract is None:
                continue
            rows.append(
                {
                    "symbol": str(getattr(contract, "symbol", "") or ""),
                    "exchange": str(getattr(contract, "exchange", "") or ""),
                    "primary_exchange": str(getattr(contract, "primaryExchange", "") or ""),
                    "currency": str(getattr(contract, "currency", "") or ""),
                }
            )
        return {
            "ok": bool(rows),
            "location_code": str(location_code or ""),
            "scanner_code": str(scanner_code or ""),
            "count": int(len(rows)),
            "symbols": rows,
        }
    except Exception as e:
        return {
            "ok": False,
            "location_code": str(location_code or ""),
            "scanner_code": str(scanner_code or ""),
            "count": 0,
            "symbols": [],
            "error": f"{type(e).__name__}: {e}",
        }


def main() -> None:
    args = parse_args()
    market = resolve_market_code(getattr(args, "market", ""))
    if not market:
        raise SystemExit("--market is required")

    ibkr_cfg_path = str(market_config_path(BASE_DIR, market, args.ibkr_config or None))
    ibkr_cfg = _load_yaml(ibkr_cfg_path)
    probe_cfg = _load_probe_candidates(args.candidates_config, market)

    location_codes = _parse_csv(args.location_codes) or [
        str(item).strip()
        for item in list(probe_cfg.get("location_codes") or scanner_location_codes_from_config(ibkr_cfg, default=""))
        if str(item).strip()
    ]
    location_codes = [code for code in location_codes if code]
    scanner_codes = _parse_csv(args.scanner_codes) or [
        str(item).strip()
        for item in list(
            probe_cfg.get("scanner_codes")
            or ibkr_cfg.get("scanner_codes", ["HOT_BY_VOLUME", "TOP_PERC_GAIN", "TOP_PERC_LOSE"])
            or []
        )
        if str(item).strip()
    ]
    instrument = str(args.instrument or probe_cfg.get("instrument") or ibkr_cfg.get("scanner_instrument", "STK") or "STK").strip()
    if not location_codes:
        raise SystemExit("No scanner location codes configured")

    ib = connect_ib(
        str(ibkr_cfg["host"]),
        int(ibkr_cfg["port"]),
        int(ibkr_cfg["client_id"]),
        request_timeout=float(args.request_timeout_sec),
    )
    set_delayed_frozen(ib)
    scanner_parameters_xml = ""
    try:
        try:
            scanner_parameters_xml = str(ib.reqScannerParameters() or "")
        except Exception as e:
            log.warning("scanner parameters request failed: %s %s", type(e).__name__, e)
        probes = [
            _probe_once(
                ib,
                instrument=instrument,
                location_code=location_code,
                scanner_code=scanner_code,
                limit=int(args.limit),
            )
            for location_code in location_codes
            for scanner_code in scanner_codes
        ]
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass

    payload = {
        "market": market,
        "ibkr_config": ibkr_cfg_path,
        "instrument": instrument,
        "candidate_notes": [str(item).strip() for item in list(probe_cfg.get("notes") or []) if str(item).strip()],
        "results": probes,
        "successful": [row for row in probes if bool(row.get("ok", False))],
        "scanner_parameters_present": bool(scanner_parameters_xml),
    }
    out_dir = _resolve_project_path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"scanner_probe_{market.lower()}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if scanner_parameters_xml:
        xml_path = out_dir / f"scanner_parameters_{market.lower()}.xml"
        xml_path.write_text(scanner_parameters_xml, encoding="utf-8")
        print(f"scanner_parameters_xml={xml_path}")
    print(f"scanner_probe_json={out_path}")


if __name__ == "__main__":
    main()
