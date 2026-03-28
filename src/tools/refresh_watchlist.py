from __future__ import annotations

"""Refresh auto-updated watchlists from public constituents pages."""

import argparse
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List

import requests
import yaml

from ..common.logger import get_logger

log = get_logger("tools.refresh_watchlist")
BASE_DIR = Path(__file__).resolve().parents[2]
HK_CODE_RE = re.compile(r"\b\d{1,5}\.HK\b", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Source config yaml")
    ap.add_argument("--out", required=True, help="Resolved watchlist yaml")
    return ap.parse_args()


def _resolve_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    for candidate in (BASE_DIR / path, Path.cwd() / path, BASE_DIR / "config" / path):
        if candidate.exists():
            return candidate.resolve()
    return (BASE_DIR / path).resolve()


def _source_config_value(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(BASE_DIR).as_posix()
    except ValueError:
        return str(resolved)


def _generated_at_value(out_path: Path, resolved_symbols: List[str]) -> str:
    if out_path.exists():
        try:
            existing = yaml.safe_load(out_path.read_text(encoding="utf-8")) or {}
        except Exception:
            existing = {}
        existing_symbols = [str(x).upper() for x in list(existing.get("symbols") or [])]
        existing_generated_at = str(existing.get("generated_at") or "").strip()
        if existing_generated_at and existing_symbols == [str(x).upper() for x in list(resolved_symbols or [])]:
            return existing_generated_at
    return time.strftime("%Y-%m-%d %H:%M:%S")


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def fetch_html(url: str, timeout: float = 20.0) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
        )
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text


def normalize_hk_code(code: str) -> str:
    n = code.split(".", 1)[0]
    try:
        iv = int(n)
        if iv >= 10000:
            return f"{iv:05d}.HK"
        return f"{iv:04d}.HK"
    except Exception:
        return code.upper()


def parse_hk_codes_from_html(html: str) -> List[str]:
    return [normalize_hk_code(c) for c in HK_CODE_RE.findall(html or "")]


def dedupe_keep_order(xs: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in xs:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def main() -> None:
    args = parse_args()
    config_path = _resolve_path(args.config)
    out_path = _resolve_path(args.out)
    cfg = load_yaml(str(config_path))

    target_n = int(cfg.get("target_n", 100))
    manual_include = [str(x).upper() for x in (cfg.get("manual_include") or [])]
    sources = cfg.get("sources") or []

    collected: List[str] = []
    for src in sources:
        url = src.get("url")
        if not url:
            continue
        try:
            html = fetch_html(url)
            syms = parse_hk_codes_from_html(html)
            log.info(f"Fetched {len(syms)} symbols from {url}")
            collected.extend(syms)
        except Exception as e:
            log.warning(f"fetch failed: {url} -> {type(e).__name__} {e}")

    combined = dedupe_keep_order(manual_include + collected)
    resolved = combined[:target_n]

    out_doc = {
        "version": int(cfg.get("version", 1)),
        "name": str(cfg.get("name", "watchlist")),
        "generated_at": _generated_at_value(out_path, resolved),
        "target_n": target_n,
        "count": len(resolved),
        "symbols": resolved,
        "source_config": _source_config_value(config_path),
        "sources": sources,
    }

    os.makedirs(out_path.parent, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(out_doc, f, sort_keys=False, allow_unicode=True)

    log.info(f"Wrote {len(resolved)} symbols -> {out_path}")


if __name__ == "__main__":
    main()
