from __future__ import annotations

import json
from typing import Any, Dict, List


def _avg_defined(values: List[Any]) -> float | None:
    defined: List[float] = []
    for value in values:
        if value in (None, ""):
            continue
        try:
            defined.append(float(value))
        except Exception:
            continue
    if not defined:
        return None
    return float(sum(defined) / len(defined))


def _parse_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not isinstance(value, str) or not value:
        return {}
    try:
        data = json.loads(value)
        return dict(data) if isinstance(data, dict) else {}
    except Exception:
        return {}


def _parse_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return list(value)
    if not isinstance(value, str) or not value:
        return []
    try:
        data = json.loads(value)
        return list(data) if isinstance(data, list) else []
    except Exception:
        return []


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def _portfolio_row_map(rows: List[Dict[str, Any]] | None) -> Dict[str, Dict[str, Any]]:
    return {
        str(row.get("portfolio_id") or "").strip(): dict(row)
        for row in list(rows or [])
        if str(row.get("portfolio_id") or "").strip()
    }
