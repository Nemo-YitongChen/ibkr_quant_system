from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List


def clamp_float(value: Any, lo: float, hi: float) -> float:
    try:
        number = float(value)
    except Exception:
        number = float(lo)
    return max(float(lo), min(float(hi), number))


def parse_feedback_penalty_rows(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [dict(row) for row in value if isinstance(row, dict)]
    if not isinstance(value, str) or not value:
        return []
    try:
        parsed = json.loads(value)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [dict(row) for row in parsed if isinstance(row, dict)]


def merge_execution_feedback_penalties(
    current_rows: List[Dict[str, Any]],
    previous_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    current_map = {
        str(row.get("symbol") or "").upper().strip(): dict(row)
        for row in list(current_rows or [])
        if str(row.get("symbol") or "").strip()
    }
    previous_map = {
        str(row.get("symbol") or "").upper().strip(): dict(row)
        for row in list(previous_rows or [])
        if str(row.get("symbol") or "").strip()
    }
    out: List[Dict[str, Any]] = list(current_map.values())

    for symbol, row in previous_map.items():
        if symbol in current_map:
            continue
        decayed = dict(row)
        decayed["symbol"] = symbol
        decayed["score_penalty"] = round(float(row.get("score_penalty", 0.0) or 0.0) * 0.70, 6)
        decayed["execution_penalty"] = round(float(row.get("execution_penalty", 0.0) or 0.0) * 0.65, 6)
        decayed["expected_cost_bps_add"] = round(float(row.get("expected_cost_bps_add", 0.0) or 0.0) * 0.60, 6)
        decayed["slippage_proxy_bps_add"] = round(float(row.get("slippage_proxy_bps_add", 0.0) or 0.0) * 0.60, 6)
        decayed["decay_steps"] = int(row.get("decay_steps", 0) or 0) + 1
        decayed["reason"] = "execution_hotspot_decay"
        if (
            float(decayed.get("score_penalty", 0.0) or 0.0) < 0.005
            and float(decayed.get("execution_penalty", 0.0) or 0.0) < 0.01
            and float(decayed.get("expected_cost_bps_add", 0.0) or 0.0) < 1.0
            and float(decayed.get("slippage_proxy_bps_add", 0.0) or 0.0) < 1.0
        ):
            continue
        out.append(decayed)

    out.sort(
        key=lambda row: (
            -float(row.get("execution_penalty", 0.0) or 0.0),
            -float(row.get("expected_cost_bps_add", 0.0) or 0.0),
            str(row.get("symbol") or ""),
        )
    )
    return out


def feedback_confidence_value(row: Dict[str, Any]) -> float:
    if not row:
        return 1.0
    raw = row.get("feedback_confidence")
    if raw in (None, ""):
        return 1.0
    return clamp_float(raw, 0.0, 1.0)


def scale_feedback_delta(value: Any, row: Dict[str, Any], *, min_abs: float = 0.0) -> float:
    number = float(value or 0.0)
    if number == 0.0:
        return 0.0
    scaled = number * feedback_confidence_value(row)
    if scaled == 0.0:
        return 0.0
    if min_abs > 0.0 and abs(scaled) < float(min_abs):
        scaled = float(min_abs) if scaled > 0 else -float(min_abs)
    return float(scaled)


def scale_feedback_penalty_rows(rows: List[Dict[str, Any]], row: Dict[str, Any]) -> List[Dict[str, Any]]:
    confidence = feedback_confidence_value(row)
    if confidence >= 0.999:
        return [dict(item) for item in list(rows or [])]
    out: List[Dict[str, Any]] = []
    for raw in list(rows or []):
        item = dict(raw)
        for key in (
            "score_penalty",
            "execution_penalty",
            "expected_cost_bps_add",
            "slippage_proxy_bps_add",
        ):
            if key in item:
                item[key] = round(float(item.get(key, 0.0) or 0.0) * confidence, 6)
        if "cooldown_days" in item:
            item["cooldown_days"] = max(1, int(round(float(item.get("cooldown_days", 0) or 0) * confidence)))
        item["feedback_confidence"] = round(confidence, 6)
        if (
            float(item.get("score_penalty", 0.0) or 0.0) <= 0.0
            and float(item.get("execution_penalty", 0.0) or 0.0) <= 0.0
            and float(item.get("expected_cost_bps_add", 0.0) or 0.0) <= 0.0
            and float(item.get("slippage_proxy_bps_add", 0.0) or 0.0) <= 0.0
        ):
            continue
        out.append(item)
    return out


def parse_hhmm(s: str) -> tuple[int, int]:
    hh, mm = str(s).split(":", 1)
    return int(hh), int(mm)


def in_window(now: datetime, start_hhmm: str, end_hhmm: str, weekdays: List[int]) -> bool:
    sh, sm = parse_hhmm(start_hhmm)
    eh, em = parse_hhmm(end_hhmm)
    start = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
    end = now.replace(hour=eh, minute=em, second=0, microsecond=0)
    if end >= start:
        return now.weekday() in weekdays and start <= now <= end
    if now >= start:
        return now.weekday() in weekdays
    if now <= end:
        prev_weekday = (now.weekday() - 1) % 7
        return prev_weekday in weekdays
    return False


def past_time(now: datetime, hhmm: str) -> bool:
    hh, mm = parse_hhmm(hhmm)
    target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    return now >= target
