from __future__ import annotations

import json
from html import escape
from typing import Any, Dict, List

STATUS_CLASS = {
    "ok": "ok",
    "ready": "ok",
    "pass": "ok",
    "warn": "warn",
    "warning": "warn",
    "degraded": "warn",
    "fail": "fail",
    "failed": "fail",
    "error": "fail",
}


def _status_class(status: Any) -> str:
    return STATUS_CLASS.get(str(status or "").strip().lower(), "warn")


def _json_preview(value: Any) -> str:
    return escape(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str))


def _render_metrics(metrics: Dict[str, Any]) -> str:
    if not metrics:
        return "<p>No metrics available.</p>"
    rows = []
    for key, value in list(metrics.items())[:8]:
        rows.append(
            "<tr>"
            f"<th>{escape(str(key))}</th>"
            f"<td>{escape(str(value))}</td>"
            "</tr>"
        )
    return "<table>" + "".join(rows) + "</table>"


def _render_row_list(rows: List[Dict[str, Any]]) -> str:
    previews = []
    for row in rows[:5]:
        previews.append("<pre>" + _json_preview(row) + "</pre>")
    return "".join(previews)


def _render_rows(rows: Any) -> str:
    if isinstance(rows, list):
        clean_rows = [dict(row) for row in rows if isinstance(row, dict)]
        return _render_row_list(clean_rows)
    if isinstance(rows, dict):
        sections = []
        for key, value in list(rows.items())[:4]:
            if isinstance(value, list):
                rendered = _render_row_list([dict(row) for row in value if isinstance(row, dict)])
            else:
                rendered = "<pre>" + _json_preview(value) + "</pre>"
            if rendered:
                sections.append(f"<h4>{escape(str(key))}</h4>{rendered}")
        return "".join(sections)
    return ""


def _render_block_cards(blocks: List[Dict[str, Any]]) -> str:
    cards = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        title = escape(str(block.get("title") or block.get("id") or "Untitled"))
        status = str(block.get("status") or "warning")
        summary = escape(str(block.get("summary") or ""))
        cls = _status_class(status)
        metrics = dict(block.get("metrics") or block.get("headline") or {})
        rows_html = _render_rows(block.get("rows"))
        details_html = f'<div class="advanced-only">{rows_html}</div>' if rows_html else ""
        cards.append(
            '<div class="dashboard-v2-card">'
            f'<h3>{title} <span class="badge badge-status {cls}">{escape(status)}</span></h3>'
            f"<p>{summary}</p>"
            f"{_render_metrics(metrics)}"
            f"{details_html}"
            "</div>"
        )
    return "".join(cards)


def render_dashboard_v2_blocks(blocks: List[Dict[str, Any]]) -> str:
    clean_blocks = [dict(block) for block in list(blocks or []) if isinstance(block, dict)]
    if not clean_blocks:
        return (
            '<section class="card overview dashboard-v2-blocks">'
            "<h2>Dashboard v2 Evidence Blocks</h2>"
            '<div class="empty">No dashboard v2 blocks available.</div>'
            "</section>"
        )

    home_blocks = [
        block
        for block in clean_blocks
        if str(block.get("category") or "home") == "home" and not bool(block.get("advanced_only", False))
    ]
    advanced_blocks = [
        block
        for block in clean_blocks
        if str(block.get("category") or "") == "advanced" or bool(block.get("advanced_only", False))
    ]
    home_cards = _render_block_cards(home_blocks)
    advanced_cards = _render_block_cards(advanced_blocks)

    if not home_cards and not advanced_cards:
        return render_dashboard_v2_blocks([])
    advanced_section = (
        '<div class="advanced-only dashboard-v2-advanced">'
        "<h3>Advanced Evidence Blocks</h3>"
        '<div class="dashboard-v2-grid">'
        f"{advanced_cards}"
        "</div>"
        "</div>"
        if advanced_cards
        else ""
    )
    return (
        '<section class="card overview dashboard-v2-blocks">'
        "<h2>Dashboard v2 Evidence Blocks</h2>"
        '<div class="meta">Home shows Ops Health, Evidence Focus, Execution Quality, and Governance / Control Actions. Advanced mode expands market, walk-forward, strategy parameter governance, waterfall, unified evidence, blocked-vs-allowed, and action history blocks.</div>'
        '<div class="dashboard-v2-grid">'
        + home_cards
        + "</div>"
        + advanced_section
        + "</section>"
    )
