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


def render_dashboard_v2_blocks(blocks: List[Dict[str, Any]]) -> str:
    if not blocks:
        return (
            '<section class="card overview dashboard-v2-blocks">'
            "<h2>Dashboard v2 Evidence Blocks</h2>"
            '<div class="empty">No dashboard v2 blocks available.</div>'
            "</section>"
        )

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
        cards.append(
            '<div class="dashboard-v2-card">'
            f'<h3>{title} <span class="badge badge-status {cls}">{escape(status)}</span></h3>'
            f"<p>{summary}</p>"
            f"{_render_metrics(metrics)}"
            f"{rows_html}"
            "</div>"
        )

    if not cards:
        return render_dashboard_v2_blocks([])
    return (
        '<section class="card overview dashboard-v2-blocks">'
        "<h2>Dashboard v2 Evidence Blocks</h2>"
        '<div class="meta">Structured evidence block summaries rendered from the dashboard v2 JSON payload.</div>'
        '<div class="dashboard-v2-grid">'
        + "".join(cards)
        + "</div>"
        "</section>"
    )
