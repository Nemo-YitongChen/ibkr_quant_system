from __future__ import annotations

from src.common.dashboard_rendering import render_dashboard_v2_blocks


def test_render_dashboard_v2_blocks_handles_empty():
    html = render_dashboard_v2_blocks([])

    assert "Dashboard v2 Evidence Blocks" in html
    assert "No dashboard v2 blocks available" in html


def test_render_dashboard_v2_blocks_escapes_html():
    html = render_dashboard_v2_blocks(
        [
            {
                "title": "<script>alert(1)</script>",
                "status": "ok",
                "summary": "<b>bad</b>",
                "metrics": {"x": "<img>"},
            }
        ]
    )

    assert "<script>" not in html
    assert "&lt;script&gt;" in html
    assert "&lt;b&gt;bad&lt;/b&gt;" in html
    assert "&lt;img&gt;" in html


def test_render_dashboard_v2_blocks_includes_metrics():
    html = render_dashboard_v2_blocks(
        [
            {
                "title": "Execution Quality",
                "status": "warning",
                "summary": "Slippage elevated",
                "metrics": {"avg_slippage_bps": 12.3},
            }
        ]
    )

    assert "Execution Quality" in html
    assert "avg_slippage_bps" in html
    assert "12.3" in html
    assert 'badge badge-status warn' in html


def test_render_dashboard_v2_blocks_previews_nested_rows():
    html = render_dashboard_v2_blocks(
        [
            {
                "title": "Evidence Quality",
                "status": "ok",
                "summary": "nested rows",
                "metrics": {"row_count": 2},
                "rows": {"blocked_vs_allowed": [{"market": "US", "review_label": "GATE_OK"}]},
            }
        ]
    )

    assert "blocked_vs_allowed" in html
    assert "GATE_OK" in html
