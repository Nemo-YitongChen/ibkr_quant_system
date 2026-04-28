from __future__ import annotations

from src.common.dashboard_evidence import build_market_views
from src.common.dashboard_market_context import market_context


def test_market_context_known_markets():
    assert "趋势优先" in market_context("US")["summary"]
    assert "board_lot_mismatch" in market_context("HK")["primary_risks"]
    assert "research_only" in market_context("CN")["primary_risks"]


def test_market_context_unknown_market_falls_back():
    context = market_context("xetra")

    assert context["label"] == "XETRA"
    assert context["summary"] == ""
    assert context["primary_risks"] == []


def test_market_views_include_context_for_empty_input():
    views = build_market_views([])

    assert views["US"]["context"]
    assert views["US"]["context_summary"]
    assert views["HK"]["primary_risks"]
    assert views["CN"]["context_summary"]
