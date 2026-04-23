from __future__ import annotations

from src.tools.generate_dashboard import (
    _build_dashboard_status_rollout_summary,
    _build_health_overview,
    _build_market_data_health_overview,
    _build_overview,
    _simple_ops_overview_rows,
    _dashboard_market_state_label,
    _dashboard_report_freshness_label,
    _translate_market_status_label_en,
    _translate_report_freshness_label_en,
)


def test_dashboard_report_freshness_label_keeps_legacy_calls() -> None:
    assert _dashboard_report_freshness_label("fresh") == "报告已更新"
    assert _dashboard_report_freshness_label("stale") == "报告待刷新"


def test_dashboard_report_freshness_label_supports_market_and_time_semantics() -> None:
    stale = _dashboard_report_freshness_label(
        market="US",
        report_date="2026-04-10",
        latest_generated_at="2026-04-13T09:30:00",
        as_of_date="2026-04-19",
    )
    fresh = _dashboard_report_freshness_label(
        market="HK",
        report_date="2026-04-19",
        latest_generated_at="2026-04-19T09:30:00",
        as_of_date="2026-04-19",
    )
    assert "US" in stale
    assert "待刷新" in stale
    assert "HK" in fresh
    assert "已更新" in fresh


def test_dashboard_market_state_label_supports_none() -> None:
    assert _dashboard_market_state_label(None) == "市场状态: 暂无数据"
    assert _dashboard_market_state_label(True) == "开市中"
    assert _dashboard_market_state_label(False) == "已闭市"


def test_build_health_overview_prefers_degraded_and_merges_summary() -> None:
    rows = _build_health_overview(
        [
            {"status": "ready", "summary": "整体正常"},
            {"status": "warning", "summary": "权限有波动"},
            {"status": "degraded", "summary": "连接短时降级"},
        ]
    )
    assert rows[0]["status"] == "degraded"
    assert "连接短时降级" in rows[0]["summary"]
    assert "权限有波动" in rows[0]["summary"]


def test_build_market_data_health_overview_empty_returns_warning_summary() -> None:
    rows = _build_market_data_health_overview([])
    assert rows[0]["status"] == "warning"
    assert rows[0]["summary"] == "暂无市场数据健康检查结果"


def test_dashboard_label_translation_helpers_support_dynamic_market_labels() -> None:
    assert _translate_report_freshness_label_en("US 已更新") == "US Report Fresh"
    assert _translate_report_freshness_label_en("HK 待刷新") == "HK Report Needs Refresh"
    assert _translate_market_status_label_en("市场状态: 暂无数据") == "Market State: No Data"


def test_build_overview_includes_freshness_and_health_fields() -> None:
    rows = _build_overview(
        [
            {
                "market": "US",
                "watchlist": "watchlist",
                "mode": "paper-auto-submit",
                "exchange_open": True,
                "market_state_label": "开市中",
                "report_freshness_label": "US 待刷新",
                "report_status_label": "US 待刷新",
                "health_overview": [{"status_label": "有降级", "summary": "连接短时降级"}],
                "market_data_health_overview": [{"status_label": "待排查", "summary": "IBKR 历史覆盖不足"}],
                "priority_order": 1,
                "recommended_action": "可执行调仓",
                "recommended_detail": "BUY AAPL",
                "paper_summary": {"equity_after": 100000.0, "cash_after": 10000.0},
                "execution_summary": {"broker_equity": 100500.0, "broker_cash": 9500.0},
                "opportunity_summary": {"entry_now_count": 1, "wait_count": 2},
                "health_summary": {"status": "DEGRADED"},
                "dashboard_view": "trade",
            }
        ]
    )
    assert rows[0]["market_state_label"] == "开市中"
    assert rows[0]["report_freshness_label"] == "US 待刷新"
    assert rows[0]["health_status_label"] == "有降级"
    assert rows[0]["market_data_status_label"] == "待排查"


def test_build_dashboard_status_rollout_summary_tracks_rollout_gaps() -> None:
    summary = _build_dashboard_status_rollout_summary(
        [
            {
                "market": "US",
                "exchange_open_raw": None,
                "report_freshness_label": "US 待刷新",
                "report_status": {"fresh": False},
                "health_overview": [{"status": "degraded", "status_label": "有降级", "summary": "连接短时降级"}],
                "market_data_health_overview": [{"status": "warning", "status_label": "待排查", "summary": "IBKR 历史覆盖不足"}],
            },
            {
                "market": "US",
                "exchange_open_raw": True,
                "report_freshness_label": "US 已更新",
                "report_status": {"fresh": True},
                "health_overview": [{"status": "ready", "status_label": "已就绪", "summary": "整体正常"}],
                "market_data_health_overview": [{"status": "ready", "status_label": "IBKR正常", "summary": "覆盖稳定"}],
            },
            {
                "market": "HK",
                "exchange_open_raw": False,
                "report_freshness_label": "HK 已更新",
                "report_status": {"fresh": True},
                "health_overview": [{"status": "warning", "status_label": "有告警", "summary": "权限有波动"}],
                "market_data_health_overview": [{"status": "warning", "status_label": "研究Fallback", "summary": "research-only fallback"}],
            },
        ]
    )
    assert summary["portfolio_count"] == 3
    assert summary["market_state_missing_count"] == 1
    assert summary["report_stale_count"] == 1
    assert summary["ops_degraded_count"] == 1
    assert summary["ops_warning_count"] == 1
    assert summary["data_attention_count"] == 1
    assert summary["data_research_fallback_count"] == 1
    assert summary["market_rows"][0]["market"] == "US"


def test_simple_ops_overview_rows_include_status_rollout_counts() -> None:
    rows = _simple_ops_overview_rows(
        {
            "preflight_pass_count": 3,
            "preflight_warn_count": 1,
            "preflight_fail_count": 0,
            "ibkr_port_warning_count": 1,
            "market_state_missing_count": 2,
            "stale_report_count": 1,
            "degraded_health_count": 1,
            "data_attention_count": 0,
            "data_research_fallback_count": 1,
            "execution_mode_mismatch_count": 0,
            "control_service_status": "configured",
        }
    )
    assert any(row[0] == "市场状态缺口" and "2" in row[1] for row in rows)
    assert any(row[0] == "市场数据健康" and "研究Fallback" in row[1] for row in rows)
