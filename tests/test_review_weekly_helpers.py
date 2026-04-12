from pathlib import Path

from src.tools.review_investment_weekly import _weekly_strategy_note
from src.tools.review_weekly_io import read_csv_rows
from src.tools.review_weekly_markdown import write_weekly_review_markdown
from src.tools.review_weekly_thresholds import (
    build_feedback_threshold_suggestion_rows,
    load_feedback_threshold_overrides,
)


def test_load_feedback_threshold_overrides_normalizes_market_keys(tmp_path: Path):
    path = tmp_path / "thresholds.yaml"
    path.write_text(
        "markets:\n  us:\n    execution:\n      auto_confidence: 0.7\n",
        encoding="utf-8",
    )
    overrides = load_feedback_threshold_overrides(path)
    assert overrides == {"US": {"execution": {"auto_confidence": 0.7}}}


def test_build_feedback_threshold_suggestion_rows_relaxes_on_consistent_improvement():
    rows = build_feedback_threshold_suggestion_rows(
        [
            {
                "market": "US",
                "feedback_kind": "execution",
                "feedback_kind_label": "执行参数",
                "summary_signal": "持续改善",
                "tracked_count": 2,
                "avg_active_weeks": 3.0,
                "latest_improved_count": 1,
                "latest_deteriorated_count": 0,
                "w1_improved_count": 1,
                "w2_improved_count": 1,
                "w4_improved_count": 0,
                "w1_deteriorated_count": 0,
                "w2_deteriorated_count": 0,
                "w4_deteriorated_count": 0,
                "top_portfolios_text": "US:watchlist: 改善",
            }
        ]
    )
    assert rows[0]["suggestion_action"] == "RELAX_AUTO_APPLY"


def test_read_csv_rows_and_markdown_writer(tmp_path: Path):
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("symbol,score\nAAPL,1\n", encoding="utf-8")
    rows = read_csv_rows(csv_path)
    assert rows == [{"symbol": "AAPL", "score": "1"}]

    out_path = tmp_path / "weekly_review.md"
    write_weekly_review_markdown(
        out_path,
        summary_rows=[
            {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "weekly_return": 0.01,
                "max_drawdown": -0.02,
                "executed_rebalances": 1,
                "turnover": 0.12,
                "latest_equity": 100000.0,
                "cash_after": 10000.0,
                "holdings_count": 5,
                "account_profile_label": "小资金",
                "account_profile_summary": "先做 ETF 和高流动性基础工具，减少小额高频调仓。",
                "market_rules_summary": "settlement=T+1 | buy_lot=1 | ETF-first below 25000.00",
                "adaptive_strategy_name": "ACM-RS",
                "adaptive_strategy_summary": "ACM-RS | RS=126/63/20 | rebalance=weekly | entry_delay=15-30m",
                "weekly_strategy_note": "本周有 2 个新开仓机会因防守环境被降级为观察，先不把回撤信号直接转成加仓动作。",
            }
        ],
        trade_rows=[],
        broker_summary_rows=[],
        broker_diff_rows=[],
        reason_rows=[],
        shadow_summary_rows=[],
        shadow_feedback_rows=[],
        feedback_calibration_rows=[],
        feedback_automation_rows=[],
        feedback_effect_market_summary_rows=[],
        feedback_threshold_suggestion_rows=[],
        feedback_threshold_history_overview_rows=[],
        feedback_threshold_effect_overview_rows=[],
        feedback_threshold_cohort_overview_rows=[],
        feedback_threshold_trial_alert_rows=[],
        feedback_threshold_tuning_rows=[],
        labeling_summary={},
        labeling_skip_rows=[],
        attribution_rows=[],
        risk_review_rows=[],
        risk_feedback_rows=[],
        execution_session_rows=[],
        execution_hotspot_rows=[],
        execution_feedback_rows=[],
        window_label="2026-W13",
    )
    text = out_path.read_text(encoding="utf-8")
    assert "# Weekly Investment Review" in text
    assert "## Broker Execution Summary" in text
    assert "账户档位: 小资金" in text
    assert "市场约束: settlement=T+1" in text
    assert "策略框架: ACM-RS" in text
    assert "周度解释: 本周有 2 个新开仓机会因防守环境被降级为观察" in text


def test_weekly_strategy_note_prefers_defensive_cap_message() -> None:
    note = _weekly_strategy_note(
        market_rules={"research_only": False, "small_account_rule_active": True, "small_account_preferred_asset_classes": ["etf"]},
        account_profile={"label": "小资金", "summary": "先做 ETF"},
        adaptive_strategy={"name": "ACM-RS"},
        opportunity_summary={"adaptive_strategy_wait_count": 2},
        market_sentiment={"label": "DEFENSIVE"},
    )
    assert "2 个新开仓机会" in note
