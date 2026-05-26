from src.common.investment_owner_progression import build_no_order_diagnostics
from src.portfolio.investment_allocator import InvestmentExecutionConfig


def test_no_order_diagnostics_flags_small_account_capital_config_block():
    cfg = InvestmentExecutionConfig(
        cash_buffer_floor=1000.0,
        min_cash_buffer_pct=0.05,
        min_trade_value=500.0,
        max_order_value_pct=0.08,
        account_allocation_pct=0.30,
        allow_fractional_qty=False,
    )
    payload = build_no_order_diagnostics(
        market="US",
        portfolio_id="US:small",
        report_dir="reports",
        submitted=False,
        broker_equity=1000.0,
        broker_cash=1000.0,
        target_equity=0.0,
        target_weights={"SPY": 0.25},
        candidate_rows=[{"symbol": "SPY"}],
        plan_rows=[{"symbol": "SPY"}],
        raw_order_rows=[],
        blocked_rows=[],
        order_rows=[],
        execution_cfg=cfg,
        account_profile={"name": "small"},
    )
    assert payload["primary_no_order_reason"] == "MAX_ORDER_VALUE_BELOW_MIN_TRADE"
    capital = {row["check"]: row for row in payload["capital_constraint_rows"]}
    assert capital["max_order_value_vs_min_trade"]["status"] == "BLOCKED"
    assert payload["progression_assessment"]["overall_status"] == "PAPER_BLOCKED"


def test_no_order_diagnostics_marks_fractional_paper_order_ready():
    cfg = InvestmentExecutionConfig(
        cash_buffer_floor=100.0,
        min_cash_buffer_pct=0.10,
        min_trade_value=25.0,
        max_order_value_pct=0.10,
        account_allocation_pct=0.25,
        allow_fractional_qty=True,
    )
    order = {
        "symbol": "SPY",
        "action": "BUY",
        "order_value": 75.0,
        "expected_edge_bps": 42.0,
        "edge_gate_threshold_bps": 30.0,
    }
    payload = build_no_order_diagnostics(
        market="US",
        portfolio_id="US:small",
        report_dir="reports",
        submitted=False,
        broker_equity=1000.0,
        broker_cash=1000.0,
        target_equity=250.0,
        target_weights={"SPY": 0.25},
        candidate_rows=[{"symbol": "SPY"}],
        plan_rows=[{"symbol": "SPY"}],
        raw_order_rows=[order],
        blocked_rows=[],
        order_rows=[order],
        execution_cfg=cfg,
        account_profile={"name": "small"},
    )
    assert payload["primary_no_order_reason"] == "ORDERS_PLANNED_NOT_SUBMITTED"
    assert payload["progression_assessment"]["overall_status"] == "PAPER_PLANNED"
    p1 = next(row for row in payload["progression_assessment"]["rows"] if row["step"] == "P1")
    assert p1["status"] == "PASS"


def test_no_order_diagnostics_summarizes_whole_share_paper_sample_readiness():
    cfg = InvestmentExecutionConfig(
        cash_buffer_floor=100.0,
        min_cash_buffer_pct=0.10,
        min_trade_value=25.0,
        max_order_value_pct=0.10,
        account_allocation_pct=0.25,
        allow_fractional_qty=False,
    )
    order = {
        "symbol": "SCHX",
        "action": "BUY",
        "order_value": 29.0,
        "expected_edge_bps": 28.5,
        "edge_gate_threshold_bps": 27.0,
        "reason": "rebalance_up_whole_share_preferred_override|shadow_ml_sample_collection",
        "shadow_review_status": "SAMPLE_COLLECTION",
        "whole_share_preferred_buy_override": True,
    }
    payload = build_no_order_diagnostics(
        market="US",
        portfolio_id="US:small",
        report_dir="reports",
        submitted=False,
        broker_equity=1000.0,
        broker_cash=1000.0,
        target_equity=250.0,
        target_weights={"SCHX": 0.25},
        candidate_rows=[{"symbol": "SCHX"}],
        plan_rows=[{"symbol": "SCHX"}],
        raw_order_rows=[order],
        blocked_rows=[],
        order_rows=[order],
        execution_cfg=cfg,
        account_profile={"name": "small"},
    )
    assert payload["primary_no_order_reason"] == "ORDERS_PLANNED_NOT_SUBMITTED"
    assert payload["paper_submit_ready"] is True
    assert payload["paper_submit_readiness_status"] == "READY"
    assert payload["planned_order_symbols"] == "SCHX"
    assert payload["whole_share_sample_collection_count"] == 1
    assert payload["whole_share_sample_collection_symbols"] == "SCHX"
    assert payload["whole_share_sample_collection_avg_edge_margin_bps"] == 1.5
    readiness = {row["check"]: row for row in payload["paper_submit_readiness_rows"]}
    assert readiness["paper_submit_state"]["status"] == "PASS"


def test_no_order_diagnostics_blocks_paper_submit_when_market_session_closed():
    cfg = InvestmentExecutionConfig(
        cash_buffer_floor=100.0,
        min_cash_buffer_pct=0.10,
        min_trade_value=25.0,
        max_order_value_pct=0.10,
        account_allocation_pct=0.25,
        allow_fractional_qty=False,
    )
    order = {
        "symbol": "SPLG",
        "action": "BUY",
        "order_value": 87.0,
        "expected_edge_bps": 29.0,
        "edge_gate_threshold_bps": 27.0,
        "status": "PLANNED",
    }
    payload = build_no_order_diagnostics(
        market="US",
        portfolio_id="US:small",
        report_dir="reports",
        submitted=False,
        broker_equity=1000.0,
        broker_cash=1000.0,
        target_equity=250.0,
        target_weights={"SPLG": 0.25},
        candidate_rows=[{"symbol": "SPLG"}],
        plan_rows=[{"symbol": "SPLG"}],
        raw_order_rows=[order],
        blocked_rows=[],
        order_rows=[order],
        execution_cfg=cfg,
        account_profile={"name": "small"},
        execution_session_bucket="CLOSED",
        execution_session_label="休市",
        market_open_for_submit=False,
    )
    assert payload["paper_submit_ready"] is False
    assert payload["paper_submit_readiness_status"] == "MARKET_CLOSED"
    assert payload["primary_no_order_reason"] == "MARKET_CLOSED_FOR_SUBMIT"
    assert payload["primary_action"] == "wait_for_regular_session_or_enable_overnight_config"
    assert payload["market_open_for_submit"] is False
    readiness = {row["check"]: row for row in payload["paper_submit_readiness_rows"]}
    assert readiness["market_open_for_submit"]["status"] == "BLOCKED"
    assert readiness["paper_submit_state"]["status"] == "BLOCKED"


def test_no_order_diagnostics_marks_submitted_broker_order_ready_state():
    cfg = InvestmentExecutionConfig(
        cash_buffer_floor=100.0,
        min_cash_buffer_pct=0.10,
        min_trade_value=25.0,
        max_order_value_pct=0.10,
        account_allocation_pct=0.25,
        allow_fractional_qty=False,
    )
    order = {
        "symbol": "SCHX",
        "action": "BUY",
        "order_value": 29.0,
        "expected_edge_bps": 28.5,
        "edge_gate_threshold_bps": 27.0,
        "status": "PreSubmitted",
        "broker_order_id": 12345,
    }
    payload = build_no_order_diagnostics(
        market="US",
        portfolio_id="US:small",
        report_dir="reports",
        submitted=True,
        broker_equity=1000.0,
        broker_cash=1000.0,
        target_equity=250.0,
        target_weights={"SCHX": 0.25},
        candidate_rows=[{"symbol": "SCHX"}],
        plan_rows=[{"symbol": "SCHX"}],
        raw_order_rows=[order],
        blocked_rows=[],
        order_rows=[order],
        execution_cfg=cfg,
        account_profile={"name": "small"},
    )
    assert payload["submitted_order_count"] == 1
    assert payload["paper_submit_readiness_status"] == "SUBMITTED"
    assert payload["primary_no_order_reason"] == "PAPER_ORDERS_SUBMITTED"


def test_no_order_diagnostics_flags_submit_without_broker_ack():
    cfg = InvestmentExecutionConfig(
        cash_buffer_floor=100.0,
        min_cash_buffer_pct=0.10,
        min_trade_value=25.0,
        max_order_value_pct=0.10,
        account_allocation_pct=0.25,
        allow_fractional_qty=False,
    )
    order = {
        "symbol": "SCHX",
        "action": "BUY",
        "order_value": 29.0,
        "expected_edge_bps": 28.5,
        "edge_gate_threshold_bps": 27.0,
        "status": "PLANNED",
    }
    payload = build_no_order_diagnostics(
        market="US",
        portfolio_id="US:small",
        report_dir="reports",
        submitted=True,
        broker_equity=1000.0,
        broker_cash=1000.0,
        target_equity=250.0,
        target_weights={"SCHX": 0.25},
        candidate_rows=[{"symbol": "SCHX"}],
        plan_rows=[{"symbol": "SCHX"}],
        raw_order_rows=[order],
        blocked_rows=[],
        order_rows=[order],
        execution_cfg=cfg,
        account_profile={"name": "small"},
    )
    assert payload["submitted_order_count"] == 0
    assert payload["paper_submit_readiness_status"] == "NEEDS_REVIEW"
    assert payload["primary_no_order_reason"] == "SUBMIT_REQUESTED_NO_BROKER_ACK"


def test_no_order_diagnostics_marks_partial_broker_ack_when_one_order_rejected():
    cfg = InvestmentExecutionConfig(
        cash_buffer_floor=100.0,
        min_cash_buffer_pct=0.10,
        min_trade_value=25.0,
        max_order_value_pct=0.10,
        account_allocation_pct=0.25,
        allow_fractional_qty=False,
    )
    orders = [
        {
            "symbol": "SCHX",
            "action": "BUY",
            "order_value": 29.0,
            "expected_edge_bps": 28.5,
            "edge_gate_threshold_bps": 27.0,
            "status": "PreSubmitted",
            "broker_order_id": 41,
        },
        {
            "symbol": "SPLG",
            "action": "BUY",
            "order_value": 87.0,
            "expected_edge_bps": 29.0,
            "edge_gate_threshold_bps": 27.0,
            "status": "ERROR_200",
            "broker_order_id": 43,
        },
    ]
    payload = build_no_order_diagnostics(
        market="US",
        portfolio_id="US:small",
        report_dir="reports",
        submitted=True,
        broker_equity=1000.0,
        broker_cash=1000.0,
        target_equity=250.0,
        target_weights={"SCHX": 0.125, "SPLG": 0.125},
        candidate_rows=[{"symbol": "SCHX"}, {"symbol": "SPLG"}],
        plan_rows=[{"symbol": "SCHX"}, {"symbol": "SPLG"}],
        raw_order_rows=orders,
        blocked_rows=[],
        order_rows=orders,
        execution_cfg=cfg,
        account_profile={"name": "small"},
    )
    assert payload["submitted_order_count"] == 1
    assert payload["error_order_count"] == 1
    assert payload["primary_no_order_reason"] == "PAPER_ORDERS_PARTIAL_BROKER_ACK"


def test_no_order_diagnostics_treats_pending_broker_order_as_duplicate_prevention_not_hard_block():
    cfg = InvestmentExecutionConfig(
        cash_buffer_floor=100.0,
        min_cash_buffer_pct=0.10,
        min_trade_value=25.0,
        max_order_value_pct=0.10,
        account_allocation_pct=0.25,
        allow_fractional_qty=False,
    )
    planned = {
        "symbol": "SPLG",
        "action": "BUY",
        "order_value": 87.0,
        "expected_edge_bps": 29.0,
        "edge_gate_threshold_bps": 27.0,
        "status": "PLANNED",
    }
    blocked = {
        "symbol": "SCHX",
        "action": "BUY",
        "order_value": 29.0,
        "status": "BLOCKED_PENDING_BROKER_ORDER",
        "reason": "shadow_ml_sample_collection|pending_broker_order",
        "pending_broker_order_id": 41,
        "pending_broker_order_status": "PreSubmitted",
    }
    payload = build_no_order_diagnostics(
        market="US",
        portfolio_id="US:small",
        report_dir="reports",
        submitted=False,
        broker_equity=1000.0,
        broker_cash=1000.0,
        target_equity=250.0,
        target_weights={"SCHX": 0.125, "SPLG": 0.125},
        candidate_rows=[{"symbol": "SCHX"}, {"symbol": "SPLG"}],
        plan_rows=[{"symbol": "SCHX"}, {"symbol": "SPLG"}],
        raw_order_rows=[planned, blocked],
        blocked_rows=[blocked],
        order_rows=[planned],
        execution_cfg=cfg,
        account_profile={"name": "small"},
    )
    assert payload["blocked_order_count"] == 1
    assert payload["submit_blocking_order_count"] == 0
    assert payload["paper_submit_ready"] is True
    assert payload["paper_submit_readiness_status"] == "READY"
    assert payload["primary_no_order_reason"] == "ORDERS_PLANNED_NOT_SUBMITTED"
    assert payload["blocking_reason_rows"][0]["reason_key"] == "BLOCKED_PENDING_BROKER_ORDER"


def test_no_order_diagnostics_groups_top_blocking_reason():
    cfg = InvestmentExecutionConfig(
        cash_buffer_floor=100.0,
        min_trade_value=25.0,
        max_order_value_pct=0.10,
        account_allocation_pct=0.25,
        allow_fractional_qty=True,
    )
    blocked = [
        {"symbol": "AAA", "status": "BLOCKED_MARKET_RULE", "market_rule_status": "BLOCKED_RESEARCH_ONLY"},
        {"symbol": "BBB", "edge_gate_status": "BLOCKED", "reason": "edge_gate"},
        {"symbol": "CCC", "edge_gate_status": "BLOCKED", "reason": "edge_gate"},
    ]
    payload = build_no_order_diagnostics(
        market="US",
        portfolio_id="US:small",
        report_dir="reports",
        submitted=False,
        broker_equity=1000.0,
        broker_cash=1000.0,
        target_equity=250.0,
        target_weights={"AAA": 0.25},
        candidate_rows=[{"symbol": "AAA"}],
        plan_rows=[{"symbol": "AAA"}],
        raw_order_rows=blocked,
        blocked_rows=blocked,
        order_rows=[],
        execution_cfg=cfg,
        account_profile={"name": "small"},
    )
    assert payload["primary_no_order_reason"] == "BLOCKED_EDGE"
    assert payload["blocking_reason_rows"][0]["reason_key"] == "BLOCKED_EDGE"
    assert payload["blocking_reason_rows"][0]["count"] == 2
