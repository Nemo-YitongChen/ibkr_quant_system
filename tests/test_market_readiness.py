from __future__ import annotations

import json
from pathlib import Path

from src.common.market_readiness import build_market_readiness_payload
from src.tools import review_market_readiness


REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_execution_plan(path: Path, row: dict) -> None:
    fields = list(row.keys())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        ",".join(fields) + "\n" + ",".join(str(row.get(field, "")) for field in fields) + "\n",
        encoding="utf-8",
    )


def test_market_readiness_classifies_ready_closed_and_research_only(tmp_path: Path) -> None:
    reports_root = tmp_path / "reports"
    us_dir = reports_root / "watchlist"
    hk_dir = reports_root / "resolved_hk_top100_bluechip"
    _write_json(
        us_dir / "investment_execution_summary.json",
        {
            "market": "US",
            "portfolio_id": "US:watchlist",
            "paper_submit_ready": True,
            "paper_submit_readiness_status": "READY",
            "primary_no_order_reason": "ORDERS_PLANNED_NOT_SUBMITTED",
            "order_count": 1,
            "planned_gross_order_value": 87.0,
            "planned_buy_order_value": 87.0,
            "broker_equity": 1000.0,
            "broker_cash": 900.0,
        },
    )
    _write_execution_plan(
        us_dir / "investment_execution_plan.csv",
        {
            "symbol": "SPLG",
            "action": "BUY",
            "status": "PLANNED",
            "expected_edge_bps": 34.0,
            "expected_cost_bps": 22.0,
            "edge_gate_threshold_bps": 28.0,
            "whole_share_edge_margin_bps": 6.0,
            "dynamic_order_adv_pct": 0.0001,
            "execution_order_type": "LMT",
            "edge_gate_status": "PASS",
            "quality_status": "QUALITY_OK",
            "market_rule_status": "RULES_OK",
            "shadow_review_status": "AUTO_OK",
            "manual_review_status": "AUTO_OK",
        },
    )
    _write_json(
        hk_dir / "investment_execution_summary.json",
        {
            "market": "HK",
            "portfolio_id": "HK:resolved_hk_top100_bluechip",
            "paper_submit_ready": False,
            "paper_submit_readiness_status": "MARKET_CLOSED",
            "primary_no_order_reason": "MARKET_CLOSED_FOR_SUBMIT",
            "order_count": 2,
            "planned_gross_order_value": 600.0,
            "broker_equity": 1000.0,
        },
    )
    supervisor_cfg = {
        "markets": [
            {
                "name": "us",
                "market": "US",
                "enabled": True,
                "reports": [
                    {
                        "kind": "investment",
                        "out_dir": str(reports_root),
                        "watchlist_yaml": "config/watchlist.yaml",
                        "run_investment_execution": True,
                        "submit_investment_execution": True,
                    }
                ],
            },
            {
                "name": "hk",
                "market": "HK",
                "enabled": True,
                "reports": [
                    {
                        "kind": "investment",
                        "out_dir": str(reports_root),
                        "watchlist_yaml": "config/watchlists/resolved_hk_top100_bluechip.yaml",
                        "run_investment_execution": True,
                        "submit_investment_execution": True,
                    }
                ],
            },
            {
                "name": "cn",
                "market": "CN",
                "enabled": True,
                "reports": [
                    {
                        "kind": "investment",
                        "research_only": True,
                        "out_dir": str(reports_root),
                        "watchlist_yaml": "config/watchlists/cn_top_quality.yaml",
                        "run_investment_execution": False,
                    }
                ],
            },
        ]
    }

    payload = build_market_readiness_payload(
        base_dir=REPO_ROOT,
        supervisor_config=supervisor_cfg,
        config_path=tmp_path / "supervisor.yaml",
        runtime_root=tmp_path / "runtime",
    )
    rows = {row["market"]: row for row in payload["rows"]}
    assert rows["US"]["readiness_status"] == "READY_FOR_PAPER_REVIEW"
    assert rows["US"]["planned_buy_order_value"] == 87.0
    assert rows["US"]["account_profile_name"] == "small"
    assert rows["US"]["small_account_feasibility_status"] == "CONFIG_TRADABLE"
    assert rows["US"]["submit_quality_status"] == "PASS"
    assert rows["US"]["submit_quality_tier"] == "PASS"
    assert rows["US"]["submit_quality_min_net_edge_bps"] == 12.0
    assert rows["US"]["effective_max_order_value"] == 100.0
    assert rows["US"]["effective_min_trade_value"] == 25.0
    assert rows["HK"]["readiness_status"] == "PLANNED_MARKET_CLOSED"
    assert rows["HK"]["small_account_feasibility_status"] == "CONFIG_REVIEW_FEE_LOT_FRICTION"
    assert rows["HK"]["next_action"] == "rerun no-submit during local regular session before any submit"
    assert rows["CN"]["readiness_status"] == "RESEARCH_ONLY"
    assert payload["summary"]["ready_or_submitted_count"] == 1
    assert payload["preparation_plan"][0]["market"] == "US"
    assert payload["preparation_plan"][0]["priority_tier"] == "REVIEW_FOR_PAPER"


def test_market_readiness_blocks_submit_quality_when_post_cost_edge_is_too_low(tmp_path: Path) -> None:
    reports_root = tmp_path / "reports"
    us_dir = reports_root / "watchlist"
    _write_json(
        us_dir / "investment_execution_summary.json",
        {
            "market": "US",
            "portfolio_id": "US:watchlist",
            "paper_submit_ready": True,
            "paper_submit_readiness_status": "READY",
            "primary_no_order_reason": "ORDERS_PLANNED_NOT_SUBMITTED",
            "order_count": 1,
            "planned_gross_order_value": 87.0,
            "planned_buy_order_value": 87.0,
            "broker_equity": 1000.0,
        },
    )
    _write_execution_plan(
        us_dir / "investment_execution_plan.csv",
        {
            "symbol": "SPLG",
            "action": "BUY",
            "status": "PLANNED",
            "expected_edge_bps": 29.0,
            "expected_cost_bps": 24.0,
            "edge_gate_threshold_bps": 28.0,
            "whole_share_edge_margin_bps": 1.0,
            "dynamic_order_adv_pct": 0.0001,
            "execution_order_type": "LMT",
            "edge_gate_status": "PASS",
            "quality_status": "QUALITY_OK",
            "market_rule_status": "RULES_OK",
            "shadow_review_status": "AUTO_OK",
            "manual_review_status": "AUTO_OK",
        },
    )

    payload = build_market_readiness_payload(
        base_dir=REPO_ROOT,
        supervisor_config={
            "auto_order_readiness": {
                "min_submit_net_edge_bps": 8.0,
                "min_submit_edge_margin_bps": 3.0,
            },
            "markets": [
                {
                    "name": "us",
                    "market": "US",
                    "enabled": True,
                    "reports": [
                        {
                            "kind": "investment",
                            "out_dir": str(reports_root),
                            "watchlist_yaml": "config/watchlist.yaml",
                            "run_investment_execution": True,
                            "submit_investment_execution": True,
                        }
                    ],
                }
            ],
        },
        config_path=tmp_path / "supervisor.yaml",
        runtime_root=tmp_path / "runtime",
    )

    row = payload["rows"][0]
    assert row["readiness_status"] == "READY_FOR_PAPER_REVIEW"
    assert row["submit_quality_status"] == "BLOCKED"
    assert row["submit_quality_reason"] == "net_edge_below_min,edge_margin_below_min"
    assert row["submit_quality_tier"] == "NONE"


def test_market_readiness_submit_quality_ignores_exit_only_orders(tmp_path: Path) -> None:
    reports_root = tmp_path / "reports"
    hk_dir = reports_root / "resolved_hk_top100_bluechip"
    _write_json(
        hk_dir / "investment_execution_summary.json",
        {
            "market": "HK",
            "portfolio_id": "HK:resolved_hk_top100_bluechip",
            "paper_submit_ready": False,
            "paper_submit_readiness_status": "MARKET_CLOSED",
            "primary_no_order_reason": "MARKET_CLOSED_FOR_SUBMIT",
            "order_count": 1,
            "planned_gross_order_value": 30.0,
            "planned_sell_order_value": 30.0,
            "broker_equity": 1000.0,
        },
    )
    _write_execution_plan(
        hk_dir / "investment_execution_plan.csv",
        {
            "symbol": "SCHX.HK",
            "action": "SELL",
            "status": "PLANNED",
            "reason": "rebalance_exit|no_submit_closed",
            "expected_edge_bps": 0.0,
            "expected_cost_bps": 0.0,
            "edge_gate_threshold_bps": 55.0,
            "dynamic_order_adv_pct": 0.0001,
            "execution_order_type": "LMT",
            "edge_gate_status": "",
            "quality_status": "",
            "market_rule_status": "RULES_OK",
            "shadow_review_status": "",
            "manual_review_status": "AUTO_OK",
        },
    )

    payload = build_market_readiness_payload(
        base_dir=REPO_ROOT,
        supervisor_config={
            "markets": [
                {
                    "name": "hk",
                    "market": "HK",
                    "enabled": True,
                    "reports": [
                        {
                            "kind": "investment",
                            "out_dir": str(reports_root),
                            "watchlist_yaml": "config/watchlists/resolved_hk_top100_bluechip.yaml",
                            "run_investment_execution": True,
                            "submit_investment_execution": True,
                        }
                    ],
                }
            ],
        },
        config_path=tmp_path / "supervisor.yaml",
        runtime_root=tmp_path / "runtime",
    )

    row = payload["rows"][0]
    assert row["submit_quality_status"] == "NO_BUY_ORDERS"
    assert row["submit_quality_reason"] == "no_planned_buy_orders"
    assert row["submit_quality_order_count"] == 0
    assert row["submit_quality_buy_order_count"] == 0
    assert row["submit_quality_non_buy_order_count"] == 1


def test_market_readiness_marks_high_quality_submit_tier(tmp_path: Path) -> None:
    reports_root = tmp_path / "reports"
    us_dir = reports_root / "watchlist"
    _write_json(
        us_dir / "investment_execution_summary.json",
        {
            "market": "US",
            "portfolio_id": "US:watchlist",
            "paper_submit_ready": True,
            "paper_submit_readiness_status": "READY",
            "primary_no_order_reason": "ORDERS_PLANNED_NOT_SUBMITTED",
            "order_count": 1,
            "planned_gross_order_value": 87.0,
            "planned_buy_order_value": 87.0,
            "broker_equity": 1000.0,
        },
    )
    _write_execution_plan(
        us_dir / "investment_execution_plan.csv",
        {
            "symbol": "SPLG",
            "action": "BUY",
            "status": "PLANNED",
            "expected_edge_bps": 45.0,
            "expected_cost_bps": 18.0,
            "edge_gate_threshold_bps": 35.0,
            "whole_share_edge_margin_bps": 10.0,
            "dynamic_order_adv_pct": 0.0001,
            "execution_order_type": "LMT",
            "edge_gate_status": "PASS",
            "quality_status": "QUALITY_OK",
            "market_rule_status": "RULES_OK",
            "shadow_review_status": "AUTO_OK",
            "manual_review_status": "AUTO_OK",
        },
    )

    payload = build_market_readiness_payload(
        base_dir=REPO_ROOT,
        supervisor_config={
            "auto_order_readiness": {
                "high_quality_min_net_edge_bps": 20.0,
                "high_quality_min_edge_margin_bps": 8.0,
                "high_quality_max_expected_cost_bps": 20.0,
            },
            "markets": [
                {
                    "name": "us",
                    "market": "US",
                    "enabled": True,
                    "reports": [
                        {
                            "kind": "investment",
                            "out_dir": str(reports_root),
                            "watchlist_yaml": "config/watchlist.yaml",
                            "run_investment_execution": True,
                            "submit_investment_execution": True,
                        }
                    ],
                }
            ],
        },
        config_path=tmp_path / "supervisor.yaml",
        runtime_root=tmp_path / "runtime",
    )

    row = payload["rows"][0]
    assert row["submit_quality_status"] == "PASS"
    assert row["submit_quality_tier"] == "HIGH"
    assert row["submit_quality_min_net_edge_bps"] == 27.0


def test_market_readiness_infers_legacy_blocked_reason_from_block_counts(tmp_path: Path) -> None:
    reports_root = tmp_path / "reports"
    asx_dir = reports_root / "asx_top_quality"
    _write_json(
        asx_dir / "investment_execution_summary.json",
        {
            "market": "ASX",
            "portfolio_id": "ASX:asx_top_quality",
            "order_count": 0,
            "blocked_order_count": 7,
            "blocked_edge_order_count": 2,
            "blocked_opportunity_order_count": 5,
            "broker_equity": 1000.0,
        },
    )
    payload = build_market_readiness_payload(
        base_dir=REPO_ROOT,
        supervisor_config={
            "markets": [
                {
                    "name": "asx",
                    "market": "ASX",
                    "enabled": True,
                    "reports": [
                        {
                            "kind": "investment",
                            "out_dir": str(reports_root),
                            "watchlist_yaml": "config/watchlists/asx_top_quality.yaml",
                            "run_investment_execution": True,
                            "submit_investment_execution": True,
                        }
                    ],
                }
            ]
        },
        config_path=tmp_path / "supervisor.yaml",
        runtime_root=tmp_path / "runtime",
    )
    row = payload["rows"][0]
    assert row["readiness_status"] == "BLOCKED"
    assert row["primary_reason"] == "BLOCKED_OPPORTUNITY"
    assert row["next_action"] == "rerun opportunity scan after event/window clears; do not bypass event gate"


def test_market_readiness_marks_static_small_account_config_blocked(tmp_path: Path) -> None:
    reports_root = tmp_path / "reports"
    execution_cfg = tmp_path / "execution_asx.yaml"
    ibkr_cfg = tmp_path / "ibkr_asx.yaml"
    execution_cfg.write_text(
        "\n".join(
            [
                "execution:",
                "  account_equity_cap: 1000",
                "  cash_buffer_floor: 1200",
                "  min_cash_buffer_pct: 0.06",
                "  min_trade_value: 600",
                "  max_order_value_pct: 0.06",
                "  account_allocation_pct: 0.30",
            ]
        ),
        encoding="utf-8",
    )
    ibkr_cfg.write_text(f"mode: paper\naccount_profile_config: {tmp_path / 'missing_profiles.yaml'}\n", encoding="utf-8")
    _write_json(
        reports_root / "asx_top_quality" / "investment_execution_summary.json",
        {
            "market": "ASX",
            "portfolio_id": "ASX:asx_top_quality",
            "paper_submit_ready": False,
            "primary_no_order_reason": "BLOCKED_OPPORTUNITY",
            "broker_equity": 1000.0,
        },
    )

    payload = build_market_readiness_payload(
        base_dir=REPO_ROOT,
        supervisor_config={
            "markets": [
                {
                    "name": "asx",
                    "market": "ASX",
                    "enabled": True,
                    "reports": [
                        {
                            "kind": "investment",
                            "out_dir": str(reports_root),
                            "watchlist_yaml": "config/watchlists/asx_top_quality.yaml",
                            "ibkr_config": str(ibkr_cfg),
                            "execution_config": str(execution_cfg),
                            "run_investment_execution": True,
                            "submit_investment_execution": True,
                        }
                    ],
                }
            ]
        },
        config_path=tmp_path / "supervisor.yaml",
        runtime_root=tmp_path / "runtime",
    )

    row = payload["rows"][0]
    assert row["readiness_status"] == "CONFIG_BLOCKED"
    assert row["primary_reason"] == "CONFIG_BLOCKED_CASH_BUFFER"
    assert row["small_account_feasibility_reason"] == (
        "cash_buffer_exhausts_effective_equity,min_trade_value_exceeds_max_order_value,"
        "investable_equity_below_min_trade_value"
    )
    assert payload["preparation_plan"][0]["priority_tier"] == "FIX_CONFIG_FIRST"
    assert payload["summary"]["blocked_or_missing_count"] == 1


def test_market_readiness_cli_writes_json_csv_and_markdown(tmp_path: Path, capsys) -> None:
    reports_root = tmp_path / "reports"
    _write_json(
        reports_root / "watchlist" / "investment_execution_summary.json",
        {
            "market": "US",
            "portfolio_id": "US:watchlist",
            "paper_submit_ready": True,
            "paper_submit_readiness_status": "READY",
            "primary_no_order_reason": "ORDERS_PLANNED_NOT_SUBMITTED",
            "order_count": 1,
        },
    )
    cfg_path = tmp_path / "supervisor.yaml"
    out_dir = tmp_path / "out"
    cfg_path.write_text(
        "\n".join(
            [
                f"summary_out_dir: {out_dir}",
                "markets:",
                "  - name: us",
                "    market: US",
                "    enabled: true",
                "    reports:",
                "      - kind: investment",
                f"        out_dir: {reports_root}",
                "        watchlist_yaml: config/watchlist.yaml",
                "        run_investment_execution: true",
                "        submit_investment_execution: true",
            ]
        ),
        encoding="utf-8",
    )

    review_market_readiness.main(["--config", str(cfg_path), "--runtime_root", str(tmp_path / "runtime")])

    stdout = capsys.readouterr().out
    assert "market readiness review complete" in stdout
    assert (out_dir / "market_readiness.json").exists()
    assert (out_dir / "market_readiness.csv").exists()
    assert (out_dir / "market_readiness.md").exists()
