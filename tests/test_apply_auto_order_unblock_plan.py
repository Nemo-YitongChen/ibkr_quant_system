from __future__ import annotations

import json
from pathlib import Path

import yaml

from src.tools.apply_auto_order_unblock_plan import build_auto_order_unblock_payload
from src.tools.apply_auto_order_unblock_plan import main as auto_order_unblock_main


def _write_supervisor_config(tmp_path: Path) -> Path:
    watchlist = tmp_path / "watchlist.yaml"
    watchlist.write_text("symbols:\n  - SCHX\n", encoding="utf-8")
    for name in ("investment_us.yaml", "investment_paper_us.yaml", "investment_execution_us.yaml"):
        (tmp_path / name).write_text("{}\n", encoding="utf-8")
    cfg = {
        "summary_out_dir": "reports_supervisor",
        "scope_summary_out_dir": True,
        "markets": [
            {
                "name": "us",
                "market": "US",
                "enabled": True,
                "reports": [
                    {
                        "kind": "investment",
                        "out_dir": "reports_investment",
                        "watchlist_yaml": str(watchlist),
                        "investment_config": str(tmp_path / "investment_us.yaml"),
                        "paper_config": str(tmp_path / "investment_paper_us.yaml"),
                        "execution_config": str(tmp_path / "investment_execution_us.yaml"),
                        "run_investment_execution": True,
                        "submit_investment_execution": True,
                        "request_timeout_sec": 15,
                        "execution_timeout_sec": 300,
                        "db": "audit.db",
                        "audit_limit": 500,
                        "max_universe": 1000,
                        "top_n": 15,
                    }
                ],
            }
        ],
    }
    cfg_path = tmp_path / "supervisor.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
    return cfg_path


def _write_readiness(path: Path, *, submit_orders: bool = False) -> None:
    path.write_text(
        json.dumps(
            {
                "summary": {
                    "unblock_plan": {
                        "status": "stale_execution_refresh_required",
                        "primary_action": "refresh_stale_execution_target_no_submit",
                        "phase": "targeted_stale_execution_refresh",
                        "target_market": "US",
                        "target_portfolio_id": "US:watchlist",
                        "target_symbols": "SCHX",
                        "requires_ibkr_gateway": True,
                        "request_policy": "one_stale_execution_portfolio_after_gateway_budget_ok",
                        "submit_orders": submit_orders,
                        "does_not_relax_submit_gates": True,
                    }
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def test_auto_order_unblock_plan_builds_no_submit_recovery_commands(tmp_path: Path) -> None:
    cfg_path = _write_supervisor_config(tmp_path)
    runtime_root = tmp_path / "runtime"
    readiness_path = tmp_path / "auto_order_readiness.json"
    _write_readiness(readiness_path)

    payload = build_auto_order_unblock_payload(
        config_path=str(cfg_path),
        runtime_root=str(runtime_root),
        readiness_path=str(readiness_path),
        out_dir=str(tmp_path / "out"),
        python_executable="/usr/bin/python3",
    )

    assert payload["status"] == "ready"
    assert payload["target_market"] == "US"
    assert payload["target_portfolio_id"] == "US:watchlist"
    assert payload["submit_orders"] is False
    commands = payload["commands"]
    assert [row["step"] for row in commands] == [
        "refresh_investment_report",
        "refresh_execution_no_submit",
        "refresh_market_readiness",
        "refresh_auto_order_readiness",
        "refresh_dashboard",
    ]
    execution = next(row for row in commands if row["step"] == "refresh_execution_no_submit")
    assert "--recovery_evidence_only" in execution["argv"]
    assert "--submit" not in execution["argv"]
    assert execution["submit_orders"] is False
    assert execution["requires_gateway"] is True


def test_auto_order_unblock_plan_blocks_submit_enabled_unblock_plan(tmp_path: Path) -> None:
    cfg_path = _write_supervisor_config(tmp_path)
    readiness_path = tmp_path / "auto_order_readiness.json"
    _write_readiness(readiness_path, submit_orders=True)

    payload = build_auto_order_unblock_payload(
        config_path=str(cfg_path),
        runtime_root=str(tmp_path / "runtime"),
        readiness_path=str(readiness_path),
        out_dir=str(tmp_path / "out"),
    )

    assert payload["status"] == "blocked"
    assert payload["reason"] == "unsafe_unblock_plan_submit_orders_true"
    assert payload["commands"] == []


def test_auto_order_unblock_cli_dry_run_writes_plan(tmp_path: Path) -> None:
    cfg_path = _write_supervisor_config(tmp_path)
    readiness_path = tmp_path / "auto_order_readiness.json"
    out_dir = tmp_path / "out"
    _write_readiness(readiness_path)

    auto_order_unblock_main(
        [
            "--config",
            str(cfg_path),
            "--runtime_root",
            str(tmp_path / "runtime"),
            "--readiness",
            str(readiness_path),
            "--out_dir",
            str(out_dir),
            "--python",
            "/usr/bin/python3",
        ]
    )

    payload = json.loads((out_dir / "auto_order_unblock_plan.json").read_text(encoding="utf-8"))
    assert payload["status"] == "ready"
    assert payload["apply_requested"] is False
    assert (out_dir / "auto_order_unblock_plan.md").exists()
