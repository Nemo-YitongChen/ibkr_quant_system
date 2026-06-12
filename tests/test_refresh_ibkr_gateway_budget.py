from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from src.common.auto_order_recovery_state import (
    build_pending_recovery_checkpoint,
    mark_recovery_checkpoint_attempt,
    mark_recovery_checkpoint_complete,
    recovery_checkpoint_context,
)
from src.common.ibkr_telemetry import record_ibkr_request
from src.tools.refresh_ibkr_gateway_budget import refresh_gateway_budget_artifacts


def test_refresh_gateway_budget_artifacts_uses_local_telemetry_only(tmp_path: Path):
    now = datetime(2026, 6, 12, 10, 0, tzinfo=timezone.utc)
    telemetry_dir = tmp_path / "telemetry"
    out_dir = tmp_path / "weekly"
    config_path = tmp_path / "supervisor.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "ibkr_gateway_budgets": {
                    "enabled": True,
                    "over_budget_degraded_ratio": 1.5,
                    "markets": {"US": {"weekly_gateway_request_budget": 2}},
                }
            }
        ),
        encoding="utf-8",
    )
    record_ibkr_request(
        "positions",
        market="US",
        tool="test",
        quantity=3,
        ts=now - timedelta(hours=1),
        directory=telemetry_dir,
    )

    payload = refresh_gateway_budget_artifacts(
        out_dir=out_dir,
        supervisor_config=str(config_path),
        telemetry_directory=telemetry_dir,
        days=7,
        now=now,
    )

    assert payload["summary"]["status"] == "degraded"
    assert payload["summary"]["gateway_request_count"] == 3
    assert payload["rows"][0]["market"] == "US"
    assert (out_dir / "weekly_ibkr_gateway_budget_status.json").exists()
    assert (out_dir / "weekly_ibkr_request_summary.json").exists()
    assert not list(out_dir.glob("*.tmp"))


def test_refresh_gateway_budget_rolls_old_requests_out_of_window(tmp_path: Path):
    event_time = datetime(2026, 6, 1, 10, 0, tzinfo=timezone.utc)
    telemetry_dir = tmp_path / "telemetry"
    config_path = tmp_path / "supervisor.yaml"
    config_path.write_text("ibkr_gateway_budgets:\n  enabled: true\n", encoding="utf-8")
    record_ibkr_request(
        "historical_daily",
        market="US",
        tool="test",
        quantity=100,
        ts=event_time,
        directory=telemetry_dir,
    )

    payload = refresh_gateway_budget_artifacts(
        out_dir=tmp_path / "weekly",
        supervisor_config=str(config_path),
        telemetry_directory=telemetry_dir,
        days=7,
        now=event_time + timedelta(days=8),
    )

    assert payload["summary"]["gateway_request_count"] == 0
    assert payload["rows"][0]["reason"] == "missing_ibkr_request_telemetry"


def test_recovery_checkpoint_context_persists_retry_and_completion():
    now = datetime(2026, 6, 14, 0, 0, tzinfo=timezone.utc)
    checkpoint = build_pending_recovery_checkpoint(
        {
            "target_market": "US",
            "target_portfolio_id": "US:watchlist",
            "target_symbols": "SPLG",
            "target_submit_quality_status": "PASS",
        },
        now=now,
        retry_interval_min=60,
    )

    initial = recovery_checkpoint_context(
        checkpoint,
        now=now,
        report_refreshed=False,
        execution_refreshed=False,
    )
    assert initial["eligibility"]["eligible"] is True
    assert initial["eligibility"]["force_target_refresh"] is True

    attempted = mark_recovery_checkpoint_attempt(checkpoint, now=now)
    cooling_down = recovery_checkpoint_context(
        attempted,
        now=now + timedelta(minutes=30),
        report_refreshed=True,
        execution_refreshed=False,
    )
    assert cooling_down["eligibility"]["eligible"] is False
    assert cooling_down["eligibility"]["reason"] == "recovery_retry_cooldown"

    completed = mark_recovery_checkpoint_complete(
        attempted,
        now=now + timedelta(minutes=31),
    )
    assert completed["status"] == "COMPLETE"
    assert recovery_checkpoint_context(
        completed,
        now=now + timedelta(minutes=31),
        report_refreshed=True,
        execution_refreshed=True,
    ) == {}
