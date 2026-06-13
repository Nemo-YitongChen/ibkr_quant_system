from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from src.app.supervisor import Supervisor


def _write_config(path: Path, summary_dir: Path, seed_registry: Path) -> None:
    path.write_text(
        "\n".join(
            [
                'timezone: "Australia/Sydney"',
                f'summary_out_dir: "{summary_dir}"',
                "scope_summary_out_dir: false",
                "run_watchlist_expansion_review: true",
                "watchlist_expansion_only_when_all_markets_closed: false",
                "watchlist_expansion_interval_min: 180",
                "watchlist_expansion_timeout_sec: 90",
                "watchlist_expansion_account_equity: 1000",
                'watchlist_expansion_account_profile: "small"',
                f'watchlist_expansion_seed_source_registry: "{seed_registry}"',
                "markets: []",
            ]
        ),
        encoding="utf-8",
    )


def test_supervisor_runs_local_watchlist_expansion_review_with_scoped_outputs(tmp_path: Path) -> None:
    config_path = tmp_path / "supervisor.yaml"
    summary_dir = tmp_path / "reports_supervisor"
    seed_registry = tmp_path / "seed_sources.yaml"
    seed_registry.write_text("version: 1\nmarkets: {}\n", encoding="utf-8")
    _write_config(config_path, summary_dir, seed_registry)
    supervisor = Supervisor(str(config_path))

    with patch.object(supervisor, "_run_cmd", return_value=True) as run_cmd:
        ran = supervisor._run_watchlist_expansion_review(
            datetime(2026, 6, 14, 10, 0, tzinfo=timezone.utc)
        )

    assert ran is True
    task_name, command = run_cmd.call_args.args
    assert task_name == "expand_investment_watchlists:missing_artifact"
    assert "src.tools.expand_investment_watchlists" in command
    assert command[command.index("--analysis_dir") + 1] == str(summary_dir / "watchlist_expansion")
    assert command[command.index("--out_dir") + 1] == str(
        summary_dir / "watchlist_expansion" / "generated_watchlists"
    )
    assert command[command.index("--account_equity") + 1] == "1000.0"
    assert command[command.index("--account_profile") + 1] == "small"


def test_watchlist_expansion_review_reruns_when_seed_registry_is_newer(tmp_path: Path) -> None:
    config_path = tmp_path / "supervisor.yaml"
    summary_dir = tmp_path / "reports_supervisor"
    seed_registry = tmp_path / "seed_sources.yaml"
    seed_registry.write_text("version: 1\nmarkets: {}\n", encoding="utf-8")
    _write_config(config_path, summary_dir, seed_registry)
    marker = summary_dir / "watchlist_expansion" / "watchlist_expansion_summary.json"
    marker.parent.mkdir(parents=True)
    marker.write_text("{}", encoding="utf-8")
    marker.touch()
    marker_mtime = marker.stat().st_mtime
    seed_registry_mtime = max(seed_registry.stat().st_mtime, marker_mtime + 1.0)
    os.utime(seed_registry, (seed_registry_mtime, seed_registry_mtime))
    supervisor = Supervisor(str(config_path))

    due, reason = supervisor._watchlist_expansion_review_due(
        datetime.fromtimestamp(seed_registry_mtime + 10.0, tz=timezone.utc)
    )

    assert due is True
    assert reason == "dependency_newer_than_artifact"
