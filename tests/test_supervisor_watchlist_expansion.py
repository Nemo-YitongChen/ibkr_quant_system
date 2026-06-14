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
    assert command[command.index("--seed_evidence_root") + 1].endswith(
        "reports_investment_seed_review"
    )


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


def test_supervisor_runs_one_yfinance_only_seed_evidence_job(tmp_path: Path) -> None:
    summary_dir = tmp_path / "reports_supervisor"
    config_path = tmp_path / "supervisor.yaml"
    config_path.write_text(
        "\n".join(
            [
                'timezone: "Australia/Sydney"',
                f'summary_out_dir: "{summary_dir}"',
                "scope_summary_out_dir: false",
                "run_seed_candidate_evidence_review: true",
                "seed_candidate_evidence_only_when_all_markets_closed: false",
                "seed_candidate_evidence_max_symbols_per_run: 2",
                'seed_candidate_evidence_out_dir: "reports_investment_seed_review"',
                "markets:",
                '  - name: "asx"',
                '    market: "ASX"',
                "    enabled: true",
                "    reports:",
                '      - kind: "investment"',
                '        out_dir: "reports_investment_asx"',
                '        watchlist_yaml: "config/watchlists/asx_top_quality.yaml"',
                '        investment_config: "config/investment_asx.yaml"',
                '        ibkr_config: "config/ibkr_asx.yaml"',
            ]
        ),
        encoding="utf-8",
    )
    expansion_dir = summary_dir / "watchlist_expansion"
    review_path = expansion_dir / "seed_review" / "asx_preferred_asset_seed_review.yaml"
    review_path.parent.mkdir(parents=True)
    review_path.write_text("symbols: [DHHF.AX, BGBL.AX]\n", encoding="utf-8")
    (expansion_dir / "watchlist_expansion_summary.json").write_text(
        """
{
  "seed_evidence_queue": [
    {
      "market": "ASX",
      "status": "READY",
      "symbols": ["DHHF.AX", "BGBL.AX"],
      "evidence_mode": "YFINANCE_ONLY"
    }
  ]
}
""".strip(),
        encoding="utf-8",
    )
    supervisor = Supervisor(str(config_path))

    with patch.object(supervisor, "_run_cmd", return_value=True) as run_cmd:
        ran = supervisor._run_seed_candidate_evidence_review(
            datetime(2026, 6, 14, 10, 0, tzinfo=timezone.utc)
        )

    assert ran is True
    task_name, command = run_cmd.call_args.args
    assert task_name == "review_seed_candidate_evidence:asx:DHHF.AX,BGBL.AX"
    assert supervisor._is_ibkr_gateway_task(task_name) is False
    assert "--review_seed_only" in command
    assert command[command.index("--review_seed_symbols") + 1] == "DHHF.AX,BGBL.AX"
    assert command[command.index("--backtest_top_k") + 1] == "0"
    assert command[command.index("--fundamentals_top_k") + 1] == "0"
