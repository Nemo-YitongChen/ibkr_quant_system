from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from src.tools.preflight_supervisor import _probe_port, run_preflight


class PreflightSupervisorTests(unittest.TestCase):
    @patch("src.tools.preflight_supervisor.subprocess.run")
    @patch("src.tools.preflight_supervisor.socket.socket")
    def test_probe_port_falls_back_to_lsof_when_socket_probe_is_blocked(self, mock_socket_cls, mock_run):
        sock = Mock()
        sock.connect_ex.return_value = 1
        mock_socket_cls.return_value = sock
        mock_run.return_value = Mock(
            returncode=0,
            stdout="COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\nJavaAppli 1 nemo 1u IPv6 0 0 TCP *:4002 (LISTEN)\n",
        )

        self.assertTrue(_probe_port("127.0.0.1", 4002))
        sock.close.assert_called_once()

    def test_run_preflight_writes_summary_and_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            runtime_root = base / "runtime_data" / "paper_test"
            runtime_root.mkdir(parents=True, exist_ok=True)
            (runtime_root / "audit.db").write_text("", encoding="utf-8")
            watchlist_path = base / "watchlist.yaml"
            watchlist_path.write_text("symbols: [AAPL]\n", encoding="utf-8")
            out_dir = base / "reports_preflight"
            cfg_path.write_text(
                "\n".join(
                    [
                        'summary_out_dir: "reports_supervisor"',
                        f'dashboard_weekly_review_dir: "{base / "reports_investment_weekly"}"',
                        f'dashboard_execution_kpi_dir: "{base / "reports_investment_execution"}"',
                        'dashboard_db: "audit.db"',
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    reports:",
                        '      - kind: "investment"',
                        f'        watchlist_yaml: "{watchlist_path}"',
                        '        out_dir: "reports_investment_us"',
                    ]
                ),
                encoding="utf-8",
            )

            summary = run_preflight(str(cfg_path), runtime_root=str(runtime_root), out_dir=str(out_dir))
            self.assertIn("checks", summary)
            self.assertTrue((out_dir / "supervisor_preflight_summary.json").exists())
            self.assertTrue((out_dir / "supervisor_preflight_report.md").exists())

            payload = json.loads((out_dir / "supervisor_preflight_summary.json").read_text(encoding="utf-8"))
            names = {row["name"] for row in payload["checks"]}
            self.assertIn("config", names)
            self.assertIn("dashboard_db", names)
            self.assertIn("runtime_root", names)

    @patch("src.tools.preflight_supervisor._probe_port")
    def test_run_preflight_only_checks_ports_required_by_current_config(self, mock_probe_port):
        mock_probe_port.side_effect = lambda host, port, timeout_sec=0.25: int(port) == 4002
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            runtime_root = base / "runtime_data" / "paper_test"
            runtime_root.mkdir(parents=True, exist_ok=True)
            (runtime_root / "audit.db").write_text("", encoding="utf-8")
            watchlist_path = base / "watchlist.yaml"
            watchlist_path.write_text("symbols: [AAPL]\n", encoding="utf-8")
            out_dir = base / "reports_preflight"
            cfg_path.write_text(
                "\n".join(
                    [
                        'summary_out_dir: "reports_supervisor"',
                        'dashboard_db: "audit.db"',
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    reports:",
                        '      - kind: "investment"',
                        f'        watchlist_yaml: "{watchlist_path}"',
                        '        out_dir: "reports_investment_us"',
                    ]
                ),
                encoding="utf-8",
            )

            payload = run_preflight(str(cfg_path), runtime_root=str(runtime_root), out_dir=str(out_dir))
            port_rows = [row for row in payload["checks"] if str(row.get("name", "")).startswith("ibkr_port:")]
            self.assertEqual(len(port_rows), 1)
            self.assertEqual(port_rows[0]["name"], "ibkr_port:127.0.0.1:4002")
            self.assertEqual(port_rows[0]["status"], "PASS")

    @patch("src.tools.preflight_supervisor._probe_port")
    def test_run_preflight_gateway_fallback_only_checks_gateway_ports(self, mock_probe_port):
        mock_probe_port.return_value = False
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            runtime_root = base / "runtime_data" / "paper_test"
            runtime_root.mkdir(parents=True, exist_ok=True)
            (runtime_root / "audit.db").write_text("", encoding="utf-8")
            out_dir = base / "reports_preflight"
            cfg_path.write_text(
                "\n".join(
                    [
                        'summary_out_dir: "reports_supervisor"',
                        'dashboard_db: "audit.db"',
                        'markets: []',
                    ]
                ),
                encoding="utf-8",
            )

            payload = run_preflight(str(cfg_path), runtime_root=str(runtime_root), out_dir=str(out_dir))
            port_rows = [row for row in payload["checks"] if str(row.get("name", "")).startswith("ibkr_port:")]
            self.assertEqual([row["name"] for row in port_rows], [
                "ibkr_port:127.0.0.1:4001",
                "ibkr_port:127.0.0.1:4002",
            ])


if __name__ == "__main__":
    unittest.main()
