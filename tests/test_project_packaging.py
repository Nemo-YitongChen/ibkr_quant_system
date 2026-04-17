from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import tomllib

from src import main as main_module

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_pyproject_declares_console_scripts_and_runtime_metadata() -> None:
    data = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = data["project"]
    scripts = project["scripts"]

    assert project["requires-python"] == ">=3.11"
    assert "python-dotenv>=1.0.1" in project["dependencies"]
    assert scripts["ibkr-quant-engine"] == "src.main:main"
    assert scripts["ibkr-quant-supervisor"] == "src.app.supervisor:main"
    assert scripts["ibkr-quant-preflight"] == "src.tools.preflight_supervisor:main"
    assert scripts["ibkr-quant-dashboard"] == "src.tools.generate_dashboard:main"
    assert scripts["ibkr-quant-report"] == "src.tools.generate_investment_report:main"
    assert scripts["ibkr-quant-paper"] == "src.tools.run_investment_paper:main"
    assert scripts["ibkr-quant-execution"] == "src.tools.run_investment_execution:main"
    assert scripts["ibkr-quant-guard"] == "src.tools.run_investment_guard:main"
    assert scripts["ibkr-quant-opportunity"] == "src.tools.run_investment_opportunity:main"
    assert scripts["ibkr-quant-weekly-review"] == "src.tools.review_investment_weekly:main"
    assert scripts["ibkr-quant-reconcile"] == "src.tools.reconcile_investment_broker:main"
    assert scripts["ibkr-quant-sync-paper"] == "src.tools.sync_investment_paper_from_broker:main"
    assert scripts["ibkr-quant-execution-review"] == "src.tools.review_investment_execution:main"
    assert scripts["ibkr-quant-walk-forward"] == "src.tools.review_market_walk_forward:main"
    assert scripts["ibkr-quant-label-snapshots"] == "src.tools.label_investment_snapshots:main"
    assert scripts["ibkr-quant-trade-report"] == "src.tools.generate_trade_report:main"
    assert scripts["ibkr-quant-short-safety-sync"] == "src.tools.sync_short_safety_from_ibkr:main"


def test_main_loads_env_before_delegating_to_runtime(monkeypatch) -> None:
    calls: list[str] = []
    captured: dict[str, object] = {}

    monkeypatch.setattr(main_module, "load_project_env", lambda: calls.append("load_env"))
    monkeypatch.setattr(
        main_module,
        "parse_args",
        lambda: SimpleNamespace(market="HK", ibkr_config="config/ibkr.yaml", startup_check_only=True),
    )
    monkeypatch.setattr(main_module, "resolve_market_code", lambda value: f"{value}-RESOLVED")

    def fake_run(base_dir: Path, **kwargs: object) -> None:
        captured["base_dir"] = base_dir
        captured.update(kwargs)

    monkeypatch.setattr(main_module, "run_intraday_engine", fake_run)

    main_module.main()

    assert calls == ["load_env"]
    assert captured["base_dir"] == main_module.BASE_DIR
    assert captured["market_code"] == "HK-RESOLVED"
    assert captured["ibkr_config_arg"] == "config/ibkr.yaml"
    assert captured["startup_check_only"] is True


def test_readme_uses_repo_relative_links_and_gateway_wording() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    assert "](/Volumes" not in readme
    assert "](/Users" not in readme
    assert "IB Gateway" in readme
    assert "当前仅支持 `IB Gateway`" in readme
    assert "ibkr-quant-dashboard" in readme
    assert "ibkr-quant-execution-review" in readme
    assert "ibkr-quant-short-safety-sync" in readme
    assert "本地 `TWS`" not in readme
