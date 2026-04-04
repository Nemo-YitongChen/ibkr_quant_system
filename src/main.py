# src/main.py
from __future__ import annotations

import argparse
import os
from pathlib import Path

try:
    from .app.intraday_bootstrap import run_intraday_engine
    from .common.env import load_project_env
    from .common.markets import add_market_args, resolve_market_code
except ImportError:
    from app.intraday_bootstrap import run_intraday_engine
    from common.env import load_project_env
    from common.markets import add_market_args, resolve_market_code

BASE_DIR = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    add_market_args(ap)
    ap.add_argument(
        "--ibkr-config",
        default=os.environ.get("IBKR_CONFIG_PATH", "config/ibkr.yaml"),
        help="Path to the IBKR runtime config yaml",
    )
    ap.add_argument(
        "--startup-check-only",
        action="store_true",
        default=False,
        help="Run configuration/data self-checks and exit before connecting to IBKR.",
    )
    return ap.parse_args()


def main() -> None:
    load_project_env()
    args = parse_args()
    run_intraday_engine(
        BASE_DIR,
        market_code=resolve_market_code(getattr(args, "market", "")),
        ibkr_config_arg=str(args.ibkr_config),
        startup_check_only=bool(args.startup_check_only),
    )


if __name__ == "__main__":
    main()
