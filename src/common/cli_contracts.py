from __future__ import annotations

from dataclasses import fields, dataclass
from pathlib import Path
from typing import Any, Dict


@dataclass(frozen=True)
class CliSummaryContract:
    def to_dict(self) -> Dict[str, Any]:
        return {field.name: getattr(self, field.name) for field in fields(self)}


@dataclass(frozen=True)
class ArtifactBundle:
    summary_json: Path | None = None
    report_md: Path | None = None
    markdown: Path | None = None
    portfolio_csv: Path | None = None
    trades_csv: Path | None = None
    plan_csv: Path | None = None
    scan_csv: Path | None = None
    positions_csv: Path | None = None
    summary_csv: Path | None = None
    trade_log_csv: Path | None = None
    rows_csv: Path | None = None
    runs_csv: Path | None = None
    weekly_csv: Path | None = None
    orders_csv: Path | None = None
    fills_csv: Path | None = None
    dashboard_json: Path | None = None
    dashboard_html: Path | None = None

    def to_dict(self) -> Dict[str, Path]:
        return {
            field.name: value
            for field in fields(self)
            if (value := getattr(self, field.name)) is not None
        }


@dataclass(frozen=True)
class MarketPortfolioRunSummary(CliSummaryContract):
    market: str
    portfolio_id: str


@dataclass(frozen=True)
class InvestmentPaperSummary(MarketPortfolioRunSummary):
    rebalance_due: bool
    executed: bool
    trade_count: int
    position_count: int


@dataclass(frozen=True)
class InvestmentExecutionSummary(MarketPortfolioRunSummary):
    submitted: bool
    account_profile: str
    order_count: int
    gap_symbols: int
    gap_notional: str


@dataclass(frozen=True)
class InvestmentGuardSummary(MarketPortfolioRunSummary):
    submitted: bool
    order_count: int
    stop_count: int
    take_profit_count: int
    market_rules: str


@dataclass(frozen=True)
class InvestmentOpportunitySummary(MarketPortfolioRunSummary):
    entry_now_count: int
    near_entry_count: int
    wait_count: int
    market_structure_wait_count: int
    adaptive_strategy_wait_count: int
    market_rules: str


@dataclass(frozen=True)
class BrokerSyncSummary(MarketPortfolioRunSummary):
    account_id: str
    position_count: int
    equity_after: str


@dataclass(frozen=True)
class WeeklyReviewSummary(CliSummaryContract):
    market_filter: str
    portfolio_filter: str
    portfolio_count: int
    trade_count: int
    execution_run_count: int
    best_portfolio: str
    worst_portfolio: str


@dataclass(frozen=True)
class ReconciliationSummary(MarketPortfolioRunSummary):
    match_rows: int
    only_local_rows: int
    only_broker_rows: int
    qty_mismatch_rows: int


@dataclass(frozen=True)
class DashboardSummary(CliSummaryContract):
    market_cards: int
    trade_cards: int
    dry_run_cards: int
    preflight_warn_count: int
    preflight_fail_count: int


@dataclass(frozen=True)
class ExecutionReviewSummary(MarketPortfolioRunSummary):
    execution_runs: int
    planned_orders: int
    fills: int
    realized_net_pnl: str
