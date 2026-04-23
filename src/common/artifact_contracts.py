from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

ARTIFACT_SCHEMA_VERSION = "2026Q2.p0.v1"


@dataclass(frozen=True)
class ArtifactContract:
    artifact_key: str
    label: str
    filename: str
    format: str
    required_fields: Tuple[str, ...] = ()
    required_columns: Tuple[str, ...] = ()
    freshness_hours: int | None = None
    allow_empty: bool = True
    missing_status: str = "degraded"
    generated_at_fields: Tuple[str, ...] = ("generated_at", "ts")
    schema_version_fields: Tuple[str, ...] = ("schema_version",)
    fallback_filename: str | None = None
    fallback_section: str | None = None
    inherit_generated_at_from: str | None = None
    inherit_schema_version_from: str | None = None


def dashboard_artifact_contracts() -> Dict[str, ArtifactContract]:
    return {
        "weekly_review_summary": ArtifactContract(
            artifact_key="weekly_review_summary",
            label="Weekly Review Summary",
            filename="weekly_review_summary.json",
            format="json",
            required_fields=("window_start", "window_end", "portfolio_count"),
            freshness_hours=168,
            allow_empty=False,
        ),
        "weekly_execution_summary": ArtifactContract(
            artifact_key="weekly_execution_summary",
            label="Weekly Execution Summary",
            filename="weekly_execution_summary.csv",
            format="csv",
            required_columns=("portfolio_id", "market", "execution_runs", "submitted_order_rows"),
            freshness_hours=168,
            fallback_filename="weekly_review_summary.json",
            fallback_section="broker_summary_rows",
            inherit_generated_at_from="weekly_review_summary",
            inherit_schema_version_from="weekly_review_summary",
        ),
        "weekly_broker_positions": ArtifactContract(
            artifact_key="weekly_broker_positions",
            label="Weekly Broker Positions",
            filename="weekly_broker_positions.csv",
            format="csv",
            required_columns=("portfolio_id", "market", "symbol"),
            freshness_hours=168,
            missing_status="warning",
            fallback_filename="weekly_review_summary.json",
            fallback_section="broker_snapshot_rows",
            inherit_generated_at_from="weekly_review_summary",
            inherit_schema_version_from="weekly_review_summary",
        ),
        "weekly_broker_comparison": ArtifactContract(
            artifact_key="weekly_broker_comparison",
            label="Weekly Broker Comparison",
            filename="weekly_broker_comparison.csv",
            format="csv",
            required_columns=("portfolio_id", "market", "local_holdings_count", "broker_holdings_count"),
            freshness_hours=168,
            missing_status="warning",
            fallback_filename="weekly_review_summary.json",
            fallback_section="broker_local_diff_rows",
            inherit_generated_at_from="weekly_review_summary",
            inherit_schema_version_from="weekly_review_summary",
        ),
        "weekly_risk_review_summary": ArtifactContract(
            artifact_key="weekly_risk_review_summary",
            label="Weekly Risk Review Summary",
            filename="weekly_risk_review_summary.csv",
            format="csv",
            required_columns=("portfolio_id", "market"),
            freshness_hours=168,
            fallback_filename="weekly_review_summary.json",
            fallback_section="risk_review_summary",
            inherit_generated_at_from="weekly_review_summary",
            inherit_schema_version_from="weekly_review_summary",
        ),
        "weekly_patch_governance_summary": ArtifactContract(
            artifact_key="weekly_patch_governance_summary",
            label="Weekly Patch Governance Summary",
            filename="weekly_patch_governance_summary.csv",
            format="csv",
            required_columns=("market", "field", "latest_status_label"),
            freshness_hours=168,
            fallback_filename="weekly_review_summary.json",
            fallback_section="patch_governance_summary",
            inherit_generated_at_from="weekly_review_summary",
            inherit_schema_version_from="weekly_review_summary",
        ),
        "supervisor_preflight_summary": ArtifactContract(
            artifact_key="supervisor_preflight_summary",
            label="Supervisor Preflight Summary",
            filename="supervisor_preflight_summary.json",
            format="json",
            required_fields=("pass_count", "warn_count", "fail_count", "checks"),
            freshness_hours=24,
            allow_empty=False,
        ),
        "broker_reconciliation_summary": ArtifactContract(
            artifact_key="broker_reconciliation_summary",
            label="Broker Reconciliation Summary",
            filename="broker_reconciliation_summary.json",
            format="json",
            required_fields=("market", "portfolio_id", "match_rows", "qty_mismatch_rows"),
            freshness_hours=168,
            allow_empty=False,
            missing_status="warning",
        ),
    }


def report_artifact_contracts() -> Dict[str, ArtifactContract]:
    return {
        "investment_execution_summary": ArtifactContract(
            artifact_key="investment_execution_summary",
            label="Investment Execution Summary",
            filename="investment_execution_summary.json",
            format="json",
            required_fields=("portfolio_id", "market", "order_count", "blocked_order_count"),
            freshness_hours=72,
            allow_empty=False,
        ),
    }
