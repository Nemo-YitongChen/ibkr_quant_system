from __future__ import annotations

from src.tools.dashboard_blocks import build_dashboard_v2_blocks


def _by_id(blocks):
    return {block["id"]: block for block in blocks}


def test_dashboard_v2_blocks_include_control_market_and_evidence_layers():
    payload = {
        "ops_overview": {
            "summary_text": "ops ready",
            "preflight_fail_count": 0,
            "degraded_health_count": 1,
            "evidence_focus_action_count": 3,
            "evidence_focus_urgent_count": 2,
            "evidence_focus_primary_market": "US",
            "evidence_focus_primary_action": "Review gate thresholds",
            "ibkr_gateway_budget_status": "warning",
            "ibkr_gateway_budget_gateway_request_count": 1200,
            "ibkr_gateway_budget_cache_hit_count": 300,
            "ibkr_gateway_budget_cache_hit_ratio": 0.2,
            "ibkr_gateway_budget_max_usage_pct": 110.0,
            "ibkr_gateway_budget_over_budget_market_count": 1,
            "ibkr_gateway_budget_stale_telemetry_market_count": 0,
            "ibkr_gateway_budget_missing_telemetry_market_count": 0,
            "auto_order_status": "blocked",
            "auto_order_blocked_count": 1,
            "auto_order_ready_count": 1,
            "auto_order_primary_block_reason": "preflight_stale",
            "auto_order_offline_recovery_required_count": 1,
            "auto_order_submit_plan_status": "BLOCKED",
            "auto_order_submit_plan_reason": "no_single_safe_submit_candidate",
            "auto_order_submit_selected_portfolio_id": "",
            "alert_rows": [{"status": "warn", "name": "stale"}],
        },
        "auto_order_readiness": {
            "execution_evidence_maintenance": {
                "status": "COMPLETE",
                "reason": "execution_evidence_refreshed",
                "target_market": "ASX",
                "target_portfolio_id": "ASX:asx_top_quality",
                "submit_orders": False,
            },
            "summary": {
                "status": "blocked",
                "summary_text": "auto_order_readiness portfolios=2 ready=1 warning=0 blocked=1 disabled=0",
                "portfolio_count": 2,
                "ready_count": 1,
                "warning_count": 0,
                "blocked_count": 1,
                "disabled_count": 0,
                "primary_block_reason": "preflight_stale",
                "offline_recovery_required_count": 1,
                "offline_recovery_markets": ["US"],
                "offline_recovery_summary_text": "offline_recovery_required=1 markets=US top_reason=preflight_stale_after_offline_gap",
                "remediation_plan": [
                    {
                        "reason": "preflight_stale",
                        "severity": "block",
                        "affected_portfolio_count": 1,
                    }
                ],
                "submit_plan": {
                    "status": "BLOCKED",
                    "ready": False,
                    "reason": "no_single_safe_submit_candidate",
                    "candidate_count": 0,
                    "frontier_candidate_count": 1,
                    "selected_portfolio_id": "",
                    "submit_capacity_plan": {
                        "status": "TRIAL_SCALE_ALLOWED",
                        "reason": "trial_quality_pass_full_scale_pending",
                        "scale_allowed": True,
                        "scale_stage": "trial",
                        "fill_count": 6,
                        "matured_5d_sample_count": 6,
                        "evidence_market_count": 1,
                        "effective_max_submit_portfolios_per_run": 2,
                        "effective_max_submit_total_gross_order_value": 200.0,
                    },
                    "frontier_candidates": [
                        {
                            "market": "US",
                            "portfolio_id": "US:watchlist",
                            "frontier_reason": "preflight_stale",
                            "submit_quality_status": "PASS",
                            "submit_quality_tier": "HIGH",
                            "submit_quality_min_net_edge_bps": 22.0,
                            "submit_quality_min_edge_margin_bps": 9.0,
                        }
                    ],
                    "rejected_candidates": [
                        {
                            "market": "US",
                            "portfolio_id": "US:watchlist",
                            "reject_reasons": ["planned_gross_value_exceeds_policy"],
                        }
                    ],
                },
                "frequency_plan": {
                    "status": "candidate_supply_gap",
                    "reason": "no_safe_submit_candidate_with_seed_proposals",
                    "primary_action": "create_or_refresh_preferred_asset_seed_watchlist",
                    "seed_proposal_count": 1,
                    "manual_seed_proposal_count": 1,
                    "seed_proposal_markets": ["US"],
                    "seed_intake_plan_count": 1,
                    "seed_source_candidate_count": 2,
                    "seed_source_markets": ["US"],
                    "seed_intake_external_source_count": 0,
                    "does_not_change_submit_decision": True,
                },
                "candidate_supply_status": "candidate_supply_gap",
                "candidate_supply_reason": "no_safe_submit_candidate_with_seed_proposals",
                "candidate_supply_primary_action": "create_or_refresh_preferred_asset_seed_watchlist",
            },
            "rows": [
                {
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "status": "BLOCKED",
                    "primary_reason": "preflight_stale",
                },
                {
                    "market": "HK",
                    "portfolio_id": "HK:watchlist",
                    "status": "READY",
                    "primary_reason": "ready",
                },
            ],
        },
        "open_market_analysis_summary": {
            "status": "warning",
            "summary_text": "open_markets=1 open_portfolios=1 auto_blocked=1",
            "open_market_count": 1,
            "open_portfolio_count": 1,
            "fresh_open_report_count": 1,
            "stale_open_report_count": 0,
            "actionable_open_count": 1,
            "submit_enabled_open_count": 1,
            "auto_order_artifact_present": True,
            "auto_ready_open_count": 0,
            "auto_blocked_open_count": 1,
            "auto_missing_open_count": 0,
            "data_attention_open_count": 0,
            "missing_market_state_count": 0,
            "primary_reason": "preflight_stale",
            "market_rows": [{"market": "US", "open_portfolio_count": 1}],
            "rows": [{"market": "US", "portfolio_id": "US:watchlist"}],
        },
        "artifact_health_overview": {"fail_count": 0, "warn_count": 1},
        "governance_health_summary": {"blocked_count": 0, "warn_count": 1},
        "dashboard_control": {
            "service": {"status": "running"},
            "actions": {
                "last_action": "refresh_dashboard",
                "action_history": [
                    {"action": "run_once", "status": "completed"},
                    {"action": "refresh_dashboard", "status": "completed"},
                    {
                        "action": "run_preflight",
                        "status": "failed",
                        "error_class": "transient_io",
                        "linked_evidence_action_id": "2026W18-US-market-review_gate_thresholds",
                        "linked_strategy_parameter_suggestion_id": "2026-w19-us-watchlist-mr-weight",
                        "linked_strategy_parameter_field": "mr_weight",
                        "resolution_status": "ACKNOWLEDGED",
                    },
                ],
            },
        },
        "market_views": {
            "US": {"market": "US", "portfolio_count": 1, "open_count": 1},
            "HK": {"market": "HK", "portfolio_count": 1, "stale_report_count": 1},
            "CN": {"market": "CN", "portfolio_count": 0},
        },
        "market_evidence_action_summary": {
            "US": {
                "primary_action": "review_gate_thresholds",
                "action_label": "Review gate thresholds",
                "basis_label": "Blocked outperformed allowed",
                "evidence_row_count": 8,
            },
            "HK": {
                "primary_action": "collect_more_outcome_samples",
                "action_label": "Collect more outcome samples",
                "basis_label": "Insufficient blocked-vs-allowed sample",
                "evidence_row_count": 3,
            },
            "CN": {
                "primary_action": "build_weekly_unified_evidence",
                "action_label": "Build unified evidence",
                "basis_label": "No unified evidence",
                "evidence_row_count": 0,
            },
        },
        "evidence_focus_actions": [
            {
                "market": "US",
                "primary_action": "review_gate_thresholds",
                "action": "Review gate thresholds",
                "basis": "Blocked outperformed allowed",
                "priority_order": 10,
            },
            {
                "market": "CN",
                "primary_action": "build_weekly_unified_evidence",
                "action": "Build unified evidence",
                "basis": "No unified evidence",
                "priority_order": 30,
            },
            {
                "market": "HK",
                "primary_action": "collect_more_outcome_samples",
                "action": "Collect more outcome samples",
                "basis": "Insufficient blocked-vs-allowed sample",
                "priority_order": 60,
            },
        ],
        "evidence_focus_summary": {
            "status": "warn",
            "summary_text": "US: Review gate thresholds; basis=Blocked outperformed allowed; urgent=2/3.",
            "primary_market": "US",
            "primary_action": "review_gate_thresholds",
            "primary_action_label": "Review gate thresholds",
            "primary_basis": "Blocked outperformed allowed",
            "primary_detail": "Review edge floor and buffers.",
            "focus_action_count": 3,
            "urgent_action_count": 2,
            "read_only": True,
        },
        "unified_evidence_overview": {
            "row_count": 3,
            "allowed_row_count": 1,
            "blocked_row_count": 1,
            "candidate_only_row_count": 1,
            "outcome_labeled_row_count": 2,
            "partial_join_row_count": 1,
        },
        "blocked_vs_allowed_expost_review": [
            {"review_label": "BLOCKING_HELPED", "block_reason": "EDGE_GATE"},
        ],
        "candidate_model_review": [
            {"review_label": "SIGNAL_RANKING_WORKING", "portfolio_id": "US:watchlist"},
            {"review_label": "EXPECTED_EDGE_OVERSTATED", "portfolio_id": "HK:watchlist"},
        ],
        "walk_forward_acceptance": {
            "rows": [
                {
                    "market": "US",
                    "status": "RECOMMEND_PATCH",
                    "selected_candidate_family": "REDUCE_TURNOVER",
                    "acceptance_failed_rules": "",
                },
                {
                    "market": "HK",
                    "status": "KEEP_BASELINE",
                    "selected_candidate_family": "BASELINE",
                    "acceptance_failed_rules": "post_cost_improvement",
                },
            ]
        },
        "walk_forward_market_stability": {
            "rows": [
                {
                    "market": "US",
                    "consecutive_stable_windows": 3,
                    "min_consecutive_stable_windows": 3,
                },
                {
                    "market": "HK",
                    "consecutive_stable_windows": 1,
                    "min_consecutive_stable_windows": 3,
                },
            ]
        },
        "strategy_parameter_suggestions": [
            {
                "suggestion_id": "2026-w19-us-watchlist-mr-weight",
                "market": "US",
                "portfolio_id": "US:watchlist",
                "primary_field": "mr_weight",
                "status": "SUGGESTED",
                "auto_apply": 0,
            },
            {
                "suggestion_id": "2026-w18-hk-watchlist-bo-weight",
                "market": "HK",
                "portfolio_id": "HK:watchlist",
                "primary_field": "bo_weight",
                "status": "APPLIED",
                "auto_apply": 0,
            },
        ],
        "strategy_parameter_suggestion_followup": [
            {
                "suggestion_id": "2026-w18-hk-watchlist-bo-weight",
                "market": "HK",
                "portfolio_id": "HK:watchlist",
                "primary_field": "bo_weight",
                "followup_verdict": "DEGRADED",
            },
        ],
        "strategy_parameter_suggestion_effectiveness": {
            "status": "warn",
            "summary_text": "suggestions=2 open=1 handled=1 resolved=1 followups=1 degraded=1",
            "suggestion_count": 2,
            "open_suggestion_count": 1,
            "handled_suggestion_count": 1,
            "resolved_suggestion_count": 1,
            "applied_suggestion_count": 1,
            "stale_suggestion_count": 0,
            "auto_apply_count": 0,
            "followup_count": 1,
            "improved_followup_count": 0,
            "degraded_followup_count": 1,
            "primary_market": "US",
            "primary_portfolio_id": "US:watchlist",
            "primary_field": "mr_weight",
            "read_only": True,
        },
            "watchlist_expansion_summary": {
                "status": "ready",
                "summary_text": "markets=2 candidates=10 selected=2 zero_selected_markets=1 status=ready",
            "selected_count": 2,
            "candidate_row_count": 10,
            "zero_selected_market_count": 1,
            "rejected_count": 1,
            "age_hours": 6.0,
            "max_age_hours": 168,
            "account_profile": {"name": "small", "account_equity": 1000.0},
            "primary_recommendation_market": "HK",
            "primary_recommendation_reason": "expected_cost_above_max",
            "primary_recommendation_action": "calibrate_cost_or_expand_lower_cost_etfs",
            "primary_recommendation_note": "Review fee/spread assumptions and expand lower-cost ETF candidates.",
            "market_recommendations": [
                {
                    "market": "HK",
                    "candidate_row_count": 5,
                    "selected_count": 0,
                    "top_reject_reason": "expected_cost_above_max",
                    "top_reject_count": 1,
                    "preferred_asset_class_gap": True,
                    "expansion_target": "seed_preferred_asset_class_candidates",
                    "near_miss_candidates": [
                        {
                            "symbol": "2800.HK",
                            "asset_class": "unknown",
                            "selection_reason": "expected_cost_above_max,whole_share_not_tradable",
                        }
                    ],
                    "recommendation_action": "calibrate_cost_or_expand_lower_cost_etfs",
                }
            ],
            "seed_proposals": [
                {
                    "market": "HK",
                    "proposal_status": "MANUAL_REVIEW_REQUIRED",
                    "proposal_action": "create_or_refresh_preferred_asset_seed_watchlist",
                    "expansion_target": "seed_preferred_asset_class_candidates",
                    "auto_apply": False,
                }
            ],
            "seed_intake_plan": [
                {
                    "market": "HK",
                    "intake_status": "MANUAL_REVIEW_REQUIRED",
                    "candidate_symbols": ["2800.HK", "2833.HK"],
                    "source_candidate_count": 2,
                    "source_candidates": [
                        {
                            "symbol": "2800.HK",
                            "asset_class": "etf",
                            "broker_mapping_status": "TO_VERIFY",
                        }
                    ],
                    "next_action": "verify_seed_source_candidates_in_candidate_report",
                    "auto_apply": False,
                    "does_not_change_symbol_master": True,
                }
                ],
                "seed_promotion_review": [
                    {
                        "market": "HK",
                        "symbol": "2800.HK",
                        "promotion_status": "BROKER_MAPPING_REQUIRED",
                        "does_not_change_symbol_master": True,
                    },
                    {
                        "market": "HK",
                        "symbol": "2833.HK",
                        "promotion_status": "QUALITY_REJECTED",
                        "does_not_change_symbol_master": True,
                    },
                ],
                "seed_evidence_queue": [
                    {
                        "market": "ASX",
                        "status": "READY",
                        "symbols": ["DHHF.AX", "BGBL.AX"],
                        "evidence_mode": "YFINANCE_ONLY",
                        "submit_orders": False,
                    }
                ],
                "seed_evidence_primary_market": "ASX",
                "seed_evidence_primary_symbols": ["DHHF.AX", "BGBL.AX"],
                "seed_evidence_mode": "YFINANCE_ONLY",
                "account_growth_tier_plan": {
                    "profile": "small",
                    "primary_action": "verify_seed_etfs_in_candidate_report_before_submit",
                    "expansion_mode": "whole_share_tradable_etf_first",
                    "submit_frequency_mode": "single_small_limit_order_until_fill_quality_passes",
                    "max_orders_per_run": 1,
                    "max_order_value": 100.0,
                    "quality_gate_policy": "do_not_relax_submit_gates",
                    "read_only": True,
                },
                "markets": [
                    {"market": "US", "candidate_row_count": 5, "selected_count": 2, "selected_symbols": "SPTM,SCHB"},
                    {"market": "HK", "candidate_row_count": 5, "selected_count": 0, "selected_symbols": ""},
            ],
            "candidate_rows": [
                {"market": "US", "symbol": "SPTM", "selection_status": "SELECTED", "selection_reason": "PASS"},
                {"market": "US", "symbol": "SCHB", "selection_status": "SELECTED", "selection_reason": "PASS"},
                {
                    "market": "HK",
                    "symbol": "2800.HK",
                    "selection_status": "REJECTED",
                    "selection_reason": "expected_cost_above_max,whole_share_not_tradable",
                },
            ],
        },
        "weekly_attribution_waterfall": [{"component": "selection"}],
    }

    blocks = build_dashboard_v2_blocks(payload)
    by_id = _by_id(blocks)

    assert list(by_id) == [
        "ops_health",
        "open_market_analysis",
        "auto_order_readiness",
        "evidence_focus_actions",
        "evidence_quality",
        "dashboard_control_actions",
        "market_views",
        "watchlist_expansion",
        "walk_forward_acceptance",
        "strategy_parameter_governance",
        "weekly_attribution_waterfall",
        "unified_evidence_overview",
        "blocked_vs_allowed_expost",
        "dashboard_control_action_history",
    ]
    assert [block["id"] for block in blocks if block["category"] == "home"] == [
        "ops_health",
        "open_market_analysis",
        "auto_order_readiness",
        "evidence_focus_actions",
        "evidence_quality",
        "dashboard_control_actions",
    ]
    assert [block["id"] for block in blocks if block["advanced_only"]] == [
        "market_views",
        "watchlist_expansion",
        "walk_forward_acceptance",
        "strategy_parameter_governance",
        "weekly_attribution_waterfall",
        "unified_evidence_overview",
        "blocked_vs_allowed_expost",
        "dashboard_control_action_history",
    ]
    assert by_id["ops_health"]["metrics"]["degraded_health_count"] == 1
    assert by_id["ops_health"]["metrics"]["evidence_focus_action_count"] == 3
    assert by_id["ops_health"]["metrics"]["evidence_focus_urgent_count"] == 2
    assert by_id["ops_health"]["metrics"]["evidence_focus_primary_market"] == "US"
    assert by_id["ops_health"]["metrics"]["evidence_focus_primary_action"] == "Review gate thresholds"
    assert by_id["ops_health"]["metrics"]["ibkr_gateway_budget_status"] == "warning"
    assert by_id["ops_health"]["metrics"]["ibkr_gateway_budget_gateway_request_count"] == 1200
    assert by_id["ops_health"]["metrics"]["ibkr_gateway_budget_cache_hit_ratio"] == 0.2
    assert by_id["ops_health"]["metrics"]["ibkr_gateway_budget_over_budget_market_count"] == 1
    assert by_id["ops_health"]["metrics"]["auto_order_submit_plan_status"] == "BLOCKED"
    assert by_id["ops_health"]["metrics"]["auto_order_primary_block_reason"] == "preflight_stale"
    assert by_id["ops_health"]["metrics"]["auto_order_offline_recovery_required_count"] == 1
    assert by_id["open_market_analysis"]["metrics"]["open_market_count"] == 1
    assert by_id["open_market_analysis"]["metrics"]["auto_blocked_open_count"] == 1
    assert by_id["open_market_analysis"]["metrics"]["primary_reason"] == "preflight_stale"
    assert by_id["auto_order_readiness"]["metrics"]["portfolio_count"] == 2
    assert by_id["auto_order_readiness"]["metrics"]["blocked_count"] == 1
    assert by_id["auto_order_readiness"]["metrics"]["offline_recovery_required_count"] == 1
    assert by_id["auto_order_readiness"]["metrics"]["offline_recovery_markets"] == ["US"]
    assert by_id["auto_order_readiness"]["metrics"]["submit_plan_status"] == "BLOCKED"
    assert by_id["auto_order_readiness"]["metrics"]["submit_plan_reason"] == "no_single_safe_submit_candidate"
    assert by_id["auto_order_readiness"]["metrics"]["frontier_candidate_count"] == 1
    assert by_id["auto_order_readiness"]["metrics"]["frontier_quality_pass_count"] == 1
    assert by_id["auto_order_readiness"]["metrics"]["frontier_high_quality_count"] == 1
    assert by_id["auto_order_readiness"]["metrics"]["frontier_top_submit_quality_tier"] == "HIGH"
    assert by_id["auto_order_readiness"]["metrics"]["rejected_candidate_count"] == 1
    assert by_id["auto_order_readiness"]["metrics"]["submit_capacity_scale_stage"] == "trial"
    assert by_id["auto_order_readiness"]["metrics"]["submit_capacity_evidence_market_count"] == 1
    assert by_id["auto_order_readiness"]["metrics"]["submit_capacity_effective_max_portfolios"] == 2
    assert by_id["auto_order_readiness"]["metrics"]["candidate_supply_status"] == "candidate_supply_gap"
    assert (
        by_id["auto_order_readiness"]["metrics"]["candidate_supply_primary_action"]
        == "create_or_refresh_preferred_asset_seed_watchlist"
    )
    assert by_id["auto_order_readiness"]["metrics"]["frequency_seed_proposal_count"] == 1
    assert by_id["auto_order_readiness"]["metrics"]["frequency_seed_source_candidate_count"] == 2
    assert by_id["auto_order_readiness"]["metrics"]["frequency_seed_source_markets"] == ["US"]
    assert by_id["auto_order_readiness"]["metrics"]["frequency_plan_does_not_change_submit_decision"] == 1
    assert by_id["auto_order_readiness"]["rows"]["frequency_plan"]["seed_proposal_markets"] == ["US"]
    assert by_id["dashboard_control_actions"]["metrics"]["history_count"] == 3
    assert by_id["dashboard_control_actions"]["metrics"]["linked_action_history_count"] == 1
    assert (
        by_id["dashboard_control_actions"]["metrics"]["last_linked_evidence_action_id"]
        == "2026W18-US-market-review_gate_thresholds"
    )
    assert by_id["dashboard_control_actions"]["metrics"]["last_resolution_status"] == "ACKNOWLEDGED"
    assert by_id["dashboard_control_actions"]["metrics"]["linked_strategy_parameter_suggestion_history_count"] == 1
    assert (
        by_id["dashboard_control_actions"]["metrics"]["last_linked_strategy_parameter_suggestion_id"]
        == "2026-w19-us-watchlist-mr-weight"
    )
    assert by_id["dashboard_control_actions"]["metrics"]["last_linked_strategy_parameter_field"] == "mr_weight"
    assert by_id["dashboard_control_actions"]["metrics"]["last_strategy_parameter_resolution_status"] == "ACKNOWLEDGED"
    assert by_id["dashboard_control_actions"]["metrics"]["transient_io_error_count"] == 1
    assert by_id["dashboard_control_actions"]["metrics"]["retryable_error_count"] == 1
    assert by_id["market_views"]["metrics"]["market_count"] == 3
    assert by_id["market_views"]["metrics"]["evidence_action_market_count"] == 3
    assert by_id["market_views"]["metrics"]["evidence_row_market_count"] == 2
    assert by_id["market_views"]["metrics"]["evidence_attention_count"] == 2
    assert by_id["market_views"]["metrics"]["gate_review_market_count"] == 1
    assert by_id["market_views"]["metrics"]["missing_evidence_market_count"] == 1
    assert by_id["market_views"]["metrics"]["sample_collection_market_count"] == 1
    assert by_id["market_views"]["rows"][0]["evidence_primary_action"] == "build_weekly_unified_evidence"
    assert by_id["market_views"]["status"] == "warn"
    assert "evidence_attention=2" in by_id["market_views"]["summary"]
    assert by_id["watchlist_expansion"]["category"] == "advanced"
    assert by_id["watchlist_expansion"]["status"] == "warn"
    assert by_id["watchlist_expansion"]["metrics"]["selected_count"] == 2
    assert by_id["watchlist_expansion"]["metrics"]["zero_selected_market_count"] == 1
    assert by_id["watchlist_expansion"]["metrics"]["top_reject_reason"] == "expected_cost_above_max"
    assert by_id["watchlist_expansion"]["metrics"]["primary_recommendation_market"] == "HK"
    assert by_id["watchlist_expansion"]["metrics"]["primary_recommendation_action"] == "calibrate_cost_or_expand_lower_cost_etfs"
    assert by_id["watchlist_expansion"]["metrics"]["primary_expansion_target"] == "seed_preferred_asset_class_candidates"
    assert by_id["watchlist_expansion"]["metrics"]["seed_proposal_count"] == 1
    assert by_id["watchlist_expansion"]["metrics"]["manual_seed_proposal_count"] == 1
    assert by_id["watchlist_expansion"]["metrics"]["primary_seed_proposal_action"] == "create_or_refresh_preferred_asset_seed_watchlist"
    assert by_id["watchlist_expansion"]["metrics"]["seed_intake_plan_count"] == 1
    assert by_id["watchlist_expansion"]["metrics"]["seed_intake_external_source_count"] == 0
    assert by_id["watchlist_expansion"]["metrics"]["seed_intake_manual_review_count"] == 1
    assert by_id["watchlist_expansion"]["metrics"]["seed_source_candidate_count"] == 2
    assert by_id["watchlist_expansion"]["metrics"]["seed_source_market_count"] == 1
    assert by_id["watchlist_expansion"]["metrics"]["seed_promotion_review_count"] == 2
    assert by_id["watchlist_expansion"]["metrics"]["seed_evidence_queue_count"] == 1
    assert by_id["watchlist_expansion"]["metrics"]["seed_evidence_ready_job_count"] == 1
    assert by_id["watchlist_expansion"]["metrics"]["seed_evidence_primary_market"] == "ASX"
    assert (
        by_id["watchlist_expansion"]["metrics"]["seed_evidence_primary_symbols"]
        == "DHHF.AX,BGBL.AX"
    )
    assert by_id["watchlist_expansion"]["metrics"]["seed_promotion_ready_count"] == 0
    assert by_id["watchlist_expansion"]["metrics"]["seed_promotion_mapping_required_count"] == 1
    assert by_id["watchlist_expansion"]["metrics"]["seed_promotion_quality_rejected_count"] == 1
    assert by_id["watchlist_expansion"]["metrics"]["primary_seed_intake_status"] == "MANUAL_REVIEW_REQUIRED"
    assert by_id["watchlist_expansion"]["metrics"]["account_growth_profile"] == "small"
    assert (
        by_id["watchlist_expansion"]["metrics"]["account_growth_primary_action"]
        == "verify_seed_etfs_in_candidate_report_before_submit"
    )
    assert by_id["watchlist_expansion"]["metrics"]["account_growth_expansion_mode"] == "whole_share_tradable_etf_first"
    assert by_id["watchlist_expansion"]["metrics"]["account_growth_max_orders_per_run"] == 1
    assert by_id["watchlist_expansion"]["metrics"]["account_growth_max_order_value"] == 100.0
    assert by_id["watchlist_expansion"]["rows"]["market_recommendations"][0]["market"] == "HK"
    assert by_id["watchlist_expansion"]["rows"]["seed_proposals"][0]["market"] == "HK"
    assert by_id["watchlist_expansion"]["rows"]["seed_intake_plan"][0]["does_not_change_symbol_master"] is True
    assert by_id["watchlist_expansion"]["rows"]["seed_promotion_review"][0]["symbol"] == "2800.HK"
    assert by_id["watchlist_expansion"]["rows"]["account_growth_tier_plan"]["read_only"] is True
    assert by_id["evidence_focus_actions"]["status"] == "warn"
    assert by_id["evidence_focus_actions"]["metrics"]["focus_action_count"] == 3
    assert by_id["evidence_focus_actions"]["metrics"]["urgent_action_count"] == 2
    assert by_id["evidence_focus_actions"]["metrics"]["gate_review_count"] == 1
    assert by_id["evidence_focus_actions"]["metrics"]["missing_evidence_count"] == 1
    assert by_id["evidence_focus_actions"]["metrics"]["sample_collection_count"] == 1
    assert by_id["evidence_focus_actions"]["metrics"]["primary_market"] == "US"
    assert by_id["evidence_focus_actions"]["metrics"]["primary_action"] == "review_gate_thresholds"
    assert by_id["evidence_focus_actions"]["metrics"]["read_only"] is True
    assert by_id["evidence_focus_actions"]["rows"]["summary"]["primary_market"] == "US"
    assert by_id["evidence_focus_actions"]["rows"]["actions"][0]["market"] == "US"
    assert "US: Review gate thresholds" in by_id["evidence_focus_actions"]["summary"]
    assert by_id["evidence_quality"]["metrics"]["evidence_row_count"] == 3
    assert by_id["evidence_quality"]["metrics"]["candidate_only_row_count"] == 1
    assert by_id["evidence_quality"]["metrics"]["outcome_labeled_row_count"] == 2
    assert by_id["evidence_quality"]["metrics"]["blocked_review_count"] == 1
    assert by_id["evidence_quality"]["metrics"]["blocking_helped_count"] == 1
    assert by_id["evidence_quality"]["metrics"]["primary_action"] == "review_signal_expected_edge"
    assert by_id["evidence_quality"]["metrics"]["action_label"] == "Review signal expected edge"
    assert "Candidate model warning" in by_id["evidence_quality"]["metrics"]["action_note"]
    assert "action=Review signal expected edge" in by_id["evidence_quality"]["summary"]
    assert by_id["evidence_quality"]["metrics"]["candidate_model_review_count"] == 2
    assert by_id["evidence_quality"]["metrics"]["candidate_model_warning_count"] == 1
    assert by_id["evidence_quality"]["status"] == "warn"
    assert by_id["evidence_quality"]["rows"]["blocked_vs_allowed_label_summary"][0] == {
        "review_label": "BLOCKING_HELPED",
        "count": 1,
        "action": "keep_gate_monitor_post_cost",
    }
    assert by_id["weekly_attribution_waterfall"]["category"] == "advanced"
    assert by_id["walk_forward_acceptance"]["category"] == "advanced"
    assert by_id["walk_forward_acceptance"]["metrics"]["market_count"] == 2
    assert by_id["walk_forward_acceptance"]["metrics"]["recommend_patch_count"] == 1
    assert by_id["walk_forward_acceptance"]["metrics"]["rejected_or_baseline_count"] == 1
    assert by_id["walk_forward_acceptance"]["metrics"]["stable_market_count"] == 1
    assert by_id["walk_forward_acceptance"]["status"] == "warn"
    assert by_id["strategy_parameter_governance"]["category"] == "advanced"
    assert by_id["strategy_parameter_governance"]["status"] == "warn"
    assert by_id["strategy_parameter_governance"]["metrics"]["suggestion_count"] == 2
    assert by_id["strategy_parameter_governance"]["metrics"]["open_suggestion_count"] == 1
    assert by_id["strategy_parameter_governance"]["metrics"]["resolved_suggestion_count"] == 1
    assert by_id["strategy_parameter_governance"]["metrics"]["degraded_followup_count"] == 1
    assert by_id["strategy_parameter_governance"]["metrics"]["primary_field"] == "mr_weight"
    assert by_id["weekly_attribution_waterfall"]["metrics"]["row_count"] == 1
    assert by_id["unified_evidence_overview"]["advanced_only"] is True
    assert by_id["unified_evidence_overview"]["metrics"]["row_count"] == 3
    assert by_id["blocked_vs_allowed_expost"]["metrics"]["review_count"] == 1
    assert by_id["dashboard_control_action_history"]["metrics"]["history_count"] == 3
    assert (
        by_id["dashboard_control_action_history"]["metrics"]["linked_strategy_parameter_suggestion_history_count"]
        == 1
    )


def test_evidence_quality_block_marks_gate_review_when_blocked_outperforms():
    payload = {
        "unified_evidence_overview": {"row_count": 10},
        "blocked_vs_allowed_expost_review": [
            {
                "review_label": "BLOCKED_OUTPERFORMED_ALLOWED",
                "block_reason": "EDGE_GATE",
                "allowed_count": 6,
                "blocked_count": 7,
            },
        ],
        "candidate_model_review": [],
    }

    block = _by_id(build_dashboard_v2_blocks(payload))["evidence_quality"]

    assert block["status"] == "warn"
    assert block["metrics"]["too_restrictive_count"] == 1
    assert block["metrics"]["sample_ready_review_count"] == 1
    assert block["metrics"]["primary_action"] == "review_gate_thresholds"
    assert block["metrics"]["action_label"] == "Review gate thresholds"


def test_evidence_quality_block_keeps_insufficient_samples_non_warning():
    payload = {
        "unified_evidence_overview": {"row_count": 4},
        "blocked_vs_allowed_expost_review": [
            {
                "review_label": "INSUFFICIENT_OUTCOME_SAMPLE",
                "block_reason": "MARKET_RULE_GATE",
                "allowed_count": 1,
                "blocked_count": 2,
            },
        ],
        "candidate_model_review": [],
    }

    block = _by_id(build_dashboard_v2_blocks(payload))["evidence_quality"]

    assert block["status"] == "ok"
    assert block["metrics"]["insufficient_sample_count"] == 1
    assert block["metrics"]["sample_ready_review_count"] == 0
    assert block["metrics"]["primary_action"] == "collect_more_outcome_samples"
    assert block["metrics"]["action_label"] == "Collect more outcome samples"
    assert "sample-starved" in block["metrics"]["action_note"]


def test_market_views_block_keeps_sample_collection_non_warning():
    payload = {
        "market_views": {
            "US": {
                "market": "US",
                "portfolio_count": 1,
                "open_count": 1,
                "evidence_action_label": "Collect more outcome samples",
                "evidence_basis_label": "Insufficient blocked-vs-allowed sample",
                "evidence_row_count": 4,
            },
        },
    }

    block = _by_id(build_dashboard_v2_blocks(payload))["market_views"]

    assert block["status"] == "ok"
    assert block["metrics"]["attention_count"] == 0
    assert block["metrics"]["evidence_attention_count"] == 0
    assert block["metrics"]["sample_collection_market_count"] == 1
    assert block["rows"][0]["evidence_primary_action"] == "collect_more_outcome_samples"


def test_market_views_block_handles_malformed_evidence_summary():
    payload = {
        "market_views": {
            "US": {
                "market": "US",
                "portfolio_count": 1,
                "evidence_action_label": "Review gate thresholds",
                "evidence_row_count": 2,
            },
        },
        "market_evidence_action_summary": {"US": "legacy bad summary"},
    }

    block = _by_id(build_dashboard_v2_blocks(payload))["market_views"]

    assert block["status"] == "warn"
    assert block["metrics"]["gate_review_market_count"] == 1
    assert block["rows"][0]["evidence_primary_action"] == "review_gate_thresholds"


def test_watchlist_expansion_block_warns_when_no_growth_candidates_selected():
    payload = {
        "watchlist_expansion_summary": {
            "status": "warning",
            "reason": "no_selected_growth_candidates",
            "candidate_row_count": 4,
            "selected_count": 0,
            "markets": [
                {"market": "ASX", "candidate_row_count": 2, "selected_count": 0},
                {"market": "HK", "candidate_row_count": 2, "selected_count": 0},
            ],
            "candidate_rows": [
                {
                    "market": "ASX",
                    "symbol": "BHP.AX",
                    "selection_status": "REJECTED",
                    "selection_reason": "expected_cost_above_max,whole_share_not_tradable",
                },
                {
                    "market": "HK",
                    "symbol": "2800.HK",
                    "selection_status": "REJECTED",
                    "selection_reason": "whole_share_not_tradable",
                },
            ],
        }
    }

    block = _by_id(build_dashboard_v2_blocks(payload))["watchlist_expansion"]

    assert block["status"] == "warn"
    assert block["metrics"]["selected_count"] == 0
    assert block["metrics"]["zero_selected_market_count"] == 2
    assert block["metrics"]["top_reject_reason"] == "whole_share_not_tradable"
    assert block["metrics"]["primary_recommendation_market"] == "ASX"
    assert block["metrics"]["primary_recommendation_action"] == "calibrate_cost_or_expand_lower_cost_etfs"
    assert block["metrics"]["primary_expansion_target"] == "seed_preferred_asset_class_candidates"
    assert block["metrics"]["preferred_asset_class_gap_count"] == 2
    assert block["metrics"]["seed_proposal_count"] == 2
    assert block["metrics"]["primary_seed_proposal_action"] == "create_or_refresh_preferred_asset_seed_watchlist"
    assert block["metrics"]["seed_intake_plan_count"] == 2
    assert block["metrics"]["seed_intake_external_source_count"] == 2
    assert block["metrics"]["primary_seed_intake_next_action"] == "source_verified_low_cost_preferred_asset_candidates"
    assert block["rows"]["reason_summary"][0] == {"reason": "whole_share_not_tradable", "count": 2}
    assert block["rows"]["market_recommendations"][0]["market"] == "ASX"
    assert block["rows"]["market_recommendations"][0]["near_miss_candidates"][0]["symbol"] == "BHP.AX"
    assert block["rows"]["seed_proposals"][0]["auto_apply"] is False
    assert block["rows"]["seed_intake_plan"][0]["candidate_symbols"] == []


def test_auto_order_readiness_block_backfills_seed_metrics_for_legacy_frequency_plan():
    payload = {
        "auto_order_readiness": {
            "execution_evidence_maintenance": {
                "status": "COMPLETE",
                "reason": "execution_evidence_refreshed",
                "target_market": "ASX",
                "target_portfolio_id": "ASX:asx_top_quality",
                "submit_orders": False,
            },
            "summary": {
                "status": "blocked",
                "portfolio_count": 1,
                "blocked_count": 1,
                "primary_block_reason": "preflight_stale",
                "submit_plan": {
                    "status": "BLOCKED",
                    "reason": "no_single_safe_submit_candidate",
                    "frontier_candidates": [
                        {
                            "market": "US",
                            "portfolio_id": "US:watchlist",
                            "planned_order_symbols": "SPLG",
                            "submit_quality_status": "PASS",
                            "hard_blocks": ["gateway_budget_degraded"],
                        }
                    ],
                },
                "frequency_plan": {
                    "status": "frontier_blocked",
                    "reason": "preflight_stale",
                    "seed_proposal_count": 0,
                    "manual_seed_proposal_count": 0,
                    "seed_proposal_markets": [],
                    "does_not_change_submit_decision": True,
                },
            },
            "rows": [
                {
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "hard_blocks": ["gateway_budget_degraded"],
                    "hard_block_details": [
                        {
                            "reason": "gateway_budget_degraded",
                            "detail": (
                                "market=US reason=gateway_request_budget_exceeded "
                                "projected_recovery_at=2026-06-12T23:59:59+00:00"
                            ),
                        }
                    ],
                }
            ],
        },
        "watchlist_expansion_summary": {
            "seed_proposals": [
                {"market": "ASX", "auto_apply": False},
                {"market": "HK", "auto_apply": False},
            ],
            "seed_intake_plan": [
                {
                    "market": "ASX",
                    "intake_status": "MANUAL_REVIEW_REQUIRED",
                    "source_candidate_count": 2,
                },
                {
                    "market": "HK",
                    "intake_status": "MANUAL_REVIEW_REQUIRED",
                    "source_candidate_count": 2,
                },
            ],
        },
    }

    block = _by_id(build_dashboard_v2_blocks(payload))["auto_order_readiness"]

    assert block["metrics"]["frequency_seed_proposal_count"] == 2
    assert block["metrics"]["frequency_manual_seed_proposal_count"] == 2
    assert block["metrics"]["frequency_seed_proposal_markets"] == ["ASX", "HK"]
    assert block["metrics"]["frequency_seed_intake_plan_count"] == 2
    assert block["metrics"]["frequency_seed_source_candidate_count"] == 4
    assert block["metrics"]["frequency_seed_source_markets"] == ["ASX", "HK"]
    assert block["metrics"]["frequency_plan_does_not_change_submit_decision"] == 1
    assert block["metrics"]["recovery_plan_status"] == "wait_gateway_budget"
    assert block["metrics"]["recovery_plan_target_portfolio_id"] == "US:watchlist"
    assert block["metrics"]["recovery_plan_target_symbols"] == "SPLG"
    assert block["metrics"]["recovery_plan_gateway_refresh_portfolio_limit"] == 1
    assert block["metrics"]["recovery_plan_does_not_submit_orders"] == 1
    assert block["metrics"]["recovery_eligibility_active"] == 1
    assert block["metrics"]["recovery_eligibility_eligible"] == 0
    assert block["metrics"]["recovery_eligibility_reason"] in {
        "gateway_budget_recovery_not_reached",
        "gateway_budget_evidence_refresh_required",
    }
    assert block["metrics"]["execution_evidence_maintenance_status"] == "COMPLETE"
    assert (
        block["metrics"]["execution_evidence_maintenance_reason"]
        == "execution_evidence_refreshed"
    )
    assert block["metrics"]["execution_evidence_maintenance_target_market"] == "ASX"
    assert (
        block["metrics"]["execution_evidence_maintenance_target_portfolio_id"]
        == "ASX:asx_top_quality"
    )
    assert block["metrics"]["execution_evidence_maintenance_submit_orders"] == 0
    assert block["rows"]["frequency_plan"]["seed_source_candidate_count"] == 4
    assert (
        block["rows"]["recovery_plan"]["gateway_budget_projected_recovery_at"]
        == "2026-06-12T23:59:59+00:00"
    )
    assert block["rows"]["recovery_plan"]["steps"][0]["submit_orders"] is False
    assert block["rows"]["execution_evidence_maintenance"]["status"] == "COMPLETE"


def test_evidence_focus_actions_block_keeps_sample_collection_non_warning():
    payload = {
        "evidence_focus_actions": [
            {
                "market": "HK",
                "primary_action": "collect_more_outcome_samples",
                "priority_order": 60,
            },
            "legacy malformed row",
        ]
    }

    block = _by_id(build_dashboard_v2_blocks(payload))["evidence_focus_actions"]

    assert block["id"] == "evidence_focus_actions"
    assert block["status"] == "ok"
    assert block["metrics"]["focus_action_count"] == 1
    assert block["metrics"]["urgent_action_count"] == 0
    assert block["metrics"]["sample_collection_count"] == 1
    assert block["metrics"]["primary_market"] == "HK"
    assert block["rows"]["summary"]["primary_action"] == "collect_more_outcome_samples"
    assert block["rows"]["actions"][0]["market"] == "HK"


def test_auto_order_readiness_block_warns_when_submit_plan_not_ready():
    payload = {
        "auto_order_readiness": {
            "summary": {
                "status": "ready",
                "ready_count": 1,
                "blocked_count": 0,
                "submit_plan": {
                    "status": "BLOCKED",
                    "ready": False,
                    "reason": "no_single_safe_submit_candidate",
                    "frontier_candidates": [{"portfolio_id": "US:watchlist"}],
                },
                "frequency_plan": {
                    "status": "candidate_supply_gap",
                    "reason": "no_safe_submit_candidate_with_seed_proposals",
                    "primary_action": "create_or_refresh_preferred_asset_seed_watchlist",
                    "seed_proposal_count": 1,
                    "manual_seed_proposal_count": 1,
                    "seed_proposal_markets": ["US"],
                    "does_not_change_submit_decision": True,
                },
            }
        }
    }

    block = _by_id(build_dashboard_v2_blocks(payload))["auto_order_readiness"]

    assert block["status"] == "warn"
    assert block["metrics"]["submit_plan_status"] == "BLOCKED"
    assert block["metrics"]["submit_plan_reason"] == "no_single_safe_submit_candidate"
    assert block["metrics"]["frontier_candidate_count"] == 1
    assert block["metrics"]["candidate_supply_status"] == "candidate_supply_gap"
    assert block["metrics"]["frequency_seed_proposal_markets"] == ["US"]


def test_auto_order_readiness_block_warns_on_stale_readiness_health():
    payload = {
        "auto_order_readiness": {
            "summary": {
                "status": "ready",
                "summary_text": "ready single candidate",
                "ready_count": 1,
                "blocked_count": 0,
                "submit_plan": {
                    "status": "READY_SINGLE_CANDIDATE",
                    "ready": True,
                    "selected_portfolio_id": "US:watchlist",
                },
            }
        },
        "auto_order_readiness_health": {
            "status": "warning",
            "reason": "older_than_gateway_budget",
            "summary_text": "自动下单证据过旧: older_than_gateway_budget",
            "generated_at": "2026-05-27T04:24:31+00:00",
            "age_hours": 23.59,
            "max_age_hours": 168,
            "gateway_budget_generated_at": "2026-05-28T23:04:35+00:00",
            "older_than_gateway_budget": True,
            "secondary_reasons": ["older_than_gateway_budget"],
        },
    }

    block = _by_id(build_dashboard_v2_blocks(payload))["auto_order_readiness"]

    assert block["status"] == "warn"
    assert block["metrics"]["readiness_health_status"] == "warning"
    assert block["metrics"]["readiness_health_reason"] == "older_than_gateway_budget"
    assert block["metrics"]["readiness_age_hours"] == 23.59
    assert block["metrics"]["readiness_older_than_gateway_budget"] == 1
    assert "自动下单证据过旧" in block["summary"]
