# Change Archive: Strategy Suggestion Control Audit Linkage

Date: 2026-05-09

## Summary

Extended dashboard control audit linkage from evidence actions to strategy parameter suggestions.

This is the second P0 step after `weekly_strategy_parameter_suggestions`: operator/dashboard actions can now carry a `strategy_parameter_suggestion_id` and persist a sanitized audit link without applying configuration automatically.

## Supported Payload Fields

Dashboard control payloads may now include:

- `strategy_parameter_suggestion_id`
- `primary_field` or `strategy_parameter_field`
- `config_path` or `strategy_parameter_config_path`
- `market`
- `portfolio_id`
- `resolution_status`
- `resolution_note`

Resolution status continues to use:

- `ACKNOWLEDGED`
- `APPLIED`
- `REJECTED`
- `SUPERSEDED`

Unknown statuses still degrade to `ACKNOWLEDGED`.

## Outputs

Sanitized audit rows can now include:

- `linked_strategy_parameter_suggestion_id`
- `linked_strategy_parameter_field`
- `linked_strategy_parameter_config_path`
- `linked_market`
- `linked_portfolio_id`
- `resolution_status`
- `resolution_note`

Dashboard v2 control blocks now expose:

- `linked_strategy_parameter_suggestion_history_count`
- `last_linked_strategy_parameter_suggestion_id`
- `last_linked_strategy_parameter_field`
- `last_strategy_parameter_resolution_status`

The advanced dashboard control audit table also displays linked strategy suggestion id and field.

## Guardrails

- This linkage is audit-only; dashboard control still does not write strategy config.
- Existing `linked_evidence_action_id` behavior remains compatible.
- Resolution note sanitization continues to redact account ids, tokens, secrets, user paths, and volume paths.

## Validation

Targeted tests:

- `tests/test_dashboard_control_audit.py`
- `tests/test_dashboard_blocks.py`
- `tests/test_generate_dashboard_helpers.py`
- `tests/test_dashboard_rendering.py`
