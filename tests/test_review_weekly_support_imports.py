from __future__ import annotations

import importlib


def test_review_weekly_decision_support_imports_directly():
    module = importlib.import_module("src.tools.review_weekly_decision_support")

    assert hasattr(module, "_build_weekly_decision_evidence_rows")
    assert hasattr(module, "_build_candidate_model_review_rows")


def test_review_weekly_feedback_support_imports_directly():
    module = importlib.import_module("src.tools.review_weekly_feedback_support")

    assert hasattr(module, "_build_execution_parent_rows")
    assert hasattr(module, "_build_weekly_decision_evidence_rows")
