import importlib
import subprocess
import sys


SUPPORT_MODULES = [
    "src.tools.review_weekly_common_support",
    "src.tools.review_weekly_decision_support",
    "src.tools.review_weekly_execution_support",
    "src.tools.review_weekly_feedback_support",
    "src.tools.review_weekly_governance_support",
    "src.tools.review_weekly_strategy_support",
    "src.tools.review_weekly_output_support",
]


def test_review_weekly_support_modules_import_in_fresh_processes():
    for module_name in SUPPORT_MODULES:
        result = subprocess.run(
            [sys.executable, "-c", f"import importlib; importlib.import_module({module_name!r})"],
            check=False,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"{module_name} failed: {result.stderr}"


def test_review_weekly_decision_support_imports_directly():
    module = importlib.import_module("src.tools.review_weekly_decision_support")

    assert hasattr(module, "_build_weekly_decision_evidence_rows")
    assert hasattr(module, "_build_candidate_model_review_rows")


def test_review_weekly_execution_support_imports_directly():
    module = importlib.import_module("src.tools.review_weekly_execution_support")

    assert hasattr(module, "_build_execution_parent_rows")
    assert hasattr(module, "_build_execution_hotspot_rows")


def test_review_weekly_feedback_support_imports_directly():
    module = importlib.import_module("src.tools.review_weekly_feedback_support")

    assert hasattr(module, "_build_execution_parent_rows")
    assert hasattr(module, "_build_weekly_decision_evidence_rows")


def test_review_weekly_strategy_support_imports_directly():
    module = importlib.import_module("src.tools.review_weekly_strategy_support")

    assert hasattr(module, "_build_attribution_rows")
    assert hasattr(module, "_weekly_strategy_note")
