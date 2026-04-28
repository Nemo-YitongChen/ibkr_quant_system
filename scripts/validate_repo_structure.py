from __future__ import annotations

import ast
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]


class ValidationError(Exception):
    pass


def parse_module(path: Path) -> ast.Module:
    try:
        return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except FileNotFoundError as exc:
        raise ValidationError(f"Missing file: {path}") from exc
    except SyntaxError as exc:
        raise ValidationError(f"Syntax error in {path}: {exc}") from exc


def get_class_def(module: ast.Module, class_name: str) -> ast.ClassDef:
    for node in module.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return node
    raise ValidationError(f"Class `{class_name}` not found")


def class_method_names(class_def: ast.ClassDef) -> set[str]:
    return {
        node.name
        for node in class_def.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def validate_storage() -> None:
    path = ROOT / "src" / "common" / "storage.py"
    module = parse_module(path)
    storage_cls = get_class_def(module, "Storage")
    methods = class_method_names(storage_cls)

    required = {"_init_db", "insert_signal_audit", "upsert_md_quality"}
    missing = required - methods
    if missing:
        raise ValidationError(
            f"Storage is missing required methods: {', '.join(sorted(missing))}"
        )


def validate_engine() -> None:
    path = ROOT / "src" / "app" / "engine.py"
    module = parse_module(path)
    engine_cls = get_class_def(module, "TradingEngine")
    methods = class_method_names(engine_cls)

    required = {"_update_quality", "run_forever", "_sleep_with_runner"}
    missing = required - methods
    if missing:
        raise ValidationError(
            f"TradingEngine is missing required methods: {', '.join(sorted(missing))}"
        )


def validate_main() -> None:
    path = ROOT / "src" / "main.py"
    module = parse_module(path)

    found_main = False
    found_intraday_bootstrap_call = False
    for node in ast.walk(module):
        if isinstance(node, ast.FunctionDef) and node.name == "main":
            found_main = True
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id == "run_intraday_engine":
                found_intraday_bootstrap_call = True

    if not found_main:
        raise ValidationError("src/main.py does not define main()")
    if not found_intraday_bootstrap_call:
        raise ValidationError("src/main.py does not dispatch to run_intraday_engine()")


def main() -> int:
    validators = [validate_storage, validate_engine, validate_main]
    errors: list[str] = []

    for validator in validators:
        try:
            validator()
        except ValidationError as exc:
            errors.append(str(exc))

    if errors:
        print("Repository structure validation failed:", file=sys.stderr)
        for err in errors:
            print(f"- {err}", file=sys.stderr)
        return 1

    print("Repository structure validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
