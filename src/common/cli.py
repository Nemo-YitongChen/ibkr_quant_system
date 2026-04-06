from __future__ import annotations

import argparse
from typing import Sequence


class CliHelpFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
    """Show default values while preserving multi-line help layout."""


def build_cli_parser(
    *,
    description: str,
    command: str = "",
    examples: Sequence[str] | None = None,
    notes: Sequence[str] | None = None,
) -> argparse.ArgumentParser:
    epilog_lines: list[str] = []
    if command:
        epilog_lines.append(f"Command: {command}")

    example_rows = [str(item).strip() for item in list(examples or []) if str(item).strip()]
    if example_rows:
        if epilog_lines:
            epilog_lines.append("")
        epilog_lines.append("Examples:")
        epilog_lines.extend(f"  {row}" for row in example_rows)

    note_rows = ["Relative paths resolve from the repository root by default."]
    note_rows.extend(str(item).strip() for item in list(notes or []) if str(item).strip())
    if note_rows:
        if epilog_lines:
            epilog_lines.append("")
        epilog_lines.append("Notes:")
        epilog_lines.extend(f"  - {row}" for row in note_rows)

    return argparse.ArgumentParser(
        description=description,
        epilog="\n".join(epilog_lines),
        formatter_class=CliHelpFormatter,
    )
