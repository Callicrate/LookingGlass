"""Enforce and report statement, branch-only, and combined coverage floors."""

from __future__ import annotations

import json
import sys
from pathlib import Path

STATEMENT_FLOOR = 85.0
BRANCH_FLOOR = 75.0
COMBINED_FLOOR = 80.0
MAX_REPORT_BYTES = 16 * 1024 * 1024


def _percentage(covered: int, total: int) -> float:
    return 100.0 if total == 0 else covered * 100.0 / total


def validate_coverage_totals(totals: object) -> tuple[float, float, float]:
    if not isinstance(totals, dict):
        raise RuntimeError("coverage totals are not an object")
    fields = ("covered_lines", "num_statements", "covered_branches", "num_branches")
    values: dict[str, int] = {}
    for field in fields:
        value = totals.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise RuntimeError(f"coverage total {field} is invalid")
        values[field] = value
    if (
        values["covered_lines"] > values["num_statements"]
        or values["covered_branches"] > values["num_branches"]
    ):
        raise RuntimeError("covered coverage totals exceed measured totals")
    statements = _percentage(values["covered_lines"], values["num_statements"])
    branches = _percentage(values["covered_branches"], values["num_branches"])
    combined = _percentage(
        values["covered_lines"] + values["covered_branches"],
        values["num_statements"] + values["num_branches"],
    )
    if statements < STATEMENT_FLOOR:
        raise RuntimeError(f"statement coverage {statements:.2f}% is below {STATEMENT_FLOOR:.0f}%")
    if branches < BRANCH_FLOOR:
        raise RuntimeError(f"branch coverage {branches:.2f}% is below {BRANCH_FLOOR:.0f}%")
    if combined < COMBINED_FLOOR:
        raise RuntimeError(f"combined coverage {combined:.2f}% is below {COMBINED_FLOOR:.0f}%")
    return statements, branches, combined


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    if len(arguments) != 1:
        raise RuntimeError("usage: check_coverage.py <coverage.json>")
    report = Path(arguments[0])
    if not report.is_file() or report.stat().st_size > MAX_REPORT_BYTES:
        raise RuntimeError("coverage JSON is missing or exceeds the size limit")
    payload = json.loads(report.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("coverage report is not an object")
    statements, branches, combined = validate_coverage_totals(payload.get("totals"))
    print(
        f"Coverage: statements {statements:.2f}% (floor {STATEMENT_FLOOR:.0f}%), "
        f"branches {branches:.2f}% (floor {BRANCH_FLOOR:.0f}%), "
        f"combined {combined:.2f}% (floor {COMBINED_FLOOR:.0f}%)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
