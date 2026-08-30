import json
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest


def _load_script(name: str, filename: str):
    spec = spec_from_file_location(name, Path(__file__).parents[1] / "scripts" / filename)
    if spec is None or spec.loader is None:  # pragma: no cover - test environment invariant
        raise RuntimeError(f"could not load {filename}")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_COVERAGE = _load_script("rookery_check_coverage", "check_coverage.py")
_DISTRIBUTION = _load_script("rookery_verify_distribution_unit", "verify_distribution.py")
validate_coverage_totals = _COVERAGE.validate_coverage_totals
locked_installed_requirements = _DISTRIBUTION.locked_installed_requirements
validate_installed_audit = _DISTRIBUTION.validate_installed_audit


def test_locked_installed_requirements_excludes_local_wheel_and_requires_exact_versions() -> None:
    frozen = """
async-api-view @ file:///tmp/async_api_view-0.1.0-py3-none-any.whl
uvicorn==0.52.4
fastapi==0.141.1
"""

    assert locked_installed_requirements(frozen) == (
        "fastapi==0.141.1",
        "uvicorn==0.52.4",
    )

    with pytest.raises(RuntimeError, match="unpinned requirement"):
        locked_installed_requirements("fastapi>=0.115")


@pytest.mark.parametrize(
    "summary",
    [
        {"audited_packages": 1, "vulnerabilities": 0, "adverse_statuses": 0},
        {"audited_packages": 2, "vulnerabilities": 1, "adverse_statuses": 0},
        {"audited_packages": 2, "vulnerabilities": 0, "adverse_statuses": 1},
    ],
)
def test_installed_audit_rejects_count_vulnerability_and_status_mismatches(
    summary: dict[str, int],
) -> None:
    with pytest.raises(RuntimeError, match="exact dependency audit"):
        validate_installed_audit(json.dumps({"summary": summary}), expected_packages=2)

    validate_installed_audit(
        json.dumps(
            {
                "summary": {
                    "audited_packages": 2,
                    "vulnerabilities": 0,
                    "adverse_statuses": 0,
                }
            }
        ),
        expected_packages=2,
    )
    validate_installed_audit(
        json.dumps(
            {
                "summary": {
                    "audited_packages": 3,
                    "vulnerabilities": 0,
                    "adverse_statuses": 0,
                }
            }
        ),
        expected_packages=2,
    )


def test_coverage_floors_distinguish_statement_branch_and_combined_results() -> None:
    assert validate_coverage_totals(
        {
            "covered_lines": 900,
            "num_statements": 1000,
            "covered_branches": 750,
            "num_branches": 1000,
        }
    ) == (90.0, 75.0, 82.5)

    with pytest.raises(RuntimeError, match="branch coverage"):
        validate_coverage_totals(
            {
                "covered_lines": 950,
                "num_statements": 1000,
                "covered_branches": 749,
                "num_branches": 1000,
            }
        )
