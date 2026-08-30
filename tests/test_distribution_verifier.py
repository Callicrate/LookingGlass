import json

import pytest
from scripts.verify_distribution import (
    locked_installed_requirements,
    validate_installed_audit,
)


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
