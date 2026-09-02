import json
import os
import subprocess
import sys
import time
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_script(name: str, filename: str):
    spec = spec_from_file_location(name, Path(__file__).parents[1] / "scripts" / filename)
    if spec is None or spec.loader is None:  # pragma: no cover - test environment invariant
        raise RuntimeError(f"could not load {filename}")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_COVERAGE = _load_script("lookingglass_check_coverage", "check_coverage.py")
_DISTRIBUTION = _load_script("lookingglass_verify_distribution_unit", "verify_distribution.py")
validate_coverage_totals = _COVERAGE.validate_coverage_totals
locked_installed_requirements = _DISTRIBUTION.locked_installed_requirements
publish_release_evidence = _DISTRIBUTION.publish_release_evidence
untracked_release_sources = _DISTRIBUTION.untracked_release_sources
validate_installed_audit = _DISTRIBUTION.validate_installed_audit
run_owned = _DISTRIBUTION._run_owned


def test_locked_installed_requirements_excludes_local_wheel_and_requires_exact_versions() -> None:
    frozen = """
lookingglass @ file:///tmp/lookingglass-0.2.0-py3-none-any.whl
uvicorn==0.52.4
fastapi==0.141.1
"""

    assert locked_installed_requirements(frozen) == (
        "fastapi==0.141.1",
        "uvicorn==0.52.4",
    )

    with pytest.raises(RuntimeError, match="unpinned requirement"):
        locked_installed_requirements("fastapi>=0.115")


def test_dirty_release_verification_preserves_existing_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    distribution = tmp_path / "dist"
    distribution.mkdir()
    source = distribution / "lookingglass-0.2.0.tar.gz"
    wheel = distribution / "lookingglass-0.2.0-py3-none-any.whl"
    constraints = tmp_path / "runtime-constraints.txt"
    published_constraints = distribution / "runtime-constraints.txt"
    manifest = distribution / "lookingglass-0.2.0-clean-SHA256SUMS.txt"
    for path, content in (
        (source, b"source"),
        (wheel, b"wheel"),
        (constraints, b"new constraints"),
        (published_constraints, b"published constraints"),
        (manifest, b"clean manifest"),
    ):
        path.write_bytes(content)
    before = {path.name: path.read_bytes() for path in distribution.iterdir()}
    monkeypatch.delenv("GITHUB_SHA", raising=False)
    monkeypatch.setattr(_DISTRIBUTION.shutil, "which", lambda _name: "C:/git.exe")
    monkeypatch.setattr(
        _DISTRIBUTION,
        "_run_owned",
        lambda *_args, **_kwargs: SimpleNamespace(stdout=b" M current-status.md\n"),
    )

    assert publish_release_evidence(source, wheel, constraints) is None

    after = {path.name: path.read_bytes() for path in distribution.iterdir()}
    assert after == before


def test_untracked_package_source_is_never_release_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    generated = tmp_path / "src" / "lookingglass" / "generated.py"
    generated.parent.mkdir(parents=True)
    generated.write_text("GENERATED = True\n", encoding="utf-8")
    progress = tmp_path / "progress" / "checkpoint.md"
    progress.parent.mkdir()
    progress.write_text("workspace only\n", encoding="utf-8")
    monkeypatch.setattr(
        _DISTRIBUTION,
        "_git_file_names",
        lambda *_arguments: (
            "src/lookingglass/generated.py",
            "progress/checkpoint.md",
        ),
    )

    assert untracked_release_sources() == ("src/lookingglass/generated.py",)


def test_release_publication_rejects_archive_replacement_before_writing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    distribution = tmp_path / "dist"
    distribution.mkdir()
    source = distribution / "lookingglass-0.2.0.tar.gz"
    wheel = distribution / "lookingglass-0.2.0-py3-none-any.whl"
    constraints = tmp_path / "runtime-constraints.txt"
    stale_manifest = distribution / "lookingglass-0.2.0-stale-SHA256SUMS.txt"
    for path, content in (
        (source, b"verified source"),
        (wheel, b"verified wheel"),
        (constraints, b"verified constraints"),
        (stale_manifest, b"stale evidence"),
    ):
        path.write_bytes(content)
    snapshots = {
        source: source.read_bytes(),
        wheel: wheel.read_bytes(),
        constraints: constraints.read_bytes(),
    }
    before = {path.name: path.read_bytes() for path in distribution.iterdir()}
    calls = 0

    def mutate_during_git_check(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return SimpleNamespace(stdout=b"")
        wheel.write_bytes(b"unverified replacement")
        return SimpleNamespace(stdout="f" * 40)

    monkeypatch.delenv("GITHUB_SHA", raising=False)
    monkeypatch.setattr(_DISTRIBUTION.shutil, "which", lambda _name: "C:/git.exe")
    monkeypatch.setattr(_DISTRIBUTION, "_run_owned", mutate_during_git_check)

    with pytest.raises(RuntimeError, match="changed before publication"):
        publish_release_evidence(
            source,
            wheel,
            constraints,
            verified_bytes=snapshots,
        )

    assert {path.name: path.read_bytes() for path in distribution.iterdir()} == (
        before | {wheel.name: b"unverified replacement"}
    )


def test_release_publication_creates_one_commit_qualified_verified_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    distribution = tmp_path / "dist"
    distribution.mkdir()
    source = distribution / "lookingglass-0.2.0.tar.gz"
    wheel = distribution / "lookingglass-0.2.0-py3-none-any.whl"
    constraints = tmp_path / "runtime-constraints.txt"
    for path, content in (
        (source, b"verified source"),
        (wheel, b"verified wheel"),
        (constraints, b"verified constraints"),
    ):
        path.write_bytes(content)
    snapshots = {
        source: source.read_bytes(),
        wheel: wheel.read_bytes(),
        constraints: constraints.read_bytes(),
    }
    calls = 0

    def clean_git_state(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return SimpleNamespace(stdout=b"" if calls == 1 else "a" * 40)

    monkeypatch.delenv("GITHUB_SHA", raising=False)
    monkeypatch.setattr(_DISTRIBUTION.shutil, "which", lambda _name: "C:/git.exe")
    monkeypatch.setattr(_DISTRIBUTION, "_run_owned", clean_git_state)

    manifest = publish_release_evidence(
        source,
        wheel,
        constraints,
        verified_bytes=snapshots,
    )

    assert manifest is not None
    assert manifest.name == "SHA256SUMS.txt"
    assert manifest.parent.name == f"lookingglass-0.2.0-{'a' * 40}-verified"
    assert {path.name for path in manifest.parent.iterdir()} == {
        source.name,
        wheel.name,
        constraints.name,
        manifest.name,
    }
    assert (manifest.parent / source.name).read_bytes() == snapshots[source]
    assert (manifest.parent / wheel.name).read_bytes() == snapshots[wheel]
    assert (manifest.parent / constraints.name).read_bytes() == snapshots[constraints]


def test_owned_verifier_timeout_reaps_delayed_descendant(tmp_path: Path) -> None:
    marker = tmp_path / "orphaned-descendant.txt"
    child = (
        "import time; from pathlib import Path; "
        f"time.sleep(1); Path({str(marker)!r}).write_text('orphan', encoding='utf-8')"
    )
    parent = (
        "import subprocess, sys, time; "
        f"subprocess.Popen([sys.executable, '-c', {child!r}]); time.sleep(30)"
    )

    with pytest.raises(subprocess.TimeoutExpired):
        run_owned(
            [sys.executable, "-c", parent],
            check=True,
            capture_output=True,
            text=True,
            timeout=0.2,
        )
    time.sleep(1.2)

    assert not marker.exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows suspended-process ownership regression")
def test_windows_job_setup_failure_reaps_suspended_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = tmp_path / "suspended-parent-ran.txt"

    def fail_job_setup(_process_id: int) -> int:
        raise OSError("injected verifier Job setup failure")

    monkeypatch.setattr(_DISTRIBUTION, "_create_windows_job", fail_job_setup)
    with pytest.raises(OSError, match="Job setup failure"):
        run_owned(
            [
                sys.executable,
                "-c",
                f"from pathlib import Path; Path({str(marker)!r}).write_text('ran')",
            ],
            check=True,
            timeout=5,
        )
    time.sleep(0.2)

    assert not marker.exists()


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
