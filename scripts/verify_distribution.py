"""Verify archive scope, runtime assets, and an isolated wheel installation."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path
from tarfile import open as open_tar
from tempfile import TemporaryDirectory
from zipfile import ZipFile

FORBIDDEN_SDIST_PATHS = (
    "/progress/",
    "/.murmuration/",
    "/.github/",
    "/current-status.md",
)
RUNTIME_ASSET_DIRECTORIES = (
    Path("async_api_view/storage/migrations"),
    Path("async_api_view/web/templates"),
    Path("async_api_view/web/static"),
)


def expected_runtime_assets(source_root: Path = Path("src")) -> frozenset[str]:
    package_root = source_root / "async_api_view"
    assets: set[str] = set()
    for relative_directory in RUNTIME_ASSET_DIRECTORIES:
        source_directory = source_root / relative_directory
        if not source_directory.is_dir():
            raise RuntimeError(f"runtime asset directory is missing: {relative_directory}")
        directory_assets = {
            path.relative_to(source_root).as_posix()
            for path in source_directory.rglob("*")
            if path.is_file() and "__pycache__" not in path.parts
        }
        if not directory_assets:
            raise RuntimeError(f"runtime asset directory is empty: {relative_directory}")
        assets.update(directory_assets)
    if not assets or not package_root.is_dir():
        raise RuntimeError("runtime asset source directories are unavailable")
    return frozenset(assets)


def verify_wheel_runtime_assets(
    wheel_archive: Path,
    expected_assets: frozenset[str],
    source_root: Path = Path("src"),
) -> int:
    with ZipFile(wheel_archive) as archive:
        wheel_names = set(archive.namelist())
        actual_assets = {
            name
            for name in wheel_names
            if any(
                name == directory.as_posix() or name.startswith(f"{directory.as_posix()}/")
                for directory in RUNTIME_ASSET_DIRECTORIES
            )
        }
        missing = expected_assets - actual_assets
        unexpected = actual_assets - expected_assets
        if missing or unexpected:
            raise RuntimeError(
                "wheel runtime asset manifest mismatch: "
                f"missing={sorted(missing)}, unexpected={sorted(unexpected)}"
            )
        mismatched = [
            name
            for name in sorted(expected_assets)
            if archive.read(name) != (source_root / name).read_bytes()
        ]
        if mismatched:
            raise RuntimeError(f"wheel runtime asset content mismatch: {mismatched}")
    return len(wheel_names)


def _single_archive(distribution_dir: Path, pattern: str, label: str) -> Path:
    archives = tuple(sorted(distribution_dir.glob(pattern)))
    if len(archives) != 1:
        raise RuntimeError(f"expected exactly one {label} archive, found {len(archives)}")
    return archives[0]


def _verify_archive_versions(source_archive: Path, wheel_archive: Path) -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))["project"]
    prefix = f"{project['name'].replace('-', '_')}-{project['version']}"
    if (
        source_archive.name != f"{prefix}.tar.gz"
        or not wheel_archive.name.startswith(f"{prefix}-")
        or wheel_archive.suffix != ".whl"
    ):
        raise RuntimeError(f"distribution archives do not match project version {prefix}")


def _venv_python(environment: Path) -> Path:
    return environment / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")


def _venv_cli(environment: Path) -> Path:
    return environment / (
        "Scripts/async-api-view.exe" if sys.platform == "win32" else "bin/async-api-view"
    )


def _uv_executable() -> Path:
    executable = shutil.which("uv")
    if executable is None:
        raise RuntimeError("uv is required for isolated wheel verification")
    return Path(executable).absolute()


def locked_installed_requirements(freeze_output: str) -> tuple[str, ...]:
    """Return exact third-party requirements from one isolated wheel environment."""

    installed: list[str] = []
    for line in freeze_output.splitlines():
        requirement = line.strip()
        if not requirement:
            continue
        local_name = requirement.split("@", 1)[0].strip().casefold().replace("_", "-")
        if local_name == "async-api-view":
            continue
        name, separator, version = requirement.partition("==")
        if (
            separator != "=="
            or not name
            or not version
            or any(character.isspace() for character in requirement)
        ):
            raise RuntimeError(f"installed wheel produced an unpinned requirement: {requirement}")
        installed.append(requirement)
    if not installed:
        raise RuntimeError("installed wheel environment has no runtime dependencies")
    return tuple(sorted(installed))


def validate_installed_audit(audit_output: str, expected_packages: int) -> None:
    """Require the exact installed dependency count and a clean vulnerability result."""

    audit = json.loads(audit_output)
    summary = audit.get("summary", {})
    if (
        not isinstance(summary.get("audited_packages"), int)
        or summary["audited_packages"] < expected_packages
        or summary.get("vulnerabilities") != 0
        or summary.get("adverse_statuses") != 0
    ):
        raise RuntimeError(
            "installed wheel environment failed its exact dependency audit: "
            f"expected_packages={expected_packages}, summary={summary}"
        )


def smoke_installed_wheel(
    wheel_archive: Path,
    expected_assets: frozenset[str],
) -> None:
    relative_assets = tuple(
        Path(asset).relative_to("async_api_view").as_posix() for asset in sorted(expected_assets)
    )
    project_root = Path.cwd().resolve()
    with TemporaryDirectory(prefix="rookery-wheel-smoke-") as temporary:
        runtime_constraints = Path(temporary) / "runtime-constraints.txt"
        environment = Path(temporary) / "venv"
        process_environment = {
            key: value
            for key, value in os.environ.items()
            if key.upper() not in {"PYTHONHOME", "PYTHONPATH"}
        }
        process_environment["PYTHONNOUSERSITE"] = "1"
        uv = _uv_executable()
        subprocess.run(  # noqa: S603 - absolute uv from the verified build environment
            [
                str(uv),
                "export",
                "--locked",
                "--no-dev",
                "--no-emit-project",
                "--no-header",
                "--format",
                "requirements.txt",
                "--output-file",
                str(runtime_constraints),
            ],
            check=True,
            capture_output=True,
            cwd=project_root,
            env=process_environment,
            text=True,
            timeout=30,
        )
        constraints_text = runtime_constraints.read_text(encoding="utf-8")
        if "--hash=sha256:" not in constraints_text or "async-api-view==" in constraints_text:
            raise RuntimeError("locked runtime constraints are incomplete")
        subprocess.run(  # noqa: S603 - absolute uv from the verified build environment
            [
                str(uv),
                "venv",
                "--quiet",
                "--python",
                sys.executable,
                str(environment),
            ],
            check=True,
            cwd=temporary,
            env=process_environment,
            timeout=30,
        )
        python = _venv_python(environment)
        subprocess.run(  # noqa: S603 - verified absolute uv and private venv interpreter
            [
                str(uv),
                "pip",
                "install",
                "--quiet",
                "--python",
                str(python),
                "--require-hashes",
                "-r",
                str(runtime_constraints),
            ],
            check=True,
            cwd=temporary,
            env=process_environment,
            timeout=180,
        )
        subprocess.run(  # noqa: S603 - verified absolute uv and private venv interpreter
            [
                str(uv),
                "pip",
                "install",
                "--quiet",
                "--python",
                str(python),
                "--no-deps",
                str(wheel_archive.resolve()),
            ],
            check=True,
            cwd=temporary,
            env=process_environment,
            timeout=180,
        )
        subprocess.run(  # noqa: S603 - verified absolute uv and private venv interpreter
            [str(uv), "pip", "check", "--python", str(python)],
            check=True,
            capture_output=True,
            cwd=temporary,
            env=process_environment,
            text=True,
            timeout=30,
        )
        freeze_result = subprocess.run(  # noqa: S603 - verified absolute uv and interpreter
            [str(uv), "pip", "freeze", "--python", str(python)],
            check=True,
            capture_output=True,
            cwd=temporary,
            env=process_environment,
            text=True,
            timeout=30,
        )
        installed_requirements = locked_installed_requirements(freeze_result.stdout)
        audit_project = Path(temporary) / "audit-project"
        audit_project.mkdir()
        (audit_project / "pyproject.toml").write_text(
            "[project]\n"
            'name = "rookery-installed-audit"\n'
            'version = "0"\n'
            'requires-python = ">=3.12,<3.13"\n'
            f"dependencies = {json.dumps(installed_requirements)}\n",
            encoding="utf-8",
        )
        audit_result = subprocess.run(  # noqa: S603 - absolute uv from verified environment
            [
                str(uv),
                "audit",
                "--project",
                str(audit_project),
                "--python-version",
                f"{sys.version_info.major}.{sys.version_info.minor}",
                "--preview-features",
                "audit-command",
                "--preview-features",
                "json-output",
                "--output-format",
                "json",
            ],
            check=True,
            capture_output=True,
            cwd=temporary,
            env=process_environment,
            text=True,
            timeout=180,
        )
        audit_lock = tomllib.loads((audit_project / "uv.lock").read_text(encoding="utf-8"))
        audited_requirements = {
            f"{package['name'].casefold().replace('_', '-')}=={package['version']}"
            for package in audit_lock.get("package", [])
            if package.get("name") != "rookery-installed-audit"
        }
        expected_requirements = {
            f"{requirement.partition('==')[0].casefold().replace('_', '-')}=="
            f"{requirement.partition('==')[2]}"
            for requirement in installed_requirements
        }
        if not expected_requirements.issubset(audited_requirements):
            raise RuntimeError(
                "installed wheel audit lock does not cover the environment: "
                f"missing={sorted(expected_requirements - audited_requirements)}"
            )
        validate_installed_audit(audit_result.stdout, len(installed_requirements))
        smoke = (
            "from importlib.resources import files; from pathlib import Path; "
            "import async_api_view, async_api_view.composition; "
            f"assets={json.dumps(relative_assets)}; "
            f"venv=Path({json.dumps(str(environment.resolve()))}); "
            "location=Path(async_api_view.__file__).resolve(); "
            "assert location.is_relative_to(venv), location; "
            "root=files('async_api_view'); "
            "missing=[asset for asset in assets "
            "if not root.joinpath(*asset.split('/')).is_file()]; "
            "assert not missing, missing"
        )
        subprocess.run(  # noqa: S603 - absolute interpreter in the private test venv
            [str(python), "-c", smoke],
            check=True,
            cwd=temporary,
            env=process_environment,
            timeout=30,
        )
        cli = _venv_cli(environment)
        help_result = subprocess.run(  # noqa: S603 - local wheel's absolute entry point
            [str(cli), "--help"],
            check=True,
            capture_output=True,
            cwd=temporary,
            env=process_environment,
            text=True,
            timeout=30,
        )
        if not all(
            command in help_result.stdout
            for command in ("init-config", "doctor", "backup", "serve")
        ):
            raise RuntimeError("installed CLI help is missing required commands")
        config = Path(temporary) / "rookery.toml"
        for command in (
            (str(cli), "init-config", "--output", str(config)),
            (str(cli), "--config", str(config), "init"),
        ):
            subprocess.run(  # noqa: S603 - local wheel's absolute entry point
                command,
                check=True,
                capture_output=True,
                cwd=temporary,
                env=process_environment,
                text=True,
                timeout=30,
            )
        if not config.is_file() or not (Path(temporary) / ".local" / "rookery.sqlite3").is_file():
            raise RuntimeError("installed CLI could not complete checkout-free initialization")


def main() -> None:
    distribution_dir = Path("dist")
    source_archive = _single_archive(distribution_dir, "*.tar.gz", "source")
    wheel_archive = _single_archive(distribution_dir, "*.whl", "wheel")
    _verify_archive_versions(source_archive, wheel_archive)
    with open_tar(source_archive) as archive:
        source_names = archive.getnames()
    leaked = [
        name
        for name in source_names
        if any(forbidden in name for forbidden in FORBIDDEN_SDIST_PATHS)
    ]
    if leaked:
        raise RuntimeError(f"source distribution contains workspace-only files: {leaked}")
    expected_assets = expected_runtime_assets()
    wheel_entry_count = verify_wheel_runtime_assets(wheel_archive, expected_assets)
    smoke_installed_wheel(wheel_archive, expected_assets)
    print(
        f"Verified {source_archive.name} ({len(source_names)} entries), "
        f"{wheel_archive.name} ({wheel_entry_count} entries), "
        f"and {len(expected_assets)} installed runtime assets."
    )


if __name__ == "__main__":
    main()
