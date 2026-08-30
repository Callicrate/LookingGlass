"""Verify archive scope, runtime assets, and an isolated wheel installation."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
from email.parser import Parser
from email.policy import default
from pathlib import Path, PurePosixPath
from tarfile import open as open_tar
from tempfile import TemporaryDirectory
from zipfile import ZipFile

FORBIDDEN_SDIST_PATHS = (
    "/progress/",
    "/.murmuration/",
    "/.github/",
    "/critical-reviews/",
    "/current-status.md",
)
SDIST_EXCLUDED_PREFIXES = (
    ".github/",
    ".murmuration/",
    "critical-reviews/",
    "progress/",
)
SDIST_EXCLUDED_FILES = {"current-status.md"}
RUNTIME_ASSET_DIRECTORIES = (
    Path("async_api_view/storage/migrations"),
    Path("async_api_view/web/templates"),
    Path("async_api_view/web/static"),
)
EXTERNAL_PACKAGE_FILES = {
    "async_api_view/docs/architecture.md": Path("docs/architecture.md"),
}


def _unsafe_archive_path(name: str) -> bool:
    parsed = PurePosixPath(name)
    return (
        not name
        or parsed.is_absolute()
        or ".." in parsed.parts
        or "\\" in name
        or (bool(parsed.parts) and ":" in parsed.parts[0])
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
    for archive_name, source_path in EXTERNAL_PACKAGE_FILES.items():
        if not source_path.is_file():
            raise RuntimeError(f"external runtime asset is missing: {source_path}")
        assets.add(archive_name)
    if not assets or not package_root.is_dir():
        raise RuntimeError("runtime asset source directories are unavailable")
    return frozenset(assets)


def expected_package_sources(source_root: Path = Path("src")) -> dict[str, Path]:
    """Map every expected wheel package path to its current authoritative source."""

    package_root = source_root / "async_api_view"
    if not package_root.is_dir():
        raise RuntimeError("runtime package source directory is unavailable")
    sources = {
        path.relative_to(source_root).as_posix(): path
        for path in package_root.rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    }
    for archive_name, source_path in EXTERNAL_PACKAGE_FILES.items():
        if not source_path.is_file():
            raise RuntimeError(f"external package source is missing: {source_path}")
        sources[archive_name] = source_path
    return sources


def _canonical_requirement(value: str) -> str:
    compact = value.replace(" ", "")
    match = re.fullmatch(r"([A-Za-z0-9][A-Za-z0-9._-]*)(.*)", compact)
    if match is None or ";" in compact or "[" in compact:
        raise RuntimeError(f"unsupported project requirement syntax: {value}")
    name = re.sub(r"[-_.]+", "-", match.group(1)).casefold()
    specifiers = match.group(2)
    normalized_specifiers = ",".join(sorted(filter(None, specifiers.split(","))))
    return f"{name}{normalized_specifiers}"


def verify_wheel_metadata(wheel_archive: Path) -> None:
    """Bind wheel identity, dependency declarations, and long description to source metadata."""

    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))["project"]
    with ZipFile(wheel_archive) as archive:
        metadata_names = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_names) != 1:
            raise RuntimeError(f"expected one wheel METADATA file, found {len(metadata_names)}")
        metadata_text = archive.read(metadata_names[0]).decode("utf-8")
    headers, separator, description = metadata_text.partition("\n\n")
    if not separator:
        raise RuntimeError("wheel METADATA has no long-description boundary")
    metadata = Parser(policy=default).parsestr(f"{headers}\n\n")
    expected_dependencies = sorted(_canonical_requirement(item) for item in project["dependencies"])
    actual_dependencies = sorted(
        _canonical_requirement(item) for item in metadata.get_all("Requires-Dist", [])
    )
    expected_python = ",".join(
        sorted(filter(None, str(project["requires-python"]).replace(" ", "").split(",")))
    )
    actual_python = ",".join(
        sorted(filter(None, str(metadata.get("Requires-Python", "")).replace(" ", "").split(",")))
    )
    if (
        metadata.get("Name") != project["name"]
        or metadata.get("Version") != project["version"]
        or metadata.get("Summary") != project["description"]
        or actual_python != expected_python
        or actual_dependencies != expected_dependencies
    ):
        raise RuntimeError("wheel METADATA does not match pyproject.toml")
    if description != Path("README.md").read_text(encoding="utf-8"):
        raise RuntimeError("wheel long description does not match README.md")


def verify_wheel_record(wheel_archive: Path) -> None:
    """Verify the complete wheel RECORD manifest, sizes, and SHA-256 digests."""

    with ZipFile(wheel_archive) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)) or any(_unsafe_archive_path(name) for name in names):
            raise RuntimeError("wheel contains a duplicate or unsafe archive path")
        record_names = [name for name in names if name.endswith(".dist-info/RECORD")]
        if len(record_names) != 1:
            raise RuntimeError(f"expected one wheel RECORD file, found {len(record_names)}")
        record_name = record_names[0]
        rows = list(csv.reader(io.StringIO(archive.read(record_name).decode("utf-8"))))
        if any(len(row) != 3 for row in rows):
            raise RuntimeError("wheel RECORD contains a malformed row")
        records = {row[0]: (row[1], row[2]) for row in rows}
        if len(records) != len(rows) or set(records) != set(names):
            raise RuntimeError("wheel RECORD manifest does not match archive entries")
        for name in names:
            digest, size = records[name]
            if name == record_name:
                if digest or size:
                    raise RuntimeError("wheel RECORD must not hash itself")
                continue
            content = archive.read(name)
            expected_digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).rstrip(
                b"="
            )
            if digest != f"sha256={expected_digest.decode('ascii')}" or size != str(len(content)):
                raise RuntimeError(f"wheel RECORD digest mismatch: {name}")


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
            if name in EXTERNAL_PACKAGE_FILES
            or any(
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
            if archive.read(name)
            != EXTERNAL_PACKAGE_FILES.get(name, source_root / name).read_bytes()
        ]
        if mismatched:
            raise RuntimeError(f"wheel runtime asset content mismatch: {mismatched}")
    return len(wheel_names)


def verify_wheel_package_files(
    wheel_archive: Path,
    expected_sources: dict[str, Path],
) -> None:
    """Require one exact, source-current package tree inside the wheel."""

    with ZipFile(wheel_archive) as archive:
        actual = {name for name in archive.namelist() if name.startswith("async_api_view/")}
        expected = set(expected_sources)
        if actual != expected:
            raise RuntimeError(
                "wheel package manifest mismatch: "
                f"missing={sorted(expected - actual)}, unexpected={sorted(actual - expected)}"
            )
        mismatched = [
            name
            for name, source_path in sorted(expected_sources.items())
            if archive.read(name) != source_path.read_bytes()
        ]
        if mismatched:
            raise RuntimeError(f"wheel package content mismatch: {mismatched}")


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


def expected_sdist_files() -> frozenset[str]:
    """Return the intended tracked/unignored source manifest after explicit exclusions."""

    executable = shutil.which("git")
    if executable is None:
        raise RuntimeError("git is required for exact source-distribution verification")
    result = subprocess.run(  # noqa: S603 - absolute git and fixed read-only arguments
        [
            str(Path(executable).absolute()),
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ],
        check=True,
        capture_output=True,
        cwd=Path.cwd().resolve(),
        timeout=30,
    )
    files: set[str] = set()
    for raw_name in result.stdout.split(b"\0"):
        if not raw_name:
            continue
        name = os.fsdecode(raw_name).replace("\\", "/")
        if _unsafe_archive_path(name):
            raise RuntimeError(f"repository contains an unsafe source path: {name}")
        if name in SDIST_EXCLUDED_FILES or name.startswith(SDIST_EXCLUDED_PREFIXES):
            continue
        if Path(*PurePosixPath(name).parts).is_file():
            files.add(name)
    files.add("PKG-INFO")
    return frozenset(files)


def verify_sdist_source_files(
    source_archive: Path,
    wheel_archive: Path,
    expected_files: frozenset[str] | None = None,
) -> None:
    """Require every included sdist source byte and generated metadata byte to be current."""

    prefix = f"{source_archive.name.removesuffix('.tar.gz')}/"
    with ZipFile(wheel_archive) as wheel:
        metadata_names = [name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")]
        if len(metadata_names) != 1:
            raise RuntimeError(f"expected one wheel METADATA file, found {len(metadata_names)}")
        wheel_metadata = wheel.read(metadata_names[0])
    with open_tar(source_archive) as archive:
        actual_files: set[str] = set()
        for member in archive.getmembers():
            if not member.name.startswith(prefix):
                raise RuntimeError(f"sdist entry is outside its project root: {member.name}")
            relative_name = member.name.removeprefix(prefix)
            relative_parts = PurePosixPath(relative_name).parts
            if _unsafe_archive_path(relative_name):
                raise RuntimeError(f"sdist entry has an unsafe path: {member.name}")
            if member.isdir():
                continue
            if not member.isfile():
                raise RuntimeError(f"sdist contains an unsafe member type: {member.name}")
            if relative_name in actual_files:
                raise RuntimeError(f"sdist contains a duplicate file: {relative_name}")
            actual_files.add(relative_name)
            relative = Path(*relative_parts)
            stream = archive.extractfile(member)
            if stream is None:
                raise RuntimeError(f"sdist file could not be read: {member.name}")
            content = stream.read()
            if relative_name == "PKG-INFO":
                if content != wheel_metadata:
                    raise RuntimeError("sdist PKG-INFO does not match wheel METADATA")
                continue
            if not relative.is_file() or content != relative.read_bytes():
                raise RuntimeError(f"sdist source content mismatch: {relative_name}")
    expected = expected_sdist_files() if expected_files is None else expected_files
    if actual_files != expected:
        raise RuntimeError(
            "sdist source manifest mismatch: "
            f"missing={sorted(expected - actual_files)}, "
            f"unexpected={sorted(actual_files - expected)}"
        )


def forbidden_source_entries(source_names: list[str]) -> tuple[str, ...]:
    return tuple(
        name
        for name in source_names
        if any(forbidden in name for forbidden in FORBIDDEN_SDIST_PATHS)
    )


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
            for command in (
                "init-config",
                "export-docs",
                "fingerprint-profile",
                "authority-list",
                "authority-retire",
                "authority-unretire",
                "doctor",
                "backup",
                "serve",
            )
        ):
            raise RuntimeError("installed CLI help is missing required commands")
        config = Path(temporary) / "rookery.toml"
        architecture = Path(temporary) / "rookery-architecture.md"
        for command in (
            (str(cli), "init-config", "--output", str(config)),
            (str(cli), "export-docs", "--output", str(architecture)),
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
        rejected = subprocess.run(  # noqa: S603 - installed wheel entry point, fixed command
            [str(cli), "--config", str(config), "init"],
            check=False,
            capture_output=True,
            cwd=temporary,
            env=process_environment,
            text=True,
            timeout=30,
        )
        database = Path(temporary) / ".local" / "rookery.sqlite3"
        if (
            rejected.returncode != 2
            or database.exists()
            or "fingerprint-profile" not in (rejected.stdout + rejected.stderr)
        ):
            raise RuntimeError("installed CLI accepted the placeholder authority fingerprint")
        config.write_text(
            config.read_text(encoding="utf-8").replace("0" * 64, "1" * 64),
            encoding="utf-8",
            newline="\n",
        )
        subprocess.run(  # noqa: S603 - installed wheel entry point, fixed command
            [str(cli), "--config", str(config), "init"],
            check=True,
            capture_output=True,
            cwd=temporary,
            env=process_environment,
            text=True,
            timeout=30,
        )
        if (
            not config.is_file()
            or architecture.read_bytes() != (project_root / "docs" / "architecture.md").read_bytes()
            or not database.is_file()
        ):
            raise RuntimeError("installed CLI could not complete checkout-free initialization")


def verify_sdist_rebuild(source_archive: Path, wheel_archive: Path) -> None:
    """Rebuild the sdist under the constrained backend graph and require identical wheel bytes."""

    project_root = Path.cwd().resolve()
    process_environment = {
        key: value
        for key, value in os.environ.items()
        if key.upper() not in {"PYTHONHOME", "PYTHONPATH"}
    }
    process_environment["PYTHONNOUSERSITE"] = "1"
    with TemporaryDirectory(prefix="rookery-sdist-rebuild-") as temporary:
        output = Path(temporary) / "dist"
        subprocess.run(  # noqa: S603 - absolute uv and fixed build arguments
            [
                str(_uv_executable()),
                "build",
                "--wheel",
                "--out-dir",
                str(output),
                "--no-create-gitignore",
                "--build-constraint",
                str((project_root / "build-constraints.txt").resolve()),
                "--require-hashes",
                str(source_archive.resolve()),
            ],
            check=True,
            capture_output=True,
            cwd=temporary,
            env=process_environment,
            text=True,
            timeout=180,
        )
        rebuilt = _single_archive(output, "*.whl", "sdist-rebuilt wheel")
        if rebuilt.read_bytes() != wheel_archive.read_bytes():
            raise RuntimeError("sdist rebuild does not reproduce the directly built wheel")


def main() -> None:
    distribution_dir = Path("dist")
    source_archive = _single_archive(distribution_dir, "*.tar.gz", "source")
    wheel_archive = _single_archive(distribution_dir, "*.whl", "wheel")
    _verify_archive_versions(source_archive, wheel_archive)
    with open_tar(source_archive) as archive:
        source_names = archive.getnames()
    leaked = forbidden_source_entries(source_names)
    if leaked:
        raise RuntimeError(f"source distribution contains workspace-only files: {leaked}")
    verify_sdist_source_files(source_archive, wheel_archive)
    expected_assets = expected_runtime_assets()
    expected_sources = expected_package_sources()
    wheel_entry_count = verify_wheel_runtime_assets(wheel_archive, expected_assets)
    verify_wheel_package_files(wheel_archive, expected_sources)
    verify_wheel_metadata(wheel_archive)
    verify_wheel_record(wheel_archive)
    verify_sdist_rebuild(source_archive, wheel_archive)
    smoke_installed_wheel(wheel_archive, expected_assets)
    print(
        f"Verified {source_archive.name} ({len(source_names)} entries), "
        f"{wheel_archive.name} ({wheel_entry_count} entries), "
        f"and {len(expected_assets)} installed runtime assets."
    )


if __name__ == "__main__":
    main()
