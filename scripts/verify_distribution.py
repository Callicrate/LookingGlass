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
import signal
import sqlite3
import subprocess
import sys
import tomllib
from contextlib import closing, suppress
from email.parser import Parser
from email.policy import default
from pathlib import Path, PurePosixPath
from tarfile import open as open_tar
from tempfile import TemporaryDirectory
from zipfile import ZipFile

FORBIDDEN_SDIST_PATHS = (
    "/.coverage",
    "/.pytest_cache/",
    "/.ruff_cache/",
    "/__pycache__/",
    "/progress/",
    "/.murmuration/",
    "/.github/",
    "/critical-reviews/",
    "/current-status.md",
    "/coverage-report",
    "/htmlcov/",
)
SDIST_EXCLUDED_PREFIXES = (
    ".github/",
    ".murmuration/",
    "critical-reviews/",
    "progress/",
)
SDIST_EXCLUDED_FILES = {"current-status.md"}
RUNTIME_ASSET_DIRECTORIES = (
    Path("lookingglass/storage/migrations"),
    Path("lookingglass/web/templates"),
    Path("lookingglass/web/static"),
)
EXTERNAL_PACKAGE_FILES = {
    "lookingglass/docs/architecture.md": Path("docs/architecture.md"),
}


def _create_windows_job(process_id: int) -> int:
    import ctypes
    from ctypes import wintypes

    class IoCounters(ctypes.Structure):
        _fields_ = tuple(
            (name, ctypes.c_ulonglong)
            for name in (
                "read_operations",
                "write_operations",
                "other_operations",
                "read_bytes",
                "write_bytes",
                "other_bytes",
            )
        )

    class BasicLimitInformation(ctypes.Structure):
        _fields_ = (
            ("per_process_user_time", ctypes.c_longlong),
            ("per_job_user_time", ctypes.c_longlong),
            ("limit_flags", wintypes.DWORD),
            ("minimum_working_set", ctypes.c_size_t),
            ("maximum_working_set", ctypes.c_size_t),
            ("active_process_limit", wintypes.DWORD),
            ("affinity", ctypes.c_size_t),
            ("priority_class", wintypes.DWORD),
            ("scheduling_class", wintypes.DWORD),
        )

    class ExtendedLimitInformation(ctypes.Structure):
        _fields_ = (
            ("basic_limit_information", BasicLimitInformation),
            ("io_info", IoCounters),
            ("process_memory_limit", ctypes.c_size_t),
            ("job_memory_limit", ctypes.c_size_t),
            ("peak_process_memory_used", ctypes.c_size_t),
            ("peak_job_memory_used", ctypes.c_size_t),
        )

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.argtypes = (wintypes.LPVOID, wintypes.LPCWSTR)
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.SetInformationJobObject.argtypes = (
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    )
    kernel32.SetInformationJobObject.restype = wintypes.BOOL
    kernel32.AssignProcessToJobObject.argtypes = (wintypes.HANDLE, wintypes.HANDLE)
    kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL
    job = kernel32.CreateJobObjectW(None, None)
    if not job:
        raise OSError(ctypes.get_last_error(), "could not create verifier process job")
    try:
        limits = ExtendedLimitInformation()
        limits.basic_limit_information.limit_flags = 0x00002000
        if not kernel32.SetInformationJobObject(
            job, 9, ctypes.byref(limits), ctypes.sizeof(limits)
        ):
            raise OSError(ctypes.get_last_error(), "could not configure verifier process job")
        process = kernel32.OpenProcess(0x00000101, False, process_id)
        if not process:
            raise OSError(ctypes.get_last_error(), "could not open verifier process")
        try:
            if not kernel32.AssignProcessToJobObject(job, process):
                raise OSError(ctypes.get_last_error(), "could not own verifier process tree")
        finally:
            kernel32.CloseHandle(process)
    except BaseException:
        kernel32.CloseHandle(job)
        raise
    return int(job)


def _resume_windows_process(process_id: int) -> None:
    import ctypes
    from ctypes import wintypes

    class ThreadEntry32(ctypes.Structure):
        _fields_ = (
            ("size", wintypes.DWORD),
            ("usage_count", wintypes.DWORD),
            ("thread_id", wintypes.DWORD),
            ("owner_process_id", wintypes.DWORD),
            ("base_priority", wintypes.LONG),
            ("priority_delta", wintypes.LONG),
            ("flags", wintypes.DWORD),
        )

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateToolhelp32Snapshot.argtypes = (wintypes.DWORD, wintypes.DWORD)
    kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    kernel32.Thread32First.argtypes = (wintypes.HANDLE, ctypes.POINTER(ThreadEntry32))
    kernel32.Thread32First.restype = wintypes.BOOL
    kernel32.Thread32Next.argtypes = (wintypes.HANDLE, ctypes.POINTER(ThreadEntry32))
    kernel32.Thread32Next.restype = wintypes.BOOL
    kernel32.OpenThread.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    kernel32.OpenThread.restype = wintypes.HANDLE
    kernel32.ResumeThread.argtypes = (wintypes.HANDLE,)
    kernel32.ResumeThread.restype = wintypes.DWORD
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL
    snapshot = kernel32.CreateToolhelp32Snapshot(0x00000004, 0)
    if snapshot == ctypes.c_void_p(-1).value:
        raise OSError(ctypes.get_last_error(), "could not enumerate verifier process threads")
    resumed = 0
    try:
        entry = ThreadEntry32()
        entry.size = ctypes.sizeof(entry)
        available = kernel32.Thread32First(snapshot, ctypes.byref(entry))
        while available:
            if entry.owner_process_id == process_id:
                thread = kernel32.OpenThread(0x0002, False, entry.thread_id)
                if not thread:
                    raise OSError(ctypes.get_last_error(), "could not open verifier process thread")
                try:
                    if kernel32.ResumeThread(thread) == 0xFFFFFFFF:
                        raise OSError(ctypes.get_last_error(), "could not resume verifier process")
                    resumed += 1
                finally:
                    kernel32.CloseHandle(thread)
            available = kernel32.Thread32Next(snapshot, ctypes.byref(entry))
    finally:
        kernel32.CloseHandle(snapshot)
    if resumed == 0:
        raise OSError("suspended verifier process had no resumable thread")


def _terminate_owned_process(process: subprocess.Popen, windows_job: int | None) -> None:
    if windows_job is not None:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.TerminateJobObject.argtypes = (wintypes.HANDLE, wintypes.UINT)
        kernel32.TerminateJobObject.restype = wintypes.BOOL
        kernel32.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
        kernel32.WaitForSingleObject.restype = wintypes.DWORD
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL
        try:
            if not kernel32.TerminateJobObject(windows_job, 1):
                raise OSError(ctypes.get_last_error(), "could not terminate verifier process job")
            if kernel32.WaitForSingleObject(windows_job, 5000) != 0:
                raise OSError("verifier process job did not terminate within five seconds")
        finally:
            kernel32.CloseHandle(windows_job)
    else:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
    if process.poll() is None:
        process.kill()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired as exc:
        raise OSError("verifier parent process did not terminate within five seconds") from exc


def _run_owned(
    arguments: list[str],
    *,
    check: bool = False,
    capture_output: bool = False,
    cwd: str | Path | None = None,
    env: dict[str, str] | None = None,
    text: bool = False,
    timeout: float | None = None,
) -> subprocess.CompletedProcess:
    options = {"creationflags": 0x00000004} if os.name == "nt" else {"start_new_session": True}
    process = subprocess.Popen(  # noqa: S603 - fixed structured arguments from callers
        arguments,
        stdout=subprocess.PIPE if capture_output else None,
        stderr=subprocess.PIPE if capture_output else None,
        cwd=cwd,
        env=env,
        text=text,
        **options,
    )
    windows_job: int | None = None
    try:
        if os.name == "nt":
            windows_job = _create_windows_job(process.pid)
            _resume_windows_process(process.pid)
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except BaseException:
            _terminate_owned_process(process, windows_job)
            windows_job = None
            raise
        _terminate_owned_process(process, windows_job)
        windows_job = None
    except BaseException:
        if windows_job is None:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
        raise
    finally:
        if windows_job is not None:
            _terminate_owned_process(process, windows_job)
    result = subprocess.CompletedProcess(arguments, process.returncode, stdout, stderr)
    if check:
        result.check_returncode()
    return result


def _unsafe_archive_path(name: str) -> bool:
    parsed = PurePosixPath(name)
    return (
        not name
        or parsed.is_absolute()
        or ".." in parsed.parts
        or "\\" in name
        or (bool(parsed.parts) and ":" in parsed.parts[0])
    )


def _git_file_names(*arguments: str) -> tuple[str, ...]:
    executable = shutil.which("git")
    if executable is None:
        raise RuntimeError("git is required for exact source-distribution verification")
    result = _run_owned(
        [str(Path(executable).absolute()), "ls-files", "-z", *arguments],
        check=True,
        capture_output=True,
        cwd=Path.cwd().resolve(),
        timeout=30,
    )
    names = tuple(
        os.fsdecode(raw_name).replace("\\", "/")
        for raw_name in result.stdout.split(b"\0")
        if raw_name
    )
    if any(_unsafe_archive_path(name) for name in names):
        raise RuntimeError("repository contains an unsafe source path")
    return names


def untracked_release_sources() -> tuple[str, ...]:
    """Reject untracked files that Hatch could otherwise promote into package authority."""

    return tuple(
        name
        for name in _git_file_names("--others", "--exclude-standard")
        if name not in SDIST_EXCLUDED_FILES
        and not name.startswith(SDIST_EXCLUDED_PREFIXES)
        and Path(*PurePosixPath(name).parts).is_file()
    )


def expected_runtime_assets(source_root: Path = Path("src")) -> frozenset[str]:
    package_root = source_root / "lookingglass"
    assets: set[str] = set()
    for relative_directory in RUNTIME_ASSET_DIRECTORIES:
        source_directory = source_root / relative_directory
        if not source_directory.is_dir():
            raise RuntimeError(f"runtime asset directory is missing: {relative_directory}")
        if source_root == Path("src"):
            prefix = f"src/{relative_directory.as_posix()}/"
            directory_assets = {
                name.removeprefix("src/")
                for name in _git_file_names("--cached")
                if name.startswith(prefix) and Path(name).is_file()
            }
        else:
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

    package_root = source_root / "lookingglass"
    if not package_root.is_dir():
        raise RuntimeError("runtime package source directory is unavailable")
    if source_root == Path("src"):
        sources = {
            name.removeprefix("src/"): Path(name)
            for name in _git_file_names("--cached")
            if name.startswith("src/lookingglass/") and Path(name).is_file()
        }
    else:
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
        actual = {name for name in archive.namelist() if name.startswith("lookingglass/")}
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

    files: set[str] = set()
    for name in _git_file_names("--cached"):
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
        "Scripts/lookingglass.exe" if sys.platform == "win32" else "bin/lookingglass"
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
        if local_name == "lookingglass":
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


def smoke_pinned_cli_environment(
    wheel_archive: Path,
    *,
    uv: Path,
    temporary: str,
    process_environment: dict[str, str],
    runtime_constraints: Path,
    expected_requirements: tuple[str, ...],
) -> None:
    """Exercise documented hash-verified private-environment install and upgrade."""

    def create_environment(environment: Path) -> Path:
        _run_owned(
            [
                str(uv),
                "venv",
                "--quiet",
                "--relocatable",
                "--python",
                "3.12",
                str(environment),
            ],
            check=True,
            capture_output=True,
            cwd=temporary,
            env=process_environment,
            text=True,
            timeout=30,
        )
        python = _venv_python(environment)
        _run_owned(
            [
                str(uv),
                "pip",
                "install",
                "--python",
                str(python),
                "--require-hashes",
                "--no-build",
                "-r",
                str(runtime_constraints),
            ],
            check=True,
            capture_output=True,
            cwd=temporary,
            env=process_environment,
            text=True,
            timeout=180,
        )
        _run_owned(
            [
                str(uv),
                "pip",
                "install",
                "--python",
                str(python),
                "--no-deps",
                str(wheel_archive.resolve()),
            ],
            check=True,
            capture_output=True,
            cwd=temporary,
            env=process_environment,
            text=True,
            timeout=180,
        )
        return python

    def verify_environment(environment: Path) -> None:
        python = _venv_python(environment)
        version = _run_owned(
            [
                str(python),
                "-c",
                "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')",
            ],
            check=True,
            capture_output=True,
            cwd=temporary,
            env=process_environment,
            text=True,
            timeout=30,
        ).stdout.strip()
        if version != "3.12":
            raise RuntimeError(f"documented private environment used Python {version}")
        freeze_result = _run_owned(
            [str(uv), "pip", "freeze", "--python", str(python)],
            check=True,
            capture_output=True,
            cwd=temporary,
            env=process_environment,
            text=True,
            timeout=30,
        )
        installed_requirements = locked_installed_requirements(freeze_result.stdout)
        if installed_requirements != expected_requirements:
            raise RuntimeError(
                "documented private install diverged from the locked runtime graph: "
                f"expected={expected_requirements}, installed={installed_requirements}"
            )
        _run_owned(
            [str(_venv_cli(environment)), "--help"],
            check=True,
            capture_output=True,
            cwd=temporary,
            env=process_environment,
            text=True,
            timeout=30,
        )

    environment = Path(temporary) / "standalone-venv"
    create_environment(environment)
    verify_environment(environment)
    constraints_text = runtime_constraints.read_text(encoding="utf-8")
    click_block = re.search(r"(?ms)^click==.*?(?=^[A-Za-z0-9]|\Z)", constraints_text)
    if click_block is None:
        raise RuntimeError("runtime constraints have no Click hash block")
    corrupted_block = re.sub(
        r"sha256:[0-9a-f]{64}",
        f"sha256:{'0' * 64}",
        click_block.group(0),
    )
    corrupted_constraints = Path(temporary) / "corrupted-runtime-constraints.txt"
    corrupted_constraints.write_text(
        constraints_text.replace(click_block.group(0), corrupted_block),
        encoding="utf-8",
        newline="\n",
    )
    corrupt_environment = Path(temporary) / "corrupt-venv"
    _run_owned(
        [str(uv), "venv", "--quiet", "--python", "3.12", str(corrupt_environment)],
        check=True,
        capture_output=True,
        cwd=temporary,
        env=process_environment,
        text=True,
        timeout=30,
    )
    rejected = _run_owned(
        [
            str(uv),
            "pip",
            "install",
            "--python",
            str(_venv_python(corrupt_environment)),
            "--require-hashes",
            "--no-build",
            "-r",
            str(corrupted_constraints),
        ],
        check=False,
        capture_output=True,
        cwd=temporary,
        env=process_environment,
        text=True,
        timeout=180,
    )
    if rejected.returncode == 0 or "hash" not in (rejected.stdout + rejected.stderr).casefold():
        raise RuntimeError("corrupted runtime dependency hash was not rejected")
    verify_environment(environment)

    prior_wheel = Path(temporary) / "prior_only-1.0-py3-none-any.whl"
    marker = Path(temporary) / "prior-only-startup-marker"
    metadata_root = "prior_only-1.0.dist-info"
    startup = f"import pathlib; pathlib.Path({str(marker)!r}).write_text('ran')\n".encode()
    records = (
        "prior_only.pth,,\n"
        f"{metadata_root}/METADATA,,\n"
        f"{metadata_root}/WHEEL,,\n"
        f"{metadata_root}/RECORD,,\n"
    )
    with ZipFile(prior_wheel, "w") as wheel:
        wheel.writestr("prior_only.pth", startup)
        wheel.writestr(
            f"{metadata_root}/METADATA",
            "Metadata-Version: 2.1\nName: prior-only\nVersion: 1.0\n",
        )
        wheel.writestr(
            f"{metadata_root}/WHEEL",
            "Wheel-Version: 1.0\nGenerator: lookingglass-verifier\n"
            "Root-Is-Purelib: true\nTag: py3-none-any\n",
        )
        wheel.writestr(f"{metadata_root}/RECORD", records)
    _run_owned(
        [
            str(uv),
            "pip",
            "install",
            "--python",
            str(_venv_python(environment)),
            "--no-deps",
            str(prior_wheel),
        ],
        check=True,
        capture_output=True,
        cwd=temporary,
        env=process_environment,
        text=True,
        timeout=30,
    )
    _run_owned(
        [str(_venv_python(environment)), "-c", "pass"],
        check=True,
        cwd=temporary,
        env=process_environment,
        timeout=30,
    )
    if not marker.is_file():
        raise RuntimeError("prior-only startup hook did not execute before upgrade")

    next_environment = Path(temporary) / "standalone-venv-next"
    create_environment(next_environment)
    verify_environment(next_environment)
    marker.unlink()
    previous_environment = Path(temporary) / "standalone-venv-previous"
    environment.rename(previous_environment)
    next_environment.rename(environment)
    verify_environment(environment)
    _run_owned(
        [str(_venv_python(environment)), "-c", "pass"],
        check=True,
        cwd=temporary,
        env=process_environment,
        timeout=30,
    )
    if marker.exists():
        raise RuntimeError("prior-only startup hook survived fresh-environment upgrade")


def smoke_installed_wheel(
    wheel_archive: Path,
    expected_assets: frozenset[str],
) -> None:
    relative_assets = tuple(
        Path(asset).relative_to("lookingglass").as_posix() for asset in sorted(expected_assets)
    )
    project_root = Path.cwd().resolve()
    with TemporaryDirectory(prefix="lookingglass-wheel-smoke-") as temporary:
        runtime_constraints = Path(temporary) / "runtime-constraints.txt"
        environment = Path(temporary) / "venv"
        process_environment = {
            key: value
            for key, value in os.environ.items()
            if key.upper() not in {"PYTHONHOME", "PYTHONPATH"}
        }
        process_environment["PYTHONNOUSERSITE"] = "1"
        uv = _uv_executable()
        _run_owned(
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
        if "--hash=sha256:" not in constraints_text or "lookingglass==" in constraints_text:
            raise RuntimeError("locked runtime constraints are incomplete")
        tracked_constraints = project_root / "runtime-constraints.txt"
        if runtime_constraints.read_bytes() != tracked_constraints.read_bytes():
            raise RuntimeError("tracked runtime constraints do not match uv.lock")
        _run_owned(
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
        _run_owned(
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
        _run_owned(
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
        _run_owned(
            [str(uv), "pip", "check", "--python", str(python)],
            check=True,
            capture_output=True,
            cwd=temporary,
            env=process_environment,
            text=True,
            timeout=30,
        )
        freeze_result = _run_owned(
            [str(uv), "pip", "freeze", "--python", str(python)],
            check=True,
            capture_output=True,
            cwd=temporary,
            env=process_environment,
            text=True,
            timeout=30,
        )
        installed_requirements = locked_installed_requirements(freeze_result.stdout)
        smoke_pinned_cli_environment(
            wheel_archive,
            uv=uv,
            temporary=temporary,
            process_environment=process_environment,
            runtime_constraints=tracked_constraints,
            expected_requirements=installed_requirements,
        )
        audit_project = Path(temporary) / "audit-project"
        audit_project.mkdir()
        (audit_project / "pyproject.toml").write_text(
            "[project]\n"
            'name = "lookingglass-installed-audit"\n'
            'version = "0"\n'
            'requires-python = ">=3.12,<3.13"\n'
            f"dependencies = {json.dumps(installed_requirements)}\n",
            encoding="utf-8",
        )
        audit_result = _run_owned(
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
            if package.get("name") != "lookingglass-installed-audit"
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
            "from datetime import UTC,datetime; from uuid import uuid4; "
            "import lookingglass, lookingglass.composition; "
            "from lookingglass.contracts import ObservationBatch; "
            f"assets={json.dumps(relative_assets)}; "
            f"venv=Path({json.dumps(str(environment.resolve()))}); "
            "location=Path(lookingglass.__file__).resolve(); "
            "assert location.is_relative_to(venv), location; "
            "root=files('lookingglass'); "
            "now=datetime(2026,8,30,tzinfo=UTC); "
            "batch=ObservationBatch(uuid4(),uuid4(),uuid4(),'databricks','1',"
            "now,now,(),(),(),None,'1'); "
            "assert batch.contract_version=='1'; "
            "assert 'observed_at_is_local' not in batch.to_dict(); "
            "missing=[asset for asset in assets "
            "if not root.joinpath(*asset.split('/')).is_file()]; "
            "assert not missing, missing"
        )
        _run_owned(
            [str(python), "-c", smoke],
            check=True,
            cwd=temporary,
            env=process_environment,
            timeout=30,
        )
        cli = _venv_cli(environment)
        help_result = _run_owned(
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
                "init",
                "authority-list",
                "authority-retire",
                "authority-unretire",
                "doctor",
                "run-once",
                "backup",
                "serve",
            )
        ):
            raise RuntimeError("installed CLI help is missing required commands")
        if "LookingGlass" not in help_result.stdout or "lookingglass" not in help_result.stdout:
            raise RuntimeError("installed CLI help does not map the product and command names")
        config = Path(temporary) / "lookingglass.toml"
        architecture = Path(temporary) / "lookingglass-architecture.md"
        for command in (
            (str(cli), "init-config", "--output", str(config)),
            (str(cli), "export-docs", "--output", str(architecture)),
        ):
            _run_owned(
                command,
                check=True,
                capture_output=True,
                cwd=temporary,
                env=process_environment,
                text=True,
                timeout=30,
            )
        rejected = _run_owned(
            [str(cli), "--config", str(config), "init"],
            check=False,
            capture_output=True,
            cwd=temporary,
            env=process_environment,
            text=True,
            timeout=30,
        )
        database = Path(temporary) / ".local" / "lookingglass.sqlite3"
        if (
            rejected.returncode != 1
            or database.exists()
            or "fingerprint-profile" not in (rejected.stdout + rejected.stderr)
        ):
            raise RuntimeError("installed CLI accepted the placeholder authority fingerprint")
        config.write_text(
            config.read_text(encoding="utf-8").replace("0" * 64, "1" * 64),
            encoding="utf-8",
            newline="\n",
        )
        _run_owned(
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
        with closing(sqlite3.connect(database)) as installed_state:
            provenance_rows = installed_state.execute(
                """
                SELECT version, ordinal, content_sha256, content_bytes, chain_sha256, basis
                FROM migration_provenance ORDER BY ordinal
                """
            ).fetchall()
        expected_provenance = _wheel_migration_provenance(wheel_archive)
        if tuple(provenance_rows) != expected_provenance:
            raise RuntimeError("installed migration provenance is incomplete")


def _wheel_migration_provenance(wheel_archive: Path) -> tuple[tuple[object, ...], ...]:
    """Derive deterministic provenance directly from packaged bytes, outside runtime code."""

    prefix = "lookingglass/storage/migrations/"
    manifest_name = f"{prefix}MANIFEST.sha256"
    with ZipFile(wheel_archive) as archive:
        manifest = archive.read(manifest_name).decode("ascii")
        manifest_rows: list[tuple[str, str]] = []
        for line in manifest.splitlines():
            match = re.fullmatch(r"([0-9a-f]{64})  ([0-9]{4}_[a-z0-9_]+\.sql)", line)
            if match is None:
                raise RuntimeError("installed migration manifest is malformed")
            manifest_rows.append((match.group(2), match.group(1)))
        sql_names = sorted(
            name.removeprefix(prefix)
            for name in archive.namelist()
            if name.startswith(prefix) and name.endswith(".sql")
        )
        if sql_names != [name for name, _digest in manifest_rows]:
            raise RuntimeError("installed migration manifest does not match packaged resources")
        previous_chain = bytes(32)
        expected: list[tuple[object, ...]] = []
        for ordinal, (name, manifest_digest) in enumerate(manifest_rows, start=1):
            payload = archive.read(f"{prefix}{name}")
            content_sha256 = hashlib.sha256(payload).hexdigest()
            if content_sha256 != manifest_digest:
                raise RuntimeError("installed migration bytes do not match the packaged manifest")
            version = name.removesuffix(".sql")
            chain_sha256 = hashlib.sha256(
                b"".join(
                    (
                        # Must match the legacy persisted migration-chain domain separator.
                        b"lookingglass-migration-chain-v1\0",
                        previous_chain,
                        ordinal.to_bytes(4, "big"),
                        version.encode("utf-8"),
                        b"\0",
                        bytes.fromhex(content_sha256),
                    )
                )
            ).hexdigest()
            previous_chain = bytes.fromhex(chain_sha256)
            expected.append(
                (version, ordinal, content_sha256, len(payload), chain_sha256, "executed")
            )
    return tuple(expected)


def verify_sdist_rebuild(source_archive: Path, wheel_archive: Path) -> None:
    """Rebuild the sdist under the constrained backend graph and require identical wheel bytes."""

    project_root = Path.cwd().resolve()
    process_environment = {
        key: value
        for key, value in os.environ.items()
        if key.upper() not in {"PYTHONHOME", "PYTHONPATH"}
    }
    process_environment["PYTHONNOUSERSITE"] = "1"
    with TemporaryDirectory(prefix="lookingglass-sdist-rebuild-") as temporary:
        output = Path(temporary) / "dist"
        _run_owned(
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


def publish_release_evidence(
    source_archive: Path,
    wheel_archive: Path,
    runtime_constraints: Path,
    *,
    verified_bytes: dict[Path, bytes] | None = None,
) -> Path | None:
    """Publish exact constraints and a commit-bound checksum manifest for a clean checkout."""

    executable = shutil.which("git")
    if executable is None:
        raise RuntimeError("git is required for release provenance verification")
    git = str(Path(executable).absolute())
    status = _run_owned(
        [git, "status", "--porcelain", "--untracked-files=all"],
        check=True,
        capture_output=True,
        cwd=Path.cwd().resolve(),
        timeout=30,
    ).stdout
    if status.strip():
        if os.environ.get("GITHUB_SHA"):
            raise RuntimeError("CI release verification requires a clean tracked checkout")
        return None
    commit = _run_owned(
        [git, "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        cwd=Path.cwd().resolve(),
        text=True,
        timeout=30,
    ).stdout.strip()
    expected_commit = os.environ.get("GITHUB_SHA")
    if expected_commit and commit != expected_commit:
        raise RuntimeError(
            f"release provenance commit mismatch: expected {expected_commit}, observed {commit}"
        )
    distribution_dir = source_archive.parent
    published_constraints = distribution_dir / "runtime-constraints.txt"
    snapshots = verified_bytes or {
        source_archive: source_archive.read_bytes(),
        wheel_archive: wheel_archive.read_bytes(),
        runtime_constraints: runtime_constraints.read_bytes(),
    }
    for artifact in (source_archive, wheel_archive, runtime_constraints):
        if artifact.read_bytes() != snapshots[artifact]:
            raise RuntimeError("verified release artifact changed before publication")
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))["project"]
    bundle = distribution_dir / f"lookingglass-{project['version']}-{commit}-verified"
    manifest_name = "SHA256SUMS.txt"
    artifact_names = (source_archive.name, wheel_archive.name, published_constraints.name)
    artifact_bytes = (
        snapshots[source_archive],
        snapshots[wheel_archive],
        snapshots[runtime_constraints],
    )
    lines = [
        f"# project={project['name']}",
        f"# version={project['version']}",
        f"# commit={commit}",
    ]
    lines.extend(
        f"{hashlib.sha256(content).hexdigest()}  {name}"
        for name, content in zip(artifact_names, artifact_bytes, strict=True)
    )
    with TemporaryDirectory(prefix=".lookingglass-evidence-", dir=distribution_dir) as temporary:
        temporary_bundle = Path(temporary) / "bundle"
        temporary_bundle.mkdir()
        temporary_source = temporary_bundle / source_archive.name
        temporary_wheel = temporary_bundle / wheel_archive.name
        temporary_constraints = temporary_bundle / published_constraints.name
        temporary_manifest = temporary_bundle / manifest_name
        temporary_source.write_bytes(artifact_bytes[0])
        temporary_wheel.write_bytes(artifact_bytes[1])
        temporary_constraints.write_bytes(artifact_bytes[2])
        temporary_manifest.write_text(
            "\n".join(lines) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        if bundle.exists():
            expected = {
                path.name: path.read_bytes()
                for path in temporary_bundle.iterdir()
                if path.is_file()
            }
            actual = {path.name: path.read_bytes() for path in bundle.iterdir() if path.is_file()}
            if actual != expected:
                raise RuntimeError("existing verified release bundle does not match current bytes")
        else:
            temporary_bundle.replace(bundle)
    published_constraints.write_bytes(artifact_bytes[2])
    for stale in distribution_dir.glob("lookingglass-*-SHA256SUMS.txt"):
        stale.unlink()
    return bundle / manifest_name


def main() -> None:
    untracked = untracked_release_sources()
    if untracked:
        raise RuntimeError(f"untracked release source is not allowed: {list(untracked)}")
    distribution_dir = Path("dist")
    source_archive = _single_archive(distribution_dir, "*.tar.gz", "source")
    wheel_archive = _single_archive(distribution_dir, "*.whl", "wheel")
    runtime_constraints = Path("runtime-constraints.txt")
    verified_bytes = {
        source_archive: source_archive.read_bytes(),
        wheel_archive: wheel_archive.read_bytes(),
        runtime_constraints: runtime_constraints.read_bytes(),
    }
    with TemporaryDirectory(prefix="lookingglass-verify-snapshot-") as temporary:
        snapshot_root = Path(temporary)
        snapshot_source = snapshot_root / source_archive.name
        snapshot_wheel = snapshot_root / wheel_archive.name
        snapshot_source.write_bytes(verified_bytes[source_archive])
        snapshot_wheel.write_bytes(verified_bytes[wheel_archive])
        _verify_archive_versions(snapshot_source, snapshot_wheel)
        with open_tar(snapshot_source) as archive:
            source_names = archive.getnames()
        leaked = forbidden_source_entries(source_names)
        if leaked:
            raise RuntimeError(f"source distribution contains workspace-only files: {leaked}")
        verify_sdist_source_files(snapshot_source, snapshot_wheel)
        expected_assets = expected_runtime_assets()
        expected_sources = expected_package_sources()
        wheel_entry_count = verify_wheel_runtime_assets(snapshot_wheel, expected_assets)
        verify_wheel_package_files(snapshot_wheel, expected_sources)
        verify_wheel_metadata(snapshot_wheel)
        verify_wheel_record(snapshot_wheel)
        verify_sdist_rebuild(snapshot_source, snapshot_wheel)
        smoke_installed_wheel(snapshot_wheel, expected_assets)
        manifest = publish_release_evidence(
            source_archive,
            wheel_archive,
            runtime_constraints,
            verified_bytes=verified_bytes,
        )
    manifest_summary = (
        f"; manifest {manifest.as_posix()}"
        if manifest is not None
        else "; manifest deferred until clean commit"
    )
    print(
        f"Verified {source_archive.name} ({len(source_names)} entries), "
        f"{wheel_archive.name} ({wheel_entry_count} entries), "
        f"and {len(expected_assets)} installed runtime assets"
        f"{manifest_summary}."
    )


if __name__ == "__main__":
    main()
