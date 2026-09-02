from importlib.util import module_from_spec, spec_from_file_location
from io import BytesIO
from pathlib import Path
from tarfile import TarInfo
from tarfile import open as open_tar
from zipfile import ZipFile

import pytest

_SCRIPT = Path(__file__).parents[1] / "scripts" / "verify_distribution.py"
_SPEC = spec_from_file_location("lookingglass_verify_distribution", _SCRIPT)
if _SPEC is None or _SPEC.loader is None:  # pragma: no cover - test environment invariant
    raise RuntimeError("distribution verifier could not be loaded")
_VERIFIER = module_from_spec(_SPEC)
_SPEC.loader.exec_module(_VERIFIER)
expected_runtime_assets = _VERIFIER.expected_runtime_assets
forbidden_source_entries = _VERIFIER.forbidden_source_entries
verify_wheel_runtime_assets = _VERIFIER.verify_wheel_runtime_assets
verify_wheel_package_files = _VERIFIER.verify_wheel_package_files
verify_wheel_metadata = _VERIFIER.verify_wheel_metadata
verify_wheel_record = _VERIFIER.verify_wheel_record
verify_sdist_source_files = _VERIFIER.verify_sdist_source_files


def test_wheel_asset_verification_rejects_missing_and_unexpected_files(tmp_path) -> None:
    expected = expected_runtime_assets()
    app_javascript = "lookingglass/web/static/app.js"
    assert app_javascript in expected
    missing_archive = tmp_path / "missing.whl"
    with ZipFile(missing_archive, "w") as archive:
        for name in expected - {app_javascript}:
            archive.writestr(name, b"")
    with pytest.raises(RuntimeError, match=r"app\.js"):
        verify_wheel_runtime_assets(missing_archive, expected)

    unexpected_archive = tmp_path / "unexpected.whl"
    with ZipFile(unexpected_archive, "w") as archive:
        for name in expected | {"lookingglass/web/static/stale.js"}:
            archive.writestr(name, b"")
    with pytest.raises(RuntimeError, match=r"stale\.js"):
        verify_wheel_runtime_assets(unexpected_archive, expected)


def test_runtime_manifest_rejects_a_missing_declared_directory(tmp_path) -> None:
    source_root = tmp_path / "src"
    for relative_directory in (
        "lookingglass/storage/migrations",
        "lookingglass/web/templates",
    ):
        directory = source_root / relative_directory
        directory.mkdir(parents=True)
        (directory / "placeholder.txt").write_text("present", encoding="utf-8")

    with pytest.raises(RuntimeError, match=r"web[\\/]static"):
        expected_runtime_assets(source_root)


def test_wheel_asset_verification_rejects_stale_content(tmp_path) -> None:
    source_root = tmp_path / "src"
    expected: set[str] = set()
    for relative_directory in (
        "lookingglass/storage/migrations",
        "lookingglass/web/templates",
        "lookingglass/web/static",
    ):
        directory = source_root / relative_directory
        directory.mkdir(parents=True)
        asset = directory / "asset.txt"
        asset.write_text("current", encoding="utf-8")
        expected.add(asset.relative_to(source_root).as_posix())
    archive_path = tmp_path / "stale.whl"
    with ZipFile(archive_path, "w") as archive:
        for name in expected:
            archive.writestr(name, b"stale")

    with pytest.raises(RuntimeError, match="content mismatch"):
        verify_wheel_runtime_assets(archive_path, frozenset(expected), source_root)


def test_source_distribution_rejects_workspace_review_surfaces() -> None:
    assert forbidden_source_entries(
        [
            "lookingglass-0.2.0/README.md",
            "lookingglass-0.2.0/.coverage-wsl-final",
            "lookingglass-0.2.0/critical-reviews/20260830-010345.md",
            "lookingglass-0.2.0/coverage-report-wsl.json",
        ]
    ) == (
        "lookingglass-0.2.0/.coverage-wsl-final",
        "lookingglass-0.2.0/critical-reviews/20260830-010345.md",
        "lookingglass-0.2.0/coverage-report-wsl.json",
    )


def test_wheel_package_verification_rejects_stale_or_unexpected_python(tmp_path) -> None:
    current = tmp_path / "current.py"
    current.write_text("CURRENT = True\n", encoding="utf-8")
    wheel = tmp_path / "stale.whl"
    with ZipFile(wheel, "w") as archive:
        archive.writestr("lookingglass/module.py", b"CURRENT = False\n")
    with pytest.raises(RuntimeError, match="content mismatch"):
        verify_wheel_package_files(wheel, {"lookingglass/module.py": current})

    with ZipFile(wheel, "w") as archive:
        archive.writestr("lookingglass/module.py", current.read_bytes())
        archive.writestr("lookingglass/stale.py", b"")
    with pytest.raises(RuntimeError, match=r"stale\.py"):
        verify_wheel_package_files(wheel, {"lookingglass/module.py": current})


def test_wheel_metadata_rejects_missing_runtime_dependencies(tmp_path) -> None:
    wheel = tmp_path / "metadata.whl"
    metadata = """Metadata-Version: 2.5
Name: lookingglass
Version: 0.2.0
Summary: LookingGlass local canonical state and refresh service for remote systems
Requires-Python: <3.13,>=3.12

"""
    with ZipFile(wheel, "w") as archive:
        archive.writestr(
            "lookingglass-0.2.0.dist-info/METADATA",
            metadata.encode() + Path("README.md").read_bytes(),
        )

    with pytest.raises(RuntimeError, match="METADATA"):
        verify_wheel_metadata(wheel)


def test_wheel_record_rejects_a_stale_digest(tmp_path) -> None:
    wheel = tmp_path / "record.whl"
    record_name = "lookingglass-0.2.0.dist-info/RECORD"
    with ZipFile(wheel, "w") as archive:
        archive.writestr("lookingglass/module.py", b"current")
        archive.writestr(
            record_name,
            f"lookingglass/module.py,sha256=stale,7\n{record_name},,\n",
        )

    with pytest.raises(RuntimeError, match="digest mismatch"):
        verify_wheel_record(wheel)


def test_sdist_verification_rejects_stale_included_source(tmp_path) -> None:
    source_archive = tmp_path / "lookingglass-0.2.0.tar.gz"
    wheel = tmp_path / "lookingglass-0.2.0-py3-none-any.whl"
    with ZipFile(wheel, "w") as archive:
        archive.writestr("lookingglass-0.2.0.dist-info/METADATA", b"metadata")
    with open_tar(source_archive, "w:gz") as archive:
        stale = b"stale"
        source = TarInfo("lookingglass-0.2.0/README.md")
        source.size = len(stale)
        archive.addfile(source, BytesIO(stale))
        metadata = TarInfo("lookingglass-0.2.0/PKG-INFO")
        metadata.size = len(b"metadata")
        archive.addfile(metadata, BytesIO(b"metadata"))

    with pytest.raises(RuntimeError, match=r"README\.md"):
        verify_sdist_source_files(
            source_archive,
            wheel,
            frozenset({"README.md", "PKG-INFO"}),
        )


def test_sdist_verification_rejects_missing_and_unsafe_members(tmp_path) -> None:
    source_archive = tmp_path / "lookingglass-0.2.0.tar.gz"
    wheel = tmp_path / "lookingglass-0.2.0-py3-none-any.whl"
    with ZipFile(wheel, "w") as archive:
        archive.writestr("lookingglass-0.2.0.dist-info/METADATA", b"metadata")
    with open_tar(source_archive, "w:gz") as archive:
        metadata = TarInfo("lookingglass-0.2.0/PKG-INFO")
        metadata.size = len(b"metadata")
        archive.addfile(metadata, BytesIO(b"metadata"))

    with pytest.raises(RuntimeError, match="manifest mismatch"):
        verify_sdist_source_files(
            source_archive,
            wheel,
            frozenset({"README.md", "PKG-INFO"}),
        )

    with open_tar(source_archive, "w:gz") as archive:
        unsafe = TarInfo("lookingglass-0.2.0/link")
        unsafe.type = b"2"
        unsafe.linkname = "../../outside"
        archive.addfile(unsafe)

    with pytest.raises(RuntimeError, match="unsafe member type"):
        verify_sdist_source_files(source_archive, wheel, frozenset())

    with open_tar(source_archive, "w:gz") as archive:
        unsafe_directory = TarInfo("lookingglass-0.2.0/../../outside")
        unsafe_directory.type = b"5"
        archive.addfile(unsafe_directory)

    with pytest.raises(RuntimeError, match="unsafe path"):
        verify_sdist_source_files(source_archive, wheel, frozenset())
