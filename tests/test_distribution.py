from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from zipfile import ZipFile

import pytest

_SCRIPT = Path(__file__).parents[1] / "scripts" / "verify_distribution.py"
_SPEC = spec_from_file_location("rookery_verify_distribution", _SCRIPT)
if _SPEC is None or _SPEC.loader is None:  # pragma: no cover - test environment invariant
    raise RuntimeError("distribution verifier could not be loaded")
_VERIFIER = module_from_spec(_SPEC)
_SPEC.loader.exec_module(_VERIFIER)
expected_runtime_assets = _VERIFIER.expected_runtime_assets
verify_wheel_runtime_assets = _VERIFIER.verify_wheel_runtime_assets


def test_wheel_asset_verification_rejects_missing_and_unexpected_files(tmp_path) -> None:
    expected = expected_runtime_assets()
    app_javascript = "async_api_view/web/static/app.js"
    assert app_javascript in expected
    missing_archive = tmp_path / "missing.whl"
    with ZipFile(missing_archive, "w") as archive:
        for name in expected - {app_javascript}:
            archive.writestr(name, b"")
    with pytest.raises(RuntimeError, match=r"app\.js"):
        verify_wheel_runtime_assets(missing_archive, expected)

    unexpected_archive = tmp_path / "unexpected.whl"
    with ZipFile(unexpected_archive, "w") as archive:
        for name in expected | {"async_api_view/web/static/stale.js"}:
            archive.writestr(name, b"")
    with pytest.raises(RuntimeError, match=r"stale\.js"):
        verify_wheel_runtime_assets(unexpected_archive, expected)


def test_runtime_manifest_rejects_a_missing_declared_directory(tmp_path) -> None:
    source_root = tmp_path / "src"
    for relative_directory in (
        "async_api_view/storage/migrations",
        "async_api_view/web/templates",
    ):
        directory = source_root / relative_directory
        directory.mkdir(parents=True)
        (directory / "placeholder.txt").write_text("present", encoding="utf-8")

    with pytest.raises(RuntimeError, match=r"web[\\/]static"):
        expected_runtime_assets(source_root)
