"""Verify that built package archives contain only the intended project surfaces."""

from pathlib import Path
from tarfile import open as open_tar
from zipfile import ZipFile

FORBIDDEN_SDIST_PATHS = (
    "/progress/",
    "/.murmuration/",
    "/.github/",
    "/current-status.md",
)
REQUIRED_WHEEL_PATHS = {
    "async_api_view/storage/migrations/0014_coverage_policy_initialization.sql",
    "async_api_view/web/static/favicon.svg",
    "async_api_view/web/templates/index.html",
}


def main() -> None:
    distribution_dir = Path("dist")
    source_archive = next(distribution_dir.glob("*.tar.gz"))
    wheel_archive = next(distribution_dir.glob("*.whl"))
    with open_tar(source_archive) as archive:
        source_names = archive.getnames()
    leaked = [
        name
        for name in source_names
        if any(forbidden in name for forbidden in FORBIDDEN_SDIST_PATHS)
    ]
    if leaked:
        raise RuntimeError(f"source distribution contains workspace-only files: {leaked}")
    with ZipFile(wheel_archive) as archive:
        wheel_names = set(archive.namelist())
    missing = REQUIRED_WHEEL_PATHS - wheel_names
    if missing:
        raise RuntimeError(f"wheel is missing required runtime assets: {sorted(missing)}")
    print(
        f"Verified {source_archive.name} ({len(source_names)} entries) and "
        f"{wheel_archive.name} ({len(wheel_names)} entries)."
    )


if __name__ == "__main__":
    main()
