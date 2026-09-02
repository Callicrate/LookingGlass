from __future__ import annotations

import os
import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest


@pytest.fixture
def create_directory_redirect() -> Callable[[Path, Path], None]:
    """Create a real directory redirect without requiring Windows symlink privilege."""

    def create(link: Path, target: Path) -> None:
        if os.name != "nt":
            link.symlink_to(target, target_is_directory=True)
            return
        powershell = (
            Path(os.environ["SYSTEMROOT"])
            / "System32"
            / "WindowsPowerShell"
            / "v1.0"
            / "powershell.exe"
        )
        environment = dict(os.environ)
        environment["LOOKINGGLASS_TEST_LINK"] = str(link)
        environment["LOOKINGGLASS_TEST_TARGET"] = str(target)
        result = subprocess.run(  # noqa: S603 - absolute Windows system executable
            (
                str(powershell),
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "$null=New-Item -ItemType Junction "
                "-Path $env:LOOKINGGLASS_TEST_LINK -Target $env:LOOKINGGLASS_TEST_TARGET",
            ),
            check=False,
            capture_output=True,
            env=environment,
            text=True,
        )
        if result.returncode != 0:
            pytest.fail(f"could not create Windows directory junction: {result.stderr}")

    return create
