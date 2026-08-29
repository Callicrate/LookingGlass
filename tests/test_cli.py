from pathlib import Path
from types import SimpleNamespace

import pytest

from async_api_view import cli
from async_api_view.config import AppSettings, ProjectSettings


def test_init_creates_database_from_local_config(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text(
        f"""
[app]
database_path = "{(tmp_path / "state.sqlite3").as_posix()}"

[[databricks]]
id = "test"
name = "test"
profile = "TEST_PROFILE"
workspace_root = "/"
""",
        encoding="utf-8",
    )

    result = cli.main(["--config", str(config), "init"])

    assert result == 0
    assert (tmp_path / "state.sqlite3").is_file()


def test_init_rejects_missing_config(tmp_path: Path) -> None:
    result = cli.main(["--config", str(tmp_path / "missing.toml"), "init"])

    assert result == 2


def test_serve_closes_runtime_store_when_server_start_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeStore:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    store = FakeStore()
    runtime = SimpleNamespace(app=object(), store=store)
    settings = ProjectSettings(
        app=AppSettings(database_path=tmp_path / "state.sqlite3"),
        databricks_systems=(),
    )
    monkeypatch.setattr(cli, "_load", lambda _path: settings)
    monkeypatch.setattr(cli, "build_runtime", lambda _settings: runtime)

    def fail_server(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("port unavailable")

    monkeypatch.setattr(cli.uvicorn, "run", fail_server)

    result = cli.main(["serve"])

    assert result == 2
    assert store.closed
