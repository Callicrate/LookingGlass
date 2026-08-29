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


def test_doctor_runs_compatibility_check(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    checked = False
    settings = ProjectSettings(
        app=AppSettings(database_path=tmp_path / "state.sqlite3"),
        databricks_systems=(),
    )

    async def doctor(_settings: ProjectSettings) -> None:
        nonlocal checked
        checked = True

    monkeypatch.setattr(cli, "_load", lambda _path: settings)
    monkeypatch.setattr(cli, "_doctor", doctor)

    assert cli.main(["doctor"]) == 0
    assert checked


def test_run_once_drains_work_and_closes_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeStore:
        closed = False

        def close(self) -> None:
            self.closed = True

    class FakeCoordinator:
        calls = 0

        async def run_once(self) -> object | None:
            self.calls += 1
            return object() if self.calls == 1 else None

    class FakeWorker:
        started = False
        calls = 0

        async def startup(self) -> None:
            self.started = True

        async def run_once(self) -> bool:
            self.calls += 1
            return self.calls == 1

    store = FakeStore()
    coordinator = FakeCoordinator()
    worker = FakeWorker()
    runtime = SimpleNamespace(store=store, coordinator=coordinator, worker=worker)
    settings = ProjectSettings(
        app=AppSettings(database_path=tmp_path / "state.sqlite3"),
        databricks_systems=(),
    )
    monkeypatch.setattr(cli, "_load", lambda _path: settings)
    monkeypatch.setattr(cli, "build_runtime", lambda _settings: runtime)

    assert cli.main(["run-once"]) == 0
    assert worker.started
    assert coordinator.calls == 2
    assert worker.calls == 2
    assert store.closed
