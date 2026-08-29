import secrets
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from async_api_view import cli
from async_api_view.config import AppSettings, ProjectSettings
from async_api_view.web import LocalCallerAuthorizer


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


def test_backup_command_creates_snapshot_and_refuses_overwrite(tmp_path: Path) -> None:
    database = tmp_path / "state.sqlite3"
    output = tmp_path / "snapshots" / "state.sqlite3"
    config = tmp_path / "config.toml"
    config.write_text(
        f'[app]\ndatabase_path = "{database.as_posix()}"\n',
        encoding="utf-8",
    )
    assert cli.main(["--config", str(config), "init"]) == 0

    created = cli.main(["--config", str(config), "backup", "--output", str(output)])
    refused = cli.main(["--config", str(config), "backup", "--output", str(output)])

    assert created == 0
    assert refused == 2
    assert output.is_file()
    with sqlite3.connect(output) as snapshot:
        assert snapshot.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_serve_closes_runtime_store_when_server_start_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class FakeStore:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    store = FakeStore()
    bootstrap_token = secrets.token_urlsafe(32)
    runtime = SimpleNamespace(
        app=object(),
        store=store,
        local_authorizer=LocalCallerAuthorizer(bootstrap_token=bootstrap_token),
    )
    settings = ProjectSettings(
        app=AppSettings(database_path=tmp_path / "state.sqlite3"),
        databricks_systems=(),
    )
    monkeypatch.setattr(cli, "_load", lambda _path: settings)
    monkeypatch.setattr(cli, "build_runtime", lambda _settings: runtime)

    def fail_server(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("port unavailable")

    monkeypatch.setattr(cli.uvicorn, "run", fail_server)

    result = cli.main(["serve", "--allow-redirected-activation"])

    assert result == 2
    assert store.closed
    captured = capsys.readouterr()
    expected_url = (
        f"http://{runtime.local_authorizer.browser_host}:{settings.app.port}"
        f"/bootstrap#{bootstrap_token}"
    )
    assert expected_url in captured.out
    assert "valid once for 10 minutes" in captured.out
    assert bootstrap_token not in captured.err


def test_serve_refuses_to_disclose_activation_to_redirected_stdout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FakeStore:
        closed = False

        def close(self) -> None:
            self.closed = True

    store = FakeStore()
    runtime = SimpleNamespace(
        app=object(),
        store=store,
        local_authorizer=LocalCallerAuthorizer(),
    )
    settings = ProjectSettings(
        app=AppSettings(database_path=tmp_path / "state.sqlite3"),
        databricks_systems=(),
    )
    monkeypatch.setattr(cli, "_load", lambda _path: settings)
    monkeypatch.setattr(cli, "build_runtime", lambda _settings: runtime)
    monkeypatch.setattr(cli.sys.stdout, "isatty", lambda: False)
    monkeypatch.setattr(
        cli.uvicorn,
        "run",
        lambda *_args, **_kwargs: pytest.fail("server started without activation disclosure"),
    )

    result = cli.main(["serve"])

    captured = capsys.readouterr()
    assert result == 2
    assert store.closed
    assert captured.out == ""
    assert "--allow-redirected-activation" in caplog.text
    assert "/bootstrap#" not in captured.err + caplog.text


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
