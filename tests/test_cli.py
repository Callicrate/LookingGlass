import secrets
import select
import socket
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest

from async_api_view import cli
from async_api_view.config import AppSettings, ProjectSettings, load_settings
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


def test_init_config_creates_loadable_template_without_overwrite(tmp_path: Path) -> None:
    output = tmp_path / "standalone" / "rookery.toml"

    created = cli.main(
        [
            "--config",
            str(tmp_path / "missing-is-ignored.toml"),
            "init-config",
            "--output",
            str(output),
        ]
    )
    original = output.read_bytes()
    refused = cli.main(["init-config", "--output", str(output)])

    assert created == 0
    assert refused == 2
    assert original.startswith(b"[app]\n")
    assert output.read_bytes() == original
    assert output.read_text(encoding="utf-8") == Path("config.example.toml").read_text(
        encoding="utf-8"
    )
    settings = load_settings(output)
    assert settings.app.database_path == output.parent / ".local" / "rookery.sqlite3"
    assert settings.databricks_systems[0].profile == "YOUR_PROFILE"


def test_racing_init_config_writers_publish_one_complete_template(tmp_path: Path) -> None:
    output = tmp_path / "racing" / "rookery.toml"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(
            executor.map(
                lambda _index: cli.main(["init-config", "--output", str(output)]),
                range(2),
            )
        )

    assert sorted(results) == [0, 2]
    assert output.read_text(encoding="utf-8") == Path("config.example.toml").read_text(
        encoding="utf-8"
    )


def test_export_docs_is_checkout_current_and_never_overwrites(tmp_path: Path) -> None:
    output = tmp_path / "standalone" / "architecture.md"

    created = cli.main(
        [
            "--config",
            str(tmp_path / "missing-is-ignored.toml"),
            "export-docs",
            "--output",
            str(output),
        ]
    )
    original = output.read_bytes()
    refused = cli.main(["export-docs", "--output", str(output)])

    assert created == 0
    assert refused == 2
    assert original == Path("docs/architecture.md").read_bytes()
    assert output.read_bytes() == original


def test_init_rejects_missing_config(tmp_path: Path) -> None:
    result = cli.main(["--config", str(tmp_path / "missing.toml"), "init"])

    assert result == 2


def test_init_rejects_invalid_profile_before_database_creation(tmp_path: Path) -> None:
    database = tmp_path / "state.sqlite3"
    config = tmp_path / "invalid-profile.toml"
    config.write_text(
        f"""
[app]
database_path = "{database.as_posix()}"

[[databricks]]
name = "workspace"
profile = "-bad"
workspace_root = "/"
""",
        encoding="utf-8",
    )

    assert cli.main(["--config", str(config), "init"]) == 2
    assert not database.exists()


@pytest.mark.parametrize("command", ["init", "run-once", "serve"])
@pytest.mark.parametrize("corruption", ["bytes", "schema"])
def test_database_commands_fail_cleanly_on_incompatible_sqlite_state(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    command: str,
    corruption: str,
) -> None:
    database = tmp_path / f"{command}-{corruption}.sqlite3"
    config = tmp_path / f"{command}-{corruption}.toml"
    config.write_text(
        f'[app]\ndatabase_path = "{database.as_posix()}"\n',
        encoding="utf-8",
    )
    if corruption == "bytes":
        database.write_bytes(b"not a SQLite database")
    else:
        malformed = sqlite3.connect(database)
        try:
            malformed.execute("CREATE TABLE schema_migrations (wrong_column TEXT)")
            malformed.commit()
        finally:
            malformed.close()

    argv = ["--config", str(config), command]
    if command == "serve":
        argv.append("--allow-redirected-activation")
    result = cli.main(argv)

    assert result == 2
    assert "local SQLite state could not be opened or updated" in caplog.text
    moved = database.with_suffix(".moved")
    database.rename(moved)
    assert moved.is_file()


@pytest.mark.parametrize("command", ["init", "run-once", "serve"])
def test_database_commands_reject_foreign_sqlite_without_mutation_or_sidecars(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    command: str,
) -> None:
    database = tmp_path / f"foreign-{command}.sqlite3"
    config = tmp_path / f"foreign-{command}.toml"
    foreign = sqlite3.connect(database)
    try:
        foreign.execute("CREATE TABLE foreign_state (value TEXT NOT NULL)")
        foreign.execute("INSERT INTO foreign_state VALUES ('preserve me')")
        foreign.commit()
        assert foreign.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    finally:
        foreign.close()
    original = database.read_bytes()
    config.write_text(
        f'[app]\ndatabase_path = "{database.as_posix()}"\n',
        encoding="utf-8",
    )
    argv = ["--config", str(config), command]
    if command == "serve":
        argv.append("--allow-redirected-activation")

    assert cli.main(argv) == 2

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "/bootstrap#" not in captured.err
    assert database.read_bytes() == original
    assert not Path(f"{database}-wal").exists()
    assert not Path(f"{database}-shm").exists()
    check = sqlite3.connect(database)
    try:
        assert check.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
        assert check.execute("SELECT value FROM foreign_state").fetchone()[0] == "preserve me"
    finally:
        check.close()


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

    class FakeListener:
        closed = False

        def close(self) -> None:
            self.closed = True

    listeners = [FakeListener(), FakeListener()]
    monkeypatch.setattr(
        cli,
        "_reserve_loopback_sockets",
        lambda _port, *, backlog: listeners,
    )

    def fail_server(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("port unavailable")

    monkeypatch.setattr(cli.uvicorn.Server, "run", fail_server)

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
    assert all(listener.closed for listener in listeners)


def test_loopback_reservation_owns_both_address_families() -> None:
    listeners = cli._reserve_loopback_sockets(0, backlog=1)
    try:
        port = int(listeners[0].getsockname()[1])
        assert {listener.family for listener in listeners} == {
            socket.AF_INET,
            socket.AF_INET6,
        }
        assert {int(listener.getsockname()[1]) for listener in listeners} == {port}
        for listener in listeners:
            if sys.platform == "win32":
                assert listener.getsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE) == 1
                assert listener.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR) == 0
            else:
                assert listener.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR) == 1
            if listener.family == socket.AF_INET6:
                assert listener.getsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY) == 1

        endpoints = (
            (socket.AF_INET, "127.0.0.1"),
            (socket.AF_INET6, "::1"),
        )
        option_sets = [(), ((socket.SOL_SOCKET, socket.SO_REUSEADDR, 1),)]
        reuse_port = getattr(socket, "SO_REUSEPORT", None)
        if reuse_port is not None:
            option_sets.append(((socket.SOL_SOCKET, reuse_port, 1),))
        for family, host in endpoints:
            for options in option_sets:
                competitor = socket.socket(family, socket.SOCK_STREAM)
                try:
                    if family == socket.AF_INET6:
                        competitor.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
                    for level, option, value in options:
                        competitor.setsockopt(level, option, value)
                    with pytest.raises(OSError):
                        competitor.bind((host, port))
                finally:
                    competitor.close()
    finally:
        for listener in listeners:
            listener.close()


@pytest.mark.skipif(sys.platform != "win32", reason="Windows wildcard precedence contract")
def test_windows_wildcard_listener_cannot_intercept_owned_loopback() -> None:
    listeners = cli._reserve_loopback_sockets(0, backlog=1)
    port = int(listeners[0].getsockname()[1])
    wildcards: list[socket.socket] = []
    try:
        for family, host in ((socket.AF_INET, "0.0.0.0"), (socket.AF_INET6, "::")):
            wildcard = socket.socket(family, socket.SOCK_STREAM)
            if family == socket.AF_INET6:
                wildcard.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            wildcard.bind((host, port))
            wildcard.listen(1)
            wildcards.append(wildcard)

        for exact, wildcard, host in zip(
            listeners,
            wildcards,
            ("127.0.0.1", "::1"),
            strict=True,
        ):
            client = socket.create_connection((host, port), timeout=2)
            try:
                readable, _writable, _errors = select.select([exact, wildcard], [], [], 2)
                assert readable == [exact]
                accepted, _address = exact.accept()
                accepted.close()
            finally:
                client.close()
    finally:
        for listener in listeners + wildcards:
            listener.close()


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX restart contract")
def test_posix_loopback_reservation_restarts_after_server_side_close() -> None:
    listeners = cli._reserve_loopback_sockets(0, backlog=1)
    port = int(listeners[0].getsockname()[1])
    client = socket.create_connection(("127.0.0.1", port), timeout=2)
    accepted, _address = listeners[0].accept()
    try:
        accepted.close()
        assert client.recv(1) == b""
    finally:
        client.close()
        for listener in listeners:
            listener.close()

    restarted = cli._reserve_loopback_sockets(port, backlog=1)
    for listener in restarted:
        listener.close()


def test_serve_fails_before_activation_disclosure_when_ipv6_port_is_occupied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    blocker = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    try:
        blocker.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        try:
            blocker.bind(("::1", 0))
        except OSError:
            pytest.skip("IPv6 loopback is unavailable")
        blocker.listen(1)
        port = int(blocker.getsockname()[1])
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            probe.bind(("127.0.0.1", port))
        except OSError:
            pytest.skip("selected IPv6 port is unavailable on IPv4")
        finally:
            probe.close()

        class FakeStore:
            closed = False

            def close(self) -> None:
                self.closed = True

        store = FakeStore()
        bootstrap_token = secrets.token_urlsafe(32)
        authorizer = LocalCallerAuthorizer(bootstrap_token=bootstrap_token)
        runtime = SimpleNamespace(app=object(), store=store, local_authorizer=authorizer)
        settings = ProjectSettings(
            app=AppSettings(database_path=tmp_path / "state.sqlite3", port=port),
            databricks_systems=(),
        )
        monkeypatch.setattr(cli, "_load", lambda _path: settings)
        monkeypatch.setattr(cli, "build_runtime", lambda _settings: runtime)

        result = cli.main(["serve", "--allow-redirected-activation"])

        assert result == 2
        assert store.closed
        captured = capsys.readouterr()
        assert captured.out == ""
        assert "/bootstrap#" not in captured.err
        assert authorizer.take_bootstrap_token() == bootstrap_token

        released_ipv4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            released_ipv4.bind(("127.0.0.1", port))
        finally:
            released_ipv4.close()
    finally:
        blocker.close()


def test_serve_refuses_to_disclose_activation_to_redirected_stdout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(cli.sys.stdout, "isatty", lambda: False)
    monkeypatch.setattr(
        cli,
        "_load",
        lambda _path: pytest.fail("redirected serve loaded configuration"),
    )
    monkeypatch.setattr(
        cli,
        "build_runtime",
        lambda _settings: pytest.fail("redirected serve built the runtime"),
    )

    result = cli.main(["serve"])

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert "--allow-redirected-activation" in caplog.text
    assert "/bootstrap#" not in captured.err + caplog.text


def test_redirected_serve_does_not_apply_configuration_rotation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "state.sqlite3"
    config = tmp_path / "config.toml"

    def write_root(root: str) -> None:
        config.write_text(
            f"""
[app]
database_path = "{database.as_posix()}"

[[databricks]]
id = "workspace"
name = "workspace"
profile = "TEST_PROFILE"
workspace_root = "{root}"
""",
            encoding="utf-8",
        )

    write_root("/Old")
    assert cli.main(["--config", str(config), "init"]) == 0
    with sqlite3.connect(database) as before_connection:
        before = tuple(
            before_connection.execute(
                "SELECT system_id, display_name, enabled FROM systems ORDER BY system_id"
            ).fetchall()
        )
    write_root("/New")
    monkeypatch.setattr(cli.sys.stdout, "isatty", lambda: False)

    assert cli.main(["--config", str(config), "serve"]) == 2

    with sqlite3.connect(database) as after_connection:
        after = tuple(
            after_connection.execute(
                "SELECT system_id, display_name, enabled FROM systems ORDER BY system_id"
            ).fetchall()
        )
    assert after == before


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


def test_run_once_reports_bounded_incompletion_and_resumes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FakeStore:
        close_calls = 0

        def close(self) -> None:
            self.close_calls += 1

    class FakeCoordinator:
        calls = 0

        async def run_once(self) -> object | None:
            self.calls += 1
            return object() if self.calls <= 10_001 else None

    class FakeWorker:
        calls = 0

        async def startup(self) -> None:
            return None

        async def run_once(self) -> bool:
            self.calls += 1
            return False

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

    assert cli.main(["run-once", "--max-cycles", "10000"]) == 3
    assert coordinator.calls == 10_000
    assert worker.calls == 10_000
    assert "eligible work may remain" in caplog.text

    assert cli.main(["run-once", "--max-cycles", "10000"]) == 0
    assert coordinator.calls == 10_002
    assert worker.calls == 10_002
    assert store.close_calls == 2


@pytest.mark.parametrize("value", ["0", "1000001", "not-an-integer"])
def test_run_once_rejects_invalid_cycle_limits(value: str) -> None:
    with pytest.raises(SystemExit):
        cli.main(["run-once", "--max-cycles", value])
