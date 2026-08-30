import json
import logging
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
from async_api_view.local_files import ExclusiveFileLock
from async_api_view.web import LocalCallerAuthorizer


def isolated_serve_port(monkeypatch: pytest.MonkeyPatch) -> int:
    listeners = cli._reserve_loopback_sockets(0, backlog=1)
    port = int(listeners[0].getsockname()[1])

    def use_reserved_listeners(requested_port: int, *, backlog: int) -> list[socket.socket]:
        assert requested_port == port
        assert backlog > 0
        return listeners

    monkeypatch.setattr(cli, "_reserve_loopback_sockets", use_reserved_listeners)
    return port


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
authority_fingerprint = "1111111111111111111111111111111111111111111111111111111111111111"
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
    assert refused == 1
    assert original.startswith(b"[app]\n")
    assert output.read_bytes() == original
    assert output.read_text(encoding="utf-8") == Path("config.example.toml").read_text(
        encoding="utf-8"
    )
    settings = load_settings(output)
    assert settings.app.database_path == output.parent / ".local" / "rookery.sqlite3"
    assert settings.databricks_systems[0].profile == "YOUR_PROFILE"


def test_placeholder_authority_fails_before_database_creation(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = tmp_path / "rookery.toml"
    assert cli.main(["init-config", "--output", str(config)]) == 0

    result = cli.main(["--config", str(config), "init"])

    assert result == 1
    assert "fingerprint-profile" in caplog.text
    assert not (tmp_path / ".local" / "rookery.sqlite3").exists()
    for command in ("authority-list", "authority-retire", "authority-unretire"):
        argv = ["--config", str(config), command]
        if command != "authority-list":
            argv.extend(("--system-id", "11111111-1111-4111-8111-111111111111"))
        assert cli.main(argv) == 1
        assert not (tmp_path / ".local" / "rookery.sqlite3").exists()


def test_racing_init_config_writers_publish_one_complete_template(tmp_path: Path) -> None:
    output = tmp_path / "racing" / "rookery.toml"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(
            executor.map(
                lambda _index: cli.main(["init-config", "--output", str(output)]),
                range(2),
            )
        )

    assert sorted(results) == [0, 1]
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
    assert refused == 1
    assert original == Path("docs/architecture.md").read_bytes()
    assert output.read_bytes() == original


def test_fingerprint_profile_prints_only_digest_without_loading_project(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli,
        "databricks_profile_authority_fingerprint",
        lambda _profile: "a" * 64,
    )
    monkeypatch.setattr(
        cli,
        "_load",
        lambda _path: pytest.fail("fingerprint-profile loaded project configuration"),
    )

    result = cli.main(["fingerprint-profile", "--profile", "TEST_PROFILE"])

    captured = capsys.readouterr()
    assert result == 0
    assert captured.out == f"{'a' * 64}\n"
    assert captured.err == ""


def test_init_rejects_missing_config(tmp_path: Path) -> None:
    result = cli.main(["--config", str(tmp_path / "missing.toml"), "init"])

    assert result == 1


def test_cli_sanitizes_expected_operator_error_text(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    hostile_path = tmp_path / "missing\n\x1b[31m\u202etoken=opaque.toml"

    result = cli.main(["--config", str(hostile_path), "init"])

    assert result == 1
    assert "opaque" not in caplog.text
    assert "\x1b" not in caplog.text
    assert "\u202e" not in caplog.text
    assert str(tmp_path) not in caplog.text
    assert "[local-path]" in caplog.text


def test_doctor_interrupt_is_a_controlled_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = ProjectSettings(
        app=AppSettings(database_path=tmp_path / "state.sqlite3"),
        databricks_systems=(),
    )

    async def interrupted(_settings: ProjectSettings) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "_load", lambda _path: settings)
    monkeypatch.setattr(cli, "_doctor", interrupted)

    assert cli.main(["doctor"]) == 130
    assert "Rookery command interrupted" in caplog.text
    assert "Traceback" not in caplog.text


def test_usage_error_sanitizes_hostile_argument(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as raised:
        cli.main(["--log-level", "\x1b[31m\u202etoken=opaque", "init"])

    captured = capsys.readouterr()
    assert raised.value.code == 2
    assert captured.out == ""
    assert "usage: async-api-view" in captured.err
    assert "opaque" not in captured.err
    assert "\x1b" not in captured.err
    assert "\u202e" not in captured.err
    assert "[redacted]" in captured.err

    with pytest.raises(SystemExit):
        cli.main(["--log-level", "/root/synthetic-user/state.sqlite3", "init"])
    assert "/root/" not in capsys.readouterr().err


@pytest.mark.parametrize(
    "secret_value",
    [
        "ghp_FAKESTANDALONETOKEN123456789",
        '{"DATABRICKS_TOKEN":"FAKE_JSON_SECRET"}',
        "{'DATABRICKS_TOKEN':'FAKE_SINGLE_SECRET'}",
        "/root/synthetic-user/FAKE_state.sqlite3",
    ],
)
def test_cli_redacts_standalone_and_prefixed_json_secrets(
    secret_value: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    assert cli.main(["--config", secret_value, "init"]) == 1
    assert "FAKE" not in caplog.text


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

    assert cli.main(["--config", str(config), "init"]) == 1
    assert not database.exists()


@pytest.mark.parametrize("command", ["init", "run-once", "serve"])
@pytest.mark.parametrize("corruption", ["bytes", "schema"])
def test_database_commands_fail_cleanly_on_incompatible_sqlite_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    command: str,
    corruption: str,
) -> None:
    database = tmp_path / f"{command}-{corruption}.sqlite3"
    config = tmp_path / f"{command}-{corruption}.toml"
    port = isolated_serve_port(monkeypatch) if command == "serve" else None
    config.write_text(
        f'[app]\ndatabase_path = "{database.as_posix()}"\n'
        + (f"port = {port}\n" if port is not None else ""),
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

    assert result == 1
    assert "local SQLite state could not be opened or updated" in caplog.text
    moved = database.with_suffix(".moved")
    database.rename(moved)
    assert moved.is_file()


@pytest.mark.parametrize("command", ["init", "run-once", "serve"])
def test_database_commands_reject_foreign_sqlite_without_mutation_or_sidecars(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
    port = isolated_serve_port(monkeypatch) if command == "serve" else None
    config.write_text(
        f'[app]\ndatabase_path = "{database.as_posix()}"\n'
        + (f"port = {port}\n" if port is not None else ""),
        encoding="utf-8",
    )
    argv = ["--config", str(config), command]
    if command == "serve":
        argv.append("--allow-redirected-activation")

    assert cli.main(argv) == 1

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
    assert refused == 1
    assert output.is_file()
    with sqlite3.connect(output) as snapshot:
        assert snapshot.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_serve_closes_runtime_store_when_server_start_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
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

    def fail_server(server: cli._RookeryServer, **_kwargs: object) -> None:
        assert server.config.access_log is False
        logging.getLogger("uvicorn.error").error(
            "Traceback (most recent call last):\ntoken=opaque C:\\Users\\person\\app.py"
        )
        raise SystemExit(3)

    monkeypatch.setattr(cli.uvicorn.Server, "run", fail_server)

    result = cli.main(["serve", "--allow-redirected-activation"])

    assert result == 1
    assert store.closed
    captured = capsys.readouterr()
    assert captured.out == ""
    assert bootstrap_token not in captured.err
    assert "opaque" not in caplog.text
    assert "Traceback" not in caplog.text
    assert "Rookery application startup failed" in caplog.text
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
        settings = ProjectSettings(
            app=AppSettings(database_path=tmp_path / "state.sqlite3", port=port),
            databricks_systems=(),
        )
        monkeypatch.setattr(cli, "_load", lambda _path: settings)
        monkeypatch.setattr(
            cli,
            "build_runtime",
            lambda _settings: pytest.fail("occupied port built the runtime"),
        )

        result = cli.main(["serve", "--allow-redirected-activation"])

        assert result == 1
        assert not store.closed
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


def test_second_serve_owner_fails_before_runtime_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "private" / "state.sqlite3"
    config = tmp_path / "config.toml"

    def write_config(profile: str) -> None:
        config.write_text(
            f"""
[app]
database_path = "{database.as_posix()}"
port = 8766

[[databricks]]
id = "workspace"
name = "workspace"
profile = "{profile}"
authority_fingerprint = "1111111111111111111111111111111111111111111111111111111111111111"
workspace_root = "/"
""",
            encoding="utf-8",
        )

    write_config("PROFILE_ONE")
    assert cli.main(["--config", str(config), "init"]) == 0
    with sqlite3.connect(database) as connection:
        before = tuple(
            connection.execute(
                "SELECT system_id, display_name, enabled FROM systems ORDER BY system_id"
            ).fetchall()
        )
    write_config("PROFILE_TWO")

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
    monkeypatch.setattr(
        cli,
        "build_runtime",
        lambda _settings: pytest.fail("second serve built the runtime"),
    )
    lock_path = cli._serve_lock_path(database)
    with ExclusiveFileLock(lock_path):
        result = cli.main(
            [
                "--config",
                str(config),
                "serve",
                "--allow-redirected-activation",
            ]
        )

    assert result == 1
    assert all(listener.closed for listener in listeners)
    with sqlite3.connect(database) as connection:
        after = tuple(
            connection.execute(
                "SELECT system_id, display_name, enabled FROM systems ORDER BY system_id"
            ).fetchall()
        )
    assert after == before


@pytest.mark.parametrize("command", ["init", "run-once"])
def test_live_owner_blocks_other_stateful_commands_before_reconciliation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command: str,
) -> None:
    database = tmp_path / "private" / "state.sqlite3"
    config = tmp_path / "config.toml"

    def write_config(fingerprint: str) -> None:
        config.write_text(
            f"""
[app]
database_path = "{database.as_posix()}"

[[databricks]]
id = "workspace"
name = "workspace"
profile = "PROFILE"
authority_fingerprint = "{fingerprint}"
workspace_root = "/"
""",
            encoding="utf-8",
        )

    write_config("1" * 64)
    assert cli.main(["--config", str(config), "init"]) == 0
    with sqlite3.connect(database) as connection:
        before = tuple(connection.execute("SELECT system_id, enabled FROM systems").fetchall())
    write_config("2" * 64)
    monkeypatch.setattr(
        cli,
        "build_runtime",
        lambda _settings: pytest.fail(f"locked {command} built the runtime"),
    )
    argv = ["--config", str(config), command]
    if command == "run-once":
        argv.extend(("--max-cycles", "1"))

    with ExclusiveFileLock(cli._serve_lock_path(database)):
        result = cli.main(argv)

    assert result == 1
    with sqlite3.connect(database) as connection:
        after = tuple(connection.execute("SELECT system_id, enabled FROM systems").fetchall())
    assert after == before


def test_authority_retire_and_unretire_preserve_cache_and_require_reinit(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database = tmp_path / "private" / "state.sqlite3"
    config = tmp_path / "config.toml"
    fingerprint = "1" * 64
    config.write_text(
        f"""
[app]
database_path = "{database.as_posix()}"

[[databricks]]
id = "workspace"
name = "workspace"
profile = "PROFILE"
authority_fingerprint = "{fingerprint}"
workspace_root = "/"
""",
        encoding="utf-8",
    )
    assert cli.main(["--config", str(config), "init"]) == 0
    with sqlite3.connect(database) as connection:
        system_id = connection.execute("SELECT system_id FROM systems").fetchone()[0]
        object_count = connection.execute("SELECT COUNT(*) FROM remote_objects").fetchone()[0]

    assert cli.main(["--config", str(config), "authority-list"]) == 0
    listed = capsys.readouterr()
    assert system_id in listed.out
    assert listed.err == ""
    assert cli.main(["--config", str(config), "authority-retire", "--system-id", system_id]) == 0
    assert cli.main(["--config", str(config), "init"]) == 0
    with sqlite3.connect(database) as connection:
        assert (
            connection.execute(
                "SELECT enabled FROM systems WHERE system_id = ?", (system_id,)
            ).fetchone()[0]
            == 0
        )
        assert connection.execute("SELECT COUNT(*) FROM remote_objects").fetchone()[0] == (
            object_count
        )

    assert cli.main(["--config", str(config), "authority-unretire", "--system-id", system_id]) == 0
    with sqlite3.connect(database) as connection:
        assert (
            connection.execute(
                "SELECT enabled FROM systems WHERE system_id = ?", (system_id,)
            ).fetchone()[0]
            == 0
        )
    assert cli.main(["--config", str(config), "init"]) == 0
    with sqlite3.connect(database) as connection:
        assert (
            connection.execute(
                "SELECT enabled FROM systems WHERE system_id = ?", (system_id,)
            ).fetchone()[0]
            == 1
        )


def test_authority_list_uses_readable_state_and_stdout(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database = tmp_path / "private" / "state.sqlite3"
    config = tmp_path / "config.toml"
    config.write_text(
        f"""
[app]
database_path = "{database.as_posix()}"

[[databricks]]
id = "workspace"
name = "workspace"
profile = "PROFILE"
authority_fingerprint = "{"1" * 64}"
workspace_root = "/"
""",
        encoding="utf-8",
    )
    assert cli.main(["--config", str(config), "init"]) == 0
    with sqlite3.connect(database) as connection:
        system_id = connection.execute("SELECT system_id FROM systems").fetchone()[0]
        connection.execute(
            "UPDATE connection_bindings SET non_secret_settings_json = ?",
            (json.dumps({"workspace_root": "/", "authority_fingerprint": 1}),),
        )
        connection.commit()

    assert cli.main(["--log-level", "WARNING", "--config", str(config), "authority-list"]) == 0

    captured = capsys.readouterr()
    assert system_id in captured.out
    assert "legacy-unverified" in captured.out
    assert captured.err == ""
    assert "Traceback" not in captured.out


def test_local_recovery_commands_survive_invalid_remote_configuration(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database = tmp_path / "private" / "state.sqlite3"
    config = tmp_path / "config.toml"
    backup = tmp_path / "backups" / "recovery.sqlite3"
    config.write_text(
        f"""
[app]
database_path = "{database.as_posix()}"

[[databricks]]
id = "workspace"
name = "workspace"
profile = "PROFILE"
authority_fingerprint = "{"1" * 64}"
workspace_root = "/"
""",
        encoding="utf-8",
    )
    assert cli.main(["--config", str(config), "init"]) == 0
    with sqlite3.connect(database) as connection:
        system_id = connection.execute("SELECT system_id FROM systems").fetchone()[0]

    config.write_text(
        f"""
[app]
database_path = "{database.as_posix()}"

[[databricks]]
name = "semantically-invalid"
""",
        encoding="utf-8",
    )
    assert cli.main(["--config", str(config), "backup", "--output", str(backup)]) == 0
    assert cli.main(["--config", str(config), "authority-list"]) == 0
    assert system_id in capsys.readouterr().out
    assert cli.main(["--config", str(config), "authority-retire", "--system-id", system_id]) == 0
    assert cli.main(["--config", str(config), "authority-unretire", "--system-id", system_id]) == 0
    assert cli.main(["--config", str(config), "init"]) == 1

    with sqlite3.connect(backup) as backup_connection:
        assert backup_connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert (
            backup_connection.execute(
                "SELECT enabled FROM systems WHERE system_id = ?", (system_id,)
            ).fetchone()[0]
            == 1
        )
    with sqlite3.connect(database) as connection:
        assert (
            connection.execute(
                "SELECT enabled FROM systems WHERE system_id = ?", (system_id,)
            ).fetchone()[0]
            == 0
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM retired_system_authorities WHERE system_id = ?", (system_id,)
            ).fetchone()[0]
            == 0
        )


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
    assert result == 1
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
authority_fingerprint = "1111111111111111111111111111111111111111111111111111111111111111"
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

    assert cli.main(["--config", str(config), "serve"]) == 1

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
