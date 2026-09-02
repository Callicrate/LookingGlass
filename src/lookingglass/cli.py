"""Command-line entry points for the local service."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import socket
import sqlite3
import sys
from collections.abc import Callable, Sequence
from importlib.resources import files
from pathlib import Path
from typing import NoReturn

import uvicorn

from lookingglass.adapters import CliRunner
from lookingglass.adapters.databricks import (
    databricks_profile_authority_fingerprint,
    redact_diagnostic,
)
from lookingglass.adapters.ssh import ssh_host_authority_fingerprint
from lookingglass.composition import ApplicationRuntime, build_runtime
from lookingglass.config import (
    AppSettings,
    ConfigError,
    ProjectSettings,
    load_app_settings,
    load_settings,
)
from lookingglass.local_files import ExclusiveFileLock, absolute_local_path
from lookingglass.storage import SQLiteStore, backup_sqlite_database

logger = logging.getLogger(__name__)
DEFAULT_RUN_ONCE_CYCLES = 10_000
MAX_RUN_ONCE_CYCLES = 1_000_000
_UVICORN_BACKLOG = 2_048
EXIT_FAILURE = 1
EXIT_BOUNDED_INCOMPLETE = 3
EXIT_INTERRUPTED = 130


def _operator_diagnostic(value: object, *, limit: int = 1024) -> str:
    """Return bounded terminal-safe text for all dynamic CLI log fields."""

    return redact_diagnostic(str(value), limit=limit)


class _OperatorArgumentParser(argparse.ArgumentParser):
    """Keep argparse usage failures on the same bounded diagnostic boundary."""

    def error(self, message: str) -> NoReturn:
        self.print_usage(sys.stderr)
        self.exit(2, f"{self.prog}: error: {_operator_diagnostic(message)}\n")


class _SanitizedUvicornFilter(logging.Filter):
    """Remove dependency tracebacks from the closed operator log boundary."""

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if (
            record.exc_info is not None
            or "Traceback (most recent call last)" in message
            or "Exception in 'lifespan' protocol" in message
        ):
            record.msg = "LookingGlass application component failed"
            record.args = ()
            record.exc_info = None
            record.exc_text = None
        return True


class _LookingGlassServer(uvicorn.Server):
    """Disclose browser activation only after application startup succeeds."""

    def __init__(self, config: uvicorn.Config, *, on_started: Callable[[], None]) -> None:
        super().__init__(config)
        self._on_started = on_started

    async def startup(self, sockets: list[socket.socket] | None = None) -> None:
        await super().startup(sockets=sockets)
        if not self.should_exit:
            self._on_started()


_EXAMPLE_CONFIG = """[app]
database_path = "./.local/lookingglass.sqlite3"
host = "127.0.0.1"
port = 8765
worker_poll_seconds = 1.0
cli_timeout_seconds = 30.0
cli_output_limit_bytes = 8388608
ssh_config_path = "./.ssh/config"
ssh_known_hosts_path = "./.ssh/known_hosts"

[[databricks]]
id = "primary-workspace"
name = "primary-workspace"
profile = "YOUR_PROFILE"
authority_fingerprint = "0000000000000000000000000000000000000000000000000000000000000000"
workspace_root = "/"

[[ssh]]
id = "primary-host"
name = "primary-host"
host_alias = "YOUR_SSH_HOST_ALIAS"
authority_fingerprint = "0000000000000000000000000000000000000000000000000000000000000000"
path_root = "/"
"""


def _parser() -> argparse.ArgumentParser:
    parser = _OperatorArgumentParser(
        prog="lookingglass",
        description=("LookingGlass local state viewer and refresher."),
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.local.toml"),
        help="Path to local TOML configuration (default: config.local.toml).",
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    init_config = subparsers.add_parser(
        "init-config",
        help="Write a no-overwrite starter configuration without opening the database.",
    )
    init_config.add_argument(
        "--output",
        type=Path,
        default=Path("config.local.toml"),
        help="New TOML path (default: config.local.toml); never overwrites an existing path.",
    )
    export_docs = subparsers.add_parser(
        "export-docs",
        help="Write the packaged architecture contract without opening the database.",
    )
    export_docs.add_argument(
        "--output",
        type=Path,
        default=Path("lookingglass-architecture.md"),
        help=(
            "New Markdown path (default: lookingglass-architecture.md); never overwrites an "
            "existing path."
        ),
    )
    fingerprint_profile = subparsers.add_parser(
        "fingerprint-profile",
        help="Print the non-secret workspace-authority fingerprint for one CLI profile.",
    )
    fingerprint_profile.add_argument(
        "--profile",
        required=True,
        help="Existing named profile in the standard Databricks CLI configuration.",
    )
    fingerprint_host = subparsers.add_parser(
        "fingerprint-host",
        help="Print the non-secret route-authority fingerprint for one SSH host alias.",
    )
    fingerprint_host.add_argument(
        "--alias",
        required=True,
        help="Host alias resolved through the configured SSH client configuration.",
    )
    subparsers.add_parser("init", help="Initialize the database and configured systems.")
    subparsers.add_parser(
        "doctor",
        help="Check the existing Databricks CLI without querying inventory.",
    )
    subparsers.add_parser(
        "authority-list",
        help="List local verified, historical, and retired authority identities.",
    )
    for command, help_text in (
        ("authority-retire", "Retire one authority without deleting its cached facts."),
        ("authority-unretire", "Remove a retirement tombstone; init is still required."),
    ):
        authority = subparsers.add_parser(command, help=help_text)
        authority.add_argument("--system-id", required=True, help="Local system UUID.")
    run_once = subparsers.add_parser(
        "run-once",
        help="Process a bounded number of eligible local work cycles, then stop.",
    )
    run_once.add_argument(
        "--max-cycles",
        type=_run_once_cycle_limit,
        default=DEFAULT_RUN_ONCE_CYCLES,
        help=(
            f"Maximum coordinator/worker cycles (default: {DEFAULT_RUN_ONCE_CYCLES}; "
            "exit 3 means eligible work may remain)."
        ),
    )
    backup = subparsers.add_parser(
        "backup",
        help="Create a consistent no-overwrite snapshot of the local database.",
    )
    backup.add_argument(
        "--output",
        type=Path,
        required=True,
        help="New SQLite snapshot path; existing paths are never overwritten.",
    )
    serve = subparsers.add_parser(
        "serve", help="Run the loopback dashboard and background workers."
    )
    serve.add_argument(
        "--allow-redirected-activation",
        action="store_true",
        help="Allow the one-time browser activation link on redirected stdout.",
    )
    return parser


def _load(path: Path) -> ProjectSettings:
    try:
        return load_settings(path)
    except (ConfigError, FileNotFoundError, OSError) as exc:
        raise ConfigError(f"could not load {path}: {exc}") from exc


def _load_app(path: Path) -> AppSettings:
    try:
        return load_app_settings(path)
    except (ConfigError, FileNotFoundError, OSError) as exc:
        raise ConfigError(f"could not load {path}: {exc}") from exc


def _initialize(settings: ProjectSettings) -> None:
    runtime = build_runtime(settings)
    try:
        logger.info(
            "Initialized %s configured Databricks system(s) in %s",
            len(settings.databricks_systems),
            _operator_diagnostic(settings.app.database_path),
        )
    finally:
        runtime.store.close()


def _initialize_config(output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(_EXAMPLE_CONFIG)
    logger.info("Created starter configuration at %s", _operator_diagnostic(output.resolve()))


def _architecture_text() -> str:
    packaged = files("lookingglass").joinpath("docs", "architecture.md")
    if packaged.is_file():
        return packaged.read_text(encoding="utf-8")
    checkout_copy = Path(__file__).parents[2] / "docs" / "architecture.md"
    return checkout_copy.read_text(encoding="utf-8")


def _export_docs(output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(_architecture_text())
    logger.info(
        "Exported the LookingGlass architecture contract to %s",
        _operator_diagnostic(output.resolve()),
    )


async def _doctor(settings: ProjectSettings) -> None:
    runner = CliRunner(
        timeout_seconds=settings.app.cli_timeout_seconds,
        stdout_cap=settings.app.cli_output_limit_bytes,
        stderr_cap=min(settings.app.cli_output_limit_bytes, 1024 * 1024),
    )
    await runner.doctor()
    logger.info(
        "Databricks CLI is compatible; %s profile reference(s) declared "
        "(authentication not probed)",
        len(settings.databricks_systems),
    )


def _authority_list(settings: AppSettings) -> None:
    if not os.path.lexists(settings.database_path):
        raise RuntimeError("authority inventory requires an initialized LookingGlass database")
    with SQLiteStore(settings.database_path) as store:
        lines: list[str] = []
        for authority in store.list_authorities():
            fingerprint = (
                authority.authority_fingerprint[:12]
                if authority.authority_fingerprint
                else "legacy-unverified"
            )
            status = (
                "retired" if authority.retired else "enabled" if authority.enabled else "paused"
            )
            last_activity = (
                authority.last_activity_at.isoformat() if authority.last_activity_at else "none"
            )
            lines.append(
                "Authority "
                f"{authority.system_id} | {_operator_diagnostic(authority.display_name)} | "
                f"{status} | config={_operator_diagnostic(authority.config_id or 'legacy')} | "
                f"root={_operator_diagnostic(authority.workspace_root or 'unknown')} | "
                f"fingerprint={fingerprint} | "
                f"last={last_activity}\n"
            )
    sys.stdout.writelines(lines)
    sys.stdout.flush()


def _set_authority_retired(
    settings: AppSettings,
    *,
    system_id: str,
    retired: bool,
) -> None:
    if not os.path.lexists(settings.database_path):
        raise RuntimeError("authority retirement requires an initialized LookingGlass database")
    with SQLiteStore(settings.database_path) as store:
        store.set_authority_retired(system_id, retired=retired)
    logger.info(
        "Authority %s %s; cached facts were preserved",
        system_id,
        "retired" if retired else "unretired (run init to re-enable if configured)",
    )


def _run_once_cycle_limit(value: str) -> int:
    try:
        cycles = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("max cycles must be an integer") from exc
    if cycles <= 0 or cycles > MAX_RUN_ONCE_CYCLES:
        raise argparse.ArgumentTypeError(f"max cycles must be between 1 and {MAX_RUN_ONCE_CYCLES}")
    return cycles


async def _run_once(
    runtime: ApplicationRuntime,
    *,
    max_cycles: int = DEFAULT_RUN_ONCE_CYCLES,
) -> bool:
    """Process a bounded batch and report whether the eligible queue became idle."""

    workers = tuple(getattr(runtime, "workers", None) or (runtime.worker,))
    for worker in workers:
        await worker.startup()
    for _ in range(max_cycles):
        coordinated = await runtime.coordinator.run_once()
        # Drive every adapter worker each cycle without short-circuiting, so a
        # worker with pending work is never skipped because an earlier one
        # already reported progress.
        results = [await worker.run_once() for worker in workers]
        worked = any(results)
        if coordinated is None and not worked:
            return True
    return False


def _show_browser_activation(
    runtime: ApplicationRuntime,
    settings: ProjectSettings,
    *,
    allow_redirected: bool,
) -> None:
    _require_browser_activation_output(allow_redirected=allow_redirected)
    token = runtime.local_authorizer.take_bootstrap_token()
    url = f"http://{runtime.local_authorizer.browser_host}:{settings.app.port}/bootstrap#{token}"
    # The one-time capability must reach the controlling terminal without entering
    # application or access logs. It is consumed by the first successful exchange.
    sys.stdout.write(
        "Local browser activation (valid once for 10 minutes):\n"
        f"{url}\n"
        "Keep this link private. Restart serve to issue a new link.\n"
    )
    sys.stdout.flush()


def _require_browser_activation_output(*, allow_redirected: bool) -> None:
    if not sys.stdout.isatty() and not allow_redirected:
        raise RuntimeError(
            "refusing to write the browser activation link to redirected stdout; "
            "use an interactive terminal or pass --allow-redirected-activation"
        )


def _reserve_loopback_sockets(port: int, *, backlog: int) -> list[socket.socket]:
    """Own both loopback address families before an activation token is disclosed."""

    listeners: list[socket.socket] = []
    selected_port = port
    endpoints = ((socket.AF_INET, "127.0.0.1"), (socket.AF_INET6, "::1"))
    try:
        for family, host in endpoints:
            listener = socket.socket(family, socket.SOCK_STREAM)
            try:
                if family == socket.AF_INET6:
                    listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
                if sys.platform == "win32":
                    exclusive_address_use = getattr(socket, "SO_EXCLUSIVEADDRUSE", None)
                    if exclusive_address_use is None:  # pragma: no cover - platform invariant
                        raise RuntimeError("exclusive Windows socket binding is unavailable")
                    listener.setsockopt(socket.SOL_SOCKET, exclusive_address_use, 1)
                else:
                    # POSIX needs this for immediate restart after accepted sockets
                    # enter TIME_WAIT. Active listeners remain exclusive without
                    # SO_REUSEPORT, which LookingGlass never enables.
                    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind((host, selected_port))
                if selected_port == 0:
                    selected_port = int(listener.getsockname()[1])
                # Listen before disclosure so another local process cannot win a
                # bind-to-listen race on either address family.
                listener.listen(backlog)
            except BaseException:
                listener.close()
                raise
            listeners.append(listener)
    except OSError as exc:
        for listener in listeners:
            listener.close()
        raise RuntimeError(
            f"could not reserve IPv4 and IPv6 loopback listeners on port {port}"
        ) from exc
    except BaseException:
        for listener in listeners:
            listener.close()
        raise
    return listeners


def _serve_loopback(
    runtime: ApplicationRuntime,
    settings: ProjectSettings,
    listeners: list[socket.socket],
    *,
    log_level: str,
    allow_redirected: bool,
) -> None:
    config = uvicorn.Config(
        runtime.app,
        host=settings.app.host,
        port=settings.app.port,
        log_level=log_level.lower(),
        access_log=False,
    )
    logger.info(
        "Reserved dashboard listeners on 127.0.0.1:%s and [::1]:%s",
        settings.app.port,
        settings.app.port,
    )
    server = _LookingGlassServer(
        config,
        on_started=lambda: _show_browser_activation(
            runtime,
            settings,
            allow_redirected=allow_redirected,
        ),
    )
    error_logger = logging.getLogger("uvicorn.error")
    diagnostic_filter = _SanitizedUvicornFilter()
    error_logger.addFilter(diagnostic_filter)
    try:
        server.run(sockets=listeners)
    except SystemExit as exc:
        if exc.code == 3:
            raise RuntimeError("LookingGlass application startup failed") from None
        raise
    finally:
        error_logger.removeFilter(diagnostic_filter)


def _serve_lock_path(database_path: Path) -> Path:
    database = absolute_local_path(database_path)
    if os.path.lexists(database.parent / ".git"):
        raise OSError("LookingGlass serve state requires a dedicated private directory")
    return database.with_name(f".{database.name}.serve.lock")


def main(argv: Sequence[str] | None = None) -> int:
    """Run one local command and return a process exit code."""
    args = _parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        if args.command == "init-config":
            _initialize_config(args.output)
            return 0
        if args.command == "export-docs":
            _export_docs(args.output)
            return 0
        if args.command == "fingerprint-profile":
            fingerprint = databricks_profile_authority_fingerprint(args.profile)
            sys.stdout.write(f"{fingerprint}\n")
            return 0
        if args.command == "fingerprint-host":
            app_settings = _load_app(args.config)
            if (
                app_settings.ssh_config_path is None
                or app_settings.ssh_known_hosts_path is None
            ):
                raise ConfigError(
                    "SSH host fingerprinting requires app.ssh_config_path and "
                    "app.ssh_known_hosts_path"
                )
            fingerprint = ssh_host_authority_fingerprint(
                args.alias,
                ssh_config_path=app_settings.ssh_config_path,
                known_hosts_path=app_settings.ssh_known_hosts_path,
            )
            sys.stdout.write(f"{fingerprint}\n")
            return 0
        if args.command == "serve":
            _require_browser_activation_output(allow_redirected=args.allow_redirected_activation)
        local_only_commands = {
            "authority-list",
            "authority-retire",
            "authority-unretire",
            "backup",
        }
        if args.command in local_only_commands:
            app_settings = _load_app(args.config)
            if args.command == "authority-list":
                with ExclusiveFileLock(_serve_lock_path(app_settings.database_path)):
                    _authority_list(app_settings)
            elif args.command in {"authority-retire", "authority-unretire"}:
                with ExclusiveFileLock(_serve_lock_path(app_settings.database_path)):
                    _set_authority_retired(
                        app_settings,
                        system_id=args.system_id,
                        retired=args.command == "authority-retire",
                    )
            else:
                destination = backup_sqlite_database(app_settings.database_path, args.output)
                logger.info(
                    "Created consistent SQLite backup at %s",
                    _operator_diagnostic(destination),
                )
            return 0

        settings = _load(args.config)
        if args.command == "init":
            with ExclusiveFileLock(_serve_lock_path(settings.app.database_path)):
                _initialize(settings)
        elif args.command == "doctor":
            asyncio.run(_doctor(settings))
        elif args.command == "run-once":
            with ExclusiveFileLock(_serve_lock_path(settings.app.database_path)):
                runtime = build_runtime(settings)
                try:
                    drained = asyncio.run(_run_once(runtime, max_cycles=args.max_cycles))
                finally:
                    runtime.store.close()
            if not drained:
                logger.warning(
                    "run-once reached --max-cycles=%s; eligible work may remain; rerun "
                    "run-once to continue",
                    args.max_cycles,
                )
                return EXIT_BOUNDED_INCOMPLETE
        elif args.command == "serve":
            listeners = _reserve_loopback_sockets(
                settings.app.port,
                backlog=_UVICORN_BACKLOG,
            )
            try:
                with ExclusiveFileLock(_serve_lock_path(settings.app.database_path)):
                    runtime = build_runtime(settings)
                    try:
                        _serve_loopback(
                            runtime,
                            settings,
                            listeners,
                            log_level=args.log_level,
                            allow_redirected=args.allow_redirected_activation,
                        )
                    finally:
                        runtime.store.close()
            finally:
                for listener in listeners:
                    listener.close()
        else:  # pragma: no cover - argparse owns the command vocabulary
            raise RuntimeError(f"unsupported command {args.command}")
    except KeyboardInterrupt:
        logger.warning("LookingGlass command interrupted")
        return EXIT_INTERRUPTED
    except sqlite3.Error:
        logger.error("local SQLite state could not be opened or updated")
        return EXIT_FAILURE
    except (ConfigError, RuntimeError, ValueError, OSError) as exc:
        logger.error("%s", _operator_diagnostic(exc))
        return EXIT_FAILURE
    return 0
