"""Command-line entry points for the local service."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import socket
import sqlite3
import sys
from collections.abc import Sequence
from importlib.resources import files
from pathlib import Path

import uvicorn

from async_api_view.adapters import CliRunner
from async_api_view.adapters.databricks import databricks_profile_authority_fingerprint
from async_api_view.composition import ApplicationRuntime, build_runtime
from async_api_view.config import (
    AppSettings,
    ConfigError,
    ProjectSettings,
    load_app_settings,
    load_settings,
)
from async_api_view.local_files import ExclusiveFileLock, absolute_local_path
from async_api_view.storage import SQLiteStore, backup_sqlite_database

logger = logging.getLogger(__name__)
DEFAULT_RUN_ONCE_CYCLES = 10_000
MAX_RUN_ONCE_CYCLES = 1_000_000
_UVICORN_BACKLOG = 2_048

_EXAMPLE_CONFIG = """[app]
database_path = "./.local/rookery.sqlite3"
host = "127.0.0.1"
port = 8765
worker_poll_seconds = 1.0
cli_timeout_seconds = 30.0
cli_output_limit_bytes = 8388608

[[databricks]]
id = "primary-workspace"
name = "primary-workspace"
profile = "YOUR_PROFILE"
authority_fingerprint = "0000000000000000000000000000000000000000000000000000000000000000"
workspace_root = "/"
"""


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="async-api-view",
        description=(
            "Rookery local state viewer and refresher, distributed as the "
            "async-api-view compatibility command."
        ),
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
        default=Path("rookery-architecture.md"),
        help=(
            "New Markdown path (default: rookery-architecture.md); never overwrites an "
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
        help="Process one bounded batch of eligible local work, then stop.",
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
            settings.app.database_path,
        )
    finally:
        runtime.store.close()


def _initialize_config(output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(_EXAMPLE_CONFIG)
    logger.info("Created starter configuration at %s", output.resolve())


def _architecture_text() -> str:
    packaged = files("async_api_view").joinpath("docs", "architecture.md")
    if packaged.is_file():
        return packaged.read_text(encoding="utf-8")
    checkout_copy = Path(__file__).parents[2] / "docs" / "architecture.md"
    return checkout_copy.read_text(encoding="utf-8")


def _export_docs(output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(_architecture_text())
    logger.info("Exported the Rookery architecture contract to %s", output.resolve())


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
        raise RuntimeError("authority inventory requires an initialized Rookery database")
    with SQLiteStore(settings.database_path) as store:
        for authority in store.list_authorities():
            fingerprint = (
                authority.authority_fingerprint[:12]
                if authority.authority_fingerprint
                else "legacy-unverified"
            )
            status = (
                "retired" if authority.retired else "enabled" if authority.enabled else "paused"
            )
            logger.info(
                "Authority %s | %s | %s | config=%s | root=%s | fingerprint=%s | last=%s",
                authority.system_id,
                authority.display_name,
                status,
                authority.config_id or "legacy",
                authority.workspace_root or "unknown",
                fingerprint,
                authority.last_activity_at.isoformat() if authority.last_activity_at else "none",
            )


def _set_authority_retired(
    settings: AppSettings,
    *,
    system_id: str,
    retired: bool,
) -> None:
    if not os.path.lexists(settings.database_path):
        raise RuntimeError("authority retirement requires an initialized Rookery database")
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

    await runtime.worker.startup()
    for _ in range(max_cycles):
        coordinated = await runtime.coordinator.run_once()
        worked = await runtime.worker.run_once()
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
                    # SO_REUSEPORT, which Rookery never enables.
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
    )
    logger.info(
        "Reserved dashboard listeners on 127.0.0.1:%s and [::1]:%s",
        settings.app.port,
        settings.app.port,
    )
    _show_browser_activation(
        runtime,
        settings,
        allow_redirected=allow_redirected,
    )
    uvicorn.Server(config).run(sockets=listeners)


def _serve_lock_path(database_path: Path) -> Path:
    database = absolute_local_path(database_path)
    if os.path.lexists(database.parent / ".git"):
        raise OSError("Rookery serve state requires a dedicated private directory")
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
        else:
            settings = _load(args.config)
        if args.command == "init":
            with ExclusiveFileLock(_serve_lock_path(settings.app.database_path)):
                _initialize(settings)
        elif args.command == "doctor":
            asyncio.run(_doctor(settings))
        elif args.command == "authority-list":
            with ExclusiveFileLock(_serve_lock_path(app_settings.database_path)):
                _authority_list(app_settings)
        elif args.command in {"authority-retire", "authority-unretire"}:
            with ExclusiveFileLock(_serve_lock_path(app_settings.database_path)):
                _set_authority_retired(
                    app_settings,
                    system_id=args.system_id,
                    retired=args.command == "authority-retire",
                )
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
                return 3
        elif args.command == "backup":
            destination = backup_sqlite_database(app_settings.database_path, args.output)
            logger.info("Created consistent SQLite backup at %s", destination)
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
    except sqlite3.Error:
        logger.error("local SQLite state could not be opened or updated")
        return 2
    except (ConfigError, RuntimeError, ValueError, OSError) as exc:
        logger.error("%s", exc)
        return 2
    return 0
