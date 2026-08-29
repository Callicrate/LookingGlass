"""Command-line entry points for the local service."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

import uvicorn

from async_api_view.adapters import CliRunner
from async_api_view.composition import ApplicationRuntime, build_runtime
from async_api_view.config import ConfigError, ProjectSettings, load_settings
from async_api_view.storage import backup_sqlite_database

logger = logging.getLogger(__name__)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="async-api-view",
        description="View and refresh locally cached remote-system state.",
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
    subparsers.add_parser("init", help="Initialize the database and configured systems.")
    subparsers.add_parser(
        "doctor",
        help="Check the existing Databricks CLI without querying inventory.",
    )
    subparsers.add_parser(
        "run-once",
        help="Drain currently eligible local coordinator and adapter work, then stop.",
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


async def _run_once(runtime: ApplicationRuntime) -> None:
    await runtime.worker.startup()
    for _ in range(10_000):
        coordinated = await runtime.coordinator.run_once()
        worked = await runtime.worker.run_once()
        if coordinated is None and not worked:
            break
    else:
        raise RuntimeError("run-once exceeded its bounded work limit")


def _show_browser_activation(
    runtime: ApplicationRuntime,
    settings: ProjectSettings,
    *,
    allow_redirected: bool,
) -> None:
    if not sys.stdout.isatty() and not allow_redirected:
        raise RuntimeError(
            "refusing to write the browser activation link to redirected stdout; "
            "use an interactive terminal or pass --allow-redirected-activation"
        )
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


def main(argv: Sequence[str] | None = None) -> int:
    """Run one local command and return a process exit code."""
    args = _parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        settings = _load(args.config)
        if args.command == "init":
            _initialize(settings)
        elif args.command == "doctor":
            asyncio.run(_doctor(settings))
        elif args.command == "run-once":
            runtime = build_runtime(settings)
            try:
                asyncio.run(_run_once(runtime))
            finally:
                runtime.store.close()
        elif args.command == "backup":
            destination = backup_sqlite_database(settings.app.database_path, args.output)
            logger.info("Created consistent SQLite backup at %s", destination)
        elif args.command == "serve":
            runtime = build_runtime(settings)
            try:
                _show_browser_activation(
                    runtime,
                    settings,
                    allow_redirected=args.allow_redirected_activation,
                )
                uvicorn.run(
                    runtime.app,
                    host=settings.app.host,
                    port=settings.app.port,
                    log_level=args.log_level.lower(),
                )
            finally:
                runtime.store.close()
        else:  # pragma: no cover - argparse owns the command vocabulary
            raise RuntimeError(f"unsupported command {args.command}")
    except (ConfigError, RuntimeError, ValueError, OSError) as exc:
        logger.error("%s", exc)
        return 2
    return 0
