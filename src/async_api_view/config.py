"""Validated local configuration for the single-user service."""

from __future__ import annotations

import ipaddress
import math
import tomllib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

MAX_CONFIG_BYTES = 1024 * 1024
MAX_DATABRICKS_SYSTEMS = 32


class ConfigError(ValueError):
    """Raised when local configuration violates the application contract."""


@dataclass(frozen=True, slots=True)
class AppSettings:
    database_path: Path
    host: str = "127.0.0.1"
    port: int = 8765
    worker_poll_seconds: float = 1.0
    cli_timeout_seconds: float = 30.0
    cli_output_limit_bytes: int = 8 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class DatabricksSystemSettings:
    name: str
    profile: str
    workspace_root: str


@dataclass(frozen=True, slots=True)
class ProjectSettings:
    app: AppSettings
    databricks_systems: tuple[DatabricksSystemSettings, ...]


def _mapping(value: object, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"{field_name} must be a TOML table")
    return value


def _bounded_text(value: object, field_name: str, *, max_length: int) -> str:
    if not isinstance(value, str):
        raise ConfigError(f"{field_name} must be text")
    if not value or len(value) > max_length or any(ord(char) < 32 for char in value):
        raise ConfigError(f"{field_name} must contain 1 to {max_length} printable characters")
    return value


def _positive_float(value: object, field_name: str, *, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ConfigError(f"{field_name} must be a number")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0 or normalized > maximum:
        raise ConfigError(f"{field_name} must be greater than 0 and at most {maximum}")
    return normalized


def _positive_int(value: object, field_name: str, *, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{field_name} must be an integer")
    if value <= 0 or value > maximum:
        raise ConfigError(f"{field_name} must be greater than 0 and at most {maximum}")
    return value


def _loopback_host(value: object) -> str:
    host = _bounded_text(value, "app.host", max_length=64)
    if host == "localhost":
        return host
    try:
        address = ipaddress.ip_address(host)
    except ValueError as exc:
        raise ConfigError("app.host must be localhost or an IPv4 loopback address") from exc
    if address.version != 4 or not address.is_loopback:
        raise ConfigError("app.host must be an IPv4 loopback address")
    return host


def _workspace_root(value: object, field_name: str) -> str:
    root = _bounded_text(value, field_name, max_length=2048)
    path = PurePosixPath(root)
    if not root.startswith("/") or ".." in path.parts:
        raise ConfigError(f"{field_name} must be an absolute Workspace path without '..'")
    return str(path)


def _profile(value: object, field_name: str) -> str:
    profile = _bounded_text(value, field_name, max_length=128)
    allowed = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")
    if any(char not in allowed for char in profile):
        raise ConfigError(
            f"{field_name} may contain only letters, digits, periods, underscores, and hyphens"
        )
    return profile


def load_settings(path: Path) -> ProjectSettings:
    """Load a bounded TOML file without accepting secrets or remote command data."""
    config_path = path.resolve(strict=True)
    if config_path.stat().st_size > MAX_CONFIG_BYTES:
        raise ConfigError(f"configuration exceeds {MAX_CONFIG_BYTES} bytes")
    try:
        with config_path.open("rb") as handle:
            raw = tomllib.load(handle)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"invalid TOML configuration: {exc}") from exc

    allowed_top_level = {"app", "databricks"}
    unknown_top_level = set(raw) - allowed_top_level
    if unknown_top_level:
        raise ConfigError(f"unknown top-level settings: {sorted(unknown_top_level)}")

    app_raw = _mapping(raw.get("app", {}), "app")
    allowed_app = {
        "database_path",
        "host",
        "port",
        "worker_poll_seconds",
        "cli_timeout_seconds",
        "cli_output_limit_bytes",
    }
    unknown_app = set(app_raw) - allowed_app
    if unknown_app:
        raise ConfigError(f"unknown app settings: {sorted(unknown_app)}")

    database_value = app_raw.get("database_path", ".local/state.sqlite3")
    database_text = _bounded_text(database_value, "app.database_path", max_length=2048)
    database_path = Path(database_text)
    if not database_path.is_absolute():
        database_path = config_path.parent / database_path

    app = AppSettings(
        database_path=database_path.resolve(strict=False),
        host=_loopback_host(app_raw.get("host", "127.0.0.1")),
        port=_positive_int(app_raw.get("port", 8765), "app.port", maximum=65535),
        worker_poll_seconds=_positive_float(
            app_raw.get("worker_poll_seconds", 1.0),
            "app.worker_poll_seconds",
            maximum=60.0,
        ),
        cli_timeout_seconds=_positive_float(
            app_raw.get("cli_timeout_seconds", 30.0),
            "app.cli_timeout_seconds",
            maximum=300.0,
        ),
        cli_output_limit_bytes=_positive_int(
            app_raw.get("cli_output_limit_bytes", 8 * 1024 * 1024),
            "app.cli_output_limit_bytes",
            maximum=64 * 1024 * 1024,
        ),
    )

    databricks_raw = raw.get("databricks", [])
    if not isinstance(databricks_raw, list):
        raise ConfigError("databricks must be an array of tables")
    if len(databricks_raw) > MAX_DATABRICKS_SYSTEMS:
        raise ConfigError(f"at most {MAX_DATABRICKS_SYSTEMS} Databricks systems are allowed")

    systems: list[DatabricksSystemSettings] = []
    for index, item in enumerate(databricks_raw):
        field_prefix = f"databricks[{index}]"
        entry = _mapping(item, field_prefix)
        unknown = set(entry) - {"name", "profile", "workspace_root"}
        if unknown:
            raise ConfigError(f"unknown {field_prefix} settings: {sorted(unknown)}")
        systems.append(
            DatabricksSystemSettings(
                name=_bounded_text(entry.get("name"), f"{field_prefix}.name", max_length=128),
                profile=_profile(entry.get("profile"), f"{field_prefix}.profile"),
                workspace_root=_workspace_root(
                    entry.get("workspace_root", "/"),
                    f"{field_prefix}.workspace_root",
                ),
            )
        )

    names = [system.name.casefold() for system in systems]
    if len(set(names)) != len(names):
        raise ConfigError("Databricks system names must be unique")
    return ProjectSettings(app=app, databricks_systems=tuple(systems))
