"""Closed, CLI-backed Databricks observation adapter.

This module deliberately knows no canonical storage implementation.  It invokes
only the installed ``databricks`` executable through an allow-listed argv map and
passes normalized observations through the shared ports.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import configparser
import hashlib
import ipaddress
import json
import math
import os
import re
import signal
import stat
import tempfile
import threading
import time
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from io import StringIO
from pathlib import Path, PurePosixPath
from typing import Any, Protocol
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, UUID, uuid5

from async_api_view.contracts import (
    ActionAttempt,
    ActionCompletion,
    ActionLease,
    ActionLeaseLost,
    ActionLifecyclePort,
    ActionOutcome,
    ActionQueuePort,
    AdapterAction,
    BindingQueryPort,
    CapabilityBinding,
    CollectionCoverage,
    ConnectionBinding,
    CoverageDeclaration,
    ErrorClass,
    FacetObservation,
    FieldCoverage,
    GuardDisposition,
    IngestionResult,
    ObjectLocator,
    ObservationBatch,
    ObservationIngestionPort,
    OperationClass,
    PreDispatchGuardPort,
    PresenceState,
    RelationshipObservation,
    UpdateMode,
    canonical_json_bytes,
    canonical_observation_batch_bytes,
)
from async_api_view.local_files import (
    ExclusiveFileLock,
    ExclusiveLockUnavailable,
    RegularFileGuard,
    harden_private_file,
    prepare_private_directory,
)

DATABRICKS_ADAPTER_KEY = "databricks"
DATABRICKS_ADAPTER_VERSION = "1"
CERTIFIED_CLI_VERSIONS = frozenset({(0, 298, 0)})
MAX_JSON_BYTES = 4 * 1024 * 1024
DEFAULT_STDOUT_CAP = 8 * 1024 * 1024
DEFAULT_STDERR_CAP = 64 * 1024
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_HEARTBEAT_SECONDS = 15.0
MAX_RETRY_DELAY_SECONDS = 30.0
MAX_DOWNSTREAM_RETRY_AFTER_SECONDS = 24 * 60 * 60
MAX_COLLECTION_ITEMS = 10_000
MAX_TABLE_COLUMNS = 1_000
MAX_JSON_DEPTH = 32
MAX_INGESTION_BATCH_BYTES = 1_000_000
MAX_INGESTION_BATCH_UNITS = 250
MAX_DATABRICKS_CONFIG_BYTES = 1024 * 1024
MAX_CLI_EXECUTABLE_BYTES = 512 * 1024 * 1024
_CLI_WORK_PREFIX = "rookery-databricks-"
_CLI_ACTIVE_LOCK = ".active.lock"
_CLI_PROFILE_SNAPSHOT = ".databrickscfg"
_LEGACY_ORPHAN_GRACE_SECONDS = 5 * 60
_CLI_RECOVERY_GUARD = threading.Lock()
_BUNDLE_CONFIG_FILENAMES = (
    "databricks.yml",
    "databricks.yaml",
    "bundle.yml",
    "bundle.yaml",
)

CAPABILITIES = frozenset(
    {
        "databricks.workspace.children.read",
        "databricks.workspace.metadata.read",
        "databricks.workspace.content.read",
        "databricks.uc.catalogs.read",
        "databricks.uc.schemas.read",
        "databricks.uc.relations.read",
        "databricks.uc.volumes.read",
    }
)

_PROFILE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_PROFILE_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_SECTION_HEADER = re.compile(r"^\s*\[([^\]\r\n]+)\]")
_RESERVED_PROFILE = "__settings__"
_INI_TRUE = frozenset({"1", "t", "true"})
_INI_FALSE = frozenset({"0", "f", "false"})
_VERSION = re.compile(r"Databricks CLI v(\d+)\.(\d+)\.(\d+)")
_BEARER_SECRET = re.compile(r"(?i)\bbearer\s+[^\s,;]+")
_SECRET = re.compile(
    r"(?i)(authorization\s*[:=]\s*(?:bearer\s+)?|"
    r"(?:token|password|secret|client_secret|access[_-]?(?:key|token)|refresh[_-]?token|"
    r"api[_-]?key)\s*[:=]\s*)[^\s,;]+"
)
_PROFILE_OUTPUT = re.compile(r"(?i)(--profile\s+)([^\s]+)")
_LOCAL_SETTING = re.compile(
    r"(?i)((?:databricks_cli_path|config(?:_file)?|config_path)\s*[:=]\s*)[^,;]+"
)
_JSON_SECRET = re.compile(
    r'(?i)("(?:token|password|secret|client_secret|access[_-]?(?:key|token)|'
    r'refresh[_-]?token|api[_-]?key)"\s*:\s*")[^"]*'
)
_WINDOWS_PATH = re.compile(r"(?i)\b[a-z]:[\\/][^,;\r\n]*")
_UNC_PATH = re.compile(r"\\\\[^\\\s,;]+\\[^\r\n,;]*")
_POSIX_PATH = re.compile(r"(?i)/(?:users|home|etc|var|tmp|private|mnt)/[^\s\"',;]*")
_CONTROL = re.compile(r"[\x00-\x1f\x7f\x1b]")
_BLOCKED_VALUE = re.compile(r"(^-|[\r\n\x00])")
_RETRY_AFTER_SECONDS = re.compile(r"(?i)\bretry[-_ ]?after\b[\"']?\s*[:=]\s*[\"']?(\d+)\b")
_RETRY_AFTER_HEADER = re.compile(r"(?im)^\s*retry-after\s*:\s*([^\r\n]{1,128})")


class DatabricksAdapterError(RuntimeError):
    """Base class for safe adapter failures."""


class CommandRejected(DatabricksAdapterError):
    pass


class CliUnavailable(DatabricksAdapterError):
    pass


class CliIncompatible(DatabricksAdapterError):
    pass


class CliTimeout(DatabricksAdapterError):
    pass


class CliOutputLimit(DatabricksAdapterError):
    pass


class DownstreamFailure(DatabricksAdapterError):
    def __init__(
        self,
        message: str,
        *,
        exit_code: int,
        diagnostic: str,
        retry_after: timedelta | None = None,
        retry_after_out_of_bounds: bool = False,
    ) -> None:
        super().__init__(message)
        self.exit_code = exit_code
        self.diagnostic = diagnostic
        self.retry_after = retry_after
        self.retry_after_out_of_bounds = retry_after_out_of_bounds


class InvalidDownstreamResponse(DatabricksAdapterError):
    pass


class ContentPolicyError(DatabricksAdapterError):
    pass


class LifecyclePersistenceFailure(DatabricksAdapterError):
    """A non-fencing lifecycle write failure that must reach runtime supervision."""


@dataclass(frozen=True, slots=True)
class ResolvedTarget:
    """Validated adapter-local target data, never taken from arbitrary action input."""

    workspace_path: str | None = None
    workspace_root: str | None = None
    catalog_name: str | None = None
    schema_name: str | None = None
    display_name: str | None = None
    canonical_object_id: str | None = None
    canonical_object_type: str | None = None
    canonical_parent_external_key: str | None = None


class TargetResolver(Protocol):
    async def resolve(
        self, *, action: AdapterAction, binding: ConnectionBinding
    ) -> ResolvedTarget: ...


@dataclass(frozen=True, slots=True)
class CliInvocation:
    capability_key: str
    argv: tuple[str, ...]
    authority_fingerprint: str | None = None


@dataclass(frozen=True, slots=True)
class CliExecution:
    correlation_id: str
    duration: timedelta
    exit_code: int
    stdout: bytes
    stderr: bytes


@dataclass(frozen=True, slots=True)
class _ExecutableWitness:
    identity: tuple[int, int]
    size: int
    modified_ns: int
    sha256: str


@dataclass(frozen=True, slots=True)
class ContentArtifact:
    """Ephemeral content handoff; persistence is intentionally outside this adapter."""

    object_locator: ObjectLocator
    content: bytes
    media_type: str | None
    source_revision: str | None


@dataclass(frozen=True, slots=True)
class NormalizedResult:
    batches: tuple[ObservationBatch, ...]
    artifacts: tuple[ContentArtifact, ...] = ()

    def __post_init__(self) -> None:
        if not self.batches or not all(
            isinstance(batch, ObservationBatch) for batch in self.batches
        ):
            raise ValueError("normalized result requires observation batches")

    @property
    def batch(self) -> ObservationBatch:
        if len(self.batches) != 1:
            raise ValueError("multipart normalization must be consumed through batches")
        return self.batches[0]


def _safe_text(value: object, *, name: str, maximum: int = 4096) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise CommandRejected(f"invalid {name}")
    if _BLOCKED_VALUE.search(value):
        raise CommandRejected(f"unsafe {name}")
    return value


def _workspace_path(value: object, root: object | None = None) -> str:
    path = _safe_text(value, name="workspace path")
    if not path.startswith("/"):
        raise CommandRejected("workspace paths must be absolute")
    normalized = str(PurePosixPath(path))
    if ".." in PurePosixPath(normalized).parts:
        raise CommandRejected("workspace paths must not traverse parents")
    if root is not None:
        allowed_root = _workspace_path(root)
        if normalized != allowed_root and not normalized.startswith(allowed_root.rstrip("/") + "/"):
            raise CommandRejected("workspace path is outside configured root")
    return normalized


def _name(value: object, *, label: str) -> str:
    return _safe_text(value, name=label, maximum=512)


def _profile(settings: Mapping[str, Any]) -> str:
    value = settings.get("profile")
    if not isinstance(value, str) or _PROFILE.fullmatch(value) is None:
        raise CommandRejected("binding requires a safe named CLI profile")
    return value


def _authority_fingerprint(settings: Mapping[str, Any]) -> str:
    value = settings.get("authority_fingerprint")
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise CommandRejected("binding requires a valid workspace authority fingerprint")
    return value


def _normalized_workspace_host(value: object) -> str:
    if not isinstance(value, str) or any(character.isspace() for character in value):
        raise CliUnavailable("Databricks profile has no workspace authority")
    try:
        parsed = urlsplit(value.strip())
        port = parsed.port
    except ValueError:
        raise CliUnavailable("Databricks profile workspace authority is invalid") from None
    if (
        parsed.scheme.casefold() != "https"
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise CliUnavailable("Databricks profile workspace authority is invalid")
    raw_hostname = parsed.hostname.rstrip(".")
    try:
        address = ipaddress.ip_address(raw_hostname)
    except ValueError:
        try:
            hostname = raw_hostname.encode("idna").decode("ascii").casefold()
        except UnicodeError:
            raise CliUnavailable("Databricks profile workspace authority is invalid") from None
        labels = hostname.split(".")
        if len(hostname) > 253 or any(
            re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label) is None for label in labels
        ):
            raise CliUnavailable("Databricks profile workspace authority is invalid") from None
    else:
        hostname = address.compressed.casefold()
        if address.version == 6:
            hostname = f"[{hostname}]"
    normalized_port = None if port == 443 else port
    return f"https://{hostname}" + (f":{normalized_port}" if normalized_port is not None else "")


def _normalized_route_selector(value: object, *, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise CliUnavailable(f"Databricks profile {name} selector is invalid")
    normalized = value.strip().casefold()
    if (
        not normalized
        or len(normalized) > 512
        or any(ord(character) < 32 or ord(character) == 127 for character in normalized)
    ):
        raise CliUnavailable(f"Databricks profile {name} selector is invalid")
    return normalized


def workspace_authority_fingerprint(
    host: object,
    *,
    workspace_id: object = None,
    account_id: object = None,
    azure_workspace_resource_id: object = None,
    azure_environment: object = None,
) -> str:
    """Return a non-reversible fingerprint for one resolved workspace route."""

    normalized = _normalized_workspace_host(host)
    selectors = (
        _normalized_route_selector(workspace_id, name="workspace_id"),
        _normalized_route_selector(account_id, name="account_id"),
        _normalized_route_selector(
            azure_workspace_resource_id,
            name="azure_workspace_resource_id",
        ),
        _normalized_route_selector(azure_environment, name="azure_environment"),
    )
    if all(selector is None for selector in selectors):
        payload = f"databricks-workspace-host-v1\0{normalized}"
    else:
        payload = "\0".join(
            (
                "databricks-workspace-route-v2",
                normalized,
                *(selector or "" for selector in selectors),
            )
        )
    return hashlib.sha256(payload.encode()).hexdigest()


def _profile_parser() -> configparser.ConfigParser:
    """Build an INI parser without Python's cross-section DEFAULT inheritance."""

    parser = configparser.ConfigParser(
        default_section=None,
        empty_lines_in_values=False,
        interpolation=None,
        strict=False,
    )
    parser.optionxform = str
    return parser


def _preprocess_selected_profile(payload: str, *, profile: str) -> str:
    """Reject permissive key/continuation syntax and apply pinned CLI comment precedence."""

    processed: list[str] = []
    selected = False
    for line in payload.splitlines(keepends=True):
        content = line.rstrip("\r\n")
        line_ending = line[len(content) :]
        section = _SECTION_HEADER.match(content)
        if section is not None:
            selected = section.group(1).strip() == profile
            processed.append(line)
            continue
        stripped = content.lstrip()
        if not stripped or stripped.startswith(("#", ";")):
            processed.append(line)
            continue
        delimiter_indexes = tuple(
            index for index in (stripped.find("="), stripped.find(":")) if index >= 0
        )
        if not delimiter_indexes:
            raise CliUnavailable("Databricks profile configuration uses unsupported key syntax")
        delimiter_index = min(delimiter_indexes)
        key = stripped[:delimiter_index].strip()
        if _PROFILE_KEY.fullmatch(key) is None:
            raise CliUnavailable("Databricks profile configuration uses noncanonical keys")
        value = stripped[delimiter_index + 1 :].lstrip()
        if value.rstrip().endswith("\\"):
            raise CliUnavailable("Databricks profile configuration uses unsupported continuation")
        if value.startswith(("`", '"""')):
            raise CliUnavailable("Databricks profile configuration uses unsupported quoted values")
        if not selected:
            processed.append(line)
            continue
        hash_comment = value.find(" #")
        if hash_comment >= 0:
            value = value[:hash_comment]
        else:
            semicolon_comment = value.find(" ;")
            if semicolon_comment >= 0:
                value = value[:semicolon_comment]
        value = value.rstrip()
        processed.append(f"{key} = {value}{line_ending}")
    return "".join(processed)


def _normalized_profile_value(value: str) -> str:
    if "\n" in value or "\r" in value:
        raise CliUnavailable("Databricks profile configuration uses unsupported multiline values")
    normalized = value.strip()
    if len(normalized) >= 2 and normalized[0] == normalized[-1] and normalized[0] in {'"', "'"}:
        quote = normalized[0]
        if quote not in normalized[1:-1]:
            normalized = normalized[1:-1]
    if normalized.startswith(("`", '"""')):
        raise CliUnavailable("Databricks profile configuration uses unsupported quoted values")
    return normalized


def _selected_profile_items(
    parser: configparser.ConfigParser,
    *,
    profile: str,
) -> dict[str, str]:
    if not parser.has_section(profile):
        raise CliUnavailable("Databricks profile has no workspace authority")
    items: dict[str, str] = {}
    for key, value in parser.items(profile, raw=True):
        if _PROFILE_KEY.fullmatch(key) is None:
            raise CliUnavailable("Databricks profile configuration uses noncanonical keys")
        items[key] = _normalized_profile_value(value)
    return items


def _quoted_profile_value(value: str) -> str:
    if " #" in value or " ;" in value:
        if "`" in value:
            raise CliUnavailable("Databricks profile value cannot be represented safely")
        return f"`{value}`"
    for quote in ('"', "'", "`"):
        if quote not in value:
            return f"{quote}{value}{quote}"
    raise CliUnavailable("Databricks profile value cannot be represented safely")


def _minimal_profile_snapshot(
    *,
    profile: str,
    items: Mapping[str, str],
) -> bytes:
    """Serialize only the selected profile's own values from the verified parse."""

    output = StringIO()
    output.write(f"[{profile}]\n")
    for key, value in items.items():
        output.write(f"{key} = {_quoted_profile_value(value)}\n")
    payload = output.getvalue().encode()
    if len(payload) > MAX_DATABRICKS_CONFIG_BYTES:  # pragma: no cover - subset invariant
        raise CliUnavailable("Databricks profile configuration exceeds the size limit")
    return payload


def _databricks_profile_authority(
    profile: str,
    *,
    config_file: Path | None = None,
) -> tuple[str, bytes]:
    """Return the route fingerprint and a minimal verified CLI config snapshot."""

    if profile == _RESERVED_PROFILE or _PROFILE.fullmatch(profile) is None:
        raise CliUnavailable("Databricks profile name is invalid")
    path = (config_file or (Path.home() / ".databrickscfg")).absolute()
    try:
        with RegularFileGuard(path) as guard:
            if path.stat().st_size > MAX_DATABRICKS_CONFIG_BYTES:
                raise CliUnavailable("Databricks profile configuration exceeds the size limit")
            payload_bytes = path.read_bytes()
            guard.verify()
    except CliUnavailable:
        raise
    except (OSError, UnicodeError):
        raise CliUnavailable("Databricks profile configuration is unavailable") from None
    parser = _profile_parser()
    invalid_configuration = False
    try:
        payload = payload_bytes.decode("utf-8-sig")
        payload = _preprocess_selected_profile(payload, profile=profile)
        parser.read_string(payload)
        items = _selected_profile_items(parser, profile=profile)
        host = items.get("host")
    except (configparser.Error, UnicodeError):
        invalid_configuration = True
        host = None
    if invalid_configuration:
        raise CliUnavailable("Databricks profile configuration is invalid")
    if host is None:
        raise CliUnavailable("Databricks profile has no workspace authority")
    skip_verify = items.get("skip_verify")
    if skip_verify is not None:
        normalized_skip_verify = skip_verify.casefold()
        if normalized_skip_verify in _INI_TRUE:
            raise CliUnavailable("Databricks profile cannot disable TLS certificate verification")
        if normalized_skip_verify not in _INI_FALSE:
            raise CliUnavailable("Databricks profile skip_verify setting is invalid")
    fingerprint = workspace_authority_fingerprint(
        host,
        workspace_id=items.get("workspace_id"),
        account_id=items.get("account_id"),
        azure_workspace_resource_id=items.get("azure_workspace_resource_id"),
        azure_environment=items.get("azure_environment"),
    )
    return fingerprint, _minimal_profile_snapshot(profile=profile, items=items)


def databricks_profile_authority_fingerprint(
    profile: str,
    *,
    config_file: Path | None = None,
) -> str:
    """Read the bounded route witness from one standard Databricks CLI profile."""

    fingerprint, _snapshot = _databricks_profile_authority(
        profile,
        config_file=config_file,
    )
    return fingerprint


def _enforce_binding_target(
    capability_key: str, binding: ConnectionBinding, target: ResolvedTarget
) -> ResolvedTarget:
    """Apply the configured Workspace allowlist at the execution boundary."""
    if not capability_key.startswith("databricks.workspace."):
        return target
    root = _workspace_path(binding.non_secret_settings.get("workspace_root"))
    return ResolvedTarget(
        workspace_path=target.workspace_path,
        workspace_root=root,
        catalog_name=target.catalog_name,
        schema_name=target.schema_name,
        display_name=target.display_name,
        canonical_object_id=target.canonical_object_id,
        canonical_object_type=target.canonical_object_type,
        canonical_parent_external_key=target.canonical_parent_external_key,
    )


def redact_diagnostic(value: bytes | str, *, limit: int = 2048) -> str:
    """Return bounded diagnostic text suitable for lifecycle persistence."""
    text = value.decode("utf-8", "replace") if isinstance(value, bytes) else value
    text = _CONTROL.sub(" ", text)
    text = _BEARER_SECRET.sub("Bearer [redacted]", text)
    text = _PROFILE_OUTPUT.sub(r"\1[redacted]", text)
    text = _JSON_SECRET.sub(r"\1[redacted]", text)
    text = _SECRET.sub(r"\1[redacted]", text)
    text = _LOCAL_SETTING.sub(r"\1[local-path]", text)
    text = _WINDOWS_PATH.sub("[local-path]", text)
    text = _UNC_PATH.sub("[local-path]", text)
    text = _POSIX_PATH.sub("[local-path]", text)
    return " ".join(text.split())[:limit]


def _downstream_retry_after(
    stderr: bytes,
    *,
    now: datetime | None = None,
) -> tuple[timedelta | None, bool]:
    """Extract one bounded HTTP Retry-After delay before diagnostic redaction."""

    text = stderr.decode("utf-8", "replace")
    seconds_match = _RETRY_AFTER_SECONDS.search(text)
    seconds: int | None = None
    if seconds_match is not None:
        digits = seconds_match.group(1).lstrip("0") or "0"
        maximum = str(MAX_DOWNSTREAM_RETRY_AFTER_SECONDS)
        if len(digits) > len(maximum) or (len(digits) == len(maximum) and digits > maximum):
            return None, True
        seconds = int(digits)
    if seconds is None:
        header_match = _RETRY_AFTER_HEADER.search(text)
        if header_match is None:
            return None, False
        try:
            retry_at = parsedate_to_datetime(header_match.group(1).strip())
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)
            current = (now or datetime.now(UTC)).astimezone(UTC)
            seconds = max(0, math.ceil((retry_at.astimezone(UTC) - current).total_seconds()))
        except (TypeError, ValueError, OverflowError, OSError):
            return None, False
    if seconds > MAX_DOWNSTREAM_RETRY_AFTER_SECONDS:
        return None, True
    return timedelta(seconds=seconds), False


def safe_diagnostic(error: ErrorClass) -> str:
    """Closed diagnostics allowed to cross the lifecycle persistence boundary."""
    return {
        ErrorClass.AUTHENTICATION: "Databricks authentication failed.",
        ErrorClass.AUTHORIZATION: "Databricks authorization was denied.",
        ErrorClass.CONNECTION_TIMEOUT: "Databricks CLI timed out.",
        ErrorClass.DOWNSTREAM_RATE_LIMIT: "Databricks rate limit prevented completion.",
        ErrorClass.TRANSIENT_DOWNSTREAM: "Databricks was temporarily unavailable.",
        ErrorClass.NOT_FOUND: "The configured Databricks target was not found.",
        ErrorClass.UNSUPPORTED: "The configured Databricks operation is unsupported.",
        ErrorClass.INVALID_DOWNSTREAM_RESPONSE: "Databricks returned an invalid response.",
        ErrorClass.ADAPTER_CONTRACT_MISMATCH: "The Databricks action does not match its binding.",
        ErrorClass.LOCAL_CANCELLATION: "The Databricks action was cancelled locally.",
        ErrorClass.UNKNOWN_ADAPTER_FAILURE: "The Databricks adapter failed unexpectedly.",
    }[error]


def _validate_observation_invocation(invocation: CliInvocation) -> None:
    """Reject manually assembled argv even if a caller bypasses the registry."""
    if invocation.capability_key not in CAPABILITIES or len(invocation.argv) < 3:
        raise CommandRejected("unregistered Databricks CLI invocation")
    args = invocation.argv[1:]
    group, command = args[:2]
    expected = {
        "databricks.workspace.children.read": ("workspace", "list"),
        "databricks.workspace.metadata.read": ("workspace", "get-status"),
        "databricks.workspace.content.read": ("workspace", "export"),
        "databricks.uc.catalogs.read": ("catalogs", "list"),
        "databricks.uc.schemas.read": ("schemas", "list"),
        "databricks.uc.relations.read": ("tables", "list"),
        "databricks.uc.volumes.read": ("volumes", "list"),
    }[invocation.capability_key]
    if (group, command) != expected:
        raise CommandRejected("unregistered Databricks command group or subcommand")
    if args[-4:-2] != ("--profile", args[-3]) or args[-2:] != ("--output", "json"):
        raise CommandRejected("Databricks command requires fixed profile and JSON output flags")
    if not _PROFILE.fullmatch(args[-3]):
        raise CommandRejected("unsafe CLI profile")
    positional = args[2:-4]
    if invocation.capability_key == "databricks.workspace.content.read":
        if len(positional) != 3 or positional[1:] != ("--format", "SOURCE"):
            raise CommandRejected("Workspace content command shape is invalid")
        _workspace_path(positional[0])
    elif invocation.capability_key.startswith("databricks.workspace."):
        if len(positional) != 1:
            raise CommandRejected("Workspace command shape is invalid")
        _workspace_path(positional[0])
    elif invocation.capability_key == "databricks.uc.catalogs.read":
        if positional:
            raise CommandRejected("Catalog command accepts no positional values")
    elif invocation.capability_key == "databricks.uc.schemas.read":
        if len(positional) != 1:
            raise CommandRejected("Schema command shape is invalid")
        _name(positional[0], label="catalog name")
    else:
        if len(positional) != 2:
            raise CommandRejected("Unity Catalog command shape is invalid")
        _name(positional[0], label="catalog name")
        _name(positional[1], label="schema name")


class DatabricksCommandRegistry:
    """Maps closed canonical capabilities to fixed CLI command shapes."""

    @staticmethod
    def build(
        *, capability_key: str, profile: str, target: ResolvedTarget, executable: str = "databricks"
    ) -> CliInvocation:
        if capability_key not in CAPABILITIES:
            raise CommandRejected("unregistered Databricks capability")
        if not _PROFILE.fullmatch(profile):
            raise CommandRejected("unsafe CLI profile")
        executable = _safe_text(executable, name="executable", maximum=1024)
        suffix = ("--profile", profile, "--output", "json")
        if capability_key == "databricks.workspace.children.read":
            path = _workspace_path(target.workspace_path, target.workspace_root)
            args = ("workspace", "list", path, *suffix)
        elif capability_key == "databricks.workspace.metadata.read":
            path = _workspace_path(target.workspace_path, target.workspace_root)
            args = ("workspace", "get-status", path, *suffix)
        elif capability_key == "databricks.workspace.content.read":
            path = _workspace_path(target.workspace_path, target.workspace_root)
            args = ("workspace", "export", path, "--format", "SOURCE", *suffix)
        elif capability_key == "databricks.uc.catalogs.read":
            args = ("catalogs", "list", *suffix)
        elif capability_key == "databricks.uc.schemas.read":
            args = ("schemas", "list", _name(target.catalog_name, label="catalog name"), *suffix)
        elif capability_key == "databricks.uc.relations.read":
            args = (
                "tables",
                "list",
                _name(target.catalog_name, label="catalog name"),
                _name(target.schema_name, label="schema name"),
                *suffix,
            )
        else:
            args = (
                "volumes",
                "list",
                _name(target.catalog_name, label="catalog name"),
                _name(target.schema_name, label="schema name"),
                *suffix,
            )
        return CliInvocation(capability_key=capability_key, argv=(executable, *args))


async def _read_limited(stream: asyncio.StreamReader | None, cap: int) -> bytes:
    if stream is None:
        return b""
    chunks = bytearray()
    while block := await stream.read(min(65536, cap + 1)):
        remaining = cap - len(chunks)
        if len(block) > remaining:
            raise CliOutputLimit("Databricks CLI output exceeded configured limit")
        chunks.extend(block)
    return bytes(chunks)


class _ProcessTree:
    """Own one CLI process tree until all output readers and descendants are done."""

    def __init__(self, process: asyncio.subprocess.Process) -> None:
        self.process = process
        self._windows_job: int | None = None
        if os.name == "nt" and isinstance(process, asyncio.subprocess.Process):
            self._windows_job = self._assign_windows_job(process)
            try:
                self._resume_windows_process(process.pid)
            except BaseException:
                self._close_windows_job()
                raise

    @staticmethod
    def _assign_windows_job(process: asyncio.subprocess.Process) -> int:
        import ctypes
        from ctypes import wintypes

        class IoCounters(ctypes.Structure):
            _fields_ = (
                ("read_operations", ctypes.c_ulonglong),
                ("write_operations", ctypes.c_ulonglong),
                ("other_operations", ctypes.c_ulonglong),
                ("read_bytes", ctypes.c_ulonglong),
                ("write_bytes", ctypes.c_ulonglong),
                ("other_bytes", ctypes.c_ulonglong),
            )

        class BasicLimitInformation(ctypes.Structure):
            _fields_ = (
                ("per_process_user_time", ctypes.c_longlong),
                ("per_job_user_time", ctypes.c_longlong),
                ("limit_flags", wintypes.DWORD),
                ("minimum_working_set", ctypes.c_size_t),
                ("maximum_working_set", ctypes.c_size_t),
                ("active_process_limit", wintypes.DWORD),
                ("affinity", ctypes.c_size_t),
                ("priority_class", wintypes.DWORD),
                ("scheduling_class", wintypes.DWORD),
            )

        class ExtendedLimitInformation(ctypes.Structure):
            _fields_ = (
                ("basic_limit_information", BasicLimitInformation),
                ("io_info", IoCounters),
                ("process_memory_limit", ctypes.c_size_t),
                ("job_memory_limit", ctypes.c_size_t),
                ("peak_process_memory_used", ctypes.c_size_t),
                ("peak_job_memory_used", ctypes.c_size_t),
            )

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.argtypes = (wintypes.LPVOID, wintypes.LPCWSTR)
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = (
            wintypes.HANDLE,
            ctypes.c_int,
            wintypes.LPVOID,
            wintypes.DWORD,
        )
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.AssignProcessToJobObject.argtypes = (wintypes.HANDLE, wintypes.HANDLE)
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL
        job = kernel32.CreateJobObjectW(None, None)
        if not job:
            code = ctypes.get_last_error()
            raise OSError(code, f"could not create CLI process job: {ctypes.FormatError(code)}")
        try:
            limits = ExtendedLimitInformation()
            limits.basic_limit_information.limit_flags = 0x00002000
            if not kernel32.SetInformationJobObject(
                job,
                9,
                ctypes.byref(limits),
                ctypes.sizeof(limits),
            ):
                code = ctypes.get_last_error()
                raise OSError(
                    code,
                    f"could not configure CLI process job: {ctypes.FormatError(code)}",
                )
            popen = process._transport.get_extra_info("subprocess")
            process_handle = getattr(popen, "_handle", None)
            if process_handle is None or not kernel32.AssignProcessToJobObject(
                job, int(process_handle)
            ):
                code = ctypes.get_last_error()
                raise OSError(code, f"could not own CLI process tree: {ctypes.FormatError(code)}")
        except BaseException:
            kernel32.CloseHandle(job)
            raise
        return int(job)

    @staticmethod
    def _resume_windows_process(process_id: int) -> None:
        import ctypes
        from ctypes import wintypes

        class ThreadEntry32(ctypes.Structure):
            _fields_ = (
                ("size", wintypes.DWORD),
                ("usage_count", wintypes.DWORD),
                ("thread_id", wintypes.DWORD),
                ("owner_process_id", wintypes.DWORD),
                ("base_priority", wintypes.LONG),
                ("priority_delta", wintypes.LONG),
                ("flags", wintypes.DWORD),
            )

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateToolhelp32Snapshot.argtypes = (wintypes.DWORD, wintypes.DWORD)
        kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
        kernel32.Thread32First.argtypes = (wintypes.HANDLE, ctypes.POINTER(ThreadEntry32))
        kernel32.Thread32First.restype = wintypes.BOOL
        kernel32.Thread32Next.argtypes = (wintypes.HANDLE, ctypes.POINTER(ThreadEntry32))
        kernel32.Thread32Next.restype = wintypes.BOOL
        kernel32.OpenThread.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        kernel32.OpenThread.restype = wintypes.HANDLE
        kernel32.ResumeThread.argtypes = (wintypes.HANDLE,)
        kernel32.ResumeThread.restype = wintypes.DWORD
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL
        snapshot = kernel32.CreateToolhelp32Snapshot(0x00000004, 0)
        if snapshot == ctypes.c_void_p(-1).value:
            code = ctypes.get_last_error()
            raise OSError(code, f"could not enumerate CLI threads: {ctypes.FormatError(code)}")
        resumed = 0
        try:
            entry = ThreadEntry32()
            entry.size = ctypes.sizeof(entry)
            available = kernel32.Thread32First(snapshot, ctypes.byref(entry))
            while available:
                if entry.owner_process_id == process_id:
                    thread = kernel32.OpenThread(0x0002, False, entry.thread_id)
                    if not thread:
                        code = ctypes.get_last_error()
                        raise OSError(
                            code,
                            f"could not open suspended CLI thread: {ctypes.FormatError(code)}",
                        )
                    try:
                        if kernel32.ResumeThread(thread) == 0xFFFFFFFF:
                            code = ctypes.get_last_error()
                            raise OSError(
                                code,
                                f"could not resume CLI process: {ctypes.FormatError(code)}",
                            )
                        resumed += 1
                    finally:
                        kernel32.CloseHandle(thread)
                available = kernel32.Thread32Next(snapshot, ctypes.byref(entry))
        finally:
            kernel32.CloseHandle(snapshot)
        if resumed == 0:
            raise OSError("suspended CLI process had no resumable thread")

    def kill(self) -> None:
        if self._windows_job is not None:
            self._terminate_windows_job()
            return
        if os.name != "nt" and isinstance(self.process, asyncio.subprocess.Process):
            with suppress(ProcessLookupError):
                os.killpg(self.process.pid, signal.SIGKILL)
            return
        if self.process.returncode is None:
            self.process.kill()

    def _close_windows_job(self) -> None:
        handle = self._windows_job
        if handle is None:
            return
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL
        self._windows_job = None
        if not kernel32.CloseHandle(handle):
            code = ctypes.get_last_error()
            raise OSError(code, f"could not close CLI process job: {ctypes.FormatError(code)}")

    def _terminate_windows_job(self) -> None:
        handle = self._windows_job
        if handle is None:
            return
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.TerminateJobObject.argtypes = (wintypes.HANDLE, wintypes.UINT)
        kernel32.TerminateJobObject.restype = wintypes.BOOL
        kernel32.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
        kernel32.WaitForSingleObject.restype = wintypes.DWORD
        try:
            if not kernel32.TerminateJobObject(handle, 1):
                code = ctypes.get_last_error()
                raise OSError(
                    code,
                    f"could not terminate CLI process job: {ctypes.FormatError(code)}",
                )
            if kernel32.WaitForSingleObject(handle, 5000) != 0:
                raise OSError("CLI process job did not terminate within five seconds")
        finally:
            self._close_windows_job()

    def close(self) -> None:
        self.kill()


async def _terminate_process(
    process: asyncio.subprocess.Process,
    process_tree: _ProcessTree,
    *tasks: asyncio.Task[Any],
) -> None:
    process_tree.kill()
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except TimeoutError:
        if process.returncode is None:
            process.kill()
    for task in tasks:
        if not task.done():
            task.cancel()
    try:
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=5)
    except TimeoutError:
        for task in tasks:
            task.cancel()


def _absolute_path_entries(value: str) -> tuple[str, ...]:
    """Return explicit absolute PATH entries without an implicit current directory."""

    entries: list[str] = []
    for raw_entry in value.split(os.pathsep):
        entry = (
            raw_entry[1:-1] if raw_entry.startswith('"') and raw_entry.endswith('"') else raw_entry
        )
        entry = os.path.expandvars(entry)
        if entry and Path(entry).is_absolute():
            entries.append(entry)
    return tuple(entries)


def _controlled_cli_environment() -> dict[str, str]:
    """Preserve ordinary process settings while removing Databricks auth overrides."""

    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.upper().startswith(("DATABRICKS_", "BUNDLE_"))
    }
    for key in tuple(environment):
        if key.upper() == "PATH":
            environment[key] = os.pathsep.join(_absolute_path_entries(environment[key]))
    return environment


def _trusted_cli_work_root(*, home: Path | None = None) -> Path:
    """Create a private work root without a CLI-recognized bundle ancestor."""

    try:
        resolved_home = (home or Path.home()).resolve(strict=True)
        state_root = resolved_home / ".rookery"
        if state_root.is_symlink() or state_root.is_junction():
            raise CliUnavailable("Rookery state directory cannot be a filesystem redirect")
        state_root = prepare_private_directory(state_root)
        requested_root = state_root / "cli-work"
        if requested_root.is_symlink() or requested_root.is_junction():
            raise CliUnavailable("Rookery CLI work directory cannot be a filesystem redirect")
        resolved_root = prepare_private_directory(requested_root)
        for directory in (resolved_root, *resolved_root.parents):
            if any((directory / filename).exists() for filename in _BUNDLE_CONFIG_FILENAMES):
                raise CliUnavailable(
                    "Rookery CLI work directory has a Databricks bundle configuration ancestor"
                )
    except OSError as exc:
        raise CliUnavailable("Rookery CLI work directory is unavailable") from exc
    return resolved_root


def _remove_guarded_private_file(path: Path) -> None:
    if not os.path.lexists(path):
        return
    with RegularFileGuard(path) as guard:
        guard.verify()
    path.unlink()


def _recover_orphaned_cli_work_directories(root: Path) -> bool:
    """Remove crash-retained profile snapshots without touching active commands."""

    now = time.time()
    recovery_complete = True
    for directory in sorted(root.iterdir(), key=lambda item: item.name):
        if not directory.name.startswith(_CLI_WORK_PREFIX):
            continue
        if directory.is_symlink() or directory.is_junction() or not directory.is_dir():
            raise OSError("Rookery CLI work entries must be private directories")
        active_lock_path = directory / _CLI_ACTIVE_LOCK
        if (
            not os.path.lexists(active_lock_path)
            and now - directory.stat().st_mtime < _LEGACY_ORPHAN_GRACE_SECONDS
        ):
            recovery_complete = False
            continue
        try:
            active_lock = ExclusiveFileLock(active_lock_path)
        except ExclusiveLockUnavailable:
            recovery_complete = False
            continue
        with active_lock:
            _remove_guarded_private_file(directory / _CLI_PROFILE_SNAPSHOT)
        _remove_guarded_private_file(active_lock_path)
        # The CLI may have left non-secret diagnostics. The credential snapshot is gone.
        with suppress(OSError):
            directory.rmdir()
    return recovery_complete


def _recover_cli_work_root(root: Path) -> None:
    with _CLI_RECOVERY_GUARD:
        try:
            with ExclusiveFileLock(root.parent / ".cli-work-recovery.lock"):
                _recover_orphaned_cli_work_directories(root)
        except ExclusiveLockUnavailable:
            return


def _candidate_executable_names(executable: str) -> tuple[str, ...]:
    if os.name != "nt" or Path(executable).suffix:
        return (executable,)
    return (f"{executable}.COM", f"{executable}.EXE")


def _usable_executable(path: Path) -> bool:
    if os.name == "nt" and path.suffix.upper() not in {".COM", ".EXE"}:
        return False
    return path.is_file() and (os.name == "nt" or os.access(path, os.X_OK))


def _executable_witness(path: Path) -> _ExecutableWitness:
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_CLOEXEC", 0)
    if os.name != "nt":
        flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    digest = hashlib.sha256()
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= MAX_CLI_EXECUTABLE_BYTES:
            raise OSError("Databricks CLI executable has an invalid file shape")
        while block := os.read(descriptor, 1024 * 1024):
            digest.update(block)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    path_details = path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ) or (path_details.st_dev, path_details.st_ino) != (before.st_dev, before.st_ino):
        raise OSError("Databricks CLI executable changed while being witnessed")
    return _ExecutableWitness(
        identity=(before.st_dev, before.st_ino),
        size=before.st_size,
        modified_ns=before.st_mtime_ns,
        sha256=digest.hexdigest(),
    )


class CliRunner:
    """Async structured-argv runner with bounded in-memory process output."""

    def __init__(
        self,
        *,
        executable: str = "databricks",
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        stdout_cap: int = DEFAULT_STDOUT_CAP,
        stderr_cap: int = DEFAULT_STDERR_CAP,
        profile_config_path: Path | None = None,
    ) -> None:
        if timeout_seconds <= 0 or stdout_cap <= 0 or stderr_cap <= 0:
            raise ValueError("runner limits must be positive")
        self.executable = executable
        self.timeout_seconds = timeout_seconds
        self.stdout_cap = stdout_cap
        self.stderr_cap = stderr_cap
        self.profile_config_path = profile_config_path
        self._resolved_executable: str | None = None
        self._certified_executable_witness: _ExecutableWitness | None = None

    def verify_profile_authority(self, *, profile: str, expected_fingerprint: str) -> None:
        """Fail before dispatch if the named profile no longer selects this workspace."""

        actual, _snapshot = _databricks_profile_authority(
            profile,
            config_file=self.profile_config_path,
        )
        if actual != expected_fingerprint:
            raise CommandRejected(
                "Databricks profile authority does not match the configured workspace"
            )

    def profile_authority_snapshot(
        self,
        *,
        profile: str,
        expected_fingerprint: str,
    ) -> bytes:
        actual, snapshot = _databricks_profile_authority(
            profile,
            config_file=self.profile_config_path,
        )
        if actual != expected_fingerprint:
            raise CommandRejected(
                "Databricks profile authority does not match the configured workspace"
            )
        return snapshot

    def resolve_executable(self) -> str:
        if self._resolved_executable is None:
            requested = Path(self.executable)
            if requested.is_absolute():
                candidates = (requested,)
            elif requested.name == self.executable:
                candidates = tuple(
                    Path(entry) / name
                    for entry in _absolute_path_entries(os.environ.get("PATH", ""))
                    for name in _candidate_executable_names(self.executable)
                )
            else:
                candidates = ()
            for candidate in candidates:
                if _usable_executable(candidate):
                    try:
                        self._resolved_executable = str(candidate.resolve(strict=True))
                    except OSError:
                        continue
                    else:
                        break
            if self._resolved_executable is None:
                raise CliUnavailable("Databricks CLI executable was not found on an absolute PATH")
        return self._resolved_executable

    def _current_executable_witness(self) -> _ExecutableWitness:
        try:
            return _executable_witness(Path(self.resolve_executable()))
        except OSError as exc:
            raise CliUnavailable("Databricks CLI executable could not be witnessed") from exc

    async def ensure_executable_compatibility(self) -> None:
        current = await asyncio.to_thread(self._current_executable_witness)
        if self._certified_executable_witness == current:
            return
        self._certified_executable_witness = None
        await self.doctor()

    async def _execute(
        self,
        argv: tuple[str, ...],
        *,
        correlation_id: str,
        timeout_message: str,
        profile_config_snapshot: bytes | None = None,
    ) -> CliExecution:
        started = time.monotonic()
        work_root = _trusted_cli_work_root()
        try:
            _recover_cli_work_root(work_root)
        except OSError as exc:
            raise CliUnavailable("Rookery CLI work recovery failed") from exc
        with (
            tempfile.TemporaryDirectory(
                prefix=_CLI_WORK_PREFIX,
                dir=work_root,
            ) as working_directory,
            ExclusiveFileLock(Path(working_directory) / _CLI_ACTIVE_LOCK),
        ):
            process_environment = _controlled_cli_environment()
            snapshot_path: Path | None = None
            try:
                if profile_config_snapshot is not None:
                    snapshot_path = Path(working_directory) / _CLI_PROFILE_SNAPSHOT
                    descriptor = os.open(
                        snapshot_path,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0),
                        0o600,
                    )
                    try:
                        remaining = memoryview(profile_config_snapshot)
                        while remaining:
                            written = os.write(descriptor, remaining)
                            if written <= 0:  # pragma: no cover - OS invariant
                                raise OSError("Databricks profile snapshot write made no progress")
                            remaining = remaining[written:]
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)
                    harden_private_file(snapshot_path)
                    process_environment["DATABRICKS_CONFIG_FILE"] = str(snapshot_path)
                try:
                    process_options = (
                        {"creationflags": 0x00000004}
                        if os.name == "nt"
                        else {"start_new_session": True}
                    )
                    process = await asyncio.create_subprocess_exec(
                        *argv,
                        stdin=asyncio.subprocess.DEVNULL,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                        env=process_environment,
                        cwd=working_directory,
                        **process_options,
                    )
                except OSError as exc:
                    raise CliUnavailable("Databricks CLI process could not start") from exc
                try:
                    process_tree = _ProcessTree(process)
                except OSError as exc:
                    if process.returncode is None:
                        process.kill()
                    await process.wait()
                    raise CliUnavailable("Rookery could not own the CLI process tree") from exc
                try:
                    stdout_task = asyncio.create_task(
                        _read_limited(process.stdout, self.stdout_cap)
                    )
                    stderr_task = asyncio.create_task(
                        _read_limited(process.stderr, self.stderr_cap)
                    )
                    wait_task = asyncio.create_task(process.wait())
                    try:
                        stdout, stderr, exit_code = await asyncio.wait_for(
                            asyncio.gather(stdout_task, stderr_task, wait_task),
                            timeout=self.timeout_seconds,
                        )
                    except TimeoutError as exc:
                        await _terminate_process(
                            process,
                            process_tree,
                            stdout_task,
                            stderr_task,
                            wait_task,
                        )
                        raise CliTimeout(timeout_message) from exc
                    except CliOutputLimit:
                        await _terminate_process(
                            process,
                            process_tree,
                            stdout_task,
                            stderr_task,
                            wait_task,
                        )
                        raise
                    except asyncio.CancelledError:
                        await _terminate_process(
                            process,
                            process_tree,
                            stdout_task,
                            stderr_task,
                            wait_task,
                        )
                        raise
                finally:
                    process_tree.close()
            finally:
                if snapshot_path is not None:
                    _remove_guarded_private_file(snapshot_path)
        return CliExecution(
            correlation_id,
            timedelta(seconds=time.monotonic() - started),
            exit_code,
            stdout,
            stderr,
        )

    async def run(self, invocation: CliInvocation, *, correlation_id: str) -> CliExecution:
        if invocation.argv[0] != self.executable:
            raise CommandRejected("invocation does not use configured executable")
        _validate_observation_invocation(invocation)
        if (
            invocation.authority_fingerprint is None
            or re.fullmatch(r"[0-9a-f]{64}", invocation.authority_fingerprint) is None
        ):
            raise CommandRejected("Databricks invocation requires an authority fingerprint")
        await self.ensure_executable_compatibility()
        profile = invocation.argv[-3]
        profile_config_snapshot = self.profile_authority_snapshot(
            profile=profile,
            expected_fingerprint=invocation.authority_fingerprint,
        )
        executable = self.resolve_executable()
        argv = (executable, *invocation.argv[1:])
        execution = await self._execute(
            argv,
            correlation_id=str(correlation_id),
            timeout_message="Databricks CLI timed out",
            profile_config_snapshot=profile_config_snapshot,
        )
        if execution.exit_code != 0:
            retry_after, retry_after_out_of_bounds = _downstream_retry_after(execution.stderr)
            raise DownstreamFailure(
                "Databricks CLI exited unsuccessfully",
                exit_code=execution.exit_code,
                diagnostic=redact_diagnostic(execution.stderr),
                retry_after=retry_after,
                retry_after_out_of_bounds=retry_after_out_of_bounds,
            )
        return execution

    async def doctor(self) -> None:
        """Verify one certified CLI release and every reachable leaf command surface."""
        self.resolve_executable()
        self._certified_executable_witness = None
        initial_witness = await asyncio.to_thread(self._current_executable_witness)
        version_result = await self.run_unmapped(
            CliInvocation("doctor", (self.executable, "--version"))
        )
        if version_result.exit_code != 0:
            raise CliIncompatible("Databricks CLI does not support '--version'")
        version_text = (version_result.stdout + b"\n" + version_result.stderr).decode(
            "utf-8", "replace"
        )
        matched = _VERSION.fullmatch(version_text.strip())
        version = tuple(int(part) for part in matched.groups()) if matched is not None else None
        if version not in CERTIFIED_CLI_VERSIONS:
            raise CliIncompatible(
                "Databricks CLI must be the certified version 0.298.0; other releases require "
                "compatibility review"
            )
        checks = (
            (("workspace", "list", "--help"), ("databricks workspace list PATH",)),
            (("workspace", "get-status", "--help"), ("databricks workspace get-status PATH",)),
            (
                ("workspace", "export", "--help"),
                ("databricks workspace export SOURCE_PATH", "--format"),
            ),
            (("catalogs", "list", "--help"), ("databricks catalogs list",)),
            (("schemas", "list", "--help"), ("databricks schemas list CATALOG_NAME",)),
            (
                ("tables", "list", "--help"),
                ("databricks tables list CATALOG_NAME SCHEMA_NAME",),
            ),
            (
                ("volumes", "list", "--help"),
                ("databricks volumes list CATALOG_NAME SCHEMA_NAME",),
            ),
        )
        for args, required_text in checks:
            invocation = CliInvocation("doctor", (self.executable, *args))
            result = await self.run_unmapped(invocation)
            text = (result.stdout + b"\n" + result.stderr).decode("utf-8", "replace")
            if result.exit_code != 0:
                raise CliIncompatible(f"Databricks CLI does not support {' '.join(args)!r}")
            required = (*required_text, "--profile", "--output")
            missing = tuple(token for token in required if token not in text)
            if missing:
                raise CliIncompatible(
                    f"Databricks CLI {' '.join(args[:-1])!r} help lacks required contract: "
                    f"{', '.join(missing)}"
                )
        final_witness = await asyncio.to_thread(self._current_executable_witness)
        if final_witness != initial_witness:
            raise CliIncompatible("Databricks CLI executable changed during compatibility checks")
        self._certified_executable_witness = final_witness

    async def run_unmapped(self, invocation: CliInvocation) -> CliExecution:
        """Internal doctor path; public observation calls must use ``run``."""
        if invocation.capability_key != "doctor" or invocation.argv[1:] not in {
            ("--version",),
            ("workspace", "list", "--help"),
            ("workspace", "get-status", "--help"),
            ("workspace", "export", "--help"),
            ("catalogs", "list", "--help"),
            ("schemas", "list", "--help"),
            ("tables", "list", "--help"),
            ("volumes", "list", "--help"),
        }:
            raise CommandRejected("unregistered Databricks compatibility check")
        resolved = self.resolve_executable()
        return await self._execute(
            (resolved, *invocation.argv[1:]),
            correlation_id="doctor",
            timeout_message="Databricks CLI compatibility check timed out",
        )


def _json_output(stdout: bytes) -> Mapping[str, Any] | Sequence[Mapping[str, Any]]:
    if len(stdout) > MAX_JSON_BYTES:
        raise InvalidDownstreamResponse("Databricks JSON response exceeds configured limit")
    try:
        value = json.loads(stdout)
    except (RecursionError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidDownstreamResponse("Databricks CLI returned invalid JSON") from exc
    _validate_json_shape(value)
    if isinstance(value, Mapping):
        return value
    if isinstance(value, list) and all(isinstance(item, Mapping) for item in value):
        return value
    raise InvalidDownstreamResponse(
        "Databricks CLI JSON root must be an object or a homogeneous object array"
    )


def _validate_json_shape(value: object, *, depth: int = 0) -> None:
    if depth > MAX_JSON_DEPTH:
        raise InvalidDownstreamResponse("Databricks JSON nesting exceeds the configured limit")
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise InvalidDownstreamResponse("Databricks JSON object keys must be text")
            _validate_json_shape(item, depth=depth + 1)
    elif isinstance(value, list):
        for item in value:
            _validate_json_shape(item, depth=depth + 1)
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise InvalidDownstreamResponse("Databricks JSON contains an unsupported value")


def _mapping_payload(
    payload: Mapping[str, Any] | Sequence[Mapping[str, Any]], *, capability: str
) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        raise InvalidDownstreamResponse(f"{capability} requires a JSON object response")
    return payload


def _items(
    payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    *keys: str,
    cap: int = MAX_COLLECTION_ITEMS,
) -> Sequence[Mapping[str, Any]]:
    if not isinstance(payload, Mapping):
        value = payload
    else:
        next_page_token = payload.get("next_page_token")
        if next_page_token is not None and next_page_token != "":
            raise InvalidDownstreamResponse(
                "Databricks CLI returned an incomplete paginated collection"
            )
        value: Any = None
        for key in keys:
            if key in payload:
                value = payload[key]
                break
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise InvalidDownstreamResponse("Databricks collection response has invalid item structure")
    if len(value) > cap:
        raise InvalidDownstreamResponse("Databricks collection exceeds the configured item limit")
    return value


def _id(action: AdapterAction, label: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"async-api-view/databricks/{action.action_id}/{label}"))


def _scopes(action: AdapterAction, facet: str) -> tuple:
    return tuple(scope for scope in action.requested_scopes if scope.facet == facet)


def _workspace_identity_witnesses(item: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    witnesses: list[tuple[str, str]] = []
    for key in ("object_id", "resource_id"):
        value = item.get(key)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise InvalidDownstreamResponse(f"Workspace {key} has an invalid identity value")
        witness = str(value)
        if not witness or len(witness) > 1024 or _BLOCKED_VALUE.search(witness):
            raise InvalidDownstreamResponse(f"Workspace {key} has an unsafe identity value")
        witnesses.append((key, witness))
    return tuple(witnesses)


def _workspace_locator(item: Mapping[str, Any]) -> ObjectLocator:
    path = item.get("path")
    if not isinstance(path, str) or not path.startswith("/"):
        raise InvalidDownstreamResponse("workspace object lacks an absolute path")
    kind = item.get("object_type")
    if kind == "DIRECTORY":
        object_type, source_kind = "folder", "databricks.workspace.folder"
    elif kind in {"FILE", "NOTEBOOK"}:
        object_type, source_kind = "file", "databricks.workspace.file"
    else:
        object_type, source_kind = "generic_object", "databricks.workspace.object"
    identity_witnesses = _workspace_identity_witnesses(item)
    if identity_witnesses:
        canonical_kind, canonical_value = identity_witnesses[0]
        external_key = f"workspace:{canonical_kind}:{canonical_value}"
    else:
        external_key = f"workspace:{path}"
    return ObjectLocator(
        object_type=object_type,
        object_id=None,
        source_kind=source_kind,
        external_key=external_key,
        display_name=path.rsplit("/", 1)[-1] or "/",
    )


def _canonical_workspace_locator(target: ResolvedTarget, *, expected_type: str) -> ObjectLocator:
    if target.canonical_object_id is None or target.canonical_object_type is None:
        raise InvalidDownstreamResponse("Workspace target lacks canonical object identity")
    if target.canonical_object_type != expected_type:
        raise InvalidDownstreamResponse(
            "Workspace target has an incompatible canonical object type"
        )
    return ObjectLocator(
        object_type=target.canonical_object_type, object_id=target.canonical_object_id
    )


def _validate_workspace_target_response(
    item: Mapping[str, Any], target: ResolvedTarget, *, expected_type: str
) -> ObjectLocator:
    requested_path = _workspace_path(target.workspace_path, target.workspace_root)
    response_path = _workspace_path(item.get("path"), target.workspace_root)
    if response_path != requested_path:
        raise InvalidDownstreamResponse("Workspace response does not match the requested target")
    response = _workspace_locator(item)
    if response.object_type != expected_type:
        raise InvalidDownstreamResponse(
            "Workspace response type does not match the requested target"
        )
    expected_external_key = target.canonical_parent_external_key
    if expected_external_key is not None:
        if not expected_external_key.startswith("workspace:"):
            raise InvalidDownstreamResponse(
                "Workspace target has an incompatible external identity"
            )
        expected_witness = expected_external_key.removeprefix("workspace:")
        identity_witnesses = _workspace_identity_witnesses(item)
        if expected_witness.startswith("/"):
            if response_path != expected_witness:
                raise InvalidDownstreamResponse(
                    "Workspace response path contradicts canonical identity"
                )
        elif expected_witness.startswith(("object_id:", "resource_id:")):
            expected_kind, expected_value = expected_witness.split(":", 1)
            if (expected_kind, expected_value) not in identity_witnesses:
                raise InvalidDownstreamResponse(
                    "Workspace response identity does not match the canonical target"
                )
        elif not identity_witnesses or expected_witness not in {
            value for _kind, value in identity_witnesses
        }:
            raise InvalidDownstreamResponse(
                "Workspace response identity does not match the canonical target"
            )
    return _canonical_workspace_locator(target, expected_type=expected_type)


def _direct_workspace_child(parent_path: str, item: Mapping[str, Any]) -> None:
    try:
        child_path = _workspace_path(item.get("path"), parent_path)
    except CommandRejected as exc:
        raise InvalidDownstreamResponse("Workspace list returned an out-of-scope child") from exc
    if child_path == parent_path or PurePosixPath(child_path).parent != PurePosixPath(parent_path):
        raise InvalidDownstreamResponse("Workspace list returned an out-of-scope child")


def _workspace_payload(item: Mapping[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "namespace": "databricks.workspace",
        "schema_version": "1",
        "path": str(item["path"]),
        "source_type": str(item.get("object_type", "UNKNOWN")),
    }
    for source, destination in (
        ("language", "language"),
        ("size", "byte_count"),
        ("created_at", "remote_created_at"),
        ("modified_at", "remote_modified_at"),
    ):
        value = item.get(source)
        if isinstance(value, (str, int, float, bool)) or (value is None and source in item):
            payload[destination] = value
    return payload


def _generic_locator(source_kind: str, key: str, display_name: str) -> ObjectLocator:
    return ObjectLocator(
        object_type="generic_object",
        source_kind=source_kind,
        external_key=key,
        display_name=display_name,
    )


def _canonical_uc_parent(
    target: ResolvedTarget,
    fallback: ObjectLocator,
) -> ObjectLocator:
    if target.canonical_object_id is None:
        return fallback
    if target.canonical_object_type != "generic_object":
        raise InvalidDownstreamResponse("Unity Catalog parent has incompatible canonical identity")
    return ObjectLocator(object_type="generic_object", object_id=target.canonical_object_id)


def _generic_payload(item: Mapping[str, Any], *, entity: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "namespace": "databricks.unity_catalog",
        "schema_version": "1",
        "entity": entity,
    }
    allowed = {
        "name",
        "full_name",
        "comment",
        "owner",
        "created_at",
        "updated_at",
        "table_type",
        "data_source_format",
        "schema_id",
        "table_id",
        "volume_id",
    }
    for key in sorted(allowed):
        value = item.get(key)
        if isinstance(value, str) and len(value) > (4096 if key == "comment" else 1024):
            raise InvalidDownstreamResponse(f"Databricks {key} exceeds the configured limit")
        if isinstance(value, (str, int, float, bool)) or (value is None and key in item):
            payload[key] = value
    if entity in {"table", "view"} and "columns" in item:
        if not isinstance(item["columns"], list) or not all(
            isinstance(column, Mapping) for column in item["columns"]
        ):
            raise InvalidDownstreamResponse("Databricks table columns have invalid structure")
        if len(item["columns"]) > MAX_TABLE_COLUMNS:
            raise InvalidDownstreamResponse("Databricks table exceeds the configured column limit")
        columns: list[dict[str, Any]] = []
        for column in item["columns"]:
            retained = {
                key: column[key]
                for key in ("name", "type_name", "type_text", "comment", "position")
                if key in column and isinstance(column[key], (str, int, float, bool))
            }
            if any(
                isinstance(value, str) and len(value) > (4096 if key == "comment" else 1024)
                for key, value in retained.items()
            ):
                raise InvalidDownstreamResponse(
                    "Databricks column metadata exceeds the configured limit"
                )
            if retained:
                columns.append(retained)
        payload["columns"] = columns
    return payload


def _uc_source_uuid(
    item: Mapping[str, Any],
    key: str,
    *,
    label: str,
) -> str | None:
    if key not in item:
        return None
    value = item[key]
    if not isinstance(value, str):
        raise InvalidDownstreamResponse(f"Databricks {label} {key} is not a UUID")
    try:
        return str(UUID(value))
    except ValueError as exc:
        raise InvalidDownstreamResponse(f"Databricks {label} {key} is not a UUID") from exc


def _uc_child_name(
    item: Mapping[str, Any],
    *,
    catalog: str,
    schema: str | None = None,
    label: str,
) -> str:
    if "catalog_name" in item and _name(item["catalog_name"], label="catalog name") != catalog:
        raise InvalidDownstreamResponse(f"Databricks {label} belongs to another catalog")
    if (
        schema is not None
        and "schema_name" in item
        and _name(item["schema_name"], label="schema name") != schema
    ):
        raise InvalidDownstreamResponse(f"Databricks {label} belongs to another schema")
    name = _name(item["name"], label=f"{label} name") if "name" in item else None
    if "full_name" not in item:
        if name is None:
            raise InvalidDownstreamResponse(f"Databricks {label} lacks a name")
        return name
    full_name = _name(item["full_name"], label=f"{label} full name")
    parts = full_name.split(".")
    expected_parent = (catalog,) if schema is None else (catalog, schema)
    if len(parts) != len(expected_parent) + 1 or tuple(parts[:-1]) != expected_parent:
        raise InvalidDownstreamResponse(f"Databricks {label} full name contradicts its target")
    if name is not None and name != parts[-1]:
        raise InvalidDownstreamResponse(f"Databricks {label} name contradicts its full name")
    return parts[-1]


def _canonical_batch_size(batch: ObservationBatch) -> int:
    return len(canonical_observation_batch_bytes(batch))


def _observation_units(
    batch: ObservationBatch,
) -> tuple[tuple[tuple[FacetObservation, ...], tuple[RelationshipObservation, ...]], ...]:
    facets_by_target: dict[ObjectLocator, list[FacetObservation]] = {}
    relationships_by_target: dict[ObjectLocator, list[RelationshipObservation]] = {}
    for observation in batch.facet_observations:
        facets_by_target.setdefault(observation.target, []).append(observation)
    for observation in batch.relationship_observations:
        relationships_by_target.setdefault(observation.object, []).append(observation)

    def target_key(target: ObjectLocator) -> bytes:
        return canonical_json_bytes(target.to_dict(), "observation target")

    all_targets = set(facets_by_target) | set(relationships_by_target)
    standalone = sorted(
        (target for target in all_targets if target not in relationships_by_target),
        key=target_key,
    )
    linked = sorted(
        (target for target in all_targets if target in relationships_by_target),
        key=target_key,
    )
    return tuple(
        (
            tuple(
                sorted(
                    facets_by_target.get(target, ()),
                    key=lambda item: item.observation_id,
                )
            ),
            tuple(
                sorted(
                    relationships_by_target.get(target, ()),
                    key=lambda item: item.observation_id,
                )
            ),
        )
        for target in (*linked, *standalone)
    )


def _chunk_normalized_batch(
    *,
    action: AdapterAction,
    delivery_id: str,
    batch: ObservationBatch,
) -> tuple[ObservationBatch, ...]:
    units = _observation_units(batch)
    batch = replace(
        batch,
        facet_observations=tuple(
            observation for unit_facets, _unit_relationships in units for observation in unit_facets
        ),
        relationship_observations=tuple(
            observation
            for _unit_facets, unit_relationships in units
            for observation in unit_relationships
        ),
    )
    if (
        len(units) <= MAX_INGESTION_BATCH_UNITS
        and _canonical_batch_size(batch) <= MAX_INGESTION_BATCH_BYTES
    ):
        return (batch,)

    if not units:
        raise InvalidDownstreamResponse("normalized collection coverage exceeds the batch limit")
    packed: list[tuple[tuple[FacetObservation, ...], tuple[RelationshipObservation, ...]]] = []
    current_facets: tuple[FacetObservation, ...] = ()
    current_relationships: tuple[RelationshipObservation, ...] = ()
    current_unit_count = 0
    current_facet_bytes = 0
    current_relationship_bytes = 0

    def candidate(
        index: int,
        facets: tuple[FacetObservation, ...],
        relationships: tuple[RelationshipObservation, ...],
        coverage: tuple[CoverageDeclaration, ...],
    ) -> ObservationBatch:
        return replace(
            batch,
            batch_id=_id(action, f"batch:{delivery_id}:collection-v1:data:{index}"),
            facet_observations=facets,
            relationship_observations=relationships,
            coverage=coverage,
        )

    empty_size = _canonical_batch_size(candidate(0, (), (), batch.coverage))

    def observation_size(
        observation: FacetObservation | RelationshipObservation,
    ) -> int:
        value = observation.to_dict()
        if not value.get("authorized_by"):
            value.pop("authorized_by", None)
        return len(canonical_json_bytes(value, "observation item"))

    def packed_size(
        facet_bytes: int,
        facet_count: int,
        relationship_bytes: int,
        relationship_count: int,
    ) -> int:
        return (
            empty_size
            + facet_bytes
            + max(0, facet_count - 1)
            + relationship_bytes
            + max(0, relationship_count - 1)
        )

    for unit_facets, unit_relationships in units:
        unit_facet_bytes = sum(observation_size(item) for item in unit_facets)
        unit_relationship_bytes = sum(observation_size(item) for item in unit_relationships)
        proposed_facets = (*current_facets, *unit_facets)
        proposed_relationships = (*current_relationships, *unit_relationships)
        proposed_size = packed_size(
            current_facet_bytes + unit_facet_bytes,
            len(proposed_facets),
            current_relationship_bytes + unit_relationship_bytes,
            len(proposed_relationships),
        )
        if (
            current_unit_count + 1 <= MAX_INGESTION_BATCH_UNITS
            and proposed_size <= MAX_INGESTION_BATCH_BYTES
        ):
            current_facets = proposed_facets
            current_relationships = proposed_relationships
            current_unit_count += 1
            current_facet_bytes += unit_facet_bytes
            current_relationship_bytes += unit_relationship_bytes
            continue
        if current_facets or current_relationships:
            packed.append((current_facets, current_relationships))
            current_facets = unit_facets
            current_relationships = unit_relationships
            current_unit_count = 1
            current_facet_bytes = unit_facet_bytes
            current_relationship_bytes = unit_relationship_bytes
            proposed_size = packed_size(
                current_facet_bytes,
                len(current_facets),
                current_relationship_bytes,
                len(current_relationships),
            )
        if proposed_size > MAX_INGESTION_BATCH_BYTES:
            raise InvalidDownstreamResponse(
                "one normalized collection item exceeds the ingestion batch limit"
            )
    if current_facets or current_relationships:
        packed.append((current_facets, current_relationships))
    if len(packed) <= 1:  # pragma: no cover - full batch already exceeded the target
        raise InvalidDownstreamResponse("normalized collection exceeds the ingestion batch limit")
    if any(
        declaration.completeness is CollectionCoverage.COMPLETE or declaration.absence_authority
        for declaration in batch.coverage
    ):
        raise InvalidDownstreamResponse(
            "complete collection evidence cannot span ingestion batches"
        )

    parts = tuple(
        candidate(
            index,
            facets,
            relationships,
            batch.coverage,
        )
        for index, (facets, relationships) in enumerate(packed)
    )
    if any(_canonical_batch_size(part) > MAX_INGESTION_BATCH_BYTES for part in parts):
        raise RuntimeError("normalized collection chunk exceeded its packing boundary")
    return parts


def normalize(
    *,
    action: AdapterAction,
    binding: ConnectionBinding,
    target: ResolvedTarget,
    delivery_id: str,
    stdout: bytes,
    observed_at: datetime,
) -> NormalizedResult:
    """Validate sanitized JSON and create deterministic canonical evidence."""
    payload = _json_output(stdout)
    observed_at = observed_at.astimezone(UTC)
    action = replace(
        action,
        requested_scopes=tuple(
            replace(scope, capability_key=action.capability_key)
            if scope.capability_key is None
            else scope
            for scope in action.requested_scopes
        ),
    )

    def evidence_id(label: str) -> str:
        return _id(action, f"delivery:{delivery_id}:{label}")

    base = dict(
        batch_id=_id(action, f"batch:{delivery_id}"),
        system_id=action.system_id,
        connection_binding_id=binding.binding_id,
        adapter_key=DATABRICKS_ADAPTER_KEY,
        adapter_version=DATABRICKS_ADAPTER_VERSION,
        observed_at=observed_at,
        received_at=observed_at,
        action_id=action.action_id,
    )
    facets: list[FacetObservation] = []
    relationships: list[RelationshipObservation] = []
    coverage: list[CoverageDeclaration] = []
    capability = action.capability_key
    if capability == "databricks.workspace.children.read":
        root_path = _workspace_path(target.workspace_path, target.workspace_root)
        if target.canonical_object_id is None and action.target.kind.value == "configured_scope":
            root = ObjectLocator(object_type="folder", object_id=action.target.target_id)
        else:
            root = _canonical_workspace_locator(target, expected_type="folder")
        children = _items(payload, "objects")
        facets.append(
            FacetObservation(
                evidence_id("root-membership"),
                root,
                "membership",
                "1",
                UpdateMode.PATCH,
                FieldCoverage.PARTIAL,
                {
                    "collection_completeness": CollectionCoverage.UNKNOWN.value,
                    "namespace": "databricks.workspace",
                    "schema_version": "1",
                    "member_count": len(children),
                },
                (
                    "collection_completeness",
                    "namespace",
                    "schema_version",
                    "member_count",
                ),
                _scopes(action, "membership"),
            )
        )
        child_paths: set[str] = set()
        child_keys: set[str] = set()
        child_identity_witnesses: set[tuple[str, str]] = set()
        for item in children:
            _direct_workspace_child(root_path, item)
            child = _workspace_locator(item)
            child_path = _workspace_path(item["path"], root_path)
            identity_witnesses = set(_workspace_identity_witnesses(item))
            parent_witness = (
                target.canonical_parent_external_key.removeprefix("workspace:")
                if target.canonical_parent_external_key
                and target.canonical_parent_external_key.startswith("workspace:")
                else None
            )
            parent_typed_witness = (
                tuple(parent_witness.split(":", 1))
                if parent_witness is not None
                and parent_witness.startswith(("object_id:", "resource_id:"))
                else None
            )
            if (
                child_path in child_paths
                or child.external_key in child_keys
                or not child_identity_witnesses.isdisjoint(identity_witnesses)
                or (
                    parent_witness is not None
                    and parent_typed_witness is None
                    and parent_witness in {value for _kind, value in identity_witnesses}
                )
                or (parent_typed_witness is not None and parent_typed_witness in identity_witnesses)
                or child.external_key == target.canonical_parent_external_key
            ):
                raise InvalidDownstreamResponse("Workspace list contains duplicate child identity")
            child_paths.add(child_path)
            child_keys.add(child.external_key or "")
            child_identity_witnesses.update(identity_witnesses)
            facets.append(
                FacetObservation(
                    evidence_id(f"metadata:{child.external_key}"),
                    child,
                    "metadata" if child.object_type != "generic_object" else "attributes",
                    "1",
                    UpdateMode.PATCH,
                    FieldCoverage.PARTIAL,
                    _workspace_payload(item),
                    tuple(_workspace_payload(item)),
                    _scopes(action, "metadata"),
                )
            )
            relationships.append(
                RelationshipObservation(
                    evidence_id(f"contains:{child.external_key}"),
                    root,
                    "contains",
                    child,
                    PresenceState.PRESENT,
                )
            )
        coverage.extend(
            CoverageDeclaration(scope, CollectionCoverage.UNKNOWN)
            for scope in action.requested_scopes
            if scope.facet == "membership"
        )
    elif capability == "databricks.workspace.metadata.read":
        payload = _mapping_payload(payload, capability=capability)
        item = payload.get("object", payload)
        if not isinstance(item, Mapping):
            raise InvalidDownstreamResponse("workspace metadata response lacks object")
        inferred = _workspace_locator(item)
        if inferred.object_type not in {"folder", "file"}:
            raise InvalidDownstreamResponse(
                "Workspace metadata response has an unsupported object type"
            )
        locator = _validate_workspace_target_response(
            item, target, expected_type=inferred.object_type
        )
        facet = "metadata"
        body = _workspace_payload(item)
        facets.append(
            FacetObservation(
                evidence_id(f"metadata:{locator.external_key}"),
                locator,
                facet,
                "1",
                UpdateMode.PATCH,
                FieldCoverage.PARTIAL,
                body,
                tuple(body),
                _scopes(action, "metadata"),
            )
        )
        coverage.extend(
            CoverageDeclaration(scope, CollectionCoverage.COMPLETE)
            for scope in _scopes(action, "metadata")
        )
    elif capability == "databricks.workspace.content.read":
        payload = _mapping_payload(payload, capability=capability)
        _normalize_content(
            action,
            binding,
            target,
            payload,
            facets,
            delivery_id=delivery_id,
        )
    elif capability == "databricks.uc.catalogs.read":
        if target.canonical_object_id is None or target.canonical_object_type != "generic_object":
            raise InvalidDownstreamResponse("catalog collection lacks canonical target identity")
        parent = ObjectLocator(
            object_type="generic_object",
            object_id=target.canonical_object_id,
        )
        for item in _items(payload, "catalogs"):
            name = _name(item.get("name"), label="catalog name")
            locator = _generic_locator("databricks.uc.catalog", f"catalog:{name}", name)
            body = _generic_payload(item, entity="catalog")
            facets.append(
                FacetObservation(
                    evidence_id(f"catalog:{name}"),
                    locator,
                    "attributes",
                    "1",
                    UpdateMode.PATCH,
                    FieldCoverage.PARTIAL,
                    body,
                    tuple(body),
                    _scopes(action, "attributes"),
                )
            )
            relationships.append(
                RelationshipObservation(
                    evidence_id(f"contains:{locator.external_key}"),
                    parent,
                    "contains",
                    locator,
                    PresenceState.PRESENT,
                )
            )
        coverage.extend(
            CoverageDeclaration(scope, CollectionCoverage.UNKNOWN)
            for scope in action.requested_scopes
        )
    elif capability == "databricks.uc.schemas.read":
        catalog = _name(target.catalog_name, label="catalog name")
        parent = _canonical_uc_parent(
            target,
            _generic_locator("databricks.uc.catalog", f"catalog:{catalog}", catalog),
        )
        for item in _items(payload, "schemas"):
            name = _uc_child_name(item, catalog=catalog, label="schema")
            source_id = _uc_source_uuid(item, "schema_id", label="schema")
            external_key = (
                f"schema:schema_id:{source_id}"
                if source_id is not None
                else f"schema:{catalog}.{name}"
            )
            locator = _generic_locator("databricks.uc.schema", external_key, name)
            body = _generic_payload(item, entity="schema")
            body.update({"name": name, "full_name": f"{catalog}.{name}"})
            if source_id is not None:
                body["schema_id"] = source_id
            facets.append(
                FacetObservation(
                    evidence_id(f"schema:{locator.external_key}"),
                    locator,
                    "attributes",
                    "1",
                    UpdateMode.PATCH,
                    FieldCoverage.PARTIAL,
                    body,
                    tuple(body),
                    _scopes(action, "attributes"),
                )
            )
            relationships.append(
                RelationshipObservation(
                    evidence_id(f"contains:{locator.external_key}"),
                    parent,
                    "contains",
                    locator,
                    PresenceState.PRESENT,
                )
            )
        coverage.extend(
            CoverageDeclaration(scope, CollectionCoverage.UNKNOWN)
            for scope in action.requested_scopes
        )
    elif capability in {"databricks.uc.relations.read", "databricks.uc.volumes.read"}:
        catalog = _name(target.catalog_name, label="catalog name")
        schema = _name(target.schema_name, label="schema name")
        parent = _canonical_uc_parent(
            target,
            _generic_locator("databricks.uc.schema", f"schema:{catalog}.{schema}", schema),
        )
        key, entity = (
            ("tables", "relation")
            if capability.endswith("relations.read")
            else ("volumes", "volume")
        )
        for item in _items(payload, key):
            item_entity = (
                "view"
                if entity == "relation" and str(item.get("table_type", "")).upper() == "VIEW"
                else "table"
                if entity == "relation"
                else entity
            )
            name = _uc_child_name(
                item,
                catalog=catalog,
                schema=schema,
                label=item_entity,
            )
            source_key = "table_id" if entity == "relation" else "volume_id"
            source_id = _uc_source_uuid(item, source_key, label=item_entity)
            external_key = (
                f"{entity}:{source_key}:{source_id}"
                if source_id is not None
                else f"{item_entity}:{catalog}.{schema}.{name}"
            )
            locator = _generic_locator(
                f"databricks.uc.{item_entity}",
                external_key,
                name,
            )
            body = _generic_payload(item, entity=item_entity)
            body.update({"name": name, "full_name": f"{catalog}.{schema}.{name}"})
            if source_id is not None:
                body[source_key] = source_id
            facets.append(
                FacetObservation(
                    evidence_id(f"{item_entity}:{locator.external_key}"),
                    locator,
                    "attributes",
                    "1",
                    UpdateMode.PATCH,
                    FieldCoverage.PARTIAL,
                    body,
                    tuple(body),
                    _scopes(action, "attributes"),
                )
            )
            relationships.append(
                RelationshipObservation(
                    evidence_id(f"contains:{locator.external_key}"),
                    parent,
                    "contains",
                    locator,
                    PresenceState.PRESENT,
                )
            )
        coverage.extend(
            CoverageDeclaration(scope, CollectionCoverage.UNKNOWN)
            for scope in action.requested_scopes
        )
    else:  # defensive even though command registry is closed
        raise CommandRejected("unregistered Databricks capability")
    artifacts = (
        _content_artifacts(action, binding, target, payload)
        if capability.endswith("content.read")
        else ()
    )
    batch = ObservationBatch(
        **base,
        facet_observations=tuple(
            replace(item, authorized_by=action.requested_scopes) for item in facets
        ),
        relationship_observations=tuple(
            replace(item, authorized_by=action.requested_scopes) for item in relationships
        ),
        coverage=tuple(coverage),
    )
    batches = _chunk_normalized_batch(
        action=action,
        delivery_id=delivery_id,
        batch=batch,
    )
    return NormalizedResult(batches, artifacts)


def _content_settings(binding: ConnectionBinding) -> tuple[int, str | None]:
    settings = binding.non_secret_settings
    if settings.get("content_capture_enabled") is not True:
        raise ContentPolicyError("Workspace content capture is disabled by policy")
    maximum = settings.get("content_max_bytes")
    if not isinstance(maximum, int) or maximum <= 0:
        raise ContentPolicyError("Workspace content policy requires a positive content_max_bytes")
    media_type = settings.get("content_media_type")
    return maximum, media_type if isinstance(media_type, str) else None


def _content_bytes(payload: Mapping[str, Any], maximum: int) -> bytes:
    value = payload.get("content")
    if not isinstance(value, str):
        raise InvalidDownstreamResponse("workspace export response lacks base64 content")
    try:
        content = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise InvalidDownstreamResponse("workspace export content is not valid base64") from exc
    if len(content) > maximum:
        raise ContentPolicyError("Workspace content exceeds configured policy limit")
    return content


def _normalize_content(
    action: AdapterAction,
    binding: ConnectionBinding,
    target: ResolvedTarget,
    payload: Mapping[str, Any],
    facets: list[FacetObservation],
    *,
    delivery_id: str,
) -> None:
    maximum, media_type = _content_settings(binding)
    content = _content_bytes(payload, maximum)
    path = _workspace_path(target.workspace_path, target.workspace_root)
    if "path" in payload and _workspace_path(payload["path"], target.workspace_root) != path:
        raise InvalidDownstreamResponse(
            "Workspace content response does not match the requested target"
        )
    locator = _canonical_workspace_locator(target, expected_type="file")
    body: dict[str, Any] = {
        "namespace": "databricks.workspace",
        "schema_version": "1",
        "byte_count": len(content),
        "capture_complete": True,
        "encoding": "base64",
    }
    if media_type:
        body["media_type"] = media_type
    facets.append(
        FacetObservation(
            _id(action, f"delivery:{delivery_id}:content:{locator.external_key}"),
            locator,
            "content",
            "1",
            UpdateMode.PATCH,
            FieldCoverage.PARTIAL,
            body,
            tuple(body),
            (),
            source_revision=str(payload["revision"]) if "revision" in payload else None,
        )
    )


def _content_artifacts(
    action: AdapterAction,
    binding: ConnectionBinding,
    target: ResolvedTarget,
    payload: Mapping[str, Any],
) -> tuple[ContentArtifact, ...]:
    maximum, media_type = _content_settings(binding)
    content = _content_bytes(payload, maximum)
    path = _workspace_path(target.workspace_path, target.workspace_root)
    if "path" in payload and _workspace_path(payload["path"], target.workspace_root) != path:
        raise InvalidDownstreamResponse(
            "Workspace content response does not match the requested target"
        )
    locator = _canonical_workspace_locator(target, expected_type="file")
    revision = str(payload["revision"]) if "revision" in payload else None
    return (ContentArtifact(locator, content, media_type, revision),)


def classify_failure(exc: BaseException) -> ErrorClass:
    if isinstance(exc, (CliTimeout, asyncio.TimeoutError)):
        return ErrorClass.CONNECTION_TIMEOUT
    if isinstance(exc, (CliUnavailable, CliIncompatible, CommandRejected, ContentPolicyError)):
        return ErrorClass.ADAPTER_CONTRACT_MISMATCH
    if isinstance(exc, (CliOutputLimit, InvalidDownstreamResponse)):
        return ErrorClass.INVALID_DOWNSTREAM_RESPONSE
    if isinstance(exc, DownstreamFailure):
        if exc.retry_after is not None or exc.retry_after_out_of_bounds:
            return ErrorClass.DOWNSTREAM_RATE_LIMIT
        message = exc.diagnostic.lower()
        if "429" in message or "rate limit" in message:
            return ErrorClass.DOWNSTREAM_RATE_LIMIT
        if "401" in message or "unauthenticated" in message or "invalid token" in message:
            return ErrorClass.AUTHENTICATION
        if "403" in message or "permission denied" in message or "not authorized" in message:
            return ErrorClass.AUTHORIZATION
        if "404" in message or "not found" in message:
            return ErrorClass.NOT_FOUND
        if any(token in message for token in ("timeout", "timed out")):
            return ErrorClass.CONNECTION_TIMEOUT
        if any(
            token in message
            for token in (
                "unavailable",
                "connection refused",
                "temporarily",
                "reset by peer",
                "502",
                "503",
                "504",
            )
        ):
            return ErrorClass.TRANSIENT_DOWNSTREAM
    return ErrorClass.UNKNOWN_ADAPTER_FAILURE


def _retryable(error: ErrorClass) -> bool:
    return error in {
        ErrorClass.CONNECTION_TIMEOUT,
        ErrorClass.TRANSIENT_DOWNSTREAM,
        ErrorClass.DOWNSTREAM_RATE_LIMIT,
    }


def _retry_delay(
    error: ErrorClass,
    ordinal: int,
    downstream_retry_after: timedelta | None = None,
) -> timedelta:
    base_seconds = 5.0 if error is ErrorClass.DOWNSTREAM_RATE_LIMIT else 1.0
    seconds = min(MAX_RETRY_DELAY_SECONDS, base_seconds * (2 ** min(ordinal - 1, 5)))
    return max(timedelta(seconds=seconds), downstream_retry_after or timedelta())


class DatabricksWorker:
    """Queue worker that has no storage dependency or direct canonical writes."""

    def __init__(
        self,
        *,
        worker_id: str,
        queue: ActionQueuePort,
        lifecycle: ActionLifecyclePort,
        guard: PreDispatchGuardPort,
        bindings: BindingQueryPort,
        ingestion: ObservationIngestionPort,
        targets: TargetResolver,
        runner: CliRunner,
        max_attempts: int = 2,
        heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
    ) -> None:
        if max_attempts < 1 or heartbeat_seconds <= 0:
            raise ValueError("worker limits must be positive")
        self.worker_id = _safe_text(worker_id, name="worker id", maximum=256)
        self.queue = queue
        self.lifecycle = lifecycle
        self.guard = guard
        self.bindings = bindings
        self.ingestion = ingestion
        self.targets = targets
        self.runner = runner
        self.max_attempts = max_attempts
        self.heartbeat_seconds = heartbeat_seconds

    async def startup(self) -> None:
        await self.runner.doctor()

    async def run_once(self, *, now: datetime | None = None) -> bool:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        lease = await self.queue.lease_next(
            adapter_key=DATABRICKS_ADAPTER_KEY, worker_id=self.worker_id, now=current
        )
        if lease is None:
            return False
        await self.process(lease, now=now)
        return True

    async def process(self, lease: ActionLease, *, now: datetime | None = None) -> None:
        action = lease.action
        current = (now or datetime.now(UTC)).astimezone(UTC)
        if (
            action.adapter_key != DATABRICKS_ADAPTER_KEY
            or action.adapter_version != DATABRICKS_ADAPTER_VERSION
            or action.capability_key not in CAPABILITIES
        ):
            await self._complete(
                lease,
                action,
                ActionOutcome.FAILED,
                ErrorClass.ADAPTER_CONTRACT_MISMATCH,
                current,
            )
            return
        decision = await self.guard.evaluate(
            action_id=action.action_id, lease_id=lease.lease_id, now=current
        )
        if decision.disposition is not GuardDisposition.DISPATCH:
            return
        try:
            binding = await self._binding(action)
            profile = _profile(binding.non_secret_settings)
            self.runner.verify_profile_authority(
                profile=profile,
                expected_fingerprint=_authority_fingerprint(binding.non_secret_settings),
            )
            if action.capability_key == "databricks.workspace.content.read":
                raise ContentPolicyError(
                    "Workspace content persistence is unavailable in this worker"
                )
            target = await self.targets.resolve(action=action, binding=binding)
            target = _enforce_binding_target(action.capability_key, binding, target)
            invocation = DatabricksCommandRegistry.build(
                capability_key=action.capability_key,
                profile=profile,
                target=target,
                executable=self.runner.executable,
            )
            invocation = replace(
                invocation,
                authority_fingerprint=_authority_fingerprint(binding.non_secret_settings),
            )
        except LifecyclePersistenceFailure:
            raise
        except Exception as exc:
            await self._complete(
                lease,
                action,
                ActionOutcome.FAILED,
                classify_failure(exc),
                current,
            )
            return
        ordinal = lease.attempt_ordinal
        if ordinal > self.max_attempts:
            await self._complete(
                lease,
                action,
                ActionOutcome.FAILED,
                ErrorClass.UNKNOWN_ADAPTER_FAILURE,
                current,
            )
            return
        started = current if now is not None else datetime.now(UTC)
        final_decision = await self.guard.authorize_start(
            action_id=action.action_id,
            lease_id=lease.lease_id,
            binding_revision=binding.revision or "missing-binding-revision",
            now=started,
        )
        if final_decision.disposition is not GuardDisposition.DISPATCH:
            return
        try:
            execution = await self._run_with_heartbeats(lease, invocation)
            if execution is None:
                return
            normalized = normalize(
                action=action,
                binding=binding,
                target=target,
                delivery_id=lease.lease_id,
                stdout=execution.stdout,
                observed_at=datetime.now(UTC),
            )
            ingestion_results: list[IngestionResult] = []
            for index, batch in enumerate(normalized.batches):
                if index:
                    try:
                        await self.lifecycle.heartbeat(
                            action_id=action.action_id,
                            lease_id=lease.lease_id,
                            worker_id=self.worker_id,
                            at=datetime.now(UTC),
                        )
                    except ActionLeaseLost:
                        return
                    except Exception as exc:
                        raise LifecyclePersistenceFailure(
                            "action lifecycle heartbeat could not be persisted"
                        ) from exc
                    await asyncio.sleep(0)
                result = await self.ingestion.ingest(
                    batch,
                    lease_id=lease.lease_id,
                )
                if result.status.value == "rejected":
                    error = ErrorClass.ADAPTER_CONTRACT_MISMATCH
                    if not await self._record_attempt(
                        lease,
                        ActionAttempt(
                            _id(action, f"attempt:{ordinal}"),
                            action.action_id,
                            ordinal,
                            started,
                            datetime.now(UTC),
                            ActionOutcome.FAILED,
                            error,
                            redacted_diagnostic=safe_diagnostic(error),
                        ),
                    ):
                        return
                    await self._complete(
                        lease,
                        action,
                        ActionOutcome.FAILED,
                        error,
                        datetime.now(UTC),
                    )
                    return
                ingestion_results.append(result)
            incomplete = any(
                declaration.completeness is not CollectionCoverage.COMPLETE
                for batch in normalized.batches
                for declaration in batch.coverage
            )
            outcome = (
                ActionOutcome.PARTIAL
                if any(result.status.value == "partial" for result in ingestion_results)
                or incomplete
                else ActionOutcome.SUCCEEDED
            )
            if not await self._record_attempt(
                lease,
                ActionAttempt(
                    _id(action, f"attempt:{ordinal}"),
                    action.action_id,
                    ordinal,
                    started,
                    datetime.now(UTC),
                    outcome,
                ),
            ):
                return
            await self._complete(lease, action, outcome, None, datetime.now(UTC))
            return
        except LifecyclePersistenceFailure:
            raise
        except Exception as exc:
            error = classify_failure(exc)
            ended = datetime.now(UTC)
            downstream_failure = exc if isinstance(exc, DownstreamFailure) else None
            retry_at: datetime | None = None
            if (
                ordinal < self.max_attempts
                and _retryable(error)
                and not (
                    downstream_failure is not None and downstream_failure.retry_after_out_of_bounds
                )
            ):
                candidate_retry_at = ended + _retry_delay(
                    error,
                    ordinal,
                    downstream_failure.retry_after if downstream_failure is not None else None,
                )
                if action.deadline is None or candidate_retry_at < action.deadline:
                    retry_at = candidate_retry_at
            if not await self._record_attempt(
                lease,
                ActionAttempt(
                    _id(action, f"attempt:{ordinal}"),
                    action.action_id,
                    ordinal,
                    started,
                    ended,
                    ActionOutcome.FAILED,
                    error,
                    retry_at=retry_at,
                    redacted_diagnostic=safe_diagnostic(error),
                ),
            ):
                return
            if retry_at is not None:
                return
            await self._complete(lease, action, ActionOutcome.FAILED, error, ended)

    async def _binding(self, action: AdapterAction) -> ConnectionBinding:
        binding = await self.bindings.get_connection_binding(action.connection_binding_id)
        if binding is None or not binding.enabled:
            raise CommandRejected("Databricks connection binding is unavailable")
        if binding.system_id != action.system_id:
            raise CommandRejected("Databricks connection binding belongs to another system")
        if (
            binding.adapter_key != DATABRICKS_ADAPTER_KEY
            or binding.adapter_version != DATABRICKS_ADAPTER_VERSION
        ):
            raise CommandRejected("Databricks binding is incompatible with action")
        capability: CapabilityBinding | None = await self.bindings.get_capability_binding(
            action.connection_binding_id, action.capability_key, action.capability_version
        )
        if (
            capability is None
            or not capability.enabled
            or capability.operation_class is not OperationClass.OBSERVE
        ):
            raise CommandRejected("Databricks capability binding is unavailable")
        if capability.connection_binding_id != action.connection_binding_id:
            raise CommandRejected("Databricks capability binding belongs to another connection")
        if action.target.kind not in capability.target_kinds:
            raise CommandRejected("Databricks target kind is not enabled")
        return binding

    async def _run_with_heartbeats(
        self, lease: ActionLease, invocation: CliInvocation
    ) -> CliExecution | None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return await self.runner.run(invocation, correlation_id=lease.action.correlation_id)
        task = asyncio.create_task(
            self.runner.run(invocation, correlation_id=lease.action.correlation_id)
        )
        try:
            while not task.done():
                try:
                    return await asyncio.wait_for(
                        asyncio.shield(task), timeout=self.heartbeat_seconds
                    )
                except TimeoutError:
                    try:
                        await self.lifecycle.heartbeat(
                            action_id=lease.action.action_id,
                            lease_id=lease.lease_id,
                            worker_id=self.worker_id,
                            at=datetime.now(UTC),
                        )
                    except ActionLeaseLost:
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                        return None
                    except Exception as exc:
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                        raise LifecyclePersistenceFailure(
                            "action lifecycle heartbeat could not be persisted"
                        ) from exc
            return await task
        except asyncio.CancelledError:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise

    async def _record_attempt(self, lease: ActionLease, attempt: ActionAttempt) -> bool:
        try:
            await self.lifecycle.record_attempt(attempt, lease_id=lease.lease_id)
        except ActionLeaseLost:
            return False
        except Exception as exc:
            raise LifecyclePersistenceFailure("action attempt could not be persisted") from exc
        return True

    async def _complete(
        self,
        lease: ActionLease,
        action: AdapterAction,
        outcome: ActionOutcome,
        error: ErrorClass | None,
        at: datetime,
    ) -> bool:
        diagnostic = safe_diagnostic(error) if error is not None else None
        try:
            await self.lifecycle.complete_action(
                ActionCompletion(action.action_id, outcome, at, error, diagnostic),
                lease_id=lease.lease_id,
            )
        except ActionLeaseLost:
            return False
        except Exception as exc:
            raise LifecyclePersistenceFailure("action completion could not be persisted") from exc
        return True
