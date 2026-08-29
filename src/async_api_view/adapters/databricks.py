"""Closed, CLI-backed Databricks observation adapter.

This module deliberately knows no canonical storage implementation.  It invokes
only the installed ``databricks`` executable through an allow-listed argv map and
passes normalized observations through the shared ports.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import re
import shutil
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath
from typing import Any, Protocol
from uuid import NAMESPACE_URL, uuid5

from async_api_view.contracts import (
    ActionAttempt,
    ActionCompletion,
    ActionLease,
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
    ObjectLocator,
    ObservationBatch,
    ObservationIngestionPort,
    OperationClass,
    PreDispatchGuardPort,
    PresenceState,
    RelationshipObservation,
    UpdateMode,
)

DATABRICKS_ADAPTER_KEY = "databricks"
DATABRICKS_ADAPTER_VERSION = "1"
MINIMUM_CLI_VERSION = (0, 298, 0)
MAX_JSON_BYTES = 4 * 1024 * 1024
DEFAULT_STDOUT_CAP = 8 * 1024 * 1024
DEFAULT_STDERR_CAP = 64 * 1024
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_HEARTBEAT_SECONDS = 15.0
MAX_RETRY_DELAY_SECONDS = 30.0
MAX_COLLECTION_ITEMS = 10_000
MAX_TABLE_COLUMNS = 1_000
MAX_JSON_DEPTH = 32

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
_VERSION = re.compile(r"(?:^|[^0-9])(\d+)\.(\d+)\.(\d+)(?:[^0-9]|$)")
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
    def __init__(self, message: str, *, exit_code: int, diagnostic: str) -> None:
        super().__init__(message)
        self.exit_code = exit_code
        self.diagnostic = diagnostic


class InvalidDownstreamResponse(DatabricksAdapterError):
    pass


class ContentPolicyError(DatabricksAdapterError):
    pass


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


@dataclass(frozen=True, slots=True)
class CliExecution:
    correlation_id: str
    duration: timedelta
    exit_code: int
    stdout: bytes
    stderr: bytes


@dataclass(frozen=True, slots=True)
class ContentArtifact:
    """Ephemeral content handoff; persistence is intentionally outside this adapter."""

    object_locator: ObjectLocator
    content: bytes
    media_type: str | None
    source_revision: str | None


@dataclass(frozen=True, slots=True)
class NormalizedResult:
    batch: ObservationBatch
    artifacts: tuple[ContentArtifact, ...] = ()


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


async def _terminate_process(
    process: asyncio.subprocess.Process, *tasks: asyncio.Task[Any]
) -> None:
    if process.returncode is None:
        process.kill()
    await process.wait()
    await asyncio.gather(*tasks, return_exceptions=True)


class CliRunner:
    """Async structured-argv runner with bounded in-memory process output."""

    def __init__(
        self,
        *,
        executable: str = "databricks",
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        stdout_cap: int = DEFAULT_STDOUT_CAP,
        stderr_cap: int = DEFAULT_STDERR_CAP,
    ) -> None:
        if timeout_seconds <= 0 or stdout_cap <= 0 or stderr_cap <= 0:
            raise ValueError("runner limits must be positive")
        self.executable = executable
        self.timeout_seconds = timeout_seconds
        self.stdout_cap = stdout_cap
        self.stderr_cap = stderr_cap
        self._resolved_executable: str | None = None

    def resolve_executable(self) -> str:
        if self._resolved_executable is None:
            resolved = shutil.which(self.executable)
            if resolved is None:
                raise CliUnavailable("Databricks CLI executable was not found")
            self._resolved_executable = resolved
        return self._resolved_executable

    async def run(self, invocation: CliInvocation, *, correlation_id: str) -> CliExecution:
        executable = self.resolve_executable()
        if invocation.argv[0] != self.executable:
            raise CommandRejected("invocation does not use configured executable")
        _validate_observation_invocation(invocation)
        argv = (executable, *invocation.argv[1:])
        started = time.monotonic()
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_task = asyncio.create_task(_read_limited(process.stdout, self.stdout_cap))
        stderr_task = asyncio.create_task(_read_limited(process.stderr, self.stderr_cap))
        wait_task = asyncio.create_task(process.wait())
        try:
            stdout, stderr, exit_code = await asyncio.wait_for(
                asyncio.gather(stdout_task, stderr_task, wait_task), timeout=self.timeout_seconds
            )
        except TimeoutError as exc:
            await _terminate_process(process, stdout_task, stderr_task, wait_task)
            raise CliTimeout("Databricks CLI timed out") from exc
        except CliOutputLimit:
            await _terminate_process(process, stdout_task, stderr_task, wait_task)
            raise
        except asyncio.CancelledError:
            await _terminate_process(process, stdout_task, stderr_task, wait_task)
            raise
        duration = timedelta(seconds=time.monotonic() - started)
        execution = CliExecution(str(correlation_id), duration, exit_code, stdout, stderr)
        if exit_code != 0:
            raise DownstreamFailure(
                "Databricks CLI exited unsuccessfully",
                exit_code=exit_code,
                diagnostic=redact_diagnostic(stderr),
            )
        return execution

    async def doctor(self) -> None:
        """Verify 0.298-compatible version and required group help surfaces."""
        executable = self.resolve_executable()
        checks = (
            ("--version",),
            *(
                (group, "--help")
                for group in ("workspace", "catalogs", "schemas", "tables", "volumes")
            ),
        )
        version_text = ""
        for args in checks:
            invocation = CliInvocation("doctor", (self.executable, *args))
            result = await self.run_unmapped(invocation, executable=executable)
            text = (result.stdout + b"\n" + result.stderr).decode("utf-8", "replace")
            if result.exit_code != 0:
                raise CliIncompatible(f"Databricks CLI does not support {' '.join(args)!r}")
            if args == ("--version",):
                version_text = text
        matched = _VERSION.search(version_text)
        if matched is None or tuple(int(part) for part in matched.groups()) < MINIMUM_CLI_VERSION:
            raise CliIncompatible("Databricks CLI must be version 0.298 or newer")

    async def run_unmapped(
        self, invocation: CliInvocation, *, executable: str | None = None
    ) -> CliExecution:
        """Internal doctor path; public observation calls must use ``run``."""
        if invocation.capability_key != "doctor" or invocation.argv[1:] not in {
            ("--version",),
            ("workspace", "--help"),
            ("catalogs", "--help"),
            ("schemas", "--help"),
            ("tables", "--help"),
            ("volumes", "--help"),
        }:
            raise CommandRejected("unregistered Databricks compatibility check")
        resolved = executable or self.resolve_executable()
        started = time.monotonic()
        process = await asyncio.create_subprocess_exec(
            resolved,
            *invocation.argv[1:],
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_task = asyncio.create_task(_read_limited(process.stdout, self.stdout_cap))
        stderr_task = asyncio.create_task(_read_limited(process.stderr, self.stderr_cap))
        wait_task = asyncio.create_task(process.wait())
        try:
            stdout, stderr, exit_code = await asyncio.wait_for(
                asyncio.gather(stdout_task, stderr_task, wait_task),
                timeout=self.timeout_seconds,
            )
        except TimeoutError as exc:
            await _terminate_process(process, stdout_task, stderr_task, wait_task)
            raise CliTimeout("Databricks CLI compatibility check timed out") from exc
        except CliOutputLimit:
            await _terminate_process(process, stdout_task, stderr_task, wait_task)
            raise
        except asyncio.CancelledError:
            await _terminate_process(process, stdout_task, stderr_task, wait_task)
            raise
        return CliExecution(
            "doctor", timedelta(seconds=time.monotonic() - started), exit_code, stdout, stderr
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
    }
    for key in allowed:
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


def normalize(
    *,
    action: AdapterAction,
    binding: ConnectionBinding,
    target: ResolvedTarget,
    stdout: bytes,
    observed_at: datetime,
) -> NormalizedResult:
    """Validate sanitized JSON and create deterministic canonical evidence."""
    payload = _json_output(stdout)
    observed_at = observed_at.astimezone(UTC)
    base = dict(
        batch_id=_id(action, "batch"),
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
                _id(action, "root-membership"),
                root,
                "membership",
                "1",
                UpdateMode.PATCH,
                FieldCoverage.PARTIAL,
                {
                    "namespace": "databricks.workspace",
                    "schema_version": "1",
                    "member_count": len(children),
                },
                ("namespace", "schema_version", "member_count"),
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
                    _id(action, f"metadata:{child.external_key}"),
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
                    _id(action, f"contains:{child.external_key}"),
                    root,
                    "contains",
                    child,
                    PresenceState.PRESENT,
                )
            )
        for scope in action.requested_scopes:
            if scope.facet == "membership":
                coverage.append(CoverageDeclaration(scope, CollectionCoverage.UNKNOWN))
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
                _id(action, f"metadata:{locator.external_key}"),
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
        _normalize_content(action, binding, target, payload, facets)
    elif capability == "databricks.uc.catalogs.read":
        for item in _items(payload, "catalogs"):
            name = _name(item.get("name"), label="catalog name")
            locator = _generic_locator("databricks.uc.catalog", f"catalog:{name}", name)
            body = _generic_payload(item, entity="catalog")
            facets.append(
                FacetObservation(
                    _id(action, f"catalog:{name}"),
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
        coverage.extend(
            CoverageDeclaration(scope, CollectionCoverage.UNKNOWN)
            for scope in action.requested_scopes
        )
    elif capability == "databricks.uc.schemas.read":
        catalog = _name(target.catalog_name, label="catalog name")
        parent = _generic_locator("databricks.uc.catalog", f"catalog:{catalog}", catalog)
        for item in _items(payload, "schemas"):
            name = _uc_child_name(item, catalog=catalog, label="schema")
            locator = _generic_locator("databricks.uc.schema", f"schema:{catalog}.{name}", name)
            body = _generic_payload(item, entity="schema")
            facets.append(
                FacetObservation(
                    _id(action, f"schema:{catalog}.{name}"),
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
                    _id(action, f"contains:{locator.external_key}"),
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
        parent = _generic_locator("databricks.uc.schema", f"schema:{catalog}.{schema}", schema)
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
            locator = _generic_locator(
                f"databricks.uc.{item_entity}",
                f"{item_entity}:{catalog}.{schema}.{name}",
                name,
            )
            body = _generic_payload(item, entity=item_entity)
            facets.append(
                FacetObservation(
                    _id(action, f"{item_entity}:{catalog}.{schema}.{name}"),
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
                    _id(action, f"contains:{locator.external_key}"),
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
    return NormalizedResult(
        ObservationBatch(
            **base,
            facet_observations=tuple(facets),
            relationship_observations=tuple(relationships),
            coverage=tuple(coverage),
        ),
        artifacts,
    )


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
            _id(action, f"content:{locator.external_key}"),
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


def _retry_delay(error: ErrorClass, ordinal: int) -> timedelta:
    base_seconds = 5.0 if error is ErrorClass.DOWNSTREAM_RATE_LIMIT else 1.0
    seconds = min(MAX_RETRY_DELAY_SECONDS, base_seconds * (2 ** min(ordinal - 1, 5)))
    return timedelta(seconds=seconds)


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
            if action.capability_key == "databricks.workspace.content.read":
                raise ContentPolicyError(
                    "Workspace content persistence is unavailable in this worker"
                )
            target = await self.targets.resolve(action=action, binding=binding)
            target = _enforce_binding_target(action.capability_key, binding, target)
            invocation = DatabricksCommandRegistry.build(
                capability_key=action.capability_key,
                profile=_profile(binding.non_secret_settings),
                target=target,
                executable=self.runner.executable,
            )
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
                stdout=execution.stdout,
                observed_at=datetime.now(UTC),
            )
            result = await self.ingestion.ingest(
                normalized.batch,
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
                await self._complete(lease, action, ActionOutcome.FAILED, error, datetime.now(UTC))
                return
            incomplete = any(
                declaration.completeness is not CollectionCoverage.COMPLETE
                for declaration in normalized.batch.coverage
            )
            outcome = (
                ActionOutcome.PARTIAL
                if result.status.value == "partial" or incomplete
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
        except Exception as exc:
            error = classify_failure(exc)
            ended = datetime.now(UTC)
            retry_at = (
                ended + _retry_delay(error, ordinal)
                if ordinal < self.max_attempts and _retryable(error)
                else None
            )
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
                    except Exception:
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                        return None
            return await task
        except asyncio.CancelledError:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise

    async def _record_attempt(self, lease: ActionLease, attempt: ActionAttempt) -> bool:
        try:
            await self.lifecycle.record_attempt(attempt, lease_id=lease.lease_id)
        except Exception:
            return False
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
        except Exception:
            return False
        return True
