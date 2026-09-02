"""Closed, OpenSSH-backed remote filesystem observation adapter.

This module knows no canonical storage implementation.  It invokes only the
installed ``ssh`` client through an allow-listed structured argv and passes
normalized observations through the shared ports.  A single remote command
string is executed by the remote login shell, but the local invocation is
always structured argv and never uses a shell.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import shlex
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, Protocol
from uuid import NAMESPACE_URL, uuid5

from lookingglass.adapters._process import (
    ProcessTree as _ProcessTree,
)
from lookingglass.adapters._process import (
    absolute_path_entries as _absolute_path_entries,
)
from lookingglass.adapters._process import (
    read_limited as _process_read_limited,
)
from lookingglass.adapters._process import (
    terminate_process as _terminate_process,
)
from lookingglass.contracts import (
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

SSH_ADAPTER_KEY = "ssh"
SSH_ADAPTER_VERSION = "1"

DEFAULT_STDOUT_CAP = 8 * 1024 * 1024
DEFAULT_STDERR_CAP = 64 * 1024
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_HEARTBEAT_SECONDS = 15.0
DEFAULT_CONNECT_TIMEOUT = 10
MAX_RETRY_DELAY_SECONDS = 30.0
MAX_COLLECTION_ITEMS = 10_000
MAX_INGESTION_BATCH_BYTES = 1_000_000
MAX_INGESTION_BATCH_UNITS = 250

CAPABILITIES = frozenset({"ssh.fs.children.read", "ssh.fs.metadata.read"})

# The remote-command format strings carry LITERAL octal escape sequences.  When
# the remote GNU coreutils honor them, ``stat``/``find`` emit real 0x1F (unit
# separator) and 0x00 (record terminator) bytes that ``normalize`` parses.
_STAT_FORMAT = "%F\\037%s\\037%Y\\037%f\\037%n"
_FIND_FORMAT = "%y\\037%s\\037%T@\\037%m\\037%f\\0"

_HOST_ALIAS = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_FINGERPRINT = re.compile(r"[0-9a-f]{64}")
_BLOCKED_VALUE = re.compile(r"(^-|[\r\n\x00])")

_BEARER_SECRET = re.compile(r"(?i)\bbearer\s+[^\s,;]+")
_SECRET = re.compile(
    r"(?i)(authorization\s*[:=]\s*(?:bearer\s+)?|"
    r"(?:token|password|secret|client_secret|access[_-]?(?:key|token)|refresh[_-]?token|"
    r"api[_-]?key)\s*[:=]\s*)[^\s,;]+"
)
_JSON_SECRET = re.compile(
    r"(?i)([\"']?(?:(?:[a-z0-9]+[_-])*(?:token|password|secret|authorization|"
    r"access[_-]?key|private[_-]?key|api[_-]?key))[\"']?\s*:\s*)"
    r'(?:"[^,}\]\r\n]*"|\'[^,}\]\r\n]*\'|[^,}\]\s]+)'
)
_STANDALONE_SECRET = re.compile(
    r"(?i)\b(?:gh[pousr]_[a-z0-9_]{20,}|"
    r"AKIA[0-9A-Z]{16}|eyJ[a-z0-9_-]{20,}\.[a-z0-9_-]{20,}\.[a-z0-9_-]{10,})\b"
)
_LOCAL_SETTING = re.compile(
    r"(?i)((?:identityfile|userknownhostsfile|globalknownhostsfile|known_hosts|"
    r"config(?:_file)?|config_path)\s*[:=]\s*)[^,;]+"
)
_WINDOWS_PATH = re.compile(r"(?i)\b[a-z]:[\\/][^,;\r\n]*")
_WINDOWS_ROOT_PATH = re.compile(r"(?i)\\root\\[^,;\r\n]*")
_UNC_PATH = re.compile(r"\\\\[^\\\s,;]+\\[^\r\n,;]*")
_POSIX_PATH = re.compile(r"(?i)/(?:users|home|etc|var|tmp|private|mnt|root)(?:/[^\s\"',;]*)?")
_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f؜‎‏‪-‮⁦-⁩]")


class SshAdapterError(RuntimeError):
    """Base class for safe adapter failures."""


class CommandRejected(SshAdapterError):
    pass


class SshUnavailable(SshAdapterError):
    pass


class SshRuntimeUnavailable(SshUnavailable):
    """A worker-global executable or process boundary failure."""


class SshIncompatible(SshAdapterError):
    pass


class SshTimeout(SshAdapterError):
    pass


class SshOutputLimit(SshAdapterError):
    pass


class DownstreamFailure(SshAdapterError):
    def __init__(
        self,
        message: str,
        *,
        exit_code: int,
        diagnostic: str,
    ) -> None:
        super().__init__(message)
        self.exit_code = exit_code
        self.diagnostic = diagnostic


class InvalidDownstreamResponse(SshAdapterError):
    pass


class LifecyclePersistenceFailure(SshAdapterError):
    """A non-fencing lifecycle write failure that must reach runtime supervision."""


@dataclass(frozen=True, slots=True)
class ResolvedTarget:
    """Validated adapter-local target data, never taken from arbitrary action input."""

    path: str | None = None
    path_root: str | None = None
    canonical_object_id: str | None = None
    canonical_object_type: str | None = None
    canonical_parent_external_key: str | None = None
    display_name: str | None = None


class TargetResolver(Protocol):
    async def resolve(
        self, *, action: AdapterAction, binding: ConnectionBinding
    ) -> ResolvedTarget: ...


@dataclass(frozen=True, slots=True)
class SshInvocation:
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
    truncated: bool = False


@dataclass(frozen=True, slots=True)
class NormalizedResult:
    batches: tuple[ObservationBatch, ...]

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


def _remote_path(value: object, root: object | None = None) -> str:
    path = _safe_text(value, name="remote path")
    if not path.startswith("/"):
        raise CommandRejected("remote paths must be absolute")
    normalized = str(PurePosixPath(path))
    if ".." in PurePosixPath(normalized).parts:
        raise CommandRejected("remote paths must not traverse parents")
    if root is not None:
        allowed_root = _remote_path(root)
        if normalized != allowed_root and not normalized.startswith(allowed_root.rstrip("/") + "/"):
            raise CommandRejected("remote path is outside configured root")
    return normalized


def _host_alias(value: object) -> str:
    alias = _safe_text(value, name="SSH host alias", maximum=128)
    if _HOST_ALIAS.fullmatch(alias) is None:
        raise CommandRejected("unsafe SSH host alias")
    return alias


def _binding_host_alias(settings: Mapping[str, Any]) -> str:
    return _host_alias(settings.get("host_alias"))


def _authority_fingerprint(settings: Mapping[str, Any]) -> str:
    value = settings.get("authority_fingerprint")
    if not isinstance(value, str) or _FINGERPRINT.fullmatch(value) is None:
        raise CommandRejected("binding requires a valid host authority fingerprint")
    return value


def _ssh_hardening_prefix(
    executable: str,
    *,
    ssh_config_path: Path,
    known_hosts_path: Path,
    connect_timeout: int,
) -> tuple[str, ...]:
    return (
        executable,
        "-T",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known_hosts_path}",
        "-o",
        "GlobalKnownHostsFile=none",
        "-o",
        "ClearAllForwardings=yes",
        "-o",
        "PermitLocalCommand=no",
        "-o",
        f"ConnectTimeout={connect_timeout}",
        "-F",
        str(ssh_config_path),
    )


def _remote_command(capability_key: str, path: str) -> str:
    if capability_key == "ssh.fs.metadata.read":
        return f"stat --printf {shlex.quote(_STAT_FORMAT)} -- {shlex.quote(path)}"
    return f"find {shlex.quote(path)} -maxdepth 1 -mindepth 1 -printf {shlex.quote(_FIND_FORMAT)}"


def _validate_remote_command(capability_key: str, tokens: list[str]) -> None:
    if capability_key == "ssh.fs.metadata.read":
        if (
            len(tokens) != 5
            or tokens[:2] != ["stat", "--printf"]
            or tokens[2] != _STAT_FORMAT
            or tokens[3] != "--"
        ):
            raise CommandRejected("SSH metadata command shape is invalid")
        _remote_path(tokens[4])
    elif capability_key == "ssh.fs.children.read":
        if (
            len(tokens) != 8
            or tokens[0] != "find"
            or tokens[2:] != ["-maxdepth", "1", "-mindepth", "1", "-printf", _FIND_FORMAT]
        ):
            raise CommandRejected("SSH children command shape is invalid")
        _remote_path(tokens[1])
    else:  # defensive; the registry is closed
        raise CommandRejected("unregistered SSH capability")


def _validate_observation_invocation(
    invocation: SshInvocation,
    *,
    executable: str,
    ssh_config_path: Path,
    known_hosts_path: Path,
    connect_timeout: int,
) -> None:
    """Reject manually assembled argv even if a caller bypasses the registry."""
    if invocation.capability_key not in CAPABILITIES:
        raise CommandRejected("unregistered SSH invocation")
    argv = invocation.argv
    if len(argv) != 20:
        raise CommandRejected("SSH invocation has an invalid shape")
    expected_prefix = _ssh_hardening_prefix(
        executable,
        ssh_config_path=ssh_config_path,
        known_hosts_path=known_hosts_path,
        connect_timeout=connect_timeout,
    )
    if tuple(argv[:18]) != expected_prefix:
        raise CommandRejected("SSH invocation hardening flags do not match")
    _host_alias(argv[18])
    try:
        tokens = shlex.split(argv[19])
    except ValueError as exc:
        raise CommandRejected("SSH remote command is not well-formed") from exc
    _validate_remote_command(invocation.capability_key, tokens)


class SshCommandRegistry:
    """Maps closed canonical capabilities to fixed hardened SSH command shapes."""

    @staticmethod
    def build(
        *,
        capability_key: str,
        alias: str,
        target: ResolvedTarget,
        ssh_config_path: Path,
        known_hosts_path: Path,
        connect_timeout: int,
        executable: str = "ssh",
    ) -> SshInvocation:
        if capability_key not in CAPABILITIES:
            raise CommandRejected("unregistered SSH capability")
        alias = _host_alias(alias)
        executable = _safe_text(executable, name="executable", maximum=1024)
        path = _remote_path(target.path, target.path_root)
        argv = (
            *_ssh_hardening_prefix(
                executable,
                ssh_config_path=ssh_config_path,
                known_hosts_path=known_hosts_path,
                connect_timeout=connect_timeout,
            ),
            alias,
            _remote_command(capability_key, path),
        )
        return SshInvocation(capability_key=capability_key, argv=argv)


async def _read_limited(stream: asyncio.StreamReader | None, cap: int) -> bytes:
    return await _process_read_limited(
        stream,
        cap,
        message="SSH output exceeded configured limit",
        error_type=SshOutputLimit,
    )


def _controlled_ssh_environment() -> dict[str, str]:
    """Allow only the agent socket, a sanitized PATH, and Windows system roots."""

    allowed: dict[str, str] = {}
    for key, value in os.environ.items():
        upper = key.upper()
        if upper == "PATH":
            allowed[key] = os.pathsep.join(_absolute_path_entries(value))
        elif upper in {"SSH_AUTH_SOCK", "SYSTEMROOT", "SYSTEMDRIVE"}:
            allowed[key] = value
    return allowed


def _candidate_executable_names(executable: str) -> tuple[str, ...]:
    if os.name != "nt" or Path(executable).suffix:
        return (executable,)
    return (f"{executable}.COM", f"{executable}.EXE")


def _usable_executable(path: Path) -> bool:
    if os.name == "nt" and path.suffix.upper() not in {".COM", ".EXE"}:
        return False
    return path.is_file() and (os.name == "nt" or os.access(path, os.X_OK))


def _authority_command_output(argv: tuple[str, ...]) -> str:
    """Run one bounded, connectionless witness command with no shell."""

    import subprocess  # local import keeps the module's default surface minimal

    completed = subprocess.run(  # noqa: S603 - structured argv, controlled environment
        argv,
        capture_output=True,
        env=_controlled_ssh_environment(),
        timeout=30,
        check=False,
    )
    return completed.stdout.decode("utf-8", "replace")


def host_authority_fingerprint(host: str, *, port: int, host_key: str) -> str:
    """Deterministic, route-sensitive digest over the normalized host witness."""

    normalized_host = host.casefold().rstrip(".")
    witness = f"{normalized_host}\n{int(port)}\n{host_key}"
    return hashlib.sha256(witness.encode("utf-8")).hexdigest()


def ssh_host_authority_fingerprint(
    alias: str,
    *,
    ssh_config_path: Path,
    known_hosts_path: Path,
    command_runner: Callable[[tuple[str, ...]], str] | None = None,
) -> str:
    """Compose the route witness from ``ssh -G`` and ``ssh-keygen -F`` with no connection."""

    runner = command_runner or _authority_command_output
    alias = _host_alias(alias)
    config_output = runner(("ssh", "-F", str(ssh_config_path), "-G", alias))
    hostname: str | None = None
    port = 22
    for line in config_output.splitlines():
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        keyword, value = parts[0].casefold(), parts[1].strip()
        if keyword == "hostname" and hostname is None:
            hostname = value
        elif keyword == "port":
            try:
                port = int(value)
            except ValueError as exc:
                raise CommandRejected("ssh reported a non-numeric port") from exc
    if not hostname:
        raise CommandRejected("ssh did not report an effective hostname")
    keygen_output = runner(("ssh-keygen", "-F", hostname, "-f", str(known_hosts_path)))
    host_key: str | None = None
    for line in keygen_output.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = stripped.split(None, 1)
        if len(parts) == 2:
            host_key = parts[1].strip()
            break
    if not host_key:
        raise CommandRejected("known hosts did not contain the effective host key")
    return host_authority_fingerprint(hostname, port=port, host_key=host_key)


class SshRunner:
    """Async structured-argv runner with bounded in-memory process output."""

    def __init__(
        self,
        *,
        ssh_config_path: Path,
        known_hosts_path: Path,
        executable: str = "ssh",
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        stdout_cap: int = DEFAULT_STDOUT_CAP,
        stderr_cap: int = DEFAULT_STDERR_CAP,
        connect_timeout: int = DEFAULT_CONNECT_TIMEOUT,
    ) -> None:
        if timeout_seconds <= 0 or stdout_cap <= 0 or stderr_cap <= 0:
            raise ValueError("runner limits must be positive")
        self.executable = executable
        self.ssh_config_path = ssh_config_path
        self.known_hosts_path = known_hosts_path
        self.timeout_seconds = timeout_seconds
        self.stdout_cap = stdout_cap
        self.stderr_cap = stderr_cap
        self.connect_timeout = connect_timeout
        self._resolved_executable: str | None = None

    def verify_host_authority(self, *, alias: str, expected_fingerprint: str) -> None:
        """Fail before dispatch if the alias no longer resolves to the pinned host route."""

        actual = ssh_host_authority_fingerprint(
            alias,
            ssh_config_path=self.ssh_config_path,
            known_hosts_path=self.known_hosts_path,
        )
        if actual != expected_fingerprint:
            raise CommandRejected("SSH host authority does not match the configured host")

    def resolve_executable(self) -> str:
        if self._resolved_executable is None:
            requested = Path(self.executable)
            if requested.is_absolute():
                candidates: tuple[Path, ...] = (requested,)
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
                raise SshUnavailable("SSH executable was not found on an absolute PATH")
        return self._resolved_executable

    async def _read_truncating(
        self, stream: asyncio.StreamReader | None, cap: int
    ) -> tuple[bytes, bool]:
        if stream is None:
            return b"", False
        collected = bytearray()
        truncated = False
        while block := await stream.read(min(65536, cap + 1)):
            if truncated:
                continue
            allowed = cap - len(collected)
            if len(block) > allowed:
                collected.extend(block[:allowed])
                truncated = True
            else:
                collected.extend(block)
        return bytes(collected), truncated

    async def _execute(
        self,
        argv: tuple[str, ...],
        *,
        correlation_id: str,
        timeout_message: str,
    ) -> CliExecution:
        started = time.monotonic()
        environment = _controlled_ssh_environment()
        try:
            if os.name == "nt":
                process = await asyncio.create_subprocess_exec(
                    *argv,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=environment,
                    creationflags=0x00000004,
                )
            else:
                process = await asyncio.create_subprocess_exec(
                    *argv,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=environment,
                    start_new_session=True,
                )
        except OSError as exc:
            raise SshRuntimeUnavailable("SSH process could not start") from exc
        try:
            process_tree = _ProcessTree(process)
        except BaseException as exc:
            try:
                if process.returncode is None:
                    process.kill()
            except ProcessLookupError:
                pass
            finally:
                await process.wait()
            if not isinstance(exc, Exception):
                raise
            raise SshRuntimeUnavailable(
                "LookingGlass could not own the SSH process tree"
            ) from exc
        truncated = False
        try:
            stdout_task = asyncio.create_task(
                self._read_truncating(process.stdout, self.stdout_cap)
            )
            stderr_task = asyncio.create_task(_read_limited(process.stderr, self.stderr_cap))
            wait_task = asyncio.create_task(process.wait())
            try:
                stdout_result, stderr, exit_code = await asyncio.wait_for(
                    asyncio.gather(stdout_task, stderr_task, wait_task),
                    timeout=self.timeout_seconds,
                )
                stdout, truncated = stdout_result
            except TimeoutError as exc:
                await _terminate_process(
                    process,
                    process_tree,
                    stdout_task,
                    stderr_task,
                    wait_task,
                )
                raise SshTimeout(timeout_message) from exc
            except SshOutputLimit:
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
        return CliExecution(
            correlation_id,
            timedelta(seconds=time.monotonic() - started),
            exit_code,
            stdout,
            stderr,
            truncated,
        )

    async def run(self, invocation: SshInvocation, *, correlation_id: str) -> CliExecution:
        if invocation.argv[0] != self.executable:
            raise CommandRejected("invocation does not use configured executable")
        _validate_observation_invocation(
            invocation,
            executable=self.executable,
            ssh_config_path=self.ssh_config_path,
            known_hosts_path=self.known_hosts_path,
            connect_timeout=self.connect_timeout,
        )
        if (
            invocation.authority_fingerprint is None
            or _FINGERPRINT.fullmatch(invocation.authority_fingerprint) is None
        ):
            raise CommandRejected("SSH invocation requires an authority fingerprint")
        executable = self.resolve_executable()
        argv = (executable, *invocation.argv[1:])
        execution = await self._execute(
            argv,
            correlation_id=str(correlation_id),
            timeout_message="SSH command timed out",
        )
        if execution.exit_code != 0:
            raise DownstreamFailure(
                "SSH command exited unsuccessfully",
                exit_code=execution.exit_code,
                diagnostic=redact_diagnostic(execution.stderr),
            )
        return execution

    async def doctor(self) -> None:
        """Confirm the client is OpenSSH; a light check with no version or digest pin."""
        result = await self.run_unmapped(SshInvocation("doctor", (self.executable, "-V")))
        banner = (result.stdout + b"\n" + result.stderr).decode("utf-8", "replace")
        if "OpenSSH" not in banner:
            raise SshIncompatible(
                "SSH client must be OpenSSH; other clients require compatibility review"
            )

    async def run_unmapped(self, invocation: SshInvocation) -> CliExecution:
        """Internal doctor path; public observation calls must use ``run``."""
        if invocation.capability_key != "doctor" or invocation.argv[1:] not in {("-V",)}:
            raise CommandRejected("unregistered SSH compatibility check")
        resolved = self.resolve_executable()
        return await self._execute(
            (resolved, *invocation.argv[1:]),
            correlation_id="doctor",
            timeout_message="SSH compatibility check timed out",
        )


def redact_diagnostic(value: bytes | str, *, limit: int = 2048) -> str:
    """Return bounded diagnostic text suitable for lifecycle persistence."""
    text = value.decode("utf-8", "replace") if isinstance(value, bytes) else value
    text = text.replace(r"\"", '"').replace(r"\'", "'")
    text = _CONTROL.sub(" ", text)
    text = _BEARER_SECRET.sub("Bearer [redacted]", text)
    text = _JSON_SECRET.sub(r'\1"[redacted]"', text)
    text = _SECRET.sub(r"\1[redacted]", text)
    text = _STANDALONE_SECRET.sub("[redacted-token]", text)
    text = _LOCAL_SETTING.sub(r"\1[local-path]", text)
    text = _WINDOWS_PATH.sub("[local-path]", text)
    text = _WINDOWS_ROOT_PATH.sub("[local-path]", text)
    text = _UNC_PATH.sub("[local-path]", text)
    text = _POSIX_PATH.sub("[local-path]", text)
    return " ".join(text.split())[:limit]


def safe_diagnostic(error: ErrorClass) -> str:
    """Closed diagnostics allowed to cross the lifecycle persistence boundary."""
    return {
        ErrorClass.AUTHENTICATION: "SSH authentication failed.",
        ErrorClass.AUTHORIZATION: "SSH authorization was denied.",
        ErrorClass.CONNECTION_TIMEOUT: "The SSH command timed out.",
        ErrorClass.DOWNSTREAM_RATE_LIMIT: "SSH rate limiting prevented completion.",
        ErrorClass.TRANSIENT_DOWNSTREAM: "The SSH host was temporarily unavailable.",
        ErrorClass.NOT_FOUND: "The configured SSH target was not found.",
        ErrorClass.UNSUPPORTED: "The configured SSH operation is unsupported.",
        ErrorClass.INVALID_DOWNSTREAM_RESPONSE: "The SSH host returned an invalid response.",
        ErrorClass.ADAPTER_CONTRACT_MISMATCH: "The SSH action does not match its binding.",
        ErrorClass.LOCAL_CANCELLATION: "The SSH action was cancelled locally.",
        ErrorClass.UNKNOWN_ADAPTER_FAILURE: "The SSH adapter failed unexpectedly.",
    }[error]


def classify_ssh_failure(exc: BaseException) -> ErrorClass:
    if isinstance(exc, (SshTimeout, asyncio.TimeoutError)):
        return ErrorClass.CONNECTION_TIMEOUT
    if isinstance(exc, (SshUnavailable, SshIncompatible, CommandRejected)):
        return ErrorClass.ADAPTER_CONTRACT_MISMATCH
    if isinstance(exc, (SshOutputLimit, InvalidDownstreamResponse)):
        return ErrorClass.INVALID_DOWNSTREAM_RESPONSE
    if isinstance(exc, DownstreamFailure):
        message = exc.diagnostic.lower()
        if "permission denied" in message:
            return ErrorClass.AUTHORIZATION
        if "host key verification failed" in message:
            return ErrorClass.ADAPTER_CONTRACT_MISMATCH
        if any(token in message for token in ("timed out", "timeout")):
            return ErrorClass.CONNECTION_TIMEOUT
        if any(
            token in message
            for token in (
                "connection refused",
                "connection reset",
                "reset by peer",
                "unreachable",
                "temporarily",
            )
        ):
            return ErrorClass.TRANSIENT_DOWNSTREAM
        if "no such file" in message or "not found" in message:
            return ErrorClass.NOT_FOUND
        if "not authorized" in message:
            return ErrorClass.AUTHORIZATION
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


def _enforce_binding_target(
    capability_key: str, binding: ConnectionBinding, target: ResolvedTarget
) -> ResolvedTarget:
    """Apply the configured path allowlist at the execution boundary."""
    if not capability_key.startswith("ssh.fs."):
        return target
    root = _remote_path(binding.non_secret_settings.get("path_root"))
    return replace(target, path_root=root)


def _id(action: AdapterAction, label: str) -> str:
    # Preserve the original UUID namespace so in-flight durable actions retain their IDs.
    return str(uuid5(NAMESPACE_URL, f"lookingglass/ssh/{action.action_id}/{label}"))


def _scopes(action: AdapterAction, facet: str) -> tuple:
    return tuple(scope for scope in action.requested_scopes if scope.facet == facet)


def _canonical_ssh_locator(target: ResolvedTarget, *, expected_type: str) -> ObjectLocator:
    if target.canonical_object_id is None or target.canonical_object_type is None:
        raise InvalidDownstreamResponse("SSH target lacks canonical object identity")
    if target.canonical_object_type != expected_type:
        raise InvalidDownstreamResponse("SSH target has an incompatible canonical object type")
    return ObjectLocator(
        object_type=target.canonical_object_type, object_id=target.canonical_object_id
    )


def _decode_field(value: bytes) -> str:
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidDownstreamResponse("SSH response contains invalid UTF-8") from exc


def _int_field(value: str, name: str) -> int:
    if not value or not value.isdigit():
        raise InvalidDownstreamResponse(f"SSH response has an invalid {name}")
    return int(value)


def _child_basename(value: str) -> str:
    if not value or value in {".", ".."} or "/" in value:
        raise InvalidDownstreamResponse("SSH listing has an unsafe child name")
    return value


def _child_kind(entry_type: str) -> tuple[str, str, str]:
    if entry_type == "d":
        return "folder", "ssh.fs.folder", "metadata"
    if entry_type == "f":
        return "file", "ssh.fs.file", "metadata"
    return "generic_object", "ssh.fs.object", "attributes"


def _metadata_object_type(descr: str) -> str:
    normalized = descr.strip().casefold()
    if normalized == "directory":
        return "folder"
    if normalized in {"regular file", "regular empty file"}:
        return "file"
    raise InvalidDownstreamResponse("unsupported object type")


def _child_records(stdout: bytes) -> list[tuple[str, str, str, str, str]]:
    # A NUL terminates each record; the trailing element drops the empty tail on a
    # complete stream and any partial final record on a truncated one.
    raw = stdout.split(b"\x00")[:-1]
    if len(raw) > MAX_COLLECTION_ITEMS:
        raise InvalidDownstreamResponse("SSH listing exceeds the configured item limit")
    records: list[tuple[str, str, str, str, str]] = []
    for chunk in raw:
        parts = chunk.split(b"\x1f", 4)
        if len(parts) != 5:
            raise InvalidDownstreamResponse("SSH listing has an invalid record structure")
        entry_type, size, mtime, mode, name = (_decode_field(part) for part in parts)
        records.append((entry_type, size, mtime, mode, name))
    return records


def _stat_fields(stdout: bytes) -> tuple[str, str, str, str, str]:
    parts = stdout.split(b"\x1f", 4)
    if len(parts) != 5:
        raise InvalidDownstreamResponse("SSH metadata response has an invalid field structure")
    descr, size, mtime, mode_hex, name = (_decode_field(part) for part in parts)
    return descr, size, mtime, mode_hex, name


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
    truncated: bool = False,
) -> NormalizedResult:
    """Parse sanitized record output and create deterministic canonical evidence."""
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

    facets: list[FacetObservation] = []
    relationships: list[RelationshipObservation] = []
    coverage: list[CoverageDeclaration] = []
    capability = action.capability_key
    if capability == "ssh.fs.children.read":
        root_path = _remote_path(target.path, target.path_root)
        if target.canonical_object_id is None and action.target.kind.value == "configured_scope":
            root = ObjectLocator(object_type="folder", object_id=action.target.target_id)
        else:
            root = _canonical_ssh_locator(target, expected_type="folder")
        records = _child_records(stdout)
        completeness = (
            CollectionCoverage.UNKNOWN if truncated else CollectionCoverage.COMPLETE
        )
        facets.append(
            FacetObservation(
                evidence_id("root-membership"),
                root,
                "membership",
                "1",
                UpdateMode.PATCH,
                FieldCoverage.PARTIAL,
                {
                    "collection_completeness": completeness.value,
                    "namespace": "ssh.fs",
                    "schema_version": "1",
                    "member_count": len(records),
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
        seen: set[str] = set()
        for entry_type, size, mtime, mode, name in records:
            basename = _child_basename(name)
            child_path = str(PurePosixPath(root_path) / basename)
            object_type, source_kind, facet = _child_kind(entry_type)
            external_key = f"ssh:{child_path}"
            if external_key in seen:
                raise InvalidDownstreamResponse("SSH listing contains a duplicate child entry")
            seen.add(external_key)
            child = ObjectLocator(
                object_type=object_type,
                source_kind=source_kind,
                external_key=external_key,
                display_name=basename,
            )
            body = {
                "namespace": "ssh.fs",
                "schema_version": "1",
                "path": child_path,
                "entry_type": entry_type,
                "byte_count": _int_field(size, "byte count"),
                "remote_modified_at": mtime,
                "mode_octal": mode,
            }
            facets.append(
                FacetObservation(
                    evidence_id(f"{facet}:{external_key}"),
                    child,
                    facet,
                    "1",
                    UpdateMode.PATCH,
                    FieldCoverage.PARTIAL,
                    body,
                    tuple(body),
                    _scopes(action, "metadata"),
                )
            )
            relationships.append(
                RelationshipObservation(
                    evidence_id(f"contains:{external_key}"),
                    root,
                    "contains",
                    child,
                    PresenceState.PRESENT,
                )
            )
        coverage.extend(
            CoverageDeclaration(scope, completeness)
            for scope in action.requested_scopes
            if scope.facet == "membership"
        )
    elif capability == "ssh.fs.metadata.read":
        descr, size, mtime, mode_hex, name = _stat_fields(stdout)
        object_type = _metadata_object_type(descr)
        path = _remote_path(target.path, target.path_root)
        if _remote_path(name) != path:
            raise InvalidDownstreamResponse(
                "SSH metadata response does not match the requested target"
            )
        locator = _canonical_ssh_locator(target, expected_type=object_type)
        body = {
            "namespace": "ssh.fs",
            "schema_version": "1",
            "path": path,
            "source_type": descr,
            "byte_count": _int_field(size, "byte count"),
            "remote_modified_at": mtime,
            "raw_mode_hex": mode_hex,
        }
        facets.append(
            FacetObservation(
                evidence_id(f"metadata:{path}"),
                locator,
                "metadata",
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
    else:  # defensive even though the command registry is closed
        raise CommandRejected("unregistered SSH capability")

    batch = ObservationBatch(
        batch_id=_id(action, f"batch:{delivery_id}"),
        system_id=action.system_id,
        connection_binding_id=binding.binding_id,
        adapter_key=SSH_ADAPTER_KEY,
        adapter_version=SSH_ADAPTER_VERSION,
        observed_at=observed_at,
        received_at=observed_at,
        action_id=action.action_id,
        observed_at_is_local=True,
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
    return NormalizedResult(batches)


class SshWorker:
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
        runner: SshRunner,
        max_attempts: int = 2,
        heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if max_attempts < 1 or heartbeat_seconds <= 0:
            raise ValueError("worker limits must be positive")
        if clock is not None and not callable(clock):
            raise TypeError("clock must be callable")
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
        self._clock = clock or (lambda: datetime.now(UTC))
        self.ingestion_generation = 0

    def _record_time(self, *, floor: datetime | None = None) -> datetime:
        sampled = self._clock()
        if not isinstance(sampled, datetime) or sampled.tzinfo is None:
            raise TypeError("worker clock must return a timezone-aware datetime")
        normalized = sampled.astimezone(UTC)
        if floor is not None and normalized < floor:
            return floor
        return normalized

    async def startup(self) -> None:
        await self.runner.doctor()

    async def run_once(self, *, now: datetime | None = None) -> bool:
        current = now.astimezone(UTC) if now is not None else self._record_time()
        lease = await self.queue.lease_next(
            adapter_key=SSH_ADAPTER_KEY, worker_id=self.worker_id, now=current
        )
        if lease is None:
            return False
        await self.process(lease, now=current)
        return True

    async def process(self, lease: ActionLease, *, now: datetime | None = None) -> None:
        action = lease.action
        current = now.astimezone(UTC) if now is not None else self._record_time()
        if (
            action.adapter_key != SSH_ADAPTER_KEY
            or action.adapter_version != SSH_ADAPTER_VERSION
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
            alias = _binding_host_alias(binding.non_secret_settings)
            fingerprint = _authority_fingerprint(binding.non_secret_settings)
            self.runner.verify_host_authority(alias=alias, expected_fingerprint=fingerprint)
            target = await self.targets.resolve(action=action, binding=binding)
            target = _enforce_binding_target(action.capability_key, binding, target)
            invocation = SshCommandRegistry.build(
                capability_key=action.capability_key,
                alias=alias,
                target=target,
                ssh_config_path=self.runner.ssh_config_path,
                known_hosts_path=self.runner.known_hosts_path,
                connect_timeout=self.runner.connect_timeout,
                executable=self.runner.executable,
            )
            invocation = replace(invocation, authority_fingerprint=fingerprint)
        except LifecyclePersistenceFailure:
            raise
        except Exception as exc:
            await self._complete(
                lease,
                action,
                ActionOutcome.FAILED,
                classify_ssh_failure(exc),
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
        started = current
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
                observed_at=self._record_time(floor=started),
                truncated=execution.truncated,
            )
            ingestion_results: list[IngestionResult] = []
            for index, batch in enumerate(normalized.batches):
                if index:
                    try:
                        await self.lifecycle.heartbeat(
                            action_id=action.action_id,
                            lease_id=lease.lease_id,
                            worker_id=self.worker_id,
                            at=self._record_time(floor=started),
                        )
                    except ActionLeaseLost:
                        return
                    except Exception as exc:
                        raise LifecyclePersistenceFailure(
                            "action lifecycle heartbeat could not be persisted"
                        ) from exc
                    await asyncio.sleep(0)
                try:
                    result = await self.ingestion.ingest(
                        batch,
                        lease_id=lease.lease_id,
                    )
                    self.ingestion_generation += 1
                except ActionLeaseLost:
                    return
                except Exception as exc:
                    raise LifecyclePersistenceFailure(
                        "canonical observation ingestion could not be persisted"
                    ) from exc
                if result.status.value == "rejected":
                    error = ErrorClass.ADAPTER_CONTRACT_MISMATCH
                    ended = self._record_time(floor=started)
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
                            redacted_diagnostic=safe_diagnostic(error),
                        ),
                    ):
                        return
                    await self._complete(lease, action, ActionOutcome.FAILED, error, ended)
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
            ended = self._record_time(floor=started)
            if not await self._record_attempt(
                lease,
                ActionAttempt(
                    _id(action, f"attempt:{ordinal}"),
                    action.action_id,
                    ordinal,
                    started,
                    ended,
                    outcome,
                ),
            ):
                return
            await self._complete(lease, action, outcome, None, ended)
            return
        except LifecyclePersistenceFailure:
            raise
        except Exception as exc:
            worker_global_failure = isinstance(exc, SshIncompatible | SshRuntimeUnavailable)
            error = classify_ssh_failure(exc)
            ended = self._record_time(floor=started)
            retry_at: datetime | None = None
            if ordinal < self.max_attempts and _retryable(error):
                candidate_retry_at = ended + _retry_delay(error, ordinal)
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
                if worker_global_failure:
                    raise
                return
            if retry_at is not None:
                return
            await self._complete(lease, action, ActionOutcome.FAILED, error, ended)
            if worker_global_failure:
                raise

    async def _binding(self, action: AdapterAction) -> ConnectionBinding:
        binding = await self.bindings.get_connection_binding(action.connection_binding_id)
        if binding is None or not binding.enabled:
            raise CommandRejected("SSH connection binding is unavailable")
        if binding.system_id != action.system_id:
            raise CommandRejected("SSH connection binding belongs to another system")
        if (
            binding.adapter_key != SSH_ADAPTER_KEY
            or binding.adapter_version != SSH_ADAPTER_VERSION
        ):
            raise CommandRejected("SSH binding is incompatible with action")
        capability: CapabilityBinding | None = await self.bindings.get_capability_binding(
            action.connection_binding_id, action.capability_key, action.capability_version
        )
        if (
            capability is None
            or not capability.enabled
            or capability.operation_class is not OperationClass.OBSERVE
        ):
            raise CommandRejected("SSH capability binding is unavailable")
        if capability.connection_binding_id != action.connection_binding_id:
            raise CommandRejected("SSH capability binding belongs to another connection")
        if action.target.kind not in capability.target_kinds:
            raise CommandRejected("SSH target kind is not enabled")
        return binding

    async def _run_with_heartbeats(
        self, lease: ActionLease, invocation: SshInvocation
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
                            at=self._record_time(),
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
