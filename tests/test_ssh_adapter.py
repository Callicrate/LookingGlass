from __future__ import annotations

import asyncio
import shlex
import sys
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from lookingglass.adapters import ssh as ssh_adapter
from lookingglass.adapters.ssh import (
    SSH_ADAPTER_KEY,
    SSH_ADAPTER_VERSION,
    CliExecution,
    CommandRejected,
    DownstreamFailure,
    InvalidDownstreamResponse,
    LifecyclePersistenceFailure,
    ResolvedTarget,
    SshCommandRegistry,
    SshIncompatible,
    SshInvocation,
    SshOutputLimit,
    SshRunner,
    SshTimeout,
    SshUnavailable,
    SshWorker,
    _FIND_FORMAT,
    _STAT_FORMAT,
    classify_ssh_failure,
    host_authority_fingerprint,
    normalize,
    redact_diagnostic,
    ssh_host_authority_fingerprint,
)
from lookingglass.contracts import (
    ActionAttempt,
    ActionCompletion,
    ActionLease,
    ActionLeaseLost,
    ActionOutcome,
    AdapterAction,
    CapabilityBinding,
    CollectionCoverage,
    ConnectionBinding,
    ErrorClass,
    GuardDecision,
    GuardDisposition,
    IngestionResult,
    IngestionStatus,
    ObservationBatch,
    OperationClass,
    PresenceState,
    RefreshScope,
    TargetKind,
    TargetRef,
)

_DELIVERY_ID = str(uuid4())
_NOW = datetime(2026, 8, 30, 12, tzinfo=UTC)
_CONFIG_PATH = Path("/etc/lookingglass/ssh_config")
_KNOWN_HOSTS_PATH = Path("/etc/lookingglass/known_hosts")


def _normalize(**kwargs):
    return ssh_adapter.normalize(delivery_id=_DELIVERY_ID, **kwargs)


def _find_record(entry_type: str, size: str, mtime: str, mode: str, name: str) -> bytes:
    return f"{entry_type}\x1f{size}\x1f{mtime}\x1f{mode}\x1f{name}\x00".encode()


def _stat_record(descr: str, size: str, mtime: str, mode_hex: str, name: str) -> bytes:
    return f"{descr}\x1f{size}\x1f{mtime}\x1f{mode_hex}\x1f{name}".encode()


def _binding() -> ConnectionBinding:
    return ConnectionBinding(
        uuid4(),
        uuid4(),
        SSH_ADAPTER_KEY,
        SSH_ADAPTER_VERSION,
        True,
        {
            "authority_fingerprint": "1" * 64,
            "host_alias": "server",
            "path_root": "/srv",
        },
    )


def _folder_root_target() -> ResolvedTarget:
    return ResolvedTarget(
        path="/srv/data",
        path_root="/srv",
        canonical_object_id=str(uuid4()),
        canonical_object_type="folder",
    )


def _action(capability: str, facet: str) -> AdapterAction:
    system_id = uuid4()
    return AdapterAction(
        uuid4(),
        uuid4(),
        system_id,
        uuid4(),
        SSH_ADAPTER_KEY,
        SSH_ADAPTER_VERSION,
        capability,
        "1",
        TargetRef(TargetKind.CONFIGURED_SCOPE, uuid4()),
        (
            RefreshScope(
                system_id, TargetRef(TargetKind.CONFIGURED_SCOPE, uuid4()), "folder", facet
            ),
        ),
    )


def _expected_argv(alias: str, remote_command: str) -> tuple[str, ...]:
    return (
        "ssh",
        "-T",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={_KNOWN_HOSTS_PATH}",
        "-o",
        "GlobalKnownHostsFile=none",
        "-o",
        "ClearAllForwardings=yes",
        "-o",
        "PermitLocalCommand=no",
        "-o",
        "ConnectTimeout=10",
        "-F",
        str(_CONFIG_PATH),
        alias,
        remote_command,
    )


def _build(capability: str, target: ResolvedTarget, *, alias: str = "server") -> SshInvocation:
    return SshCommandRegistry.build(
        capability_key=capability,
        alias=alias,
        target=target,
        ssh_config_path=_CONFIG_PATH,
        known_hosts_path=_KNOWN_HOSTS_PATH,
        connect_timeout=10,
    )


def test_registry_is_closed_and_exact() -> None:
    metadata = _build(
        "ssh.fs.metadata.read",
        ResolvedTarget(path="/srv/data", path_root="/srv"),
    )
    stat_command = f"stat --printf {shlex.quote(_STAT_FORMAT)} -- /srv/data"
    assert metadata.argv == _expected_argv("server", stat_command)

    children = _build(
        "ssh.fs.children.read",
        ResolvedTarget(path="/srv/data", path_root="/srv"),
    )
    find_command = f"find /srv/data -maxdepth 1 -mindepth 1 -printf {shlex.quote(_FIND_FORMAT)}"
    assert children.argv == _expected_argv("server", find_command)

    # The remote-command format tokens carry the literal octal escapes.
    assert "'%F\\037%s\\037%Y\\037%f\\037%n'" in metadata.argv[-1]
    assert "'%y\\037%s\\037%T@\\037%m\\037%f\\0'" in children.argv[-1]
    # A plain absolute path stays unquoted after shlex.quote.
    assert " -- /srv/data" in metadata.argv[-1]


def test_registry_rejects_unregistered_capability_and_unsafe_alias() -> None:
    with pytest.raises(CommandRejected):
        _build("ssh.fs.content.read", ResolvedTarget(path="/srv/data", path_root="/srv"))
    with pytest.raises(CommandRejected):
        _build(
            "ssh.fs.metadata.read",
            ResolvedTarget(path="/srv/data", path_root="/srv"),
            alias="-oProxyCommand=evil",
        )


@pytest.mark.parametrize(
    "path",
    ["relative/path", "/srv/../etc/passwd", "/srv/data\nrm -rf", "/srv/data\x00"],
)
def test_registry_rejects_unsafe_remote_paths(path: str) -> None:
    with pytest.raises(CommandRejected):
        _build("ssh.fs.metadata.read", ResolvedTarget(path=path, path_root="/srv"))


def test_registry_enforces_configured_root() -> None:
    with pytest.raises(CommandRejected, match="outside configured root"):
        _build("ssh.fs.metadata.read", ResolvedTarget(path="/other/data", path_root="/srv"))


@pytest.mark.parametrize(
    "invocation",
    [
        SshInvocation("ssh.fs.metadata.read", ("ssh", "server", "stat /srv")),
        SshInvocation(
            "ssh.fs.metadata.read",
            _expected_argv("server", "cat /etc/shadow"),
        ),
        SshInvocation(
            "ssh.fs.metadata.read",
            _expected_argv("-oProxyCommand=evil", "stat --printf %n -- /srv"),
        ),
        SshInvocation(
            "ssh.fs.content.read",
            _expected_argv("server", "stat --printf %n -- /srv"),
        ),
    ],
)
def test_runner_rejects_manual_invocation_before_process_creation(
    invocation: SshInvocation, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = SshRunner(
        ssh_config_path=_CONFIG_PATH,
        known_hosts_path=_KNOWN_HOSTS_PATH,
    )
    runner._resolved_executable = "C:\\trusted\\ssh.exe"

    async def unexpected_process(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("invalid invocation reached process creation")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", unexpected_process)

    with pytest.raises(CommandRejected):
        asyncio.run(runner.run(invocation, correlation_id="test"))


def test_runner_rejects_mapped_observation_without_authority_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = SshRunner(
        ssh_config_path=_CONFIG_PATH,
        known_hosts_path=_KNOWN_HOSTS_PATH,
    )
    runner._resolved_executable = "C:\\trusted\\ssh.exe"

    async def unexpected_execution(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("missing authority fingerprint reached execution")

    monkeypatch.setattr(runner, "_execute", unexpected_execution)
    invocation = _build("ssh.fs.metadata.read", ResolvedTarget(path="/srv/data", path_root="/srv"))

    with pytest.raises(CommandRejected, match="requires an authority fingerprint"):
        asyncio.run(runner.run(invocation, correlation_id="test"))


class ScriptedDoctorRunner(SshRunner):
    def __init__(self, version: bytes) -> None:
        super().__init__(ssh_config_path=_CONFIG_PATH, known_hosts_path=_KNOWN_HOSTS_PATH)
        self.version = version
        self.calls: list[tuple[str, ...]] = []
        self._resolved_executable = "C:\\trusted\\ssh.exe"

    def resolve_executable(self) -> str:
        return self._resolved_executable

    async def run_unmapped(self, invocation: SshInvocation) -> CliExecution:
        self.calls.append(invocation.argv[1:])
        # OpenSSH prints its banner on stderr.
        return CliExecution("doctor", timedelta(), 0, b"", self.version)


def test_doctor_accepts_openssh_client() -> None:
    runner = ScriptedDoctorRunner(b"OpenSSH_9.6p1, OpenSSL 3.0.13")
    asyncio.run(runner.doctor())
    assert runner.calls == [("-V",)]


def test_doctor_rejects_non_openssh_client() -> None:
    runner = ScriptedDoctorRunner(b"PuTTY plink: Release 0.80")
    with pytest.raises(SshIncompatible, match="OpenSSH"):
        asyncio.run(runner.doctor())


def test_cli_process_creation_failure_is_controlled(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fail_process(*_args: object, **_kwargs: object) -> None:
        raise FileNotFoundError("simulated executable race")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fail_process)
    runner = SshRunner(ssh_config_path=_CONFIG_PATH, known_hosts_path=_KNOWN_HOSTS_PATH)
    runner._resolved_executable = "C:\\trusted\\ssh.exe"

    with pytest.raises(SshUnavailable, match="process could not start"):
        asyncio.run(
            runner._execute(
                ("ssh", "-V"),
                correlation_id="doctor",
                timeout_message="synthetic timeout",
            )
        )


def test_cli_process_ownership_failure_reaps_child(monkeypatch: pytest.MonkeyPatch) -> None:
    class StartedProcess:
        returncode: int | None = None
        killed = False
        wait_calls = 0

        def kill(self) -> None:
            self.killed = True
            self.returncode = -9

        async def wait(self) -> int:
            self.wait_calls += 1
            return self.returncode or 0

    process = StartedProcess()

    async def create_process(*_args: object, **_kwargs: object) -> StartedProcess:
        return process

    def reject_ownership(_process: object) -> None:
        raise AttributeError("simulated asyncio transport change")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_process)
    monkeypatch.setattr(ssh_adapter, "_ProcessTree", reject_ownership)
    runner = SshRunner(ssh_config_path=_CONFIG_PATH, known_hosts_path=_KNOWN_HOSTS_PATH)
    runner._resolved_executable = "C:\\trusted\\ssh.exe"

    with pytest.raises(SshUnavailable, match="could not own the SSH process tree"):
        asyncio.run(
            runner._execute(
                ("ssh", "-V"),
                correlation_id="doctor",
                timeout_message="synthetic timeout",
            )
        )

    assert process.killed
    assert process.wait_calls == 1


@pytest.mark.parametrize("failure", ["cancel", "timeout", "output_limit"])
def test_execute_failures_reap_process_and_readers(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    async def exercise() -> None:
        stopped = asyncio.Event()
        created = asyncio.Event()

        class BlockingStream:
            def __init__(self, *, overflow: bool = False) -> None:
                self.overflow = overflow
                self.settled = False

            async def read(self, _size: int) -> bytes:
                try:
                    if self.overflow:
                        self.overflow = False
                        return b"xx"
                    await stopped.wait()
                    return b""
                finally:
                    self.settled = True

        class BlockingProcess:
            returncode: int | None = None
            killed = False
            wait_calls = 0
            # stderr overflow triggers SshOutputLimit; stdout is truncated, never raises.
            stdout = BlockingStream()
            stderr = BlockingStream(overflow=failure == "output_limit")

            def kill(self) -> None:
                self.killed = True
                self.returncode = -9
                stopped.set()

            async def wait(self) -> int:
                self.wait_calls += 1
                await stopped.wait()
                return self.returncode or 0

        process = BlockingProcess()

        async def create_process(*_args: object, **_kwargs: object) -> BlockingProcess:
            created.set()
            return process

        monkeypatch.setattr(asyncio, "create_subprocess_exec", create_process)
        runner = SshRunner(
            ssh_config_path=_CONFIG_PATH,
            known_hosts_path=_KNOWN_HOSTS_PATH,
            timeout_seconds=0.001 if failure == "timeout" else 30,
            stdout_cap=1,
            stderr_cap=1,
        )
        runner._resolved_executable = "C:\\trusted\\ssh.exe"
        task = asyncio.create_task(
            runner._execute(
                ("ssh", "-V"),
                correlation_id="doctor",
                timeout_message="synthetic timeout",
            )
        )
        try:
            await asyncio.wait_for(created.wait(), timeout=1)
        except TimeoutError:
            if task.done():
                await task
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise
        await asyncio.sleep(0)

        if failure == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        elif failure == "timeout":
            with pytest.raises(SshTimeout):
                await task
        else:
            with pytest.raises(SshOutputLimit):
                await task

        assert process.killed
        assert process.wait_calls >= 1
        assert process.stdout.settled
        assert process.stderr.settled

    asyncio.run(exercise())


def test_execute_truncates_oversized_stdout_without_raising() -> None:
    child_code = "import sys\nsys.stdout.write('x' * 5000)\n"
    runner = SshRunner(
        ssh_config_path=_CONFIG_PATH,
        known_hosts_path=_KNOWN_HOSTS_PATH,
        timeout_seconds=10,
        stdout_cap=1000,
    )

    execution = asyncio.run(
        runner._execute(
            (sys.executable, "-c", child_code),
            correlation_id="truncation",
            timeout_message="synthetic timeout",
        )
    )

    assert execution.exit_code == 0
    assert execution.truncated is True
    assert len(execution.stdout) == 1000


def test_cli_timeout_terminates_descendants_holding_output_pipes(tmp_path: Path) -> None:
    heartbeat = tmp_path / "descendant-heartbeat"
    child_code = "\n".join(
        (
            "import sys, time",
            "from pathlib import Path",
            "path = Path(sys.argv[1])",
            "while True:",
            "    path.write_text(str(time.time()), encoding='utf-8')",
            "    time.sleep(0.05)",
        )
    )
    parent_code = "\n".join(
        (
            "import subprocess, sys, time",
            "from pathlib import Path",
            "path = Path(sys.argv[1])",
            "subprocess.Popen([sys.executable, '-c', sys.argv[2], str(path)])",
            "for _ in range(100):",
            "    if path.exists(): break",
            "    time.sleep(0.01)",
            "time.sleep(30)",
        )
    )
    runner = SshRunner(
        ssh_config_path=_CONFIG_PATH,
        known_hosts_path=_KNOWN_HOSTS_PATH,
        timeout_seconds=1,
    )

    started = time.perf_counter()
    with pytest.raises(SshTimeout):
        asyncio.run(
            runner._execute(
                (sys.executable, "-c", parent_code, str(heartbeat), child_code),
                correlation_id="process-tree",
                timeout_message="synthetic SSH timed out",
            )
        )
    elapsed = time.perf_counter() - started

    assert elapsed < 3
    assert heartbeat.exists()
    settled = heartbeat.read_text(encoding="utf-8")
    time.sleep(0.3)
    assert heartbeat.read_text(encoding="utf-8") == settled


def test_children_normalization_is_deterministic_and_complete() -> None:
    action = _action("ssh.fs.children.read", "membership")
    target = _folder_root_target()
    stdout = (
        _find_record("d", "4096", "1693526400.0", "755", "logs")
        + _find_record("f", "12", "1693526400.5", "644", "notes.txt")
        + _find_record("l", "7", "1693526401.0", "777", "link")
    )
    first = _normalize(
        action=action,
        binding=_binding(),
        target=target,
        stdout=stdout,
        observed_at=datetime.now(UTC),
    )
    second = _normalize(
        action=action,
        binding=_binding(),
        target=target,
        stdout=stdout,
        observed_at=datetime.now(UTC),
    )

    assert first.batch.batch_id == second.batch.batch_id
    assert [item.observation_id for item in first.batch.facet_observations] == [
        item.observation_id for item in second.batch.facet_observations
    ]

    membership = next(
        item for item in first.batch.facet_observations if item.facet == "membership"
    )
    assert membership.payload["collection_completeness"] == "complete"
    assert membership.payload["member_count"] == 3

    metadata = {
        item.target.external_key: item
        for item in first.batch.facet_observations
        if item.facet in {"metadata", "attributes"}
    }
    assert set(metadata) == {"ssh:/srv/data/logs", "ssh:/srv/data/notes.txt", "ssh:/srv/data/link"}
    assert metadata["ssh:/srv/data/logs"].target.object_type == "folder"
    assert metadata["ssh:/srv/data/notes.txt"].target.object_type == "file"
    assert metadata["ssh:/srv/data/link"].target.object_type == "generic_object"
    assert metadata["ssh:/srv/data/link"].facet == "attributes"
    assert metadata["ssh:/srv/data/notes.txt"].payload["byte_count"] == 12
    assert metadata["ssh:/srv/data/notes.txt"].payload["mode_octal"] == "644"

    assert len(first.batch.relationship_observations) == 3
    assert all(
        rel.predicate == "contains" and rel.presence is PresenceState.PRESENT
        for rel in first.batch.relationship_observations
    )

    assert len(first.batch.coverage) == 1
    assert first.batch.coverage[0].completeness is CollectionCoverage.COMPLETE
    assert first.batch.coverage[0].absence_authority == ()


def test_children_truncation_degrades_completeness_and_drops_partial_record() -> None:
    action = _action("ssh.fs.children.read", "membership")
    stdout = (
        _find_record("f", "1", "1693526400.0", "644", "a.txt")
        + b"f\x1f2\x1f1693526400.0\x1f644\x1fpartial-no-terminator"
    )
    result = _normalize(
        action=action,
        binding=_binding(),
        target=_folder_root_target(),
        stdout=stdout,
        observed_at=_NOW,
        truncated=True,
    )
    membership = next(
        item for item in result.batch.facet_observations if item.facet == "membership"
    )
    assert membership.payload["collection_completeness"] == "unknown"
    assert membership.payload["member_count"] == 1
    assert result.batch.coverage[0].completeness is CollectionCoverage.UNKNOWN


def test_children_rejects_duplicate_basename() -> None:
    action = _action("ssh.fs.children.read", "membership")
    stdout = _find_record("f", "1", "0", "644", "dup") + _find_record("d", "2", "0", "755", "dup")
    with pytest.raises(InvalidDownstreamResponse, match="duplicate"):
        _normalize(
            action=action,
            binding=_binding(),
            target=_folder_root_target(),
            stdout=stdout,
            observed_at=_NOW,
        )


@pytest.mark.parametrize("basename", ["", ".", "..", "nested/child"])
def test_children_rejects_unsafe_basename(basename: str) -> None:
    action = _action("ssh.fs.children.read", "membership")
    stdout = _find_record("f", "1", "0", "644", basename)
    with pytest.raises(InvalidDownstreamResponse):
        _normalize(
            action=action,
            binding=_binding(),
            target=_folder_root_target(),
            stdout=stdout,
            observed_at=_NOW,
        )


def test_metadata_normalization_is_complete_and_metadata_only() -> None:
    action = _action("ssh.fs.metadata.read", "metadata")
    target = ResolvedTarget(
        path="/srv/data/report.txt",
        path_root="/srv",
        canonical_object_id=str(uuid4()),
        canonical_object_type="file",
    )
    stdout = _stat_record("regular file", "42", "1693526400", "81a4", "/srv/data/report.txt")
    result = _normalize(
        action=action,
        binding=_binding(),
        target=target,
        stdout=stdout,
        observed_at=_NOW,
    )
    assert len(result.batch.facet_observations) == 1
    facet = result.batch.facet_observations[0]
    assert facet.facet == "metadata"
    assert facet.target.object_type == "file"
    assert facet.payload["byte_count"] == 42
    assert facet.payload["raw_mode_hex"] == "81a4"
    assert len(result.batch.coverage) == 1
    assert result.batch.coverage[0].completeness is CollectionCoverage.COMPLETE
    assert result.batch.relationship_observations == ()


def test_metadata_rejects_unsupported_object_type() -> None:
    action = _action("ssh.fs.metadata.read", "metadata")
    target = ResolvedTarget(
        path="/srv/data/pipe",
        path_root="/srv",
        canonical_object_id=str(uuid4()),
        canonical_object_type="file",
    )
    stdout = _stat_record("fifo", "0", "0", "1000", "/srv/data/pipe")
    with pytest.raises(InvalidDownstreamResponse, match="unsupported object type"):
        _normalize(
            action=action,
            binding=_binding(),
            target=target,
            stdout=stdout,
            observed_at=_NOW,
        )


def test_legacy_null_action_scope_normalizes_to_selected_capability_authority() -> None:
    action = _action("ssh.fs.children.read", "membership")
    action = replace(
        action,
        requested_scopes=(replace(action.requested_scopes[0], capability_key=None),),
    )
    result = _normalize(
        action=action,
        binding=_binding(),
        target=_folder_root_target(),
        stdout=_find_record("f", "1", "0", "644", "a.txt"),
        observed_at=_NOW,
    )
    assert all(
        scope.capability_key == action.capability_key
        for batch in result.batches
        for item in (*batch.facet_observations, *batch.relationship_observations)
        for scope in item.authorized_by
    )
    assert all(
        declaration.scope.capability_key == action.capability_key
        for batch in result.batches
        for declaration in batch.coverage
    )


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (SshTimeout("t"), ErrorClass.CONNECTION_TIMEOUT),
        (SshUnavailable("u"), ErrorClass.ADAPTER_CONTRACT_MISMATCH),
        (SshIncompatible("i"), ErrorClass.ADAPTER_CONTRACT_MISMATCH),
        (CommandRejected("c"), ErrorClass.ADAPTER_CONTRACT_MISMATCH),
        (SshOutputLimit("o"), ErrorClass.INVALID_DOWNSTREAM_RESPONSE),
        (InvalidDownstreamResponse("v"), ErrorClass.INVALID_DOWNSTREAM_RESPONSE),
        (
            DownstreamFailure(
                "t", exit_code=255, diagnostic="ssh: connect to host timed out"
            ),
            ErrorClass.CONNECTION_TIMEOUT,
        ),
        (
            DownstreamFailure(
                "r", exit_code=255, diagnostic="ssh: connect to host port 22: Connection refused"
            ),
            ErrorClass.TRANSIENT_DOWNSTREAM,
        ),
        (
            DownstreamFailure(
                "a", exit_code=255, diagnostic="Permission denied (publickey)."
            ),
            ErrorClass.AUTHORIZATION,
        ),
        (
            DownstreamFailure(
                "h", exit_code=255, diagnostic="Host key verification failed."
            ),
            ErrorClass.ADAPTER_CONTRACT_MISMATCH,
        ),
        (
            DownstreamFailure(
                "n", exit_code=1, diagnostic="stat: cannot stat '/srv/x': No such file or directory"
            ),
            ErrorClass.NOT_FOUND,
        ),
        (
            DownstreamFailure(
                "p", exit_code=1, diagnostic="find: '/srv/x': Permission denied"
            ),
            ErrorClass.AUTHORIZATION,
        ),
        (RuntimeError("boom"), ErrorClass.UNKNOWN_ADAPTER_FAILURE),
    ],
)
def test_classify_ssh_failure(exc: BaseException, expected: ErrorClass) -> None:
    assert classify_ssh_failure(exc) is expected


def test_host_authority_fingerprint_is_deterministic_and_route_sensitive() -> None:
    base = host_authority_fingerprint("host.example.com", port=22, host_key="ssh-ed25519 AAAAKEY")
    assert base == host_authority_fingerprint(
        "HOST.example.com.", port=22, host_key="ssh-ed25519 AAAAKEY"
    )
    assert base != host_authority_fingerprint(
        "host.example.com", port=2222, host_key="ssh-ed25519 AAAAKEY"
    )
    assert base != host_authority_fingerprint(
        "host.example.com", port=22, host_key="ssh-ed25519 OTHERKEY"
    )


def test_ssh_host_authority_fingerprint_composes_config_and_known_hosts() -> None:
    calls: list[tuple[str, ...]] = []

    def fake_runner(argv: tuple[str, ...]) -> str:
        calls.append(argv)
        if "-G" in argv:
            return "hostname host.example.com\nport 2222\nuser someone\n"
        return (
            "# Host host.example.com found: line 3\n"
            "host.example.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAKEY\n"
        )

    fingerprint = ssh_host_authority_fingerprint(
        "server",
        ssh_config_path=_CONFIG_PATH,
        known_hosts_path=_KNOWN_HOSTS_PATH,
        command_runner=fake_runner,
    )
    assert fingerprint == host_authority_fingerprint(
        "host.example.com",
        port=2222,
        host_key="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAKEY",
    )
    assert any("-G" in argv for argv in calls)
    assert any("-F" in argv and argv[0].endswith("keygen") for argv in calls)


def test_redaction_and_import_boundary() -> None:
    diagnostic = redact_diagnostic(
        b"ssh: connect to host /home/person/.ssh/id_ed25519 C:\\Users\\person\\key "
        b"Authorization: Bearer secret-value control\x85char"
    )
    assert "/home/person" not in diagnostic
    assert "Users" not in diagnostic
    assert "secret-value" not in diagnostic
    assert "\x85" not in diagnostic

    source = Path("src/lookingglass/adapters/ssh.py").read_text(encoding="utf-8")
    assert "lookingglass.storage" not in source
    assert "local_files" not in source
    assert "databricks" not in source


class _Queue:
    def __init__(self, lease: ActionLease) -> None:
        self.lease = lease

    async def lease_next(self, **_: object) -> ActionLease | None:
        return self.lease


class _Lifecycle:
    def __init__(self) -> None:
        self.events: list[object] = []

    async def mark_running(self, **kwargs: object) -> None:
        self.events.append(("running", kwargs))

    async def record_attempt(self, attempt: object, *, lease_id: str) -> None:
        self.events.append(("attempt", lease_id, attempt))

    async def complete_action(self, completion: object, *, lease_id: str) -> None:
        self.events.append(("complete", lease_id, completion))

    async def heartbeat(self, **kwargs: object) -> None:
        self.events.append(("heartbeat", kwargs))


class _Guard:
    async def evaluate(self, **_: object) -> GuardDecision:
        return GuardDecision(GuardDisposition.DISPATCH, "allowed")

    async def authorize_start(self, **_: object) -> GuardDecision:
        return GuardDecision(GuardDisposition.DISPATCH, "allowed")


class _Bindings:
    def __init__(self, binding: ConnectionBinding) -> None:
        settings = dict(binding.non_secret_settings)
        settings.setdefault("authority_fingerprint", "1" * 64)
        settings.setdefault("host_alias", "server")
        settings.setdefault("path_root", "/srv")
        self.binding = replace(binding, non_secret_settings=settings)

    async def get_connection_binding(self, _: str) -> ConnectionBinding:
        return self.binding

    async def get_capability_binding(
        self, binding_id: str, capability: str, _: str
    ) -> CapabilityBinding:
        return CapabilityBinding(
            uuid4(),
            binding_id,
            capability,
            "1",
            OperationClass.OBSERVE,
            (TargetKind.CONFIGURED_SCOPE,),
            ("metadata",),
            True,
        )


class _ChildrenTargets:
    async def resolve(self, **_: object) -> ResolvedTarget:
        return _folder_root_target()


class _MetadataTargets:
    async def resolve(self, **_: object) -> ResolvedTarget:
        return ResolvedTarget(
            path="/srv/data/report.txt",
            path_root="/srv",
            canonical_object_id=str(uuid4()),
            canonical_object_type="file",
        )


class _Runner:
    executable = "ssh"
    ssh_config_path = _CONFIG_PATH
    known_hosts_path = _KNOWN_HOSTS_PATH
    connect_timeout = 10

    def verify_host_authority(self, *, alias: str, expected_fingerprint: str) -> None:
        assert alias
        assert len(expected_fingerprint) == 64

    async def run(self, *_: object, **__: object) -> CliExecution:
        return CliExecution(
            str(uuid4()),
            timedelta(),
            0,
            _find_record("f", "12", "1693526400.0", "644", "notes.txt"),
            b"",
        )


class _MetadataRunner(_Runner):
    async def run(self, *_: object, **__: object) -> CliExecution:
        return CliExecution(
            str(uuid4()),
            timedelta(),
            0,
            _stat_record("regular file", "42", "1693526400", "81a4", "/srv/data/report.txt"),
            b"",
        )


class _FailRunner(_Runner):
    async def run(self, *_: object, **__: object) -> CliExecution:
        raise DownstreamFailure(
            "raw downstream failure",
            exit_code=1,
            diagnostic="stat: /home/person/secret No such file or directory",
        )


class _TimeoutRunner(_Runner):
    def __init__(self) -> None:
        self.calls = 0

    async def run(self, *_: object, **__: object) -> CliExecution:
        self.calls += 1
        raise SshTimeout("timed out")


class _StaleLifecycle(_Lifecycle):
    async def record_attempt(self, attempt: object, *, lease_id: str) -> None:
        raise ActionLeaseLost(f"stale lease {lease_id}")


class _FailGuard(_Guard):
    async def evaluate(self, **_: object) -> GuardDecision:
        return GuardDecision(GuardDisposition.FAIL, "authority_lost")


class _TerminalGuard(_Guard):
    def __init__(self, disposition: GuardDisposition) -> None:
        self.disposition = disposition

    async def evaluate(self, **_: object) -> GuardDecision:
        observations = (uuid4(),) if self.disposition is GuardDisposition.SATISFY else ()
        return GuardDecision(self.disposition, "terminalized_by_guard", observations)


class _HeartbeatLostLifecycle(_Lifecycle):
    async def heartbeat(self, **_: object) -> None:
        raise ActionLeaseLost("lease lost")


class _HeartbeatFailedLifecycle(_Lifecycle):
    async def heartbeat(self, **_: object) -> None:
        raise OSError("lifecycle storage unavailable")


class _BlockingRunner(_Runner):
    def __init__(self) -> None:
        self.cancelled = False

    async def run(self, *_: object, **__: object) -> CliExecution:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise


class _Ingestion:
    def __init__(self, status: IngestionStatus = IngestionStatus.ACCEPTED) -> None:
        self.batches: list[object] = []
        self.lease_ids: list[str | None] = []
        self.status = status

    async def ingest(self, batch: object, *, lease_id: str | None = None) -> IngestionResult:
        self.batches.append(batch)
        self.lease_ids.append(lease_id)
        return IngestionResult(batch.batch_id, self.status)


def _children_binding(action: AdapterAction) -> ConnectionBinding:
    return ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        SSH_ADAPTER_KEY,
        SSH_ADAPTER_VERSION,
        True,
        {"host_alias": "server", "path_root": "/srv", "authority_fingerprint": "1" * 64},
    )


def test_worker_uses_only_ports() -> None:
    action = _action("ssh.fs.children.read", "membership")
    lifecycle = _Lifecycle()
    ingestion = _Ingestion()
    worker = SshWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(_children_binding(action)),
        ingestion=ingestion,
        targets=_ChildrenTargets(),
        runner=_Runner(),
    )
    assert asyncio.run(worker.run_once())
    assert ingestion.batches
    assert ingestion.lease_ids == [worker.queue.lease.lease_id]
    assert all(
        event[1] == worker.queue.lease.lease_id
        for event in lifecycle.events
        if isinstance(event, tuple) and event[0] in {"attempt", "complete"}
    )


def test_worker_rejects_action_that_does_not_match_binding() -> None:
    action = replace(_action("ssh.fs.metadata.read", "metadata"), adapter_version="99")
    lifecycle = _Lifecycle()
    worker = SshWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(_children_binding(action)),
        ingestion=_Ingestion(),
        targets=_MetadataTargets(),
        runner=_MetadataRunner(),
    )
    assert asyncio.run(worker.run_once())
    completed = [event[2] for event in lifecycle.events if event[0] == "complete"]
    assert len(completed) == 1
    assert completed[0].outcome is ActionOutcome.FAILED
    assert completed[0].error_class is ErrorClass.ADAPTER_CONTRACT_MISMATCH


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (IngestionStatus.ACCEPTED, ActionOutcome.SUCCEEDED),
        (IngestionStatus.DUPLICATE, ActionOutcome.SUCCEEDED),
        (IngestionStatus.PARTIAL, ActionOutcome.PARTIAL),
        (IngestionStatus.REJECTED, ActionOutcome.FAILED),
    ],
)
def test_worker_maps_ingestion_status_for_complete_metadata_read(
    status: IngestionStatus, expected: ActionOutcome
) -> None:
    action = _action("ssh.fs.metadata.read", "metadata")
    lifecycle = _Lifecycle()
    worker = SshWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(_children_binding(action)),
        ingestion=_Ingestion(status),
        targets=_MetadataTargets(),
        runner=_MetadataRunner(),
    )
    asyncio.run(worker.run_once())
    completed = [event[2] for event in lifecycle.events if event[0] == "complete"]
    assert completed[-1].outcome is expected


def test_worker_ingests_every_chunk_and_stops_when_heartbeat_is_lost() -> None:
    action = _action("ssh.fs.children.read", "membership")
    binding = _children_binding(action)
    stdout = b"".join(
        _find_record("f", "10", "1693526400.0", "644", f"file-{index:05d}.dat")
        for index in range(1200)
    )

    class ChildrenRunner(_Runner):
        async def run(self, *_: object, **__: object) -> CliExecution:
            # truncated=True degrades completeness to UNKNOWN so evidence may span batches.
            return CliExecution(str(uuid4()), timedelta(), 0, stdout, b"", True)

    lifecycle = _Lifecycle()
    ingestion = _Ingestion()
    worker = SshWorker(
        worker_id="chunk-worker",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=ingestion,
        targets=_ChildrenTargets(),
        runner=ChildrenRunner(),
    )

    assert asyncio.run(worker.run_once())
    assert len(ingestion.batches) > 1
    assert ingestion.lease_ids == [worker.queue.lease.lease_id] * len(ingestion.batches)
    attempts = [event[2] for event in lifecycle.events if event[0] == "attempt"]
    completions = [event[2] for event in lifecycle.events if event[0] == "complete"]
    assert len(attempts) == len(completions) == 1
    assert attempts[0].outcome is ActionOutcome.PARTIAL

    lost_lifecycle = _HeartbeatLostLifecycle()
    interrupted_ingestion = _Ingestion()
    interrupted = SshWorker(
        worker_id="lost-worker",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lost_lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=interrupted_ingestion,
        targets=_ChildrenTargets(),
        runner=ChildrenRunner(),
    )
    assert asyncio.run(interrupted.run_once())
    assert len(interrupted_ingestion.batches) == 1
    assert not any(event[0] in {"attempt", "complete"} for event in lost_lifecycle.events)


def test_retryable_failure_schedules_one_durable_attempt_per_lease() -> None:
    action = _action("ssh.fs.children.read", "membership")
    binding = _children_binding(action)
    lifecycle = _Lifecycle()
    timeout_runner = _TimeoutRunner()
    worker = SshWorker(
        worker_id="first",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC), attempt_ordinal=1)),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_ChildrenTargets(),
        runner=timeout_runner,
        max_attempts=2,
    )
    assert asyncio.run(worker.run_once())
    attempts = [event[2] for event in lifecycle.events if event[0] == "attempt"]
    assert timeout_runner.calls == 1
    assert len(attempts) == 1
    assert attempts[0].error_class is ErrorClass.CONNECTION_TIMEOUT
    assert attempts[0].retry_at - attempts[0].ended_at == timedelta(seconds=1)
    assert not any(event[0] == "complete" for event in lifecycle.events)


def test_stale_lease_or_guard_failure_never_finalizes_action() -> None:
    action = _action("ssh.fs.children.read", "membership")
    binding = _children_binding(action)
    for lifecycle, guard in ((_StaleLifecycle(), _Guard()), (_Lifecycle(), _FailGuard())):
        worker = SshWorker(
            worker_id="worker",
            queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
            lifecycle=lifecycle,
            guard=guard,
            bindings=_Bindings(binding),
            ingestion=_Ingestion(),
            targets=_ChildrenTargets(),
            runner=_Runner(),
        )
        asyncio.run(worker.run_once())
        assert not any(event[0] == "complete" for event in lifecycle.events)


@pytest.mark.parametrize("disposition", [GuardDisposition.CANCEL, GuardDisposition.SATISFY])
def test_guard_owned_terminal_transitions_are_not_completed(disposition: GuardDisposition) -> None:
    action = _action("ssh.fs.children.read", "membership")
    binding = _children_binding(action)
    lifecycle = _Lifecycle()
    worker = SshWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_TerminalGuard(disposition),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_ChildrenTargets(),
        runner=_Runner(),
    )
    asyncio.run(worker.run_once())
    assert not any(event[0] in {"running", "attempt", "complete"} for event in lifecycle.events)


def test_heartbeat_loss_cancels_running_command_without_finalizing() -> None:
    action = _action("ssh.fs.children.read", "membership")
    binding = _children_binding(action)
    runner = _BlockingRunner()
    lifecycle = _HeartbeatLostLifecycle()
    worker = SshWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_ChildrenTargets(),
        runner=runner,
        heartbeat_seconds=0.001,
    )
    asyncio.run(worker.run_once())
    assert runner.cancelled
    assert not any(event[0] in {"attempt", "complete"} for event in lifecycle.events)


def test_heartbeat_persistence_failure_reaches_runtime_supervision() -> None:
    action = _action("ssh.fs.children.read", "membership")
    binding = _children_binding(action)
    runner = _BlockingRunner()
    lifecycle = _HeartbeatFailedLifecycle()
    worker = SshWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_ChildrenTargets(),
        runner=runner,
        heartbeat_seconds=0.001,
    )
    with pytest.raises(LifecyclePersistenceFailure, match="heartbeat"):
        asyncio.run(worker.run_once())
    assert runner.cancelled
    assert not any(event[0] in {"attempt", "complete"} for event in lifecycle.events)


def test_worker_persists_only_closed_diagnostics() -> None:
    action = _action("ssh.fs.children.read", "membership")
    binding = _children_binding(action)
    lifecycle = _Lifecycle()
    worker = SshWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_ChildrenTargets(),
        runner=_FailRunner(),
        max_attempts=1,
    )
    asyncio.run(worker.run_once())
    persisted = str(lifecycle.events)
    assert "/home/person" not in persisted
    assert "secret" not in persisted


def test_worker_clamps_success_records_when_utc_rolls_back() -> None:
    action = _action("ssh.fs.metadata.read", "metadata")
    binding = _children_binding(action)
    current = [_NOW]

    class RollbackRunner(_MetadataRunner):
        async def run(self, *_: object, **__: object) -> CliExecution:
            current[0] = _NOW - timedelta(seconds=1)
            return await super().run()

    lifecycle = _Lifecycle()
    ingestion = _Ingestion()
    worker = SshWorker(
        worker_id="rollback-success",
        queue=_Queue(ActionLease(action, uuid4(), _NOW + timedelta(seconds=60))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=ingestion,
        targets=_MetadataTargets(),
        runner=RollbackRunner(),
        clock=lambda: current[0],
    )
    assert asyncio.run(worker.run_once())
    attempt = next(event[2] for event in lifecycle.events if event[0] == "attempt")
    completion = next(event[2] for event in lifecycle.events if event[0] == "complete")
    assert isinstance(attempt, ActionAttempt)
    assert isinstance(completion, ActionCompletion)
    assert attempt.started_at == _NOW
    assert attempt.ended_at == _NOW
    assert completion.completed_at == _NOW
    assert ingestion.batches[0].observed_at == _NOW
