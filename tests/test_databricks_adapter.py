from __future__ import annotations

import asyncio
import base64
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from async_api_view.adapters.databricks import (
    DATABRICKS_ADAPTER_KEY,
    DATABRICKS_ADAPTER_VERSION,
    MAX_COLLECTION_ITEMS,
    MAX_TABLE_COLUMNS,
    CliExecution,
    CliInvocation,
    CliOutputLimit,
    CliRunner,
    CliTimeout,
    CommandRejected,
    DatabricksCommandRegistry,
    DatabricksWorker,
    DownstreamFailure,
    InvalidDownstreamResponse,
    ResolvedTarget,
    normalize,
    redact_diagnostic,
)
from async_api_view.contracts import (
    ActionLease,
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
    OperationClass,
    RefreshScope,
    TargetKind,
    TargetRef,
)


def _binding() -> ConnectionBinding:
    return ConnectionBinding(
        uuid4(),
        uuid4(),
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local", "content_capture_enabled": True, "content_max_bytes": 1024},
    )


def _action(capability: str, facet: str = "attributes") -> AdapterAction:
    system_id = uuid4()
    return AdapterAction(
        uuid4(),
        uuid4(),
        system_id,
        uuid4(),
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        capability,
        "1",
        TargetRef(TargetKind.CONFIGURED_SCOPE, uuid4()),
        (
            RefreshScope(
                system_id, TargetRef(TargetKind.CONFIGURED_SCOPE, uuid4()), "generic_object", facet
            ),
        ),
    )


@pytest.mark.parametrize(
    ("capability", "target", "expected"),
    [
        (
            "databricks.workspace.children.read",
            ResolvedTarget(workspace_path="/Shared/root", workspace_root="/Shared"),
            (
                "databricks",
                "workspace",
                "list",
                "/Shared/root",
                "--profile",
                "local",
                "--output",
                "json",
            ),
        ),
        (
            "databricks.workspace.metadata.read",
            ResolvedTarget(workspace_path="/Shared/root", workspace_root="/Shared"),
            (
                "databricks",
                "workspace",
                "get-status",
                "/Shared/root",
                "--profile",
                "local",
                "--output",
                "json",
            ),
        ),
        (
            "databricks.workspace.content.read",
            ResolvedTarget(workspace_path="/Shared/root", workspace_root="/Shared"),
            (
                "databricks",
                "workspace",
                "export",
                "/Shared/root",
                "--format",
                "SOURCE",
                "--profile",
                "local",
                "--output",
                "json",
            ),
        ),
        (
            "databricks.uc.catalogs.read",
            ResolvedTarget(),
            ("databricks", "catalogs", "list", "--profile", "local", "--output", "json"),
        ),
        (
            "databricks.uc.schemas.read",
            ResolvedTarget(catalog_name="main"),
            ("databricks", "schemas", "list", "main", "--profile", "local", "--output", "json"),
        ),
        (
            "databricks.uc.relations.read",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            (
                "databricks",
                "tables",
                "list",
                "main",
                "sales",
                "--profile",
                "local",
                "--output",
                "json",
            ),
        ),
        (
            "databricks.uc.volumes.read",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            (
                "databricks",
                "volumes",
                "list",
                "main",
                "sales",
                "--profile",
                "local",
                "--output",
                "json",
            ),
        ),
    ],
)
def test_registry_is_closed_and_exact(
    capability: str, target: ResolvedTarget, expected: tuple[str, ...]
) -> None:
    assert (
        DatabricksCommandRegistry.build(
            capability_key=capability, profile="local", target=target
        ).argv
        == expected
    )
    with pytest.raises(CommandRejected):
        DatabricksCommandRegistry.build(
            capability_key="databricks.api.read", profile="local", target=target
        )
    with pytest.raises(CommandRejected):
        DatabricksCommandRegistry.build(capability_key=capability, profile="--debug", target=target)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "invocation",
    [
        CliInvocation(
            "databricks.uc.catalogs.read",
            ("databricks", "api", "get", "--profile", "local", "--output", "json"),
        ),
        CliInvocation(
            "databricks.uc.catalogs.read",
            ("databricks", "catalogs", "list", "--debug", "--profile", "local", "--output", "json"),
        ),
        CliInvocation(
            "databricks.uc.catalogs.read",
            ("databricks", "catalogs", "list", "--profile", "--debug", "--output", "json"),
        ),
        CliInvocation(
            "databricks.workspace.children.read",
            (
                "databricks",
                "workspace",
                "list",
                "/Shared/../secret",
                "--profile",
                "local",
                "--output",
                "json",
            ),
        ),
        CliInvocation(
            "databricks.workspace.children.read",
            ("databricks", "workspace", "list", "/Shared", "--output", "json"),
        ),
    ],
)
async def test_runner_rejects_manual_invocation_before_process_creation(
    invocation: CliInvocation, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = CliRunner()
    runner._resolved_executable = "C:\\trusted\\databricks.exe"

    async def unexpected_process(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("invalid invocation reached process creation")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", unexpected_process)

    with pytest.raises(CommandRejected):
        await runner.run(invocation, correlation_id="test")


@pytest.mark.parametrize("failure", ["cancel", "timeout", "output_limit"])
def test_compatibility_check_failures_reap_process_and_readers(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
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
            stdout = BlockingStream(overflow=failure == "output_limit")
            stderr = BlockingStream()

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
        runner = CliRunner(
            timeout_seconds=0.001 if failure == "timeout" else 30,
            stdout_cap=1,
            stderr_cap=1,
        )
        task = asyncio.create_task(
            runner.run_unmapped(
                CliInvocation("doctor", ("databricks", "--version")),
                executable="C:\\trusted\\databricks.exe",
            )
        )
        await created.wait()
        await asyncio.sleep(0)

        if failure == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        elif failure == "timeout":
            with pytest.raises(CliTimeout):
                await task
        else:
            with pytest.raises(CliOutputLimit):
                await task

        assert process.killed
        assert process.wait_calls >= 1
        assert process.stdout.settled
        assert process.stderr.settled

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("capability", "facet", "target", "payload"),
    [
        (
            "databricks.workspace.children.read",
            "membership",
            ResolvedTarget(
                workspace_path="/Shared",
                workspace_root="/Shared",
                canonical_object_id=uuid4(),
                canonical_object_type="folder",
            ),
            {"objects": [{"path": "/Shared/n", "object_type": "NOTEBOOK", "object_id": 1}]},
        ),
        (
            "databricks.workspace.metadata.read",
            "metadata",
            ResolvedTarget(
                workspace_path="/Shared/n",
                workspace_root="/Shared",
                canonical_object_id=uuid4(),
                canonical_object_type="file",
            ),
            {"path": "/Shared/n", "object_type": "FILE"},
        ),
        (
            "databricks.workspace.content.read",
            "content",
            ResolvedTarget(
                workspace_path="/Shared/n",
                workspace_root="/Shared",
                canonical_object_id=uuid4(),
                canonical_object_type="file",
            ),
            {"content": base64.b64encode(b"ok").decode()},
        ),
        (
            "databricks.uc.catalogs.read",
            "attributes",
            ResolvedTarget(),
            {"catalogs": [{"name": "main", "storage_location": "s3://secret"}]},
        ),
        (
            "databricks.uc.schemas.read",
            "attributes",
            ResolvedTarget(catalog_name="main"),
            {"schemas": [{"name": "sales"}]},
        ),
        (
            "databricks.uc.relations.read",
            "attributes",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            {
                "tables": [
                    {
                        "name": "orders",
                        "table_type": "MANAGED",
                        "storage_location": "s3://secret",
                        "columns": [{"name": "id", "type_name": "INT"}],
                    }
                ]
            },
        ),
        (
            "databricks.uc.volumes.read",
            "attributes",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            {"volumes": [{"name": "landing", "storage_location": "s3://secret"}]},
        ),
    ],
)
def test_fixture_normalization_is_deterministic_and_metadata_only(
    capability: str, facet: str, target: ResolvedTarget, payload: dict[str, object]
) -> None:
    action = _action(capability, facet)
    binding = _binding()
    first = normalize(
        action=action,
        binding=binding,
        target=target,
        stdout=json.dumps(payload).encode(),
        observed_at=datetime.now(UTC),
    )
    second = normalize(
        action=action,
        binding=binding,
        target=target,
        stdout=json.dumps(payload).encode(),
        observed_at=datetime.now(UTC),
    )
    assert first.batch.batch_id == second.batch.batch_id
    assert [item.observation_id for item in first.batch.facet_observations] == [
        item.observation_id for item in second.batch.facet_observations
    ]
    assert all(
        item.facet != "content"
        for item in first.batch.facet_observations
        if capability.endswith("children.read")
    )
    if capability == "databricks.workspace.content.read":
        assert first.batch.coverage == ()
    else:
        expected_coverage = (
            CollectionCoverage.COMPLETE
            if capability == "databricks.workspace.metadata.read"
            else CollectionCoverage.UNKNOWN
        )
        assert len(first.batch.coverage) == 1
        assert first.batch.coverage[0].scope == action.requested_scopes[0]
        assert first.batch.coverage[0].completeness is expected_coverage
        assert first.batch.coverage[0].absence_authority == ()
    assert "storage_location" not in str(first.batch.to_dict())


@pytest.mark.parametrize(
    ("capability", "facet", "target", "envelope_key", "array_key"),
    [
        (
            "databricks.workspace.children.read",
            "membership",
            ResolvedTarget(
                workspace_path="/Shared",
                workspace_root="/Shared",
                canonical_object_id=uuid4(),
                canonical_object_type="folder",
            ),
            "workspace_children",
            "workspace_children_array",
        ),
        (
            "databricks.uc.catalogs.read",
            "attributes",
            ResolvedTarget(),
            "catalogs",
            "catalogs_array",
        ),
        (
            "databricks.uc.schemas.read",
            "attributes",
            ResolvedTarget(catalog_name="main"),
            "schemas",
            "schemas_array",
        ),
        (
            "databricks.uc.relations.read",
            "attributes",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            "relations",
            "relations_array",
        ),
        (
            "databricks.uc.volumes.read",
            "attributes",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            "volumes",
            "volumes_array",
        ),
    ],
)
def test_list_normalization_accepts_envelopes_and_top_level_arrays(
    capability: str,
    facet: str,
    target: ResolvedTarget,
    envelope_key: str,
    array_key: str,
) -> None:
    fixture = json.loads(Path("tests/fixtures/databricks/capabilities.json").read_text())
    action = _action(capability, facet)
    binding = _binding()
    envelope = normalize(
        action=action,
        binding=binding,
        target=target,
        stdout=json.dumps(fixture[envelope_key]).encode(),
        observed_at=datetime.now(UTC),
    )
    array = normalize(
        action=action,
        binding=binding,
        target=target,
        stdout=json.dumps(fixture[array_key]).encode(),
        observed_at=datetime.now(UTC),
    )
    assert len(envelope.batch.facet_observations) == len(array.batch.facet_observations)
    assert len(envelope.batch.relationship_observations) == len(
        array.batch.relationship_observations
    )
    assert all(item.completeness is CollectionCoverage.UNKNOWN for item in array.batch.coverage)
    assert "storage_location" not in str(array.batch.to_dict())


@pytest.mark.parametrize(
    ("capability", "facet", "target"),
    [
        (
            "databricks.workspace.metadata.read",
            "metadata",
            ResolvedTarget(workspace_path="/Shared/n", workspace_root="/Shared"),
        ),
        (
            "databricks.workspace.content.read",
            "content",
            ResolvedTarget(workspace_path="/Shared/n", workspace_root="/Shared"),
        ),
    ],
)
def test_metadata_and_content_reject_top_level_arrays(
    capability: str, facet: str, target: ResolvedTarget
) -> None:
    with pytest.raises(InvalidDownstreamResponse):
        normalize(
            action=_action(capability, facet),
            binding=_binding(),
            target=target,
            stdout=b"[]",
            observed_at=datetime.now(UTC),
        )


@pytest.mark.parametrize("stdout", [b"1", b"[{}, false]", b'["unsafe"]', b"null"])
def test_list_normalization_rejects_scalars_and_mixed_arrays(stdout: bytes) -> None:
    with pytest.raises(InvalidDownstreamResponse):
        normalize(
            action=_action("databricks.uc.catalogs.read"),
            binding=_binding(),
            target=ResolvedTarget(),
            stdout=stdout,
            observed_at=datetime.now(UTC),
        )


@pytest.mark.parametrize(
    "child",
    [
        {"path": "/Shared", "object_type": "DIRECTORY"},
        {"path": "/Shared/nested/child", "object_type": "FILE"},
        {"path": "/Users/other/secret", "object_type": "FILE"},
    ],
)
def test_workspace_listing_rejects_children_outside_direct_scope(child: dict[str, str]) -> None:
    target = ResolvedTarget(
        workspace_path="/Shared",
        workspace_root="/Shared",
        canonical_object_id=uuid4(),
        canonical_object_type="folder",
    )
    with pytest.raises(InvalidDownstreamResponse):
        normalize(
            action=_action("databricks.workspace.children.read", "membership"),
            binding=_binding(),
            target=target,
            stdout=json.dumps({"objects": [child]}).encode(),
            observed_at=datetime.now(UTC),
        )


def test_workspace_listing_rejects_duplicate_child_locator() -> None:
    target = ResolvedTarget(
        workspace_path="/Shared",
        workspace_root="/Shared",
        canonical_object_id=uuid4(),
        canonical_object_type="folder",
    )
    child = {"path": "/Shared/one", "object_type": "FILE", "object_id": 1}
    with pytest.raises(InvalidDownstreamResponse):
        normalize(
            action=_action("databricks.workspace.children.read", "membership"),
            binding=_binding(),
            target=target,
            stdout=json.dumps({"objects": [child, child]}).encode(),
            observed_at=datetime.now(UTC),
        )


def test_workspace_listing_rejects_duplicate_identity_with_different_paths() -> None:
    target = ResolvedTarget(
        workspace_path="/Shared",
        workspace_root="/Shared",
        canonical_object_id=uuid4(),
        canonical_object_type="folder",
    )
    with pytest.raises(InvalidDownstreamResponse):
        normalize(
            action=_action("databricks.workspace.children.read", "membership"),
            binding=_binding(),
            target=target,
            stdout=json.dumps(
                {
                    "objects": [
                        {"path": "/Shared/one", "object_type": "FILE", "object_id": 1},
                        {"path": "/Shared/two", "object_type": "FILE", "object_id": 1},
                    ]
                }
            ).encode(),
            observed_at=datetime.now(UTC),
        )


def test_workspace_listing_rejects_child_with_parent_external_identity() -> None:
    target = ResolvedTarget(
        workspace_path="/Shared",
        workspace_root="/Shared",
        canonical_object_id=uuid4(),
        canonical_object_type="folder",
        canonical_parent_external_key="workspace:/Shared",
    )
    with pytest.raises(InvalidDownstreamResponse):
        normalize(
            action=_action("databricks.workspace.children.read", "membership"),
            binding=_binding(),
            target=target,
            stdout=json.dumps(
                {
                    "objects": [
                        {"path": "/Shared/child", "object_type": "FILE", "object_id": "/Shared"}
                    ]
                }
            ).encode(),
            observed_at=datetime.now(UTC),
        )


def test_normalization_rejects_collection_and_column_cap_plus_one() -> None:
    action = _action("databricks.uc.catalogs.read")
    with pytest.raises(InvalidDownstreamResponse):
        normalize(
            action=action,
            binding=_binding(),
            target=ResolvedTarget(),
            stdout=json.dumps(
                {"catalogs": [{"name": str(index)} for index in range(MAX_COLLECTION_ITEMS + 1)]}
            ).encode(),
            observed_at=datetime.now(UTC),
        )
    relation = _action("databricks.uc.relations.read")
    with pytest.raises(InvalidDownstreamResponse):
        normalize(
            action=relation,
            binding=_binding(),
            target=ResolvedTarget(catalog_name="main", schema_name="sales"),
            stdout=json.dumps(
                {
                    "tables": [
                        {"name": "orders", "columns": [{"name": "id"}] * (MAX_TABLE_COLUMNS + 1)}
                    ]
                }
            ).encode(),
            observed_at=datetime.now(UTC),
        )


def test_sanitized_fixture_covers_registered_capabilities() -> None:
    fixture = json.loads(Path("tests/fixtures/databricks/capabilities.json").read_text())
    assert set(fixture) == {
        "workspace_children",
        "workspace_children_array",
        "workspace_metadata",
        "workspace_content",
        "catalogs",
        "catalogs_array",
        "schemas",
        "schemas_array",
        "relations",
        "relations_array",
        "volumes",
        "volumes_array",
    }


def test_redaction_and_import_boundary() -> None:
    diagnostic = redact_diagnostic(
        "token=not-for-storage --profile local\nAuthorization: Bearer abc "
        "bearer lowercase-secret "
        '"client_secret":"json-secret" access_token=token-value '  # pragma: allowlist secret
        "refresh_token=refresh-value api_key=api-value \\\\server\\share\\config "
        "/home/person/.databrickscfg "
        "Config: C:\\Users\\person\\.databrickscfg, "
        "databricks_cli_path=C:\\Program Files\\Databricks\\databricks.exe"
    )
    assert (
        "not-for-storage" not in diagnostic
        and "abc" not in diagnostic
        and "lowercase-secret" not in diagnostic
        and "json-secret" not in diagnostic
        and "token-value" not in diagnostic
        and "refresh-value" not in diagnostic
        and "api-value" not in diagnostic
        and "--profile local" not in diagnostic
        and "Users" not in diagnostic
        and "Program Files" not in diagnostic
        and "databricks.exe" not in diagnostic
        and "/home/person" not in diagnostic
        and "server\\share" not in diagnostic
    )
    source = Path("src/async_api_view/adapters/databricks.py").read_text(encoding="utf-8")
    assert "async_api_view.storage" not in source
    assert "databricks api" not in source


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


class _Bindings:
    def __init__(self, binding: ConnectionBinding) -> None:
        self.binding = binding

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
            ("attributes",),
            True,
        )


class _Targets:
    async def resolve(self, **_: object) -> ResolvedTarget:
        return ResolvedTarget(catalog_name="main")


class _MetadataTargets:
    async def resolve(self, **_: object) -> ResolvedTarget:
        return ResolvedTarget(
            workspace_path="/Shared/n",
            canonical_object_id=uuid4(),
            canonical_object_type="file",
        )


class _Runner:
    executable = "databricks"

    async def run(self, *_: object, **__: object) -> CliExecution:
        return CliExecution(str(uuid4()), timedelta(), 0, b'{"catalogs": [{"name": "main"}]}', b"")


class _MetadataRunner(_Runner):
    async def run(self, *_: object, **__: object) -> CliExecution:
        return CliExecution(
            str(uuid4()), timedelta(), 0, b'{"path": "/Shared/n", "object_type": "FILE"}', b""
        )


class _FailRunner(_Runner):
    async def run(self, *_: object, **__: object) -> CliExecution:
        raise DownstreamFailure(
            "raw downstream failure",
            exit_code=1,
            diagnostic="token=worker-secret C:\\Users\\person\\.databrickscfg /home/person/config",
        )


class _TimeoutRunner(_Runner):
    def __init__(self) -> None:
        self.calls = 0

    async def run(self, *_: object, **__: object) -> CliExecution:
        self.calls += 1
        raise CliTimeout("timed out")


class _StaleLifecycle(_Lifecycle):
    async def record_attempt(self, attempt: object, *, lease_id: str) -> None:
        raise RuntimeError(f"stale lease {lease_id}")


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
        raise RuntimeError("lease lost")


class _BlockingRunner:
    executable = "databricks"

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
        self.status = status

    async def ingest(self, batch: object) -> IngestionResult:
        self.batches.append(batch)
        return IngestionResult(batch.batch_id, self.status)


def test_worker_uses_only_ports() -> None:
    action = _action("databricks.uc.catalogs.read")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    lifecycle = _Lifecycle()
    ingestion = _Ingestion()
    worker = DatabricksWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=ingestion,
        targets=_Targets(),
        runner=_Runner(),
    )
    assert asyncio.run(worker.run_once())
    assert ingestion.batches
    assert any(event[0] == "running" for event in lifecycle.events if isinstance(event, tuple))
    assert all(
        event[1] == worker.queue.lease.lease_id
        for event in lifecycle.events
        if isinstance(event, tuple) and event[0] in {"attempt", "complete"}
    )


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (IngestionStatus.ACCEPTED, ActionOutcome.PARTIAL),
        (IngestionStatus.DUPLICATE, ActionOutcome.PARTIAL),
        (IngestionStatus.PARTIAL, ActionOutcome.PARTIAL),
        (IngestionStatus.REJECTED, ActionOutcome.FAILED),
    ],
)
def test_worker_maps_ingestion_status_and_unknown_collection_coverage(
    status: IngestionStatus, expected: ActionOutcome
) -> None:
    action = _action("databricks.uc.catalogs.read")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    lifecycle = _Lifecycle()
    worker = DatabricksWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(status),
        targets=_Targets(),
        runner=_Runner(),
    )
    asyncio.run(worker.run_once())
    completed = [event[2] for event in lifecycle.events if event[0] == "complete"]
    assert completed[-1].outcome is expected
    attempts = [event[2] for event in lifecycle.events if event[0] == "attempt"]
    assert attempts[-1].outcome is expected
    if status is IngestionStatus.REJECTED:
        assert attempts[-1].error_class is not None


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (IngestionStatus.ACCEPTED, ActionOutcome.SUCCEEDED),
        (IngestionStatus.DUPLICATE, ActionOutcome.SUCCEEDED),
        (IngestionStatus.PARTIAL, ActionOutcome.PARTIAL),
        (IngestionStatus.REJECTED, ActionOutcome.FAILED),
    ],
)
def test_worker_maps_all_ingestion_statuses_for_complete_object_read(
    status: IngestionStatus, expected: ActionOutcome
) -> None:
    action = _action("databricks.workspace.metadata.read", "metadata")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local", "workspace_root": "/Shared"},
    )
    lifecycle = _Lifecycle()
    worker = DatabricksWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(status),
        targets=_MetadataTargets(),
        runner=_MetadataRunner(),
    )
    asyncio.run(worker.run_once())
    completed = [event[2] for event in lifecycle.events if event[0] == "complete"]
    assert completed[-1].outcome is expected


def test_retryable_failure_schedules_one_durable_attempt_per_lease() -> None:
    action = _action("databricks.uc.catalogs.read")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    lifecycle = _Lifecycle()
    timeout_runner = _TimeoutRunner()
    first_worker = DatabricksWorker(
        worker_id="first",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC), attempt_ordinal=1)),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_Targets(),
        runner=timeout_runner,
        max_attempts=2,
    )

    assert asyncio.run(first_worker.run_once())

    attempts = [event[2] for event in lifecycle.events if event[0] == "attempt"]
    assert timeout_runner.calls == 1
    assert len(attempts) == 1
    assert attempts[0].ordinal == 1
    assert attempts[0].error_class is ErrorClass.CONNECTION_TIMEOUT
    assert attempts[0].retry_at - attempts[0].ended_at == timedelta(seconds=1)
    assert not any(event[0] == "complete" for event in lifecycle.events)

    second_worker = DatabricksWorker(
        worker_id="second",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC), attempt_ordinal=2)),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_Targets(),
        runner=_Runner(),
        max_attempts=2,
    )

    assert asyncio.run(second_worker.run_once())
    attempts = [event[2] for event in lifecycle.events if event[0] == "attempt"]
    completed = [event[2] for event in lifecycle.events if event[0] == "complete"]
    assert [attempt.ordinal for attempt in attempts] == [1, 2]
    assert completed[-1].outcome is ActionOutcome.PARTIAL


def test_stale_lease_or_guard_failure_never_finalizes_action() -> None:
    action = _action("databricks.uc.catalogs.read")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    for lifecycle, guard in ((_StaleLifecycle(), _Guard()), (_Lifecycle(), _FailGuard())):
        worker = DatabricksWorker(
            worker_id="first-worker",
            queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
            lifecycle=lifecycle,
            guard=guard,
            bindings=_Bindings(binding),
            ingestion=_Ingestion(),
            targets=_Targets(),
            runner=_Runner(),
        )
        asyncio.run(worker.run_once())
        assert not any(event[0] == "complete" for event in lifecycle.events)


@pytest.mark.parametrize("disposition", [GuardDisposition.CANCEL, GuardDisposition.SATISFY])
def test_guard_owned_terminal_transitions_are_not_completed_twice(
    disposition: GuardDisposition,
) -> None:
    action = _action("databricks.uc.catalogs.read")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    lifecycle = _Lifecycle()
    worker = DatabricksWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_TerminalGuard(disposition),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_Targets(),
        runner=_Runner(),
    )
    asyncio.run(worker.run_once())
    assert not any(event[0] in {"running", "attempt", "complete"} for event in lifecycle.events)


def test_heartbeat_loss_cancels_running_cli_without_finalizing() -> None:
    action = _action("databricks.uc.catalogs.read")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    runner = _BlockingRunner()
    lifecycle = _HeartbeatLostLifecycle()
    worker = DatabricksWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_Targets(),
        runner=runner,
        heartbeat_seconds=0.001,
    )
    asyncio.run(worker.run_once())
    assert runner.cancelled
    assert not any(event[0] in {"attempt", "complete"} for event in lifecycle.events)


def test_worker_persists_only_closed_diagnostics() -> None:
    action = _action("databricks.uc.catalogs.read")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    lifecycle = _Lifecycle()
    worker = DatabricksWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_Targets(),
        runner=_FailRunner(),
        max_attempts=1,
    )
    asyncio.run(worker.run_once())
    persisted = str(lifecycle.events)
    assert "worker-secret" not in persisted
    assert "Users" not in persisted
    assert "/home/person" not in persisted
