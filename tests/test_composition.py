from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import httpx2
import pytest

from async_api_view.adapters.databricks import CliExecution, CliInvocation, CliRunner
from async_api_view.application import SystemBootstrapService
from async_api_view.cli import _run_once
from async_api_view.composition import build_runtime
from async_api_view.config import (
    AppSettings,
    ConfigError,
    DatabricksSystemSettings,
    ProjectSettings,
)
from async_api_view.contracts import (
    ActionCompletion,
    ActionOutcome,
    FacetState,
    KnowledgeState,
    PresenceState,
    RefreshIntent,
    RefreshOrigin,
    RefreshScope,
    RemoteObject,
    TargetKind,
    TargetRef,
)
from async_api_view.storage import (
    ActionActivityRecord,
    ActionAttemptRecord,
    FacetActionStatusRecord,
    FacetEvidenceRecord,
    SQLiteStore,
)
from async_api_view.web import (
    ActionHistoryQuery,
    AlertHistoryQuery,
    DashboardQuery,
    ObjectDetailQuery,
    RefreshRequest,
)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class FakeCliRunner(CliRunner):
    def __init__(self, stdout: bytes) -> None:
        super().__init__(executable="databricks")
        self.stdout = stdout
        self.calls: list[CliInvocation] = []

    async def doctor(self) -> None:
        return None

    async def run(self, invocation: CliInvocation, *, correlation_id: str) -> CliExecution:
        self.calls.append(invocation)
        return CliExecution(
            correlation_id=correlation_id,
            duration=timedelta(milliseconds=5),
            exit_code=0,
            stdout=self.stdout,
            stderr=b"",
        )


class CapabilityCliRunner(FakeCliRunner):
    def __init__(self, outputs: dict[str, bytes]) -> None:
        super().__init__(b"{}")
        self.outputs = outputs

    async def run(self, invocation: CliInvocation, *, correlation_id: str) -> CliExecution:
        self.calls.append(invocation)
        return CliExecution(
            correlation_id=correlation_id,
            duration=timedelta(milliseconds=5),
            exit_code=0,
            stdout=self.outputs[invocation.capability_key],
            stderr=b"",
        )


class BlockingCliRunner(FakeCliRunner):
    def __init__(self) -> None:
        super().__init__(b"[]")
        self.started = asyncio.Event()
        self.cancelled = False

    async def run(self, invocation: CliInvocation, *, correlation_id: str) -> CliExecution:
        del invocation, correlation_id
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        raise AssertionError("blocking runner unexpectedly resumed")


class FlakyStartupCliRunner(FakeCliRunner):
    def __init__(self, failures: int) -> None:
        super().__init__(b"[]")
        self.failures = failures
        self.doctor_calls = 0
        self.ready = asyncio.Event()

    async def doctor(self) -> None:
        self.doctor_calls += 1
        if self.doctor_calls <= self.failures:
            raise RuntimeError("temporary startup failure")
        self.ready.set()


class BlockingStartupCliRunner(FakeCliRunner):
    def __init__(self) -> None:
        super().__init__(b"[]")
        self.started = asyncio.Event()
        self.cancelled = False

    async def doctor(self) -> None:
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        raise AssertionError("blocking compatibility check unexpectedly resumed")


def settings(
    tmp_path: Path,
    *,
    config_id: str | None = "test-workspace",
    name: str = "test-workspace",
    profile: str = "TEST_PROFILE",
    workspace_root: str = "/",
    worker_poll_seconds: float = 1.0,
) -> ProjectSettings:
    return ProjectSettings(
        app=AppSettings(
            database_path=tmp_path / "state.sqlite3",
            worker_poll_seconds=worker_poll_seconds,
        ),
        databricks_systems=(
            DatabricksSystemSettings(
                config_id=config_id,
                name=name,
                profile=profile,
                workspace_root=workspace_root,
            ),
        ),
    )


@pytest.mark.anyio
async def test_databricks_workspace_vertical_slice_is_durable_and_throttled(
    tmp_path: Path,
) -> None:
    runner = FakeCliRunner(
        b"""[
          {"object_id": 101, "object_type": "DIRECTORY", "path": "/Shared"},
          {"object_id": 102, "object_type": "NOTEBOOK", "path": "/Demo", "language": "PYTHON"}
        ]"""
    )
    runtime = build_runtime(settings(tmp_path), runner=runner)
    unavailable = await runtime.backend.dashboard()
    assert unavailable.refresh_options
    assert all(not option.enabled for option in unavailable.refresh_options)
    assert all(
        option.disabled_reason == "Refresh worker is unavailable."
        for option in unavailable.refresh_options
    )
    runtime.worker_available = True

    dashboard = await runtime.backend.dashboard()
    refresh = next(
        option
        for option in dashboard.refresh_options
        if option.capability_key == "databricks.workspace.children.read"
        and option.target_kind == "configured_scope"
    )

    ui_session_id = str(uuid4())
    intent_id = await runtime.backend.submit_refresh(
        request=RefreshRequest(
            system_id=refresh.system_id,
            target_kind=refresh.target_kind,
            target_id=refresh.target_id,
            capability_key=refresh.capability_key,
            facet=refresh.facet,
            ui_session_id=ui_session_id,
        )
    )
    stored_intent = runtime.store.get_refresh_intent(intent_id)
    assert stored_intent is not None
    assert stored_intent.ui_session_id == ui_session_id
    admitted = await runtime.coordinator.run_once()
    assert admitted is not None
    assert admitted.action_id is not None
    assert await runtime.worker.run_once()

    refreshed = await runtime.backend.dashboard()
    assert {item.name for item in refreshed.objects} >= {"/", "Shared", "Demo"}
    assert [item.name for item in refreshed.objects].count("/") == 1
    root_scope = runtime.store.get_configured_scope(refresh.target_id)
    assert root_scope is not None
    assert root_scope.object_id is not None
    assert runtime.store.get_facet_sync(root_scope.object_id, "membership") is not None
    detail = await runtime.backend.object_detail(root_scope.object_id)
    assert detail is not None
    membership_facet = next(facet for facet in detail.object.facets if facet.name == "membership")
    assert membership_facet.provenance == (
        "databricks adapter v1 · databricks.workspace.children.read v1"
    )
    assert membership_facet.provenance_action_id == admitted.action_id
    assert membership_facet.provenance_observation_id is not None
    assert detail.relationship_total == 2
    assert {child.name for child in detail.children} == {"Shared", "Demo"}
    assert any(option.target_id == root_scope.object_id for option in detail.refresh_options)
    filtered_detail = await runtime.backend.object_detail(
        root_scope.object_id, ObjectDetailQuery(object_type="file")
    )
    assert filtered_detail is not None
    assert [child.name for child in filtered_detail.children] == ["Demo"]
    intent = await runtime.backend.intent(intent_id)
    assert intent is not None
    assert intent.terminal
    assert intent.scopes[0].state in {"succeeded", "partial"}
    assert len(runner.calls) == 1
    assert runner.calls[0].argv[1:4] == ("workspace", "list", "/")

    second_intent = await runtime.backend.submit_refresh(
        request=RefreshRequest(
            system_id=refresh.system_id,
            target_kind=refresh.target_kind,
            target_id=refresh.target_id,
            capability_key=refresh.capability_key,
            facet=refresh.facet,
        )
    )
    deferred = await runtime.coordinator.run_once()
    assert deferred is not None
    assert deferred.action_id is None
    assert deferred.state.value == "deferred"
    assert not await runtime.worker.run_once()
    assert len(runner.calls) == 1
    runtime.store._connection.execute(
        """
        UPDATE observation_batches SET action_id = ?
        WHERE batch_id = (
            SELECT batch_id FROM observation_journal WHERE observation_id = ?
        )
        """,
        (str(uuid4()), membership_facet.provenance_observation_id),
    )
    stale_provenance = await runtime.backend.object_detail(root_scope.object_id)
    assert stale_provenance is not None
    stale_membership = next(
        facet for facet in stale_provenance.object.facets if facet.name == "membership"
    )
    assert stale_membership.provenance.startswith("databricks adapter v1")
    assert stale_membership.provenance_action_id is None
    second = await runtime.backend.intent(second_intent)
    assert second is not None
    assert not second.terminal
    assert second.scopes[0].state == "deferred"
    assert second.scopes[0].eligible_at is not None

    runtime.store.close()


@pytest.mark.anyio
async def test_ten_thousand_workspace_children_ingest_in_bounded_batches(
    tmp_path: Path,
) -> None:
    runner = FakeCliRunner(
        json.dumps(
            [
                {
                    "object_id": index,
                    "object_type": "FILE",
                    "path": f"/f{index:05d}.py",
                }
                for index in range(10_000)
            ]
        ).encode()
    )
    runtime = build_runtime(settings(tmp_path), runner=runner)
    runtime.worker_available = True
    initial_object_count = runtime.store._connection.execute(
        "SELECT COUNT(*) FROM remote_objects"
    ).fetchone()[0]
    dashboard = await runtime.backend.dashboard()
    refresh = next(
        option
        for option in dashboard.refresh_options
        if option.capability_key == "databricks.workspace.children.read"
        and option.target_kind == "configured_scope"
    )
    intent_id = await runtime.backend.submit_refresh(
        RefreshRequest(
            system_id=refresh.system_id,
            target_kind=refresh.target_kind,
            target_id=refresh.target_id,
            capability_key=refresh.capability_key,
            facet=refresh.facet,
        )
    )
    admitted = await runtime.coordinator.run_once()
    assert admitted is not None and admitted.action_id is not None

    assert await runtime.worker.run_once()

    batch_rows = runtime.store._connection.execute(
        """
        SELECT status, issue_count FROM observation_batches
        WHERE action_id = ? ORDER BY batch_id
        """,
        (admitted.action_id,),
    ).fetchall()
    assert len(batch_rows) > 1
    assert {tuple(row) for row in batch_rows} == {("partial", 0)}
    assert (
        runtime.store._connection.execute(
            "SELECT COUNT(*) FROM remote_objects WHERE system_id = ?",
            (refresh.system_id,),
        ).fetchone()[0]
        == initial_object_count + 10_000
    )
    assert (
        runtime.store._connection.execute(
            "SELECT COUNT(*) FROM relationships WHERE system_id = ? AND presence = 'present'",
            (refresh.system_id,),
        ).fetchone()[0]
        == 10_000
    )
    root_scope = runtime.store.get_configured_scope(refresh.target_id)
    assert root_scope is not None and root_scope.object_id is not None
    membership = runtime.store.get_facet_sync(root_scope.object_id, "membership")
    assert membership is not None and membership.payload["member_count"] == 10_000
    intent = await runtime.backend.intent(intent_id)
    assert intent is not None and intent.terminal
    assert intent.scopes[0].state == "partial"
    runtime.store.close()


@pytest.mark.anyio
async def test_legacy_null_action_scope_finishes_with_selected_capability_authority(
    tmp_path: Path,
) -> None:
    runner = FakeCliRunner(b'[{"object_id":101,"object_type":"FILE","path":"/legacy.py"}]')
    runtime = build_runtime(settings(tmp_path), runner=runner)
    runtime.worker_available = True
    dashboard = await runtime.backend.dashboard()
    refresh = next(
        option
        for option in dashboard.refresh_options
        if option.capability_key == "databricks.workspace.children.read"
        and option.target_kind == "configured_scope"
    )
    await runtime.backend.submit_refresh(
        RefreshRequest(
            system_id=refresh.system_id,
            target_kind=refresh.target_kind,
            target_id=refresh.target_id,
            capability_key=refresh.capability_key,
            facet=refresh.facet,
        )
    )
    admitted = await runtime.coordinator.run_once()
    assert admitted is not None and admitted.action_id is not None
    runtime.store._connection.execute(
        "UPDATE adapter_action_scopes SET capability_key = NULL WHERE action_id = ?",
        (admitted.action_id,),
    )

    assert await runtime.worker.run_once()

    action = runtime.store.get_stored_action(admitted.action_id)
    assert action is not None and action.state.value == "partial"
    assert any(
        item.external_key == "workspace:object_id:101" for item in runtime.store.list_objects()
    )
    assert (
        runtime.store._connection.execute(
            "SELECT COUNT(*) FROM ingestion_issues WHERE action_id = ?",
            (admitted.action_id,),
        ).fetchone()[0]
        == 0
    )
    runtime.store.close()


@pytest.mark.anyio
async def test_direct_workspace_metadata_refresh_is_accepted_and_credited(
    tmp_path: Path,
) -> None:
    runner = CapabilityCliRunner(
        {
            "databricks.workspace.children.read": (
                b'[{"object_id":102,"object_type":"NOTEBOOK","path":"/Demo","language":"PYTHON"}]'
            ),
            "databricks.workspace.metadata.read": (
                b'{"object_id":102,"object_type":"NOTEBOOK","path":"/Demo","language":"SQL"}'
            ),
        }
    )
    runtime = build_runtime(settings(tmp_path), runner=runner)
    runtime.worker_available = True
    dashboard = await runtime.backend.dashboard()
    children_refresh = next(
        option
        for option in dashboard.refresh_options
        if option.capability_key == "databricks.workspace.children.read"
        and option.target_kind == "configured_scope"
    )
    await runtime.backend.submit_refresh(
        RefreshRequest(
            system_id=children_refresh.system_id,
            target_kind=children_refresh.target_kind,
            target_id=children_refresh.target_id,
            capability_key=children_refresh.capability_key,
            facet=children_refresh.facet,
        )
    )
    assert await runtime.coordinator.run_once() is not None
    assert await runtime.worker.run_once()

    root_scope = runtime.store.get_configured_scope(children_refresh.target_id)
    assert root_scope is not None and root_scope.object_id is not None
    root = await runtime.backend.object_detail(root_scope.object_id)
    assert root is not None
    child = root.children[0]
    child_detail = await runtime.backend.object_detail(child.object_id)
    assert child_detail is not None
    metadata_refresh = next(
        option
        for option in child_detail.refresh_options
        if option.capability_key == "databricks.workspace.metadata.read"
    )
    intent_id = await runtime.backend.submit_refresh(
        RefreshRequest(
            system_id=metadata_refresh.system_id,
            target_kind=metadata_refresh.target_kind,
            target_id=metadata_refresh.target_id,
            capability_key=metadata_refresh.capability_key,
            facet=metadata_refresh.facet,
        )
    )
    assert await runtime.coordinator.run_once() is not None
    assert await runtime.worker.run_once()

    metadata = runtime.store.get_facet_sync(child.object_id, "metadata")
    intent = await runtime.backend.intent(intent_id)
    intent_scope = runtime.store.list_intent_scopes(intent_id)[0]
    assert intent_scope.linked_action_id is not None
    stored_action = runtime.store.get_stored_action(intent_scope.linked_action_id)
    assert stored_action is not None
    assert metadata is not None
    assert metadata.payload["language"] == "SQL"
    assert intent is not None and intent.terminal
    assert intent.scopes[0].state == "succeeded"
    assert stored_action.state.value == "succeeded"
    assert stored_action.redacted_diagnostic is None
    assert (
        runtime.store.list_action_attempts(stored_action.action_id)[0].redacted_diagnostic is None
    )
    assert runtime.store.latest_qualifying_observation(stored_action.action.requested_scopes[0])

    second_intent_id = await runtime.backend.submit_refresh(
        RefreshRequest(
            system_id=metadata_refresh.system_id,
            target_kind=metadata_refresh.target_kind,
            target_id=metadata_refresh.target_id,
            capability_key=metadata_refresh.capability_key,
            facet=metadata_refresh.facet,
        )
    )
    satisfied = await runtime.coordinator.run_once()
    second_intent = await runtime.backend.intent(second_intent_id)
    assert satisfied is not None and satisfied.action_id is None
    assert satisfied.state.value == "satisfied"
    assert not await runtime.worker.run_once()
    assert second_intent is not None and second_intent.terminal
    assert second_intent.scopes[0].state == "satisfied"
    assert [call.capability_key for call in runner.calls] == [
        "databricks.workspace.children.read",
        "databricks.workspace.metadata.read",
    ]

    runtime.store.close()


@pytest.mark.anyio
async def test_expired_action_deadline_never_reaches_cli_runner(tmp_path: Path) -> None:
    runner = FakeCliRunner(b"[]")
    runtime = build_runtime(settings(tmp_path), runner=runner)
    dashboard = await runtime.backend.dashboard()
    option = next(
        item
        for item in dashboard.refresh_options
        if item.capability_key == "databricks.workspace.children.read"
        and item.target_kind == "configured_scope"
    )
    configured = runtime.store.get_configured_scope(option.target_id)
    assert configured is not None
    requested_at = datetime.now(UTC)
    intent = RefreshIntent(
        intent_id=uuid4(),
        idempotency_key=str(uuid4()),
        origin=RefreshOrigin.MANUAL,
        actor_id="local-user",
        scopes=(
            RefreshScope(
                system_id=option.system_id,
                target=TargetRef(TargetKind.CONFIGURED_SCOPE, option.target_id),
                object_type=configured.object_type,
                facet=option.facet,
                capability_key=option.capability_key,
            ),
        ),
        requested_at=requested_at,
        expires_at=requested_at + timedelta(seconds=1),
    )
    await runtime.store.submit_refresh(intent)
    admitted = await runtime.coordinator.run_once(now=requested_at)
    assert admitted is not None and admitted.action_id is not None

    assert await runtime.worker.run_once(now=requested_at + timedelta(seconds=2))

    assert runner.calls == []
    assert runtime.store.get_stored_action(admitted.action_id).state.value == "cancelled"
    assert runtime.store.list_intent_scopes(intent.intent_id)[0].state.value == "expired"
    runtime.store.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("boundary", "expected_reason"),
    (
        ("system", "system_disabled"),
        ("binding", "binding_disabled"),
        ("binding_revision", "binding_changed"),
        ("capability", "capability_disabled"),
        ("deadline", "action_deadline_expired"),
    ),
)
async def test_final_start_authorization_blocks_revoked_remote_dispatch(
    tmp_path: Path,
    boundary: str,
    expected_reason: str,
) -> None:
    runner = FakeCliRunner(b"[]")
    runtime = build_runtime(settings(tmp_path), runner=runner)
    runtime.worker_available = True
    dashboard = await runtime.backend.dashboard()
    option = next(
        item
        for item in dashboard.refresh_options
        if item.capability_key == "databricks.workspace.children.read"
        and item.target_kind == "configured_scope"
    )
    intent_id = await runtime.backend.submit_refresh(
        RefreshRequest(
            system_id=option.system_id,
            target_kind=option.target_kind,
            target_id=option.target_id,
            capability_key=option.capability_key,
            facet=option.facet,
        )
    )
    admitted = await runtime.coordinator.run_once()
    assert admitted is not None and admitted.action_id is not None
    resolver = runtime.worker.targets

    class RevokingResolver:
        async def resolve(self, **kwargs: object):  # type: ignore[no-untyped-def]
            resolved = await resolver.resolve(**kwargs)
            if boundary == "system":
                runtime.store.set_system_enabled(option.system_id, enabled=False)
            elif boundary == "binding":
                runtime.store._connection.execute(
                    "UPDATE connection_bindings SET enabled = 0 WHERE system_id = ?",
                    (option.system_id,),
                )
            elif boundary == "binding_revision":
                current_binding = runtime.store.list_connection_bindings(
                    system_id=option.system_id
                )[0]
                rotated_settings = dict(current_binding.non_secret_settings)
                rotated_settings["profile"] = "ROTATED_PROFILE"
                runtime.store.upsert_connection_binding(
                    replace(current_binding, non_secret_settings=rotated_settings)
                )
            elif boundary == "capability":
                runtime.store._connection.execute(
                    """
                    UPDATE capability_bindings
                    SET enabled = 0
                    WHERE capability_key = 'databricks.workspace.children.read'
                    """
                )
            else:
                deadline = datetime.now(UTC)
                deadline_text = deadline.isoformat().replace("+00:00", "Z")
                runtime.store._connection.execute(
                    "UPDATE adapter_actions SET deadline = ? WHERE action_id = ?",
                    (deadline_text, admitted.action_id),
                )
                runtime.store._connection.execute(
                    "UPDATE refresh_intents SET expires_at = ? WHERE intent_id = ?",
                    (deadline_text, intent_id),
                )
            return resolved

    runtime.worker.targets = RevokingResolver()

    assert await runtime.worker.run_once()

    assert runner.calls == []
    stored = runtime.store.get_stored_action(admitted.action_id)
    assert stored is not None and stored.state.value == "cancelled"
    assert stored.redacted_diagnostic == expected_reason
    assert runtime.store.list_action_attempts(admitted.action_id) == ()
    view = await runtime.backend.intent(intent_id)
    assert view is not None and view.terminal
    runtime.store.close()


@pytest.mark.anyio
async def test_expired_coalesced_receipt_is_not_overlaid_by_live_action(tmp_path: Path) -> None:
    runtime = build_runtime(settings(tmp_path), runner=FakeCliRunner(b"[]"))
    dashboard = await runtime.backend.dashboard()
    option = next(
        item
        for item in dashboard.refresh_options
        if item.capability_key == "databricks.workspace.children.read"
        and item.target_kind == "configured_scope"
    )
    configured = runtime.store.get_configured_scope(option.target_id)
    assert configured is not None
    requested_at = datetime.now(UTC)
    scope = RefreshScope(
        system_id=option.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, option.target_id),
        object_type=configured.object_type,
        facet=option.facet,
        capability_key=option.capability_key,
    )
    live = RefreshIntent(
        intent_id=uuid4(),
        idempotency_key=str(uuid4()),
        origin=RefreshOrigin.MANUAL,
        actor_id="local-user",
        scopes=(scope,),
        requested_at=requested_at,
    )
    expiring = RefreshIntent(
        intent_id=uuid4(),
        idempotency_key=str(uuid4()),
        origin=RefreshOrigin.MANUAL,
        actor_id="local-user",
        scopes=(scope,),
        requested_at=requested_at + timedelta(seconds=1),
        expires_at=requested_at + timedelta(seconds=2),
    )
    await runtime.store.submit_refresh(live)
    admitted = await runtime.coordinator.run_once(now=requested_at)
    assert admitted is not None and admitted.action_id is not None
    await runtime.store.submit_refresh(expiring)
    coalesced = await runtime.coordinator.run_once(now=requested_at + timedelta(seconds=1))
    assert coalesced is not None and coalesced.state.value == "coalesced"
    lease = await runtime.store.lease_next(
        adapter_key="databricks",
        worker_id="worker",
        now=requested_at + timedelta(seconds=3),
    )
    assert lease is not None
    decision = await runtime.store.evaluate(
        action_id=lease.action.action_id,
        lease_id=lease.lease_id,
        now=requested_at + timedelta(seconds=3),
    )
    assert decision.disposition.value == "dispatch"
    await runtime.store.complete_action(
        ActionCompletion(
            action_id=lease.action.action_id,
            outcome=ActionOutcome.SUCCEEDED,
            completed_at=requested_at + timedelta(seconds=4),
        ),
        lease_id=lease.lease_id,
    )

    live_view = await runtime.backend.intent(str(live.intent_id))
    expired_view = await runtime.backend.intent(str(expiring.intent_id))

    assert live_view is not None and live_view.scopes[0].state == "succeeded"
    assert expired_view is not None and expired_view.terminal
    assert expired_view.scopes[0].state == "expired"
    assert expired_view.scopes[0].action_id == admitted.action_id
    runtime.store.close()


@pytest.mark.anyio
async def test_malformed_action_deadline_terminalizes_without_cli_call(tmp_path: Path) -> None:
    runner = FakeCliRunner(b"[]")
    runtime = build_runtime(settings(tmp_path), runner=runner)
    runtime.worker_available = True
    dashboard = await runtime.backend.dashboard()
    option = next(
        item
        for item in dashboard.refresh_options
        if item.capability_key == "databricks.workspace.children.read"
        and item.target_kind == "configured_scope"
    )
    intent_id = await runtime.backend.submit_refresh(
        RefreshRequest(
            system_id=option.system_id,
            target_kind=option.target_kind,
            target_id=option.target_id,
            capability_key=option.capability_key,
            facet=option.facet,
        )
    )
    admitted = await runtime.coordinator.run_once()
    assert admitted is not None and admitted.action_id is not None
    runtime.store._connection.execute(
        "UPDATE adapter_actions SET deadline = ? WHERE action_id = ?",
        ("not-a-timestamp", admitted.action_id),
    )
    healthy_option = next(
        item
        for item in dashboard.refresh_options
        if item.capability_key == "databricks.uc.catalogs.read"
    )
    await runtime.backend.submit_refresh(
        RefreshRequest(
            system_id=healthy_option.system_id,
            target_kind=healthy_option.target_kind,
            target_id=healthy_option.target_id,
            capability_key=healthy_option.capability_key,
            facet=healthy_option.facet,
        )
    )
    healthy_admission = await runtime.coordinator.run_once()
    assert healthy_admission is not None and healthy_admission.action_id is not None
    runtime.store._connection.execute(
        "UPDATE adapter_actions SET record_created_at = ? WHERE action_id = ?",
        ("2026-08-29T00:00:00.000000Z", admitted.action_id),
    )
    runtime.store._connection.execute(
        "UPDATE adapter_actions SET record_created_at = ? WHERE action_id = ?",
        ("2026-08-29T00:00:01.000000Z", healthy_admission.action_id),
    )

    assert await runtime.worker.run_once()

    assert [call.capability_key for call in runner.calls] == ["databricks.uc.catalogs.read"]
    action_row = runtime.store._connection.execute(
        "SELECT state, error_class FROM adapter_actions WHERE action_id = ?",
        (admitted.action_id,),
    ).fetchone()
    assert tuple(action_row) == ("failed", "adapter_contract_mismatch")
    assert runtime.store.list_intent_scopes(intent_id)[0].state.value == "rejected"
    runtime.store.close()


@pytest.mark.anyio
async def test_run_once_skips_incompatible_intent_and_drains_valid_work(tmp_path: Path) -> None:
    runner = FakeCliRunner(b"[]")
    runtime = build_runtime(settings(tmp_path), runner=runner)
    runtime.worker_available = True
    dashboard = await runtime.backend.dashboard()
    poisoned_option = next(
        item
        for item in dashboard.refresh_options
        if item.capability_key == "databricks.workspace.children.read"
        and item.target_kind == "configured_scope"
    )
    healthy_option = next(
        item
        for item in dashboard.refresh_options
        if item.capability_key == "databricks.uc.catalogs.read"
    )
    poisoned_intent_id = await runtime.backend.submit_refresh(
        RefreshRequest(
            system_id=poisoned_option.system_id,
            target_kind=poisoned_option.target_kind,
            target_id=poisoned_option.target_id,
            capability_key=poisoned_option.capability_key,
            facet=poisoned_option.facet,
        )
    )
    await runtime.backend.submit_refresh(
        RefreshRequest(
            system_id=healthy_option.system_id,
            target_kind=healthy_option.target_kind,
            target_id=healthy_option.target_id,
            capability_key=healthy_option.capability_key,
            facet=healthy_option.facet,
        )
    )
    runtime.store._connection.execute(
        "UPDATE refresh_intents SET contract_version = '2' WHERE intent_id = ?",
        (poisoned_intent_id,),
    )

    await _run_once(runtime)

    assert [call.capability_key for call in runner.calls] == ["databricks.uc.catalogs.read"]
    assert runtime.store.list_intent_scopes(poisoned_intent_id)[0].state.value == "rejected"
    assert len(runtime.store.list_operational_events(alertable_only=True)) == 1
    poisoned_view = await runtime.backend.intent(poisoned_intent_id)
    assert poisoned_view is not None and poisoned_view.terminal
    assert poisoned_view.scopes[0].state == "rejected"
    assert poisoned_view.error == (
        "Stored request contract is unsupported; durable dispositions remain available."
    )
    runtime.store.close()


@pytest.mark.anyio
async def test_dashboard_reads_action_and_object_snapshots_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = build_runtime(
        settings(tmp_path),
        runner=FakeCliRunner(b'[{"object_id":101,"object_type":"DIRECTORY","path":"/Shared"}]'),
    )
    runtime.worker_available = True
    dashboard = await runtime.backend.dashboard()
    refresh = next(
        option
        for option in dashboard.refresh_options
        if option.capability_key == "databricks.workspace.children.read"
        and option.target_kind == "configured_scope"
    )
    await runtime.backend.submit_refresh(
        RefreshRequest(
            system_id=refresh.system_id,
            target_kind=refresh.target_kind,
            target_id=refresh.target_id,
            capability_key=refresh.capability_key,
            facet=refresh.facet,
        )
    )
    assert await runtime.coordinator.run_once() is not None
    assert await runtime.worker.run_once()

    calls = {"actions": 0, "objects": 0, "facet_actions": 0}
    list_actions = runtime.store.list_latest_system_activity
    list_objects = runtime.store.list_objects_page
    list_facet_actions = runtime.store.list_latest_facet_actions

    def counted_actions() -> tuple[ActionActivityRecord, ...]:
        calls["actions"] += 1
        return list_actions()

    def counted_objects(*, offset: int, limit: int, query: str = "") -> tuple[RemoteObject, ...]:
        calls["objects"] += 1
        return list_objects(offset=offset, limit=limit, query=query)

    def counted_facet_actions(
        object_ids: tuple[str, ...],
    ) -> tuple[FacetActionStatusRecord, ...]:
        calls["facet_actions"] += 1
        return list_facet_actions(object_ids)

    monkeypatch.setattr(runtime.store, "list_latest_system_activity", counted_actions)
    monkeypatch.setattr(runtime.store, "list_objects_page", counted_objects)
    monkeypatch.setattr(runtime.store, "list_latest_facet_actions", counted_facet_actions)

    rendered = await runtime.backend.dashboard()

    assert rendered.objects
    assert calls == {"actions": 1, "objects": 1, "facet_actions": 1}
    runtime.store.close()


def test_facet_view_distinguishes_due_failed_refreshing_and_current(tmp_path: Path) -> None:
    runtime = build_runtime(settings(tmp_path), runner=FakeCliRunner(b"[]"))
    root = runtime.store.list_objects()[0]
    observed_at = datetime.now(UTC) - timedelta(days=1)
    facet = FacetState(
        object_id=root.object_id,
        facet="metadata",
        facet_version="1",
        knowledge=KnowledgeState.KNOWN,
        payload={"name": "cached"},
        observed_at=observed_at,
        state_changed_at=observed_at,
    )
    failed_action = FacetActionStatusRecord(
        system_id=root.system_id,
        object_id=root.object_id,
        facet="metadata",
        action_id=str(uuid4()),
        state="failed",
        occurred_at=observed_at + timedelta(hours=1),
        redacted_diagnostic="connection timeout",
    )
    arguments = {
        "system_id": root.system_id,
        "object_id": root.object_id,
        "object_type": root.object_type,
        "evidence": FacetEvidenceRecord(
            facet=facet,
            observation_id=None,
            batch_id=None,
            adapter_key=None,
            adapter_version=None,
            action_id=None,
            capability_key=None,
            capability_version=None,
        ),
    }

    due = runtime.backend._facet_view(**arguments, last_action=None)
    failed = runtime.backend._facet_view(**arguments, last_action=failed_action)
    refreshing = runtime.backend._facet_view(
        **arguments, last_action=replace(failed_action, state="running")
    )
    current = runtime.backend._facet_view(
        **(
            arguments
            | {
                "evidence": replace(
                    arguments["evidence"],
                    facet=replace(facet, observed_at=datetime.now(UTC)),
                )
            }
        ),
        last_action=failed_action,
    )

    assert due.freshness == "due"
    assert failed.freshness == "failed"
    assert failed.failure == "connection timeout"
    assert failed.last_action_id == failed_action.action_id
    assert refreshing.freshness == "refreshing"
    assert current.freshness == "current"
    runtime.store.close()


@pytest.mark.anyio
async def test_dashboard_paginates_and_filters_large_cached_inventory(tmp_path: Path) -> None:
    runtime = build_runtime(settings(tmp_path), runner=FakeCliRunner(b"[]"))
    observed_at = datetime(2026, 8, 28, tzinfo=UTC)
    system_id = runtime.store.list_systems()[0].system_id
    object_ids: list[str] = []
    for index in range(500):
        stored = runtime.store.upsert_object(
            RemoteObject(
                object_id=uuid4(),
                system_id=system_id,
                object_type="file",
                object_type_version="1",
                source_kind="databricks.workspace.file",
                external_key=f"workspace-id:{index}",
                display_name=f"object-{index:03d}",
                presence=PresenceState.PRESENT,
                first_seen_at=observed_at,
                last_seen_at=observed_at,
            )
        )
        object_ids.append(str(stored.object_id))
    root_scope = next(
        scope
        for scope in runtime.store.list_configured_scopes(system_id=system_id)
        if scope.object_type == "folder"
    )
    assert root_scope.object_id is not None
    runtime.store._connection.executemany(
        """
        INSERT INTO relationships (
            relationship_id, system_id, subject_id, predicate, object_id,
            presence, observed_at, supporting_observation_id
        ) VALUES (?, ?, ?, 'contains', ?, 'present', ?, ?)
        """,
        (
            (
                str(uuid4()),
                system_id,
                root_scope.object_id,
                object_id,
                observed_at.isoformat(),
                str(uuid4()),
            )
            for object_id in object_ids[:120]
        ),
    )
    runtime.store._connection.execute(
        "UPDATE relationships SET presence = 'absent' WHERE subject_id = ? AND object_id = ?",
        (root_scope.object_id, object_ids[0]),
    )
    runtime.worker_available = True

    selects: list[str] = []
    runtime.store._connection.set_trace_callback(
        lambda statement: (
            selects.append(statement)
            if statement.lstrip().upper().startswith(("SELECT", "WITH"))
            else None
        )
    )
    first = await runtime.backend.dashboard(DashboardQuery())
    runtime.store._connection.set_trace_callback(None)
    last = await runtime.backend.dashboard(DashboardQuery(object_page=11))
    filtered = await runtime.backend.dashboard(DashboardQuery(object_query="object-499"))

    assert first.object_total == 502
    assert len(first.objects) == 50
    assert first.object_page_count == 11
    assert first.previous_page_url is None
    assert first.next_page_url == "/?page=2"
    assert len(selects) <= 70
    assert len(last.objects) == 2
    assert last.object_page_start == 501
    assert last.object_page_end == 502
    assert last.next_page_url is None
    assert filtered.object_total == 1
    assert [item.name for item in filtered.objects] == ["object-499"]
    object_refresh = next(
        option for option in filtered.refresh_options if option.target_kind == "object"
    )
    assert await runtime.backend.is_refresh_registered(
        RefreshRequest(
            system_id=object_refresh.system_id,
            target_kind=object_refresh.target_kind,
            target_id=object_refresh.target_id,
            capability_key=object_refresh.capability_key,
            facet=object_refresh.facet,
        )
    )
    first_detail = await runtime.backend.object_detail(root_scope.object_id)
    last_detail = await runtime.backend.object_detail(
        root_scope.object_id, ObjectDetailQuery(relationship_page=3)
    )
    assert first_detail is not None and len(first_detail.children) == 50
    assert first_detail.relationship_total == 119
    assert first_detail.next_page_url == f"/objects/{root_scope.object_id}?page=2"
    assert last_detail is not None and len(last_detail.children) == 19
    assert last_detail.relationship_page_start == 101
    assert last_detail.next_page_url is None
    type_filtered = await runtime.backend.object_detail(
        root_scope.object_id,
        ObjectDetailQuery(object_type="file"),
    )
    assert type_filtered is not None
    assert type_filtered.relationship_total == 119
    assert type_filtered.object_type_filter == "file"
    assert type_filtered.next_page_url == (f"/objects/{root_scope.object_id}?type=file&page=2")
    runtime.store.close()


@pytest.mark.anyio
async def test_configuration_reconciliation_rotates_profile_and_disables_removed_system(
    tmp_path: Path,
) -> None:
    first = build_runtime(
        settings(tmp_path, name="Original", profile="PROFILE_ONE", workspace_root="/Shared"),
        runner=FakeCliRunner(b"[]"),
    )
    first.worker_available = True
    initial_dashboard = await first.backend.dashboard()
    initial_option = next(
        option
        for option in initial_dashboard.refresh_options
        if option.capability_key == "databricks.workspace.children.read"
        and option.target_kind == "configured_scope"
    )
    initial_system_id = initial_option.system_id
    assert (
        first.store.list_connection_bindings(system_id=initial_system_id)[0].non_secret_settings[
            "profile"
        ]
        == "PROFILE_ONE"
    )
    first.store.close()

    rotated = build_runtime(
        settings(tmp_path, name="Renamed", profile="PROFILE_TWO", workspace_root="/Shared"),
        runner=FakeCliRunner(b"[]"),
    )
    systems = rotated.store.list_systems()
    assert [(system.system_id, system.display_name, system.enabled) for system in systems] == [
        (initial_system_id, "Renamed", True)
    ]
    assert (
        rotated.store.list_connection_bindings(system_id=initial_system_id)[0].non_secret_settings[
            "profile"
        ]
        == "PROFILE_TWO"
    )
    rotated.worker_available = True
    rotated_dashboard = await rotated.backend.dashboard()
    rotated_option = next(
        option
        for option in rotated_dashboard.refresh_options
        if option.capability_key == "databricks.workspace.children.read"
        and option.target_kind == "configured_scope"
    )
    await rotated.backend.submit_refresh(
        RefreshRequest(
            system_id=rotated_option.system_id,
            target_kind=rotated_option.target_kind,
            target_id=rotated_option.target_id,
            capability_key=rotated_option.capability_key,
            facet=rotated_option.facet,
        )
    )
    admitted = await rotated.coordinator.run_once()
    assert admitted is not None and admitted.action_id is not None
    rotated.store.close()

    removed_runner = FakeCliRunner(b"[]")
    removed = build_runtime(
        ProjectSettings(
            app=AppSettings(database_path=tmp_path / "state.sqlite3"),
            databricks_systems=(),
        ),
        runner=removed_runner,
    )
    removed.worker_available = True
    removed_dashboard = await removed.backend.dashboard()

    assert removed_dashboard.objects
    assert len(removed_dashboard.systems) == 1
    assert not removed_dashboard.systems[0].enabled
    assert removed_dashboard.refresh_options == ()
    with pytest.raises(ValueError, match="not registered"):
        await removed.backend.submit_refresh(
            RefreshRequest(
                system_id=initial_option.system_id,
                target_kind=initial_option.target_kind,
                target_id=initial_option.target_id,
                capability_key=initial_option.capability_key,
                facet=initial_option.facet,
            )
        )
    assert removed_runner.calls == []
    assert await removed.worker.run_once()
    assert removed_runner.calls == []
    stored = removed.store.get_stored_action(admitted.action_id)
    assert stored is not None and stored.state.value == "cancelled"
    removed.store.close()


def test_explicit_config_id_adopts_legacy_system_identity(tmp_path: Path) -> None:
    legacy = build_runtime(
        settings(
            tmp_path,
            config_id=None,
            name="Legacy name",
            profile="LEGACY_PROFILE",
            workspace_root="/Shared",
        ),
        runner=FakeCliRunner(b"[]"),
    )
    legacy_system_id = next(system.system_id for system in legacy.store.list_systems())
    legacy.store.close()

    adopted = build_runtime(
        settings(
            tmp_path,
            config_id="stable-workspace",
            name="Legacy name",
            profile="LEGACY_PROFILE",
            workspace_root="/Shared",
        ),
        runner=FakeCliRunner(b"[]"),
    )
    assert [(system.system_id, system.enabled) for system in adopted.store.list_systems()] == [
        (legacy_system_id, True)
    ]
    adopted.store.close()

    renamed = build_runtime(
        settings(
            tmp_path,
            config_id="stable-workspace",
            name="Renamed",
            profile="ROTATED_PROFILE",
            workspace_root="/Shared",
        ),
        runner=FakeCliRunner(b"[]"),
    )
    assert [
        (system.system_id, system.display_name, system.enabled)
        for system in renamed.store.list_systems()
    ] == [(legacy_system_id, "Renamed", True)]
    renamed.store.close()


def test_case_only_config_id_change_preserves_identity_and_cache(tmp_path: Path) -> None:
    initial = build_runtime(
        settings(tmp_path, config_id="primary"),
        runner=FakeCliRunner(b"[]"),
    )
    system_id = initial.store.list_systems()[0].system_id
    object_ids = {item.object_id for item in initial.store.list_objects()}
    initial.store._connection.execute(
        "UPDATE configured_system_identities SET config_id = 'Primary'"
    )
    initial.store.close()

    restarted = build_runtime(
        settings(tmp_path, config_id="PRIMARY"),
        runner=FakeCliRunner(b"[]"),
    )

    assert [(system.system_id, system.enabled) for system in restarted.store.list_systems()] == [
        (system_id, True)
    ]
    assert {item.object_id for item in restarted.store.list_objects()} == object_ids
    identities = restarted.store._connection.execute(
        "SELECT config_id, system_id FROM configured_system_identities"
    ).fetchall()
    assert [tuple(row) for row in identities] == [("primary", system_id)]
    restarted.store.close()


def test_direct_settings_reject_non_ascii_config_id_before_database_creation(
    tmp_path: Path,
) -> None:
    project_settings = settings(tmp_path, config_id="İ")

    with pytest.raises(ConfigError, match="letters, digits"):
        build_runtime(project_settings, runner=FakeCliRunner(b"[]"))

    assert not project_settings.app.database_path.exists()


def test_workspace_root_change_creates_new_authority_and_pauses_predecessor(
    tmp_path: Path,
) -> None:
    original = build_runtime(
        settings(tmp_path, workspace_root="/Original"), runner=FakeCliRunner(b"[]")
    )
    original_id = next(system.system_id for system in original.store.list_systems())
    original.store.close()

    changed = build_runtime(
        settings(tmp_path, workspace_root="/Changed"), runner=FakeCliRunner(b"[]")
    )
    systems = changed.store.list_systems()

    assert len(systems) == 2
    assert {system.display_name for system in systems} == {"test-workspace"}
    assert {system.system_id for system in systems if system.enabled} != {original_id}
    assert {system.system_id for system in systems if not system.enabled} == {original_id}
    assert all(
        not binding.enabled
        for binding in changed.store.list_connection_bindings(system_id=original_id)
    )
    assert all(
        not capability.enabled
        for capability in changed.store.list_capability_bindings(system_id=original_id)
    )
    assert all(
        not scope.enabled for scope in changed.store.list_configured_scopes(system_id=original_id)
    )
    changed.store.close()


def test_composition_failure_closes_open_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    closed = False
    close = SQLiteStore.close

    def tracked_close(store: SQLiteStore) -> None:
        nonlocal closed
        closed = True
        close(store)

    def fail_bootstrap(_service: SystemBootstrapService, **_kwargs: object) -> None:
        raise RuntimeError("injected composition failure")

    monkeypatch.setattr(SQLiteStore, "close", tracked_close)
    monkeypatch.setattr(SystemBootstrapService, "configure_databricks_workspace", fail_bootstrap)

    with pytest.raises(RuntimeError, match="injected composition failure"):
        build_runtime(settings(tmp_path), runner=FakeCliRunner(b"[]"))

    assert closed


def test_configuration_application_rolls_back_all_systems_on_late_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "state.sqlite3"

    def project(*systems: DatabricksSystemSettings) -> ProjectSettings:
        return ProjectSettings(
            app=AppSettings(database_path=database_path),
            databricks_systems=systems,
        )

    first = DatabricksSystemSettings("A", "OLD_PROFILE", "/A", "a")
    retained = DatabricksSystemSettings("B", "B_PROFILE", "/B", "b")
    initial = build_runtime(project(first, retained), runner=FakeCliRunner(b"[]"))
    initial.store.close()

    original = SystemBootstrapService.configure_databricks_workspace

    def fail_late(
        service: SystemBootstrapService,
        **kwargs: object,
    ):
        if kwargs["display_name"] == "C":
            raise RuntimeError("injected late configuration failure")
        return original(service, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        SystemBootstrapService,
        "configure_databricks_workspace",
        fail_late,
    )
    rotated = DatabricksSystemSettings("A", "NEW_PROFILE", "/A", "a")
    failing = DatabricksSystemSettings("C", "C_PROFILE", "/C", "c")

    with pytest.raises(RuntimeError, match="injected late configuration failure"):
        build_runtime(project(rotated, failing), runner=FakeCliRunner(b"[]"))

    with SQLiteStore(database_path) as reopened:
        systems = {system.display_name: system for system in reopened.list_systems()}
        assert set(systems) == {"A", "B"}
        assert systems["A"].enabled
        assert systems["B"].enabled
        binding = reopened.list_connection_bindings(system_id=systems["A"].system_id)[0]
        assert binding.non_secret_settings["profile"] == "OLD_PROFILE"


@pytest.mark.anyio
async def test_coordinator_failure_is_durable_and_recovers_automatically(tmp_path: Path) -> None:
    runtime = build_runtime(
        settings(tmp_path, worker_poll_seconds=0.05), runner=FakeCliRunner(b"[]")
    )

    class FlakyCoordinator:
        def __init__(self) -> None:
            self.calls = 0
            self.recovered = asyncio.Event()

        async def run_once(self) -> None:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("untrusted detail must not be persisted")
            self.recovered.set()
            return None

    coordinator = FlakyCoordinator()
    runtime.coordinator = coordinator  # type: ignore[assignment]
    await runtime.start()
    await asyncio.wait_for(coordinator.recovered.wait(), timeout=1)
    await asyncio.sleep(0)

    available, error = runtime.status()
    assert available
    assert error is None
    events = runtime.store.list_operational_events(alertable_only=True)
    coordinator_events = [
        event for event in events if event.event_type == "queue.coordinator.failed"
    ]
    assert len(coordinator_events) == 1
    assert coordinator_events[0].redacted_summary == (
        "coordinator stopped unexpectedly (RuntimeError)"
    )
    assert "untrusted detail" not in coordinator_events[0].redacted_summary
    dashboard = await runtime.backend.dashboard()
    assert len(dashboard.alerts) == 1
    assert dashboard.alerts[0].event_type == "queue.coordinator.failed"
    assert dashboard.alerts[0].summary == coordinator_events[0].redacted_summary

    await runtime.stop()


@pytest.mark.anyio
async def test_worker_startup_retries_with_one_event_and_clears_dashboard_error(
    tmp_path: Path,
) -> None:
    runner = FlakyStartupCliRunner(failures=2)
    runtime = build_runtime(
        settings(tmp_path, worker_poll_seconds=0.05),
        runner=runner,
    )

    await runtime.start()
    unavailable = await runtime.backend.dashboard()
    assert not unavailable.disconnected
    assert unavailable.error is None
    assert unavailable.refresh_unavailable
    assert unavailable.refresh_error == "worker stopped unexpectedly (RuntimeError)"
    await asyncio.wait_for(runner.ready.wait(), timeout=1)
    await asyncio.sleep(0)

    recovered = await runtime.backend.dashboard()
    assert runner.doctor_calls == 3
    assert not recovered.disconnected
    assert recovered.error is None
    assert not recovered.refresh_unavailable
    assert recovered.refresh_error is None
    worker_events = [
        event
        for event in runtime.store.list_operational_events(alertable_only=True)
        if event.event_type == "queue.adapter_worker.failed"
    ]
    assert len(worker_events) == 1

    await runtime.stop()


@pytest.mark.anyio
async def test_recovered_worker_uses_a_new_delivery_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = FakeCliRunner(b"[]")
    runtime = build_runtime(settings(tmp_path), runner=runner)
    runtime.worker_available = True
    dashboard = await runtime.backend.dashboard()
    refresh = next(
        option
        for option in dashboard.refresh_options
        if option.capability_key == "databricks.workspace.children.read"
        and option.target_kind == "configured_scope"
    )
    await runtime.backend.submit_refresh(
        RefreshRequest(
            system_id=refresh.system_id,
            target_kind=refresh.target_kind,
            target_id=refresh.target_id,
            capability_key=refresh.capability_key,
            facet=refresh.facet,
        )
    )
    assert await runtime.coordinator.run_once() is not None
    first_started = datetime.now(UTC)
    first = await runtime.store.lease_next(
        adapter_key="databricks",
        worker_id="first-worker",
        now=first_started,
    )
    assert first is not None
    original_record_attempt = runtime.store.record_attempt

    async def lose_lease_before_attempt(*_args: object, **_kwargs: object) -> None:
        raise ValueError("injected crash boundary")

    monkeypatch.setattr(runtime.store, "record_attempt", lose_lease_before_attempt)
    await runtime.worker.process(first, now=first_started)
    assert runtime.store.list_actions()[0].state.value == "running"
    assert (
        runtime.store._connection.execute(
            "SELECT COUNT(*) FROM observation_batches WHERE action_id = ?",
            (first.action.action_id,),
        ).fetchone()[0]
        == 1
    )

    monkeypatch.setattr(runtime.store, "record_attempt", original_record_attempt)
    recovered_at = datetime.now(UTC)
    runtime.store._connection.execute(
        "UPDATE adapter_actions SET leased_until = ? WHERE action_id = ?",
        (
            (recovered_at - timedelta(microseconds=1))
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z"),
            first.action.action_id,
        ),
    )
    recovered = await runtime.store.lease_next(
        adapter_key="databricks",
        worker_id="recovered-worker",
        now=recovered_at,
    )
    assert recovered is not None
    await runtime.worker.process(recovered, now=recovered_at)

    batches = runtime.store._connection.execute(
        """
        SELECT batch_id, issue_count
        FROM observation_batches WHERE action_id = ? ORDER BY batch_id
        """,
        (first.action.action_id,),
    ).fetchall()
    credit_count = runtime.store._connection.execute(
        "SELECT COUNT(*) FROM refresh_credit WHERE system_id = ?",
        (first.action.system_id,),
    ).fetchone()[0]
    journal_ids = {
        row["observation_id"]
        for row in runtime.store._connection.execute(
            """
            SELECT journal.observation_id
            FROM observation_journal AS journal
            JOIN observation_batches AS batch ON batch.batch_id = journal.batch_id
            WHERE batch.action_id = ?
            """,
            (first.action.action_id,),
        ).fetchall()
    }
    assert runtime.store.list_actions()[0].state.value == "partial"
    assert len(batches) == 2
    assert batches[0]["batch_id"] != batches[1]["batch_id"]
    assert {batch["issue_count"] for batch in batches} == {0}
    assert credit_count == 0
    assert len(journal_ids) == 2
    assert len(runner.calls) == 2
    runtime.store.close()


@pytest.mark.anyio
async def test_blocked_worker_startup_does_not_hold_cached_authenticated_dashboard(
    tmp_path: Path,
) -> None:
    runner = BlockingStartupCliRunner()
    runtime = build_runtime(settings(tmp_path), runner=runner)
    origin = f"https://{runtime.local_authorizer.browser_host}"
    lifespan = runtime.app.router.lifespan_context(runtime.app)
    entered_lifespan = False

    try:
        await asyncio.wait_for(lifespan.__aenter__(), timeout=1)
        entered_lifespan = True
        await asyncio.wait_for(runner.started.wait(), timeout=1)
        token = runtime.local_authorizer.take_bootstrap_token()
        transport = httpx2.ASGITransport(app=runtime.app)
        async with httpx2.AsyncClient(transport=transport, base_url=origin) as client:
            bootstrap = await client.post(
                "/bootstrap",
                data={"bootstrap_token": token},
                headers={"Origin": origin},
                follow_redirects=False,
            )
            response = await client.get("/")

        assert bootstrap.status_code == 303
        assert response.status_code == 200
        assert "test-workspace" in response.text
        assert "Refresh unavailable" in response.text
        assert "Cached snapshot loaded" in response.text
        assert "Disconnected" not in response.text
        assert "Worker compatibility check is in progress." in response.text
        assert runtime.status() == (
            False,
            "Worker compatibility check is in progress.",
        )
    finally:
        if entered_lifespan:
            await asyncio.wait_for(lifespan.__aexit__(None, None, None), timeout=1)
        else:
            await asyncio.wait_for(runtime.stop(), timeout=1)

    assert runner.cancelled


@pytest.mark.anyio
async def test_repeated_worker_loop_failures_do_not_flood_events(tmp_path: Path) -> None:
    runtime = build_runtime(
        settings(tmp_path, worker_poll_seconds=0.05), runner=FakeCliRunner(b"[]")
    )

    class FlakyWorker:
        def __init__(self) -> None:
            self.startup_calls = 0
            self.run_calls = 0
            self.recovered = asyncio.Event()

        async def startup(self) -> None:
            self.startup_calls += 1

        async def run_once(self) -> bool:
            self.run_calls += 1
            if self.run_calls <= 2:
                raise RuntimeError("temporary worker loop failure")
            self.recovered.set()
            return False

    worker = FlakyWorker()
    runtime.worker = worker  # type: ignore[assignment]
    await runtime.start()
    await asyncio.wait_for(worker.recovered.wait(), timeout=1)
    await asyncio.sleep(0)

    assert worker.startup_calls == 3
    assert runtime.status() == (True, None)
    worker_events = [
        event
        for event in runtime.store.list_operational_events(alertable_only=True)
        if event.event_type == "queue.adapter_worker.failed"
    ]
    assert len(worker_events) == 1

    await runtime.stop()


@pytest.mark.anyio
async def test_alert_history_pages_and_filters_durable_events(tmp_path: Path) -> None:
    runtime = build_runtime(settings(tmp_path), runner=FakeCliRunner(b"[]"))
    started = datetime(2026, 8, 28, tzinfo=UTC)
    for index in range(120):
        runtime.store.record_runtime_failure(
            event_type=(
                "queue.coordinator.failed" if index % 2 == 0 else "queue.adapter_worker.failed"
            ),
            summary=f"Runtime failure {index}",
            occurred_at=started + timedelta(seconds=index),
        )

    first = await runtime.backend.alert_history()
    filtered = await runtime.backend.alert_history(
        AlertHistoryQuery(
            page=2,
            event_type="queue.coordinator.failed",
            severity="error",
        )
    )

    assert first.total == 120
    assert len(first.alerts) == 50
    assert first.alerts[0].summary == "Runtime failure 119"
    assert first.next_page_url == "/alerts?page=2"
    assert filtered.total == 60
    assert len(filtered.alerts) == 10
    assert filtered.page_start == 51
    assert filtered.page_end == 60
    assert filtered.previous_page_url == ("/alerts?type=queue.coordinator.failed&severity=error")
    assert filtered.next_page_url is None
    assert {alert.event_type for alert in filtered.alerts} == {"queue.coordinator.failed"}
    runtime.store.close()


@pytest.mark.anyio
async def test_action_history_maps_bounded_pages_and_preserves_filters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = build_runtime(settings(tmp_path), runner=FakeCliRunner(b"[]"))
    system = runtime.store.list_systems()[0]
    created_at = datetime(2026, 8, 28, tzinfo=UTC)
    records = tuple(
        ActionActivityRecord(
            action_id=str(uuid4()),
            system_id=system.system_id,
            capability_key="databricks.workspace.metadata.read",
            target_kind="object",
            target_id=str(uuid4()),
            state="failed",
            created_at=created_at + timedelta(seconds=index),
            started_at=created_at + timedelta(seconds=index),
            completed_at=created_at + timedelta(seconds=index + 1),
            retry_at=None,
            error_class="connection_timeout",
            redacted_diagnostic="redacted failure",
        )
        for index in range(25)
    )

    monkeypatch.setattr(runtime.store, "count_action_activity", lambda **_filters: 75)

    def action_page(**filters: object) -> tuple[ActionActivityRecord, ...]:
        assert filters == {
            "offset": 50,
            "limit": 50,
            "system_id": system.system_id,
            "state": "failed",
            "action_id": None,
        }
        return records

    monkeypatch.setattr(runtime.store, "list_action_activity_page", action_page)

    view = await runtime.backend.action_history(
        ActionHistoryQuery(page=2, state="failed", system_id=system.system_id)
    )

    assert view.total == 75
    assert len(view.actions) == 25
    assert view.page_start == 51
    assert view.page_end == 75
    assert view.actions[0].system_name == system.display_name
    assert view.actions[0].diagnostic == "redacted failure"
    assert view.previous_page_url == f"/actions?state=failed&system={system.system_id}"
    assert view.next_page_url is None
    assert view.systems[0].system_id == system.system_id
    runtime.store.close()


@pytest.mark.anyio
async def test_action_detail_maps_bounded_attempt_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = build_runtime(settings(tmp_path), runner=FakeCliRunner(b"[]"))
    system = runtime.store.list_systems()[0]
    action_id = str(uuid4())
    created_at = datetime(2026, 8, 29, tzinfo=UTC)
    action = ActionActivityRecord(
        action_id=action_id,
        system_id=system.system_id,
        capability_key="databricks.workspace.metadata.read",
        target_kind="object",
        target_id=str(uuid4()),
        state="retry_wait",
        created_at=created_at,
        started_at=created_at,
        completed_at=None,
        retry_at=created_at + timedelta(seconds=5),
        error_class="connection_timeout",
        redacted_diagnostic="redacted action",
    )
    attempt = ActionAttemptRecord(
        attempt_id=str(uuid4()),
        action_id=action_id,
        ordinal=1,
        started_at=created_at,
        ended_at=created_at + timedelta(seconds=1),
        outcome="failed",
        error_class="connection_timeout",
        retry_at=created_at + timedelta(seconds=5),
        redacted_diagnostic="redacted attempt",
    )
    monkeypatch.setattr(runtime.store, "get_action_activity", lambda _action_id: action)

    def action_attempts(requested_id: str, *, limit: int) -> tuple[ActionAttemptRecord, ...]:
        assert requested_id == action_id
        assert limit == 100
        return (attempt,)

    monkeypatch.setattr(runtime.store, "list_action_attempts", action_attempts)
    monkeypatch.setattr(runtime.store, "count_action_attempts", lambda _action_id: 1)

    view = await runtime.backend.action_detail(action_id)

    assert view is not None
    assert view.action.system_name == system.display_name
    assert view.action.retry_at == action.retry_at
    assert len(view.attempts) == 1
    assert view.attempt_total == 1
    assert view.attempts[0].ordinal == 1
    assert view.attempts[0].diagnostic == "redacted attempt"
    runtime.store.close()


def test_runtime_recovery_backoff_has_a_floor(tmp_path: Path) -> None:
    runtime = build_runtime(
        settings(tmp_path, worker_poll_seconds=0.05), runner=FakeCliRunner(b"[]")
    )
    runtime._failure_counts["worker"] = 1

    assert runtime._retry_delay("worker") == 0.1
    runtime.store.close()


@pytest.mark.anyio
async def test_runtime_stop_cancels_in_flight_worker_without_waiting_for_cli_timeout(
    tmp_path: Path,
) -> None:
    runner = BlockingCliRunner()
    runtime = build_runtime(settings(tmp_path), runner=runner)
    await runtime.start()
    dashboard = await runtime.backend.dashboard()
    refresh = next(
        option
        for option in dashboard.refresh_options
        if option.capability_key == "databricks.workspace.children.read"
        and option.target_kind == "configured_scope"
    )
    await runtime.backend.submit_refresh(
        RefreshRequest(
            system_id=refresh.system_id,
            target_kind=refresh.target_kind,
            target_id=refresh.target_id,
            capability_key=refresh.capability_key,
            facet=refresh.facet,
        )
    )
    await asyncio.wait_for(runner.started.wait(), timeout=1)
    actions = runtime.store.list_actions()
    assert len(actions) == 1
    interrupted = actions[0]
    assert interrupted.state.value == "running"
    assert interrupted.leased_until is not None
    assert runtime.store.list_action_attempts(interrupted.action.action_id) == ()
    database_path = runtime.settings.app.database_path

    await asyncio.wait_for(runtime.stop(), timeout=1)

    assert runner.cancelled

    with SQLiteStore(database_path) as reopened:
        recovered = await reopened.lease_next(
            adapter_key="databricks",
            worker_id="recovered",
            now=interrupted.leased_until + timedelta(microseconds=1),
        )
        assert recovered is not None
        assert recovered.action.action_id == interrupted.action.action_id


@pytest.mark.anyio
async def test_unity_catalog_metadata_capabilities_cascade_without_content(
    tmp_path: Path,
) -> None:
    schema_id = str(uuid4())
    table_id = str(uuid4())
    volume_id = str(uuid4())
    runner = CapabilityCliRunner(
        {
            "databricks.uc.catalogs.read": b'[{"name":"main"}]',
            "databricks.uc.schemas.read": json.dumps(
                [{"schema_id": schema_id, "name": "sales"}]
            ).encode(),
            "databricks.uc.relations.read": json.dumps(
                [
                    {
                        "table_id": table_id,
                        "name": "orders",
                        "full_name": "main.sales.orders",
                        "table_type": "MANAGED",
                        "storage_location": "s3://hidden",
                        "columns": [{"name": "id", "type_name": "LONG"}],
                    }
                ]
            ).encode(),
            "databricks.uc.volumes.read": json.dumps(
                [
                    {
                        "volume_id": volume_id,
                        "name": "raw",
                        "full_name": "main.sales.raw",
                        "storage_location": "s3://hidden-volume",
                    }
                ]
            ).encode(),
        }
    )
    runtime = build_runtime(settings(tmp_path), runner=runner)
    runtime.worker_available = True

    async def execute(capability_key: str) -> None:
        dashboard = await runtime.backend.dashboard()
        option = next(
            item for item in dashboard.refresh_options if item.capability_key == capability_key
        )
        await runtime.backend.submit_refresh(
            RefreshRequest(
                system_id=option.system_id,
                target_kind=option.target_kind,
                target_id=option.target_id,
                capability_key=option.capability_key,
                facet=option.facet,
            )
        )
        assert await runtime.coordinator.run_once() is not None
        assert await runtime.worker.run_once()

    await execute("databricks.uc.catalogs.read")
    await execute("databricks.uc.schemas.read")
    schema_object = next(
        item for item in runtime.store.list_objects() if item.source_kind == "databricks.uc.schema"
    )
    assert schema_object.external_key == f"schema:schema_id:{schema_id}"
    runtime.store._connection.execute(
        "UPDATE facets SET payload_json = ? WHERE object_id = ? AND facet = 'attributes'",
        (
            json.dumps(
                {
                    "full_name": "other.sales",
                    "name": "sales",
                    "schema_id": schema_id,
                }
            ),
            schema_object.object_id,
        ),
    )
    await execute("databricks.uc.relations.read")
    await execute("databricks.uc.volumes.read")

    assert [call.capability_key for call in runner.calls] == [
        "databricks.uc.catalogs.read",
        "databricks.uc.schemas.read",
        "databricks.uc.relations.read",
        "databricks.uc.volumes.read",
    ]
    relations_call = next(
        call for call in runner.calls if call.capability_key == "databricks.uc.relations.read"
    )
    assert relations_call.argv[1:5] == ("tables", "list", "main", "sales")
    objects = runtime.store.list_objects()
    assert {item.source_kind for item in objects} >= {
        "databricks.uc.catalog",
        "databricks.uc.schema",
        "databricks.uc.table",
        "databricks.uc.volume",
    }
    payloads = [
        dict(facet.payload)
        for remote_object in objects
        for facet in runtime.store.list_facets(remote_object.object_id)
    ]
    serialized = json.dumps(payloads, sort_keys=True)
    assert schema_id in serialized and table_id in serialized and volume_id in serialized
    assert "storage_location" not in serialized
    assert "s3://hidden" not in serialized

    canonical_parent = runtime.store.get_present_parent_sync(schema_object.object_id)
    assert canonical_parent is not None and canonical_parent.external_key == "catalog:main"
    ambiguous_parent = runtime.store.upsert_object(
        RemoteObject(
            object_id=uuid4(),
            system_id=schema_object.system_id,
            object_type="generic_object",
            object_type_version="1",
            source_kind="databricks.uc.catalog",
            external_key="catalog:other",
            display_name="other",
            presence=PresenceState.PRESENT,
            first_seen_at=datetime.now(UTC),
        )
    )
    now_text = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    runtime.store._connection.execute(
        """
        INSERT INTO relationships (
            relationship_id, system_id, subject_id, predicate, object_id,
            presence, observed_at, supporting_observation_id, received_at
        ) VALUES (?, ?, ?, 'contains', ?, 'present', ?, ?, ?)
        """,
        (
            str(uuid4()),
            schema_object.system_id,
            ambiguous_parent.object_id,
            schema_object.object_id,
            now_text,
            str(uuid4()),
            now_text,
        ),
    )
    assert runtime.store.get_present_parent_sync(schema_object.object_id) is None

    runtime.store.close()
