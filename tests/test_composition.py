from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from pathlib import Path

import pytest

from async_api_view.adapters.databricks import CliExecution, CliInvocation, CliRunner
from async_api_view.composition import build_runtime
from async_api_view.config import AppSettings, DatabricksSystemSettings, ProjectSettings
from async_api_view.contracts import RemoteObject
from async_api_view.storage import StoredAction
from async_api_view.web import RefreshRequest


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


def settings(
    tmp_path: Path,
    *,
    config_id: str | None = "test-workspace",
    name: str = "test-workspace",
    profile: str = "TEST_PROFILE",
    workspace_root: str = "/",
) -> ProjectSettings:
    return ProjectSettings(
        app=AppSettings(database_path=tmp_path / "state.sqlite3"),
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
    runtime.worker_available = True

    dashboard = await runtime.backend.dashboard()
    refresh = next(
        option
        for option in dashboard.refresh_options
        if option.capability_key == "databricks.workspace.children.read"
        and option.target_kind == "configured_scope"
    )

    intent_id = await runtime.backend.submit_refresh(
        request=RefreshRequest(
            system_id=refresh.system_id,
            target_kind=refresh.target_kind,
            target_id=refresh.target_id,
            capability_key=refresh.capability_key,
            facet=refresh.facet,
        )
    )
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
    second = await runtime.backend.intent(second_intent)
    assert second is not None
    assert not second.terminal
    assert second.scopes[0].state == "deferred"
    assert second.scopes[0].eligible_at is not None

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

    calls = {"actions": 0, "objects": 0}
    list_actions = runtime.store.list_actions
    list_objects = runtime.store.list_objects

    def counted_actions(*, system_id: str | None = None) -> tuple[StoredAction, ...]:
        calls["actions"] += 1
        return list_actions(system_id=system_id)

    def counted_objects(*, system_id: str | None = None) -> tuple[RemoteObject, ...]:
        calls["objects"] += 1
        return list_objects(system_id=system_id)

    monkeypatch.setattr(runtime.store, "list_actions", counted_actions)
    monkeypatch.setattr(runtime.store, "list_objects", counted_objects)

    rendered = await runtime.backend.dashboard()

    assert rendered.objects
    assert calls == {"actions": 1, "objects": 1}
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


@pytest.mark.anyio
async def test_background_failure_is_durable_and_reports_unavailable(tmp_path: Path) -> None:
    runtime = build_runtime(settings(tmp_path), runner=FakeCliRunner(b"[]"))

    class FailingCoordinator:
        async def run_once(self) -> None:
            raise RuntimeError("untrusted detail must not be persisted")

    runtime.coordinator = FailingCoordinator()  # type: ignore[assignment]
    await runtime.start()
    task = runtime._background_task
    assert task is not None
    await task

    available, error = runtime.status()
    assert not available
    assert error == "coordinator stopped unexpectedly (RuntimeError)"
    events = runtime.store.list_operational_events(alertable_only=True)
    assert events[-1].event_type == "queue.coordinator.failed"
    assert events[-1].redacted_summary == error
    assert "untrusted detail" not in events[-1].redacted_summary

    await runtime.stop()


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

    await asyncio.wait_for(runtime.stop(), timeout=1)

    assert runner.cancelled


@pytest.mark.anyio
async def test_unity_catalog_metadata_capabilities_cascade_without_content(
    tmp_path: Path,
) -> None:
    runner = CapabilityCliRunner(
        {
            "databricks.uc.catalogs.read": b'[{"name":"main"}]',
            "databricks.uc.schemas.read": b'[{"name":"sales"}]',
            "databricks.uc.relations.read": (
                b'[{"name":"orders","full_name":"main.sales.orders",'
                b'"table_type":"MANAGED","storage_location":"s3://hidden",'
                b'"columns":[{"name":"id","type_name":"LONG"}]}]'
            ),
            "databricks.uc.volumes.read": (
                b'[{"name":"raw","full_name":"main.sales.raw",'
                b'"storage_location":"s3://hidden-volume"}]'
            ),
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
    await execute("databricks.uc.relations.read")
    await execute("databricks.uc.volumes.read")

    assert [call.capability_key for call in runner.calls] == [
        "databricks.uc.catalogs.read",
        "databricks.uc.schemas.read",
        "databricks.uc.relations.read",
        "databricks.uc.volumes.read",
    ]
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
    assert "storage_location" not in serialized
    assert "s3://hidden" not in serialized

    runtime.store.close()
