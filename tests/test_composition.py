from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest

from async_api_view.adapters.databricks import CliExecution, CliInvocation, CliRunner
from async_api_view.composition import build_runtime
from async_api_view.config import AppSettings, DatabricksSystemSettings, ProjectSettings
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


def settings(tmp_path: Path) -> ProjectSettings:
    return ProjectSettings(
        app=AppSettings(database_path=tmp_path / "state.sqlite3"),
        databricks_systems=(
            DatabricksSystemSettings(
                name="test-workspace",
                profile="TEST_PROFILE",
                workspace_root="/",
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
