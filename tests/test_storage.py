import asyncio
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from async_api_view.application import SystemBootstrapService
from async_api_view.contracts import (
    ActionAttempt,
    ActionCompletion,
    ActionOutcome,
    AdapterAction,
    ErrorClass,
    FacetObservation,
    FieldCoverage,
    ObjectLocator,
    ObservationBatch,
    PresenceState,
    RefreshCoverage,
    RefreshScope,
    RemoteObject,
    TargetKind,
    TargetRef,
    UpdateMode,
)
from async_api_view.storage import SQLiteStore
from async_api_view.storage.sqlite import _redact

NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)


def run(awaitable):
    return asyncio.run(awaitable)


def test_migrations_reopen_with_durable_wal_state(tmp_path) -> None:
    path = tmp_path / "state.sqlite3"
    with SQLiteStore(path) as store:
        system = SystemBootstrapService(store).create_system(
            display_name="local", system_kind="databricks.workspace", now=NOW
        )
        assert store._connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert store.get_system(system.system_id) is not None

    with SQLiteStore(path) as reopened:
        assert [system.display_name for system in reopened.list_systems()] == ["local"]
        versions = reopened._connection.execute("SELECT version FROM schema_migrations").fetchall()
        assert [row[0] for row in versions] == [
            "0001_initial",
            "0002_refresh_scope_capability_key",
            "0003_observation_batch_digest",
            "0004_configured_system_identities",
            "0005_operational_event_recency",
            "0006_relationship_navigation",
            "0007_operational_event_filters",
            "0008_action_activity",
            "0009_facet_action_status",
        ]
        child_plan = reopened._connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT relationships.object_id, objects.display_name
            FROM relationships AS relationships
            INNER JOIN remote_objects AS objects
                ON objects.object_id = relationships.object_id
            WHERE relationships.subject_id = ?
                AND relationships.predicate = ?
                AND relationships.presence = 'present'
                AND objects.object_type = ?
            ORDER BY relationships.object_id
            LIMIT 50
            """,
            (str(uuid4()), "contains", "file"),
        ).fetchall()
        assert any("ix_relationships_subject_predicate" in row[3] for row in child_plan)
        assert not any("TEMP B-TREE" in row[3] for row in child_plan)


def test_store_close_is_idempotent(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")

    store.close()
    store.close()


def test_existing_v1_database_upgrades_scope_capability_columns(tmp_path) -> None:
    path = tmp_path / "legacy.sqlite3"
    legacy = sqlite3.connect(path)
    legacy.executescript(
        "CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL);"
    )
    migration = Path("src/async_api_view/storage/migrations/0001_initial.sql").read_text(
        encoding="utf-8"
    )
    legacy.executescript(migration)
    legacy.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
        ("0001_initial", "2026-08-24T12:00:00Z"),
    )
    legacy.commit()
    legacy.close()

    with SQLiteStore(path) as store:
        table_columns = (
            store._connection.execute("PRAGMA table_info(refresh_credit)").fetchall(),
            store._connection.execute("PRAGMA table_info(refresh_intent_scopes)").fetchall(),
            store._connection.execute("PRAGMA table_info(adapter_action_scopes)").fetchall(),
        )
        for columns in table_columns:
            assert "capability_key" in {column[1] for column in columns}


def test_concurrent_store_initialization_serializes_migrations(tmp_path) -> None:
    path = tmp_path / "concurrent.sqlite3"
    workers = 8
    barrier = threading.Barrier(workers)

    def open_store() -> tuple[str, ...]:
        barrier.wait(timeout=5)
        with SQLiteStore(path) as store:
            rows = store._connection.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall()
            return tuple(row[0] for row in rows)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        versions = tuple(executor.map(lambda _index: open_store(), range(workers)))

    expected = (
        "0001_initial",
        "0002_refresh_scope_capability_key",
        "0003_observation_batch_digest",
        "0004_configured_system_identities",
        "0005_operational_event_recency",
        "0006_relationship_navigation",
        "0007_operational_event_filters",
        "0008_action_activity",
        "0009_facet_action_status",
    )
    assert versions == (expected,) * workers


def test_configuration_reconciliation_cannot_enable_another_system_kind(tmp_path) -> None:
    with SQLiteStore(tmp_path / "state.sqlite3") as store:
        other = SystemBootstrapService(store).create_system(
            display_name="other",
            system_kind="server.host",
            enabled=False,
            now=NOW,
        )

        store.reconcile_configured_resources(
            system_kind="databricks.workspace",
            system_ids=(other.system_id,),
            connection_binding_ids=(),
            capability_binding_ids=(),
            scope_ids=(),
            now=NOW,
        )

        unchanged = store.get_system(other.system_id)
        assert unchanged is not None and not unchanged.enabled


def test_migration_failure_rolls_back_schema_and_ledger(tmp_path, monkeypatch) -> None:
    path = tmp_path / "legacy.sqlite3"
    legacy = sqlite3.connect(path)
    legacy.executescript(
        "CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL);"
    )
    legacy.executescript(
        Path("src/async_api_view/storage/migrations/0001_initial.sql").read_text(encoding="utf-8")
    )
    legacy.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
        ("0001_initial", "2026-08-24T12:00:00Z"),
    )
    legacy.commit()
    legacy.close()

    original = SQLiteStore._execute_migration_statement

    def fail_mid_0002(store: SQLiteStore, statement: str) -> None:
        if "refresh_intent_scopes" in statement:
            raise RuntimeError("injected migration failure")
        original(store, statement)

    monkeypatch.setattr(SQLiteStore, "_execute_migration_statement", fail_mid_0002)
    with pytest.raises(RuntimeError, match="injected migration failure"):
        SQLiteStore(path)

    check = sqlite3.connect(path)
    assert [row[0] for row in check.execute("SELECT version FROM schema_migrations")] == [
        "0001_initial"
    ]
    assert "capability_key" not in {
        row[1] for row in check.execute("PRAGMA table_info(refresh_credit)")
    }
    check.close()

    monkeypatch.setattr(SQLiteStore, "_execute_migration_statement", original)
    with SQLiteStore(path) as recovered:
        assert "capability_key" in {
            row[1] for row in recovered._connection.execute("PRAGMA table_info(refresh_credit)")
        }


def test_reopen_repairs_legacy_partial_0002_before_recording_ledger(tmp_path) -> None:
    path = tmp_path / "legacy-partial.sqlite3"
    legacy = sqlite3.connect(path)
    legacy.executescript(
        "CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL);"
    )
    legacy.executescript(
        Path("src/async_api_view/storage/migrations/0001_initial.sql").read_text(encoding="utf-8")
    )
    legacy.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
        ("0001_initial", "2026-08-24T12:00:00Z"),
    )
    legacy.execute("ALTER TABLE refresh_credit ADD COLUMN capability_key TEXT")
    legacy.commit()
    legacy.close()

    with SQLiteStore(path) as recovered:
        versions = recovered._connection.execute("SELECT version FROM schema_migrations").fetchall()
        assert [row[0] for row in versions] == [
            "0001_initial",
            "0002_refresh_scope_capability_key",
            "0003_observation_batch_digest",
            "0004_configured_system_identities",
            "0005_operational_event_recency",
            "0006_relationship_navigation",
            "0007_operational_event_filters",
            "0008_action_activity",
            "0009_facet_action_status",
        ]
        for table in ("refresh_credit", "refresh_intent_scopes", "adapter_action_scopes"):
            if table == "refresh_credit":
                columns = recovered._connection.execute(
                    "PRAGMA table_info(refresh_credit)"
                ).fetchall()
            elif table == "refresh_intent_scopes":
                columns = recovered._connection.execute(
                    "PRAGMA table_info(refresh_intent_scopes)"
                ).fetchall()
            else:
                columns = recovered._connection.execute(
                    "PRAGMA table_info(adapter_action_scopes)"
                ).fetchall()
            assert "capability_key" in {column[1] for column in columns}


def test_restart_read_helpers_expose_scopes_and_catalogs_root_without_secrets(tmp_path) -> None:
    path = tmp_path / "state.sqlite3"
    with SQLiteStore(path) as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local",
            profile="DEFAULT",
            workspace_root="/Shared",
            enabled_capability_keys=(
                "databricks.workspace.children.read",
                "databricks.uc.catalogs.read",
            ),
            now=NOW,
        )
        assert seeded.unity_catalog_root_object_id is not None
        assert seeded.unity_catalog_root_scope is not None
        repeated = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local",
            profile="DEFAULT",
            workspace_root="/Shared",
            enabled_capability_keys=(
                "databricks.workspace.children.read",
                "databricks.uc.catalogs.read",
            ),
            system_id=seeded.system.system_id,
            now=NOW,
        )
        assert repeated.unity_catalog_root_object_id == seeded.unity_catalog_root_object_id
        assert repeated.unity_catalog_root_scope == seeded.unity_catalog_root_scope
        assert len(store.list_configured_scopes(system_id=seeded.system.system_id)) == 2
        assert len(store.list_objects(system_id=seeded.system.system_id)) == 2
        assert {
            capability.capability_key
            for capability in store.list_capability_bindings(system_id=seeded.system.system_id)
        } == {"databricks.workspace.children.read", "databricks.uc.catalogs.read"}
        assert (
            store.list_connection_bindings(system_id=seeded.system.system_id)[0].secret_reference
            is None
        )

    with SQLiteStore(path) as reopened:
        scopes = reopened.list_configured_scopes()
        assert {scope.display_name for scope in scopes} == {"/Shared", "Unity Catalog catalogs"}
        assert reopened.get_object_sync(seeded.unity_catalog_root_object_id) is not None


def test_object_page_search_escapes_wildcards_and_bounds_results(tmp_path) -> None:
    with SQLiteStore(tmp_path / "state.sqlite3") as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/", now=NOW
        )
        for index, name in enumerate(("100% real", "under_score", "back\\slash", "ordinary")):
            store.upsert_object(
                RemoteObject(
                    object_id=uuid4(),
                    system_id=seeded.system.system_id,
                    object_type="file",
                    object_type_version="1",
                    source_kind="databricks.workspace.file",
                    external_key=f"workspace-id:{index}",
                    display_name=name,
                    presence=PresenceState.PRESENT,
                    first_seen_at=NOW,
                    last_seen_at=NOW,
                )
            )

        assert store.count_objects(query="%") == 1
        assert [
            item.display_name for item in store.list_objects_page(offset=0, limit=10, query="_")
        ] == ["under_score"]
        assert store.count_objects(query="\\") == 1
        assert len(store.list_objects_page(offset=1, limit=2)) == 2


def test_invalid_bootstrap_capability_does_not_leave_partial_state(tmp_path) -> None:
    with SQLiteStore(tmp_path / "state.sqlite3") as store:
        service = SystemBootstrapService(store)

        with pytest.raises(ValueError, match="unsupported Databricks capability"):
            service.configure_databricks_workspace(
                display_name="partial",
                profile="DEFAULT",
                workspace_root="/",
                enabled_capability_keys=("bad.read",),
            )

        assert store.list_systems() == ()


def test_bootstrap_settings_cannot_override_profile_or_workspace_root(tmp_path) -> None:
    with (
        SQLiteStore(tmp_path / "state.sqlite3") as store,
        pytest.raises(ValueError, match="reserved settings"),
    ):
        SystemBootstrapService(store).configure_databricks_workspace(
            display_name="reserved",
            profile="DEFAULT",
            workspace_root="/",
            non_secret_settings={"profile": "OTHER"},
        )


def test_failed_logical_action_anchors_cooldown_and_creates_one_event(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
        coverage=RefreshCoverage.FACET,
    )
    action = AdapterAction(
        action_id=uuid4(),
        correlation_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        capability_key="databricks.workspace.children.read",
        capability_version="1",
        target=scope.target,
        requested_scopes=(scope,),
    )
    run(store.enqueue(action))
    lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=NOW))
    assert lease is not None
    started_at = NOW
    run(
        store.mark_running(
            action_id=action.action_id, lease_id=lease.lease_id, started_at=started_at
        )
    )
    completion = ActionCompletion(
        action_id=action.action_id,
        outcome=ActionOutcome.FAILED,
        completed_at=NOW + timedelta(seconds=30),
        error_class=ErrorClass.CONNECTION_TIMEOUT,
        redacted_diagnostic="token=do-not-persist",
    )
    run(store.complete_action(completion, lease_id=lease.lease_id))
    with pytest.raises(ValueError, match="lease"):
        run(store.complete_action(completion, lease_id=lease.lease_id))

    policy = store.scope_policy_state(scope)
    assert policy.latest_targeted_action_started_at == started_at
    events = store.list_operational_events(alertable_only=True)
    assert len(events) == 1
    assert events[0].event_type == "refresh.action.failed"
    assert "do-not-persist" not in events[0].redacted_summary


def test_expired_running_lease_reopens_with_new_authority_and_preserves_start(tmp_path) -> None:
    path = tmp_path / "state.sqlite3"
    with SQLiteStore(path) as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
        )
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet="membership",
        )
        action = AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            capability_key="databricks.workspace.children.read",
            capability_version="1",
            target=scope.target,
            requested_scopes=(scope,),
        )
        run(store.enqueue(action))
        first_lease = run(store.lease_next(adapter_key="databricks", worker_id="first", now=NOW))
        assert first_lease is not None
        run(
            store.mark_running(
                action_id=action.action_id, lease_id=first_lease.lease_id, started_at=NOW
            )
        )

    with SQLiteStore(path) as reopened:
        recovery_at = NOW + timedelta(seconds=61)
        second_lease = run(
            reopened.lease_next(adapter_key="databricks", worker_id="second", now=recovery_at)
        )
        assert second_lease is not None
        assert second_lease.lease_id != first_lease.lease_id
        with pytest.raises(ValueError, match="lease"):
            run(
                reopened.record_attempt(
                    ActionAttempt(uuid4(), action.action_id, 1, recovery_at),
                    lease_id=first_lease.lease_id,
                )
            )
        with pytest.raises(ValueError, match="lease"):
            run(
                reopened.complete_action(
                    ActionCompletion(
                        action_id=action.action_id,
                        outcome=ActionOutcome.FAILED,
                        completed_at=recovery_at,
                        error_class=ErrorClass.CONNECTION_TIMEOUT,
                    ),
                    lease_id=first_lease.lease_id,
                )
            )
        assert reopened.get_stored_action(action.action_id).state.value == "leased"
        run(
            reopened.mark_running(
                action_id=action.action_id,
                lease_id=second_lease.lease_id,
                started_at=recovery_at,
            )
        )
        assert reopened.get_stored_action(action.action_id).started_at == NOW


def test_retry_wait_is_durable_and_next_lease_advances_attempt_ordinal(tmp_path) -> None:
    path = tmp_path / "state.sqlite3"
    with SQLiteStore(path) as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
        )
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet="membership",
        )
        action = AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            capability_key="databricks.workspace.children.read",
            capability_version="1",
            target=scope.target,
            requested_scopes=(scope,),
        )
        run(store.enqueue(action))
        first = run(store.lease_next(adapter_key="databricks", worker_id="first", now=NOW))
        assert first is not None and first.attempt_ordinal == 1
        run(store.mark_running(action_id=action.action_id, lease_id=first.lease_id, started_at=NOW))
        retry_at = NOW + timedelta(seconds=5)
        run(
            store.record_attempt(
                ActionAttempt(
                    uuid4(),
                    action.action_id,
                    1,
                    NOW,
                    NOW + timedelta(seconds=1),
                    ActionOutcome.FAILED,
                    ErrorClass.CONNECTION_TIMEOUT,
                    retry_at=retry_at,
                    redacted_diagnostic="token=do-not-persist",
                ),
                lease_id=first.lease_id,
            )
        )
        assert store.get_stored_action(action.action_id).state.value == "retry_wait"

    with SQLiteStore(path) as reopened:
        activity = reopened.get_action_activity(action.action_id)
        attempts = reopened.list_action_attempts(action.action_id)
        assert activity is not None and activity.action_id == action.action_id
        assert len(attempts) == 1
        assert attempts[0].ordinal == 1
        assert attempts[0].error_class == "connection_timeout"
        assert attempts[0].retry_at == retry_at
        assert "do-not-persist" not in (attempts[0].redacted_diagnostic or "")
        with pytest.raises(ValueError, match="attempt limit"):
            reopened.list_action_attempts(action.action_id, limit=101)
        assert (
            run(
                reopened.lease_next(
                    adapter_key="databricks",
                    worker_id="early",
                    now=retry_at - timedelta(microseconds=1),
                )
            )
            is None
        )
        second = run(
            reopened.lease_next(adapter_key="databricks", worker_id="second", now=retry_at)
        )
        assert second is not None
        assert second.action.action_id == action.action_id
        assert second.attempt_ordinal == 2
        run(
            reopened.mark_running(
                action_id=action.action_id,
                lease_id=second.lease_id,
                started_at=retry_at,
            )
        )
        assert reopened.get_stored_action(action.action_id).retry_at is None


def test_malformed_action_contract_terminalizes_once_and_queue_progresses(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )

    def action_for(*, facet: str, capability_key: str) -> AdapterAction:
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet=facet,
            capability_key=capability_key,
        )
        return AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            capability_key=capability_key,
            capability_version="1",
            target=scope.target,
            requested_scopes=(scope,),
        )

    poisoned = action_for(facet="membership", capability_key="databricks.workspace.children.read")
    healthy = action_for(facet="metadata", capability_key="databricks.workspace.metadata.read")
    run(store.enqueue(poisoned))
    run(store.enqueue(healthy))
    store._connection.execute(
        "UPDATE adapter_actions SET deadline = ?, record_created_at = ? WHERE action_id = ?",
        ("not-a-timestamp", "2026-08-24T12:00:00.000000Z", poisoned.action_id),
    )
    store._connection.execute(
        "UPDATE adapter_actions SET record_created_at = ? WHERE action_id = ?",
        ("2026-08-24T12:00:01.000000Z", healthy.action_id),
    )

    next_lease = run(store.lease_next(adapter_key="databricks", worker_id="first", now=NOW))
    assert next_lease is not None and next_lease.action.action_id == healthy.action_id
    poisoned_row = store._connection.execute(
        "SELECT state, error_class FROM adapter_actions WHERE action_id = ?",
        (poisoned.action_id,),
    ).fetchone()
    assert tuple(poisoned_row) == ("failed", "adapter_contract_mismatch")
    events = store.list_operational_events(alertable_only=True)
    assert len(events) == 1
    assert events[0].action_id == poisoned.action_id
    assert events[0].redacted_summary == "malformed_action_contract"

    assert len(store.list_operational_events(alertable_only=True)) == 1
    store._connection.execute(
        "UPDATE adapter_actions SET record_created_at = ? WHERE action_id = ?",
        ("2026-08-24T12:00:02.000000Z", poisoned.action_id),
    )
    latest_activity = store.list_latest_system_activity()
    assert len(latest_activity) == 1
    assert latest_activity[0].action_id == poisoned.action_id
    assert latest_activity[0].state == "failed"


def test_action_activity_pages_filters_and_uses_recency_indexes(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
        capability_key="databricks.workspace.children.read",
    )
    action_ids: list[str] = []
    for index, state in enumerate(("failed", "succeeded", "failed")):
        action = AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            capability_key="databricks.workspace.children.read",
            capability_version="1",
            target=scope.target,
            requested_scopes=(scope,),
        )
        run(store.enqueue(action))
        action_ids.append(action.action_id)
        occurred_at = NOW + timedelta(minutes=index)
        store._connection.execute(
            """
            UPDATE adapter_actions
            SET state = ?, record_created_at = ?, completed_at = ?,
                error_class = ?, redacted_diagnostic = ?
            WHERE action_id = ?
            """,
            (
                state,
                occurred_at.isoformat().replace("+00:00", "Z"),
                occurred_at.isoformat().replace("+00:00", "Z"),
                "connection_timeout" if state == "failed" else None,
                "redacted failure" if state == "failed" else None,
                action.action_id,
            ),
        )

    assert store.count_action_activity() == 3
    assert store.count_action_activity(state="failed") == 2
    assert store.count_action_activity(system_id=seeded.system.system_id) == 3
    assert store.count_action_activity(system_id=seeded.system.system_id, state="failed") == 2
    assert store.count_action_activity(action_id=action_ids[1]) == 1
    assert [item.action_id for item in store.list_action_activity_page(offset=0, limit=2)] == [
        action_ids[2],
        action_ids[1],
    ]
    assert all(
        item.state == "failed"
        for item in store.list_action_activity_page(offset=0, limit=10, state="failed")
    )
    assert (
        store.list_action_activity_page(offset=0, limit=10, action_id=action_ids[1])[0].state
        == "succeeded"
    )
    facet_actions = store.list_latest_facet_actions((seeded.workspace_root_object_id,))
    assert len(facet_actions) == 1
    assert facet_actions[0].object_id == seeded.workspace_root_object_id
    assert facet_actions[0].facet == "membership"
    assert facet_actions[0].action_id == action_ids[2]
    assert facet_actions[0].state == "failed"
    assert facet_actions[0].redacted_diagnostic == "redacted failure"
    store._connection.execute(
        "UPDATE adapter_actions SET state = 'running', completed_at = NULL WHERE action_id = ?",
        (action_ids[2],),
    )
    refreshing_actions = store.list_latest_facet_actions((seeded.workspace_root_object_id,))
    assert len(refreshing_actions) == 1
    assert refreshing_actions[0].state == "running"
    with pytest.raises(ValueError, match="100 object IDs"):
        store.list_latest_facet_actions(tuple(str(uuid4()) for _ in range(101)))

    with pytest.raises(ValueError, match="offset"):
        store.list_action_activity_page(offset=-1, limit=10)
    with pytest.raises(ValueError, match="limit"):
        store.list_action_activity_page(offset=0, limit=101)
    with pytest.raises(ValueError, match="state"):
        store.count_action_activity(state="unknown")
    with pytest.raises(ValueError, match="combined"):
        store.count_action_activity(action_id=action_ids[0], state="failed")

    plans = {
        "ix_adapter_actions_recency": (
            "SELECT * FROM adapter_actions "
            "ORDER BY record_created_at DESC, action_id LIMIT ? OFFSET ?",
            (10, 0),
        ),
        "ix_adapter_actions_state_recency": (
            "SELECT * FROM adapter_actions WHERE state = ? "
            "ORDER BY record_created_at DESC, action_id LIMIT ? OFFSET ?",
            ("failed", 10, 0),
        ),
        "ix_adapter_actions_system_recency": (
            "SELECT * FROM adapter_actions WHERE system_id = ? "
            "ORDER BY record_created_at DESC, action_id LIMIT ? OFFSET ?",
            (seeded.system.system_id, 10, 0),
        ),
        "ix_adapter_actions_system_state_recency": (
            "SELECT * FROM adapter_actions WHERE system_id = ? AND state = ? "
            "ORDER BY record_created_at DESC, action_id LIMIT ? OFFSET ?",
            (seeded.system.system_id, "failed", 10, 0),
        ),
    }
    for expected_index, (statement, parameters) in plans.items():
        plan = store._connection.execute(
            f"EXPLAIN QUERY PLAN {statement}",
            parameters,
        ).fetchall()
        assert any(expected_index in row[3] for row in plan)
        assert not any("TEMP B-TREE" in row[3] for row in plan)

    store._connection.executemany(
        """
        INSERT INTO action_attempts (attempt_id, action_id, ordinal, started_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            (
                str(uuid4()),
                action_ids[0],
                ordinal,
                (NOW + timedelta(seconds=ordinal)).isoformat().replace("+00:00", "Z"),
            )
            for ordinal in range(1, 102)
        ),
    )
    latest_attempts = store.list_action_attempts(action_ids[0], limit=100)
    assert store.count_action_attempts(action_ids[0]) == 101
    assert [attempt.ordinal for attempt in latest_attempts] == list(range(2, 102))


def test_runtime_failure_is_bounded_and_diagnostics_redact_json_and_home_paths(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    event = store.record_runtime_failure(
        event_type="queue.coordinator.failed",
        summary="Coordinator stopped unexpectedly",
        occurred_at=NOW,
    )
    assert event.alertable
    assert event == store.record_runtime_failure(
        event_type="queue.coordinator.failed",
        summary="Coordinator stopped unexpectedly",
        occurred_at=NOW,
    )
    later = store.record_runtime_failure(
        event_type="queue.adapter_worker.failed",
        summary="Worker stopped unexpectedly",
        occurred_at=NOW + timedelta(seconds=1),
    )
    store._connection.execute(
        """
        INSERT INTO operational_events (
            event_id, idempotency_key, event_type, severity, alertable,
            redacted_summary, occurred_at
        ) VALUES (?, ?, ?, ?, 0, ?, ?)
        """,
        (
            str(uuid4()),
            str(uuid4()),
            "refresh.expected",
            "info",
            "Expected local disposition",
            NOW.isoformat(),
        ),
    )
    assert len(store.list_operational_events(alertable_only=True)) == 2
    assert store.list_operational_events(alertable_only=True, limit=1) == (later,)
    assert store.count_alertable_events() == 2
    assert store.count_alertable_events(event_type="queue.coordinator.failed") == 1
    assert store.count_alertable_events(severity="error") == 2
    assert len(store.list_alertable_events_page(offset=0, limit=10)) == 2
    with pytest.raises(ValueError, match="offset"):
        store.list_alertable_events_page(offset=-1, limit=10)
    with pytest.raises(ValueError, match="limit"):
        store.list_alertable_events_page(offset=0, limit=101)
    with pytest.raises(ValueError, match="event_type"):
        store.list_alertable_events_page(offset=0, limit=10, event_type="BAD TYPE")
    with pytest.raises(ValueError, match="severity"):
        store.list_alertable_events_page(offset=0, limit=10, severity="debug")
    query_plan = store._connection.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT * FROM operational_events
        WHERE alertable = 1
        ORDER BY occurred_at DESC, event_id
        LIMIT 10
        """
    ).fetchall()
    assert any("ix_operational_events_alertable_recency" in row[3] for row in query_plan)
    combined_plan = store._connection.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT * FROM operational_events
        WHERE alertable = 1 AND event_type = ? AND severity = ?
        ORDER BY occurred_at DESC, event_id
        LIMIT 50
        """,
        ("queue.coordinator.failed", "error"),
    ).fetchall()
    type_plan = store._connection.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT * FROM operational_events
        WHERE alertable = 1 AND event_type = ?
        ORDER BY occurred_at DESC, event_id
        LIMIT 50
        """,
        ("queue.coordinator.failed",),
    ).fetchall()
    severity_plan = store._connection.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT * FROM operational_events
        WHERE alertable = 1 AND severity = ?
        ORDER BY occurred_at DESC, event_id
        LIMIT 50
        """,
        ("error",),
    ).fetchall()
    assert any(
        "ix_operational_events_alertable_type_severity_recency" in row[3] for row in combined_plan
    )
    assert any("ix_operational_events_alertable_type_recency" in row[3] for row in type_plan)
    assert any(
        "ix_operational_events_alertable_severity_recency" in row[3] for row in severity_plan
    )
    assert not any("TEMP B-TREE" in row[3] for row in (*combined_plan, *type_plan, *severity_plan))
    with pytest.raises(ValueError, match="registered"):
        store.record_runtime_failure(
            event_type="refresh.action.failed",
            summary="Coordinator stopped unexpectedly",
            occurred_at=NOW,
        )
    with pytest.raises(ValueError, match="safe message"):
        store.record_runtime_failure(
            event_type="queue.coordinator.failed",
            summary='{"token":"raw"}',
            occurred_at=NOW,
        )
    redacted = _redact(
        'payload {"token":"secret-value"} C:\\Users\\alice\\.databrickscfg /home/alice/.config'
    )
    assert "secret-value" not in redacted
    assert "alice" not in redacted


def test_workspace_root_uses_normalized_external_identity(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW,
        received_at=NOW,
        facet_observations=(
            FacetObservation(
                observation_id=uuid4(),
                target=ObjectLocator(
                    object_type="folder",
                    source_kind="databricks.workspace.folder",
                    external_key="workspace:/Shared",
                    display_name="Shared",
                ),
                facet="metadata",
                facet_version="1",
                update_mode=UpdateMode.SNAPSHOT,
                field_coverage=FieldCoverage.COMPLETE,
                payload={"path": "/Shared"},
            ),
        ),
    )
    assert run(store.ingest(batch)).status.value == "accepted"
    assert len(store.list_objects(system_id=seeded.system.system_id)) == 1
    assert store.get_facet_sync(seeded.workspace_root_object_id, "metadata") is not None


def test_legacy_pre_digest_batch_redelivery_backfills_only_exact_match(tmp_path) -> None:
    path = tmp_path / "state.sqlite3"
    with SQLiteStore(path) as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
        )
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet="membership",
        )
        action = AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            capability_key="databricks.workspace.children.read",
            capability_version="1",
            target=scope.target,
            requested_scopes=(scope,),
        )
        run(store.enqueue(action))
        lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=NOW))
        assert lease is not None
        run(store.mark_running(action_id=action.action_id, lease_id=lease.lease_id, started_at=NOW))
        observation = FacetObservation(
            observation_id=uuid4(),
            target=ObjectLocator(object_type="folder", object_id=seeded.workspace_root_object_id),
            facet="membership",
            facet_version="1",
            update_mode=UpdateMode.PATCH,
            field_coverage=FieldCoverage.PARTIAL,
            payload={"member_count": 1},
            field_mask=("member_count",),
        )
        batch = ObservationBatch(
            batch_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            action_id=action.action_id,
            observed_at=NOW,
            received_at=NOW,
            facet_observations=(observation,),
        )
        assert run(store.ingest(batch)).status.value == "accepted"
        store._connection.execute(
            "UPDATE observation_batches SET batch_digest = '' WHERE batch_id = ?",
            (batch.batch_id,),
        )

    with SQLiteStore(path) as reopened:
        exact = run(reopened.ingest(batch))
        assert exact.status.value == "duplicate"
        digest = reopened._connection.execute(
            "SELECT batch_digest FROM observation_batches WHERE batch_id = ?", (batch.batch_id,)
        ).fetchone()[0]
        assert digest
        recovered = run(
            reopened.lease_next(
                adapter_key="databricks",
                worker_id="recovered",
                now=NOW + timedelta(seconds=61),
            )
        )
        assert recovered is not None and recovered.action.action_id == action.action_id
        conflicting = ObservationBatch(
            batch_id=batch.batch_id,
            system_id=batch.system_id,
            connection_binding_id=batch.connection_binding_id,
            adapter_key=batch.adapter_key,
            adapter_version=batch.adapter_version,
            action_id=batch.action_id,
            observed_at=batch.observed_at,
            received_at=batch.received_at,
            facet_observations=(
                FacetObservation(
                    observation_id=observation.observation_id,
                    target=observation.target,
                    facet="membership",
                    facet_version="1",
                    update_mode=UpdateMode.PATCH,
                    field_coverage=FieldCoverage.PARTIAL,
                    payload={"member_count": 2},
                    field_mask=("member_count",),
                ),
            ),
        )
        assert run(reopened.ingest(conflicting)).status.value == "rejected"
