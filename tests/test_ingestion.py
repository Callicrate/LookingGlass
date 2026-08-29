import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from async_api_view.application import SystemBootstrapService
from async_api_view.contracts import (
    AbsenceAuthority,
    AdapterAction,
    CollectionCoverage,
    CoverageDeclaration,
    FacetObservation,
    FieldCoverage,
    KnowledgeState,
    ObjectLocator,
    ObservationBatch,
    PresenceState,
    RefreshCoverage,
    RefreshScope,
    RelationshipObservation,
    RemoteObject,
    TargetKind,
    TargetRef,
    UpdateMode,
)
from async_api_view.ingestion import SQLiteObservationIngestor
from async_api_view.storage import SQLiteStore

NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)


def run(awaitable):
    return asyncio.run(awaitable)


def _membership_authority(system_id: str, configured_scope_id: str) -> RefreshScope:
    return RefreshScope(
        system_id=system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, configured_scope_id),
        object_type="folder",
        facet="membership",
        capability_key="databricks.workspace.children.read",
        coverage=RefreshCoverage.COLLECTION_MEMBERS,
    )


def _membership_relationship(
    authority: RefreshScope,
    subject_id: str,
    target: ObjectLocator,
) -> RelationshipObservation:
    return RelationshipObservation(
        observation_id=uuid4(),
        subject=ObjectLocator(object_type="folder", object_id=subject_id),
        predicate="contains",
        object=target,
        presence=PresenceState.PRESENT,
        authorized_by=(authority,),
    )


def _rewind_projection_order_migration(store: SQLiteStore) -> None:
    for table_name, column_name in (
        ("remote_objects", "last_seen_received_at"),
        ("facets", "received_at"),
        ("relationships", "received_at"),
        ("relationship_coverage_watermarks", "received_at"),
        ("refresh_credit", "received_at"),
    ):
        store._connection.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_name}")


def test_partial_listing_preserves_members_and_observation_replay_is_idempotent(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    parent = seeded.workspace_root_object_id
    membership = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
        capability_key="databricks.workspace.children.read",
        coverage=RefreshCoverage.FACET,
    )
    child = ObjectLocator(
        object_type="file",
        source_kind="databricks.workspace.file",
        external_key="/Shared/report.py",
        display_name="report.py",
    )
    complete = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW,
        received_at=NOW,
        relationship_observations=(
            RelationshipObservation(
                observation_id=uuid4(),
                subject=ObjectLocator(object_type="folder", object_id=parent),
                predicate="contains",
                object=child,
                presence=PresenceState.PRESENT,
                authorized_by=(membership,),
            ),
        ),
        coverage=(
            CoverageDeclaration(
                scope=membership,
                completeness=CollectionCoverage.COMPLETE,
                absence_authority=(AbsenceAuthority.RELATIONSHIP,),
            ),
        ),
    )
    ingestor = SQLiteObservationIngestor(store)
    assert run(ingestor.ingest(complete)).status.value == "accepted"

    partial = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW + timedelta(minutes=1),
        received_at=NOW + timedelta(minutes=1),
        coverage=(CoverageDeclaration(scope=membership, completeness=CollectionCoverage.PARTIAL),),
    )
    assert run(ingestor.ingest(partial)).status.value == "partial"
    assert store.list_relationships_sync(parent)[0].presence is PresenceState.PRESENT
    assert run(ingestor.ingest(partial)).status.value == "duplicate"


def test_post_construction_json_cycle_is_rejected_without_durable_residue(tmp_path) -> None:
    with SQLiteStore(tmp_path / "state.sqlite3") as store:
        observation = FacetObservation(
            observation_id=uuid4(),
            target=ObjectLocator(object_type="file", object_id=uuid4()),
            facet="metadata",
            facet_version="1",
            update_mode=UpdateMode.SNAPSHOT,
            field_coverage=FieldCoverage.COMPLETE,
            payload={"name": "safe"},
        )
        observation.payload["cycle"] = observation.payload
        batch = ObservationBatch(
            batch_id=uuid4(),
            system_id=uuid4(),
            connection_binding_id=uuid4(),
            adapter_key="databricks",
            adapter_version="1",
            observed_at=NOW,
            received_at=NOW,
            facet_observations=(observation,),
        )

        result = run(SQLiteObservationIngestor(store).ingest(batch))

        assert result.status.value == "rejected"
        for table in (
            "observation_batches",
            "observation_journal",
            "ingestion_issues",
            "remote_objects",
        ):
            assert store._connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0


def test_complete_omission_never_overwrites_a_newer_relationship(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    parent = seeded.workspace_root_object_id
    membership = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
        capability_key="databricks.workspace.children.read",
        coverage=RefreshCoverage.FACET,
    )
    declaration = CoverageDeclaration(
        scope=membership,
        completeness=CollectionCoverage.COMPLETE,
        absence_authority=(AbsenceAuthority.RELATIONSHIP,),
    )
    child = ObjectLocator(
        object_type="file",
        source_kind="databricks.workspace.file",
        external_key="/Shared/report.py",
        display_name="report.py",
    )
    newer_at = NOW + timedelta(minutes=10)
    present = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=newer_at,
        received_at=newer_at,
        relationship_observations=(
            RelationshipObservation(
                observation_id=uuid4(),
                subject=ObjectLocator(object_type="folder", object_id=parent),
                predicate="contains",
                object=child,
                presence=PresenceState.PRESENT,
                authorized_by=(membership,),
            ),
        ),
        coverage=(declaration,),
    )
    delayed_older_omission = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW,
        received_at=NOW + timedelta(minutes=20),
        coverage=(declaration,),
    )
    genuinely_newer_omission = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW + timedelta(minutes=30),
        received_at=NOW + timedelta(minutes=30),
        coverage=(declaration,),
    )

    assert run(store.ingest(present)).status.value == "accepted"
    assert run(store.ingest(delayed_older_omission)).status.value == "accepted"
    relationship = store.list_relationships_sync(parent)[0]
    assert relationship.presence is PresenceState.PRESENT
    assert relationship.observed_at == newer_at
    assert store.count_related_objects_sync(parent) == 1

    assert run(store.ingest(genuinely_newer_omission)).status.value == "accepted"
    relationship = store.list_relationships_sync(parent)[0]
    assert relationship.presence is PresenceState.ABSENT
    assert relationship.observed_at == genuinely_newer_omission.observed_at
    assert store.count_related_objects_sync(parent) == 0


def test_newer_complete_membership_suppresses_delayed_unknown_edge(tmp_path) -> None:
    path = tmp_path / "state.sqlite3"
    with SQLiteStore(path) as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
        )
        parent = seeded.workspace_root_object_id
        membership = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet="membership",
            capability_key="databricks.workspace.children.read",
            coverage=RefreshCoverage.FACET,
        )
        boundary_at = NOW + timedelta(minutes=10)
        empty_boundary = ObservationBatch(
            batch_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            observed_at=boundary_at,
            received_at=boundary_at,
            coverage=(
                CoverageDeclaration(
                    scope=membership,
                    completeness=CollectionCoverage.COMPLETE,
                    absence_authority=(AbsenceAuthority.RELATIONSHIP,),
                ),
            ),
        )
        assert run(store.ingest(empty_boundary)).status.value == "accepted"
        _rewind_projection_order_migration(store)
        store._connection.execute("DROP TABLE relationship_coverage_watermarks")
        store._connection.execute(
            "DELETE FROM schema_migrations WHERE version IN (?, ?)",
            (
                "0015_relationship_coverage_watermarks",
                "0016_projection_received_order",
            ),
        )

    child = ObjectLocator(
        object_type="file",
        source_kind="databricks.workspace.file",
        external_key="/Shared/delayed.py",
        display_name="delayed.py",
    )
    delayed = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW,
        received_at=boundary_at + timedelta(minutes=1),
        relationship_observations=(
            RelationshipObservation(
                observation_id=uuid4(),
                subject=ObjectLocator(object_type="folder", object_id=parent),
                predicate="contains",
                object=child,
                presence=PresenceState.PRESENT,
                authorized_by=(membership,),
            ),
        ),
    )
    newer = replace(
        delayed,
        batch_id=uuid4(),
        observed_at=boundary_at + timedelta(minutes=1),
        received_at=boundary_at + timedelta(minutes=1),
        relationship_observations=(
            replace(delayed.relationship_observations[0], observation_id=uuid4()),
        ),
    )

    with SQLiteStore(path) as reopened:
        assert run(reopened.ingest(delayed)).status.value == "accepted"
        relationship = reopened.list_relationships_sync(parent)[0]
        assert relationship.presence is PresenceState.ABSENT
        assert relationship.observed_at == boundary_at
        assert reopened.count_related_objects_sync(parent) == 0

        assert run(reopened.ingest(newer)).status.value == "accepted"
        relationship = reopened.list_relationships_sync(parent)[0]
        assert relationship.presence is PresenceState.PRESENT
        assert relationship.observed_at == newer.observed_at
        assert reopened.count_related_objects_sync(parent) == 1


def test_migration_reconciles_preexisting_stale_edge_and_skips_unknown_credit(
    tmp_path,
) -> None:
    path = tmp_path / "legacy-stale.sqlite3"
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
        assert seeded.unity_catalog_root_scope is not None
        parent = seeded.workspace_root_object_id
        membership = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet="membership",
            capability_key="databricks.workspace.children.read",
            coverage=RefreshCoverage.FACET,
        )
        child = ObjectLocator(
            object_type="file",
            source_kind="databricks.workspace.file",
            external_key="/Shared/preexisting.py",
            display_name="preexisting.py",
        )
        edge = RelationshipObservation(
            observation_id=uuid4(),
            subject=ObjectLocator(object_type="folder", object_id=parent),
            predicate="contains",
            object=child,
            presence=PresenceState.PRESENT,
            authorized_by=(membership,),
        )
        delayed = ObservationBatch(
            batch_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            observed_at=NOW,
            received_at=NOW,
            relationship_observations=(edge,),
        )
        boundary_at = NOW + timedelta(minutes=10)
        boundary = ObservationBatch(
            batch_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            observed_at=boundary_at,
            received_at=boundary_at,
            coverage=(
                CoverageDeclaration(
                    scope=membership,
                    completeness=CollectionCoverage.COMPLETE,
                    absence_authority=(AbsenceAuthority.RELATIONSHIP,),
                ),
            ),
        )
        assert run(store.ingest(delayed)).status.value == "accepted"
        assert run(store.ingest(boundary)).status.value == "accepted"

        store._connection.execute(
            """
            UPDATE relationships
            SET presence = 'present', observed_at = ?, supporting_observation_id = ?
            WHERE subject_id = ? AND predicate = 'contains'
            """,
            (
                NOW.isoformat(timespec="microseconds").replace("+00:00", "Z"),
                edge.observation_id,
                parent,
            ),
        )
        store._connection.execute(
            """
            INSERT INTO refresh_credit (
                credit_id, observation_id, system_id, target_kind, target_id,
                object_type, facet, coverage, field_mask_json, observed_at, capability_key
            )
            SELECT ?, observation_id, system_id, 'configured_scope', ?, object_type, facet,
                   coverage, field_mask_json, observed_at, capability_key
            FROM refresh_credit
            WHERE observation_id = ? AND target_kind = 'configured_scope' AND target_id = ?
            """,
            (
                str(uuid4()),
                seeded.unity_catalog_root_scope.scope_id,
                store._coverage_observation_id(boundary.batch_id, membership),
                seeded.workspace_root_scope.scope_id,
            ),
        )
        store._connection.execute(
            """
            INSERT INTO refresh_credit (
                credit_id, observation_id, system_id, target_kind, target_id,
                object_type, facet, coverage, field_mask_json, observed_at, capability_key
            )
            SELECT ?, observation_id, system_id, 'object', ?, object_type, facet,
                   coverage, field_mask_json, observed_at, capability_key
            FROM refresh_credit
            WHERE observation_id = ? AND target_kind = 'configured_scope' AND target_id = ?
            """,
            (
                str(uuid4()),
                str(uuid4()),
                store._coverage_observation_id(boundary.batch_id, membership),
                seeded.workspace_root_scope.scope_id,
            ),
        )
        _rewind_projection_order_migration(store)
        store._connection.execute("DROP TABLE relationship_coverage_watermarks")
        store._connection.execute(
            "DELETE FROM schema_migrations WHERE version IN (?, ?)",
            (
                "0015_relationship_coverage_watermarks",
                "0016_projection_received_order",
            ),
        )

    with SQLiteStore(path) as migrated:
        relationship = migrated.list_relationships_sync(parent)[0]
        assert relationship.presence is PresenceState.ABSENT
        assert relationship.observed_at == boundary_at
        assert migrated.count_related_objects_sync(parent) == 0
        assert (
            migrated._connection.execute(
                "SELECT COUNT(*) FROM relationship_coverage_watermarks"
            ).fetchone()[0]
            == 1
        )
        assert migrated._connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_coverage_rejects_unknown_and_cross_system_targets(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "invalid-coverage-target.sqlite3")
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
    other = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="other", profile="OTHER", workspace_root="/Other", now=NOW
    )
    assert seeded.unity_catalog_root_scope is not None
    unity_catalog_object_id = seeded.unity_catalog_root_scope.object_id
    assert unity_catalog_object_id is not None

    def assert_rejected(target: TargetRef) -> None:
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=target,
            object_type="folder",
            facet="membership",
            capability_key="databricks.workspace.children.read",
            coverage=RefreshCoverage.FACET,
        )
        batch = ObservationBatch(
            batch_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            observed_at=NOW,
            received_at=NOW,
            coverage=(
                CoverageDeclaration(
                    scope=scope,
                    completeness=CollectionCoverage.COMPLETE,
                    absence_authority=(AbsenceAuthority.RELATIONSHIP,),
                ),
            ),
        )
        assert run(store.ingest(batch)).status.value == "rejected"
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM observation_batches WHERE batch_id = ?",
                (batch.batch_id,),
            ).fetchone()[0]
            == 0
        )

    targets = (
        TargetRef(TargetKind.OBJECT, uuid4()),
        TargetRef(TargetKind.OBJECT, other.workspace_root_object_id),
        TargetRef(TargetKind.CONFIGURED_SCOPE, other.workspace_root_scope.scope_id),
        TargetRef(TargetKind.OBJECT, unity_catalog_object_id),
        TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.unity_catalog_root_scope.scope_id),
    )
    for target in targets:
        assert_rejected(target)

    store._connection.execute(
        "UPDATE configured_scopes SET object_id = ? WHERE scope_id = ?",
        (other.workspace_root_object_id, seeded.workspace_root_scope.scope_id),
    )
    assert_rejected(TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id))
    store._connection.execute(
        "UPDATE configured_scopes SET object_id = NULL WHERE scope_id = ?",
        (seeded.workspace_root_scope.scope_id,),
    )
    assert_rejected(TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id))


def test_unscoped_incidental_fact_has_no_identity_or_journal_authority(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "unscoped-incidental.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    observation = FacetObservation(
        observation_id=uuid4(),
        target=ObjectLocator(
            object_type="generic_object",
            source_kind="databricks.uc.catalog",
            external_key="catalog:forged",
            display_name="forged",
        ),
        facet="attributes",
        facet_version="1",
        update_mode=UpdateMode.SNAPSHOT,
        field_coverage=FieldCoverage.COMPLETE,
        payload={"name": "forged"},
    )
    authority = _membership_authority(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    wrong_source = FacetObservation(
        observation_id=uuid4(),
        target=ObjectLocator(
            object_type="file",
            source_kind="server.file",
            external_key="/Shared/foreign.py",
            display_name="foreign.py",
        ),
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.SNAPSHOT,
        field_coverage=FieldCoverage.COMPLETE,
        payload={"name": "foreign.py"},
        authorized_by=(authority,),
    )
    off_collection = FacetObservation(
        observation_id=uuid4(),
        target=ObjectLocator(
            object_type="file",
            source_kind="databricks.workspace.file",
            external_key="workspace:/Elsewhere/forged.py",
            display_name="forged.py",
        ),
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.SNAPSHOT,
        field_coverage=FieldCoverage.COMPLETE,
        payload={"name": "forged.py"},
        authorized_by=(authority,),
    )
    wrong_predicate = RelationshipObservation(
        observation_id=uuid4(),
        subject=ObjectLocator(object_type="folder", object_id=seeded.workspace_root_object_id),
        predicate="owns",
        object=ObjectLocator(
            object_type="file",
            source_kind="databricks.workspace.file",
            external_key="workspace:/Shared/forged.py",
            display_name="forged.py",
        ),
        presence=PresenceState.PRESENT,
        authorized_by=(authority,),
    )
    batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW,
        received_at=NOW,
        facet_observations=(observation, wrong_source, off_collection),
        relationship_observations=(wrong_predicate,),
    )

    result = run(store.ingest(batch))

    assert result.status.value == "partial"
    assert result.accepted_observation_ids == ()
    assert result.issue_count == 4
    assert {item.external_key for item in store.list_objects()} == {"workspace:/Shared"}
    assert (
        store._connection.execute(
            "SELECT COUNT(*) FROM observation_journal WHERE batch_id = ?",
            (batch.batch_id,),
        ).fetchone()[0]
        == 0
    )


def test_action_scope_rejects_wrong_facet_target_and_relationship_items(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "action-item-authority.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local",
        profile="DEFAULT",
        workspace_root="/Shared",
        enabled_capability_keys=("databricks.workspace.metadata.read",),
        now=NOW,
    )
    metadata_scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.OBJECT, seeded.workspace_root_object_id),
        object_type="folder",
        facet="metadata",
        capability_key="databricks.workspace.metadata.read",
    )
    action = AdapterAction(
        action_id=uuid4(),
        correlation_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        capability_key="databricks.workspace.metadata.read",
        capability_version="1",
        target=metadata_scope.target,
        requested_scopes=(metadata_scope,),
    )
    run(store.enqueue(action))
    action_now = datetime.now(UTC)
    lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=action_now))
    assert lease is not None
    run(
        store.mark_running(
            action_id=action.action_id,
            lease_id=lease.lease_id,
            started_at=action_now,
        )
    )
    wrong_facet = FacetObservation(
        observation_id=uuid4(),
        target=ObjectLocator(
            object_type="generic_object",
            source_kind="databricks.uc.catalog",
            external_key="catalog:forged",
            display_name="forged",
        ),
        facet="attributes",
        facet_version="1",
        update_mode=UpdateMode.SNAPSHOT,
        field_coverage=FieldCoverage.COMPLETE,
        payload={"name": "forged"},
        authorized_by=(metadata_scope,),
    )
    wrong_target = FacetObservation(
        observation_id=uuid4(),
        target=ObjectLocator(
            object_type="folder",
            source_kind="databricks.workspace.folder",
            external_key="workspace:/Elsewhere",
            display_name="Elsewhere",
        ),
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.SNAPSHOT,
        field_coverage=FieldCoverage.COMPLETE,
        payload={"path": "/Elsewhere"},
        authorized_by=(metadata_scope,),
    )
    wrong_relationship = RelationshipObservation(
        observation_id=uuid4(),
        subject=ObjectLocator(object_type="folder", object_id=seeded.workspace_root_object_id),
        predicate="contains",
        object=ObjectLocator(
            object_type="file",
            source_kind="databricks.workspace.file",
            external_key="workspace:/Shared/forged.py",
            display_name="forged.py",
        ),
        presence=PresenceState.PRESENT,
        authorized_by=(metadata_scope,),
    )
    batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        action_id=action.action_id,
        observed_at=action_now,
        received_at=action_now,
        facet_observations=(wrong_facet, wrong_target),
        relationship_observations=(wrong_relationship,),
    )

    result = run(store.ingest(batch, lease_id=lease.lease_id))

    assert result.status.value == "partial"
    assert result.accepted_observation_ids == ()
    assert result.issue_count == 3
    assert {item.external_key for item in store.list_objects()} == {"workspace:/Shared"}
    assert (
        store._connection.execute(
            "SELECT COUNT(*) FROM observation_journal WHERE batch_id = ?",
            (batch.batch_id,),
        ).fetchone()[0]
        == 0
    )


def test_collection_action_rejects_unlinked_off_collection_fact(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "action-collection-authority.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = _membership_authority(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
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
    action_now = datetime.now(UTC)
    lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=action_now))
    assert lease is not None
    run(
        store.mark_running(
            action_id=action.action_id,
            lease_id=lease.lease_id,
            started_at=action_now,
        )
    )
    forged = FacetObservation(
        observation_id=uuid4(),
        target=ObjectLocator(
            object_type="file",
            source_kind="databricks.workspace.file",
            external_key="workspace:/Elsewhere/forged.py",
            display_name="forged.py",
        ),
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.SNAPSHOT,
        field_coverage=FieldCoverage.COMPLETE,
        payload={"name": "forged.py"},
        authorized_by=(scope,),
    )
    batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        action_id=action.action_id,
        observed_at=action_now,
        received_at=action_now,
        facet_observations=(forged,),
    )

    result = run(store.ingest(batch, lease_id=lease.lease_id))

    assert result.status.value == "partial"
    assert result.accepted_observation_ids == ()
    assert result.issue_count == 1
    assert {item.external_key for item in store.list_objects()} == {"workspace:/Shared"}


def test_collection_authority_queries_scale_linearly_with_batch_size(tmp_path) -> None:
    def select_count(item_count: int) -> int:
        store = SQLiteStore(tmp_path / f"authority-{item_count}.sqlite3")
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local",
            profile="DEFAULT",
            workspace_root="/Shared",
            now=NOW,
        )
        authority = _membership_authority(
            seeded.system.system_id,
            seeded.workspace_root_scope.scope_id,
        )
        targets = tuple(
            ObjectLocator(
                object_type="file",
                source_kind="databricks.workspace.file",
                external_key=f"workspace:/Shared/item-{index}.py",
                display_name=f"item-{index}.py",
            )
            for index in range(item_count)
        )
        batch = ObservationBatch(
            batch_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            observed_at=NOW,
            received_at=NOW,
            facet_observations=tuple(
                FacetObservation(
                    observation_id=uuid4(),
                    target=target,
                    facet="metadata",
                    facet_version="1",
                    update_mode=UpdateMode.SNAPSHOT,
                    field_coverage=FieldCoverage.COMPLETE,
                    payload={"name": target.display_name},
                    authorized_by=(authority,),
                )
                for target in targets
            ),
            relationship_observations=tuple(
                _membership_relationship(
                    authority,
                    seeded.workspace_root_object_id,
                    target,
                )
                for target in targets
            ),
        )
        selects = 0

        def trace(statement: str) -> None:
            nonlocal selects
            if statement.lstrip().upper().startswith("SELECT"):
                selects += 1

        store._connection.set_trace_callback(trace)
        try:
            result = run(store.ingest(batch))
        finally:
            store._connection.set_trace_callback(None)
        assert result.status.value == "accepted"
        assert len(result.accepted_observation_ids) == item_count * 2
        return selects

    small_count = select_count(100)
    large_count = select_count(200)

    assert large_count <= small_count * 2 + 50


def test_collection_authority_preserves_facet_first_identity_projection(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "facet-first-authority.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local",
        profile="DEFAULT",
        workspace_root="/Shared",
        now=NOW,
    )
    authority = _membership_authority(
        seeded.system.system_id,
        seeded.workspace_root_scope.scope_id,
    )
    facet_target = ObjectLocator(
        object_type="file",
        source_kind="databricks.workspace.file",
        external_key="workspace:/Shared/ordered.py",
        display_name="facet-name.py",
    )
    relationship_target = replace(facet_target, display_name="relationship-name.py")
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
                target=facet_target,
                facet="metadata",
                facet_version="1",
                update_mode=UpdateMode.SNAPSHOT,
                field_coverage=FieldCoverage.COMPLETE,
                payload={"name": "facet-name.py"},
                authorized_by=(authority,),
            ),
        ),
        relationship_observations=(
            _membership_relationship(
                authority,
                seeded.workspace_root_object_id,
                relationship_target,
            ),
        ),
    )

    assert run(store.ingest(batch)).status.value == "accepted"
    projected = next(
        item for item in store.list_objects() if item.external_key == "workspace:/Shared/ordered.py"
    )
    assert projected.display_name == "facet-name.py"


def test_collection_link_authority_cannot_cross_exact_scope_identity(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "exact-scope-authority.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local",
        profile="DEFAULT",
        workspace_root="/Shared",
        now=NOW,
    )
    relationship_scope = replace(
        _membership_authority(
            seeded.system.system_id,
            seeded.workspace_root_scope.scope_id,
        ),
        field_mask=("relationship-scope",),
    )
    facet_scope = replace(relationship_scope, field_mask=("facet-scope",))
    target = ObjectLocator(
        object_type="file",
        source_kind="databricks.workspace.file",
        external_key="workspace:/Shared/scope-bound.py",
        display_name="scope-bound.py",
    )
    facet = FacetObservation(
        observation_id=uuid4(),
        target=target,
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.SNAPSHOT,
        field_coverage=FieldCoverage.COMPLETE,
        payload={"name": "scope-bound.py"},
        authorized_by=(facet_scope,),
    )
    relationship = _membership_relationship(
        relationship_scope,
        seeded.workspace_root_object_id,
        target,
    )
    batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW,
        received_at=NOW,
        facet_observations=(facet,),
        relationship_observations=(relationship,),
    )

    result = run(store.ingest(batch))

    assert result.status.value == "partial"
    assert result.accepted_observation_ids == (relationship.observation_id,)
    assert result.issue_count == 1
    assert (
        store.get_facet_sync(
            next(
                item.object_id
                for item in store.list_objects()
                if item.external_key == "workspace:/Shared/scope-bound.py"
            ),
            "metadata",
        )
        is None
    )


def test_conflicting_relationship_ids_cannot_lend_collection_authority(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "conflicting-link-authority.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local",
        profile="DEFAULT",
        workspace_root="/Shared",
        now=NOW,
    )
    authority = _membership_authority(
        seeded.system.system_id,
        seeded.workspace_root_scope.scope_id,
    )
    target = ObjectLocator(
        object_type="file",
        source_kind="databricks.workspace.file",
        external_key="workspace:/Shared/colliding.py",
        display_name="colliding.py",
    )
    collision_id = uuid4()
    first_relationship = replace(
        _membership_relationship(
            authority,
            seeded.workspace_root_object_id,
            target,
        ),
        observation_id=collision_id,
    )
    conflicting_relationship = replace(
        first_relationship,
        object=replace(target, external_key="workspace:/Shared/other.py"),
    )
    facet = FacetObservation(
        observation_id=uuid4(),
        target=target,
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.SNAPSHOT,
        field_coverage=FieldCoverage.COMPLETE,
        payload={"name": "colliding.py"},
        authorized_by=(authority,),
    )
    batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW,
        received_at=NOW,
        facet_observations=(facet,),
        relationship_observations=(first_relationship, conflicting_relationship),
    )

    result = run(store.ingest(batch))

    assert result.status.value == "partial"
    assert result.accepted_observation_ids == ()
    assert result.issue_count == 3
    assert {item.external_key for item in store.list_objects()} == {"workspace:/Shared"}
    assert (
        store._connection.execute(
            "SELECT COUNT(*) FROM observation_journal WHERE batch_id = ?",
            (batch.batch_id,),
        ).fetchone()[0]
        == 0
    )


def test_cross_kind_observation_id_collision_cannot_lend_collection_authority(
    tmp_path,
) -> None:
    store = SQLiteStore(tmp_path / "cross-kind-link-authority.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local",
        profile="DEFAULT",
        workspace_root="/Shared",
        now=NOW,
    )
    authority = _membership_authority(
        seeded.system.system_id,
        seeded.workspace_root_scope.scope_id,
    )
    target = ObjectLocator(
        object_type="file",
        source_kind="databricks.workspace.file",
        external_key="workspace:/Shared/cross-kind.py",
        display_name="cross-kind.py",
    )
    collision_id = uuid4()
    facet = FacetObservation(
        observation_id=collision_id,
        target=target,
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.SNAPSHOT,
        field_coverage=FieldCoverage.COMPLETE,
        payload={"name": "cross-kind.py"},
        authorized_by=(authority,),
    )
    relationship = replace(
        _membership_relationship(
            authority,
            seeded.workspace_root_object_id,
            target,
        ),
        observation_id=collision_id,
    )
    batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW,
        received_at=NOW,
        facet_observations=(facet,),
        relationship_observations=(relationship,),
    )

    result = run(store.ingest(batch))

    assert result.status.value == "partial"
    assert result.accepted_observation_ids == ()
    assert result.issue_count == 2
    assert {item.external_key for item in store.list_objects()} == {"workspace:/Shared"}
    assert (
        store._connection.execute(
            "SELECT COUNT(*) FROM observation_journal WHERE batch_id = ?",
            (batch.batch_id,),
        ).fetchone()[0]
        == 0
    )


def test_partial_facet_patch_never_clears_unobserved_fields(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    authority = _membership_authority(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    locator = ObjectLocator(
        object_type="file",
        source_kind="databricks.workspace.file",
        external_key="/Shared/report.py",
        display_name="report.py",
    )
    first = FacetObservation(
        observation_id=uuid4(),
        target=locator,
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.SNAPSHOT,
        field_coverage=FieldCoverage.COMPLETE,
        payload={"name": "report.py", "digest": "abc"},
        authorized_by=(authority,),
    )
    second = FacetObservation(
        observation_id=uuid4(),
        target=locator,
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.PATCH,
        field_coverage=FieldCoverage.PARTIAL,
        payload={"name": "renamed.py"},
        field_mask=("name",),
        authorized_by=(authority,),
    )
    with pytest.raises(ValueError, match="exactly match field_mask"):
        replace(second, payload={"name": "renamed.py", "digest": "not-observed"})
    ingestor = SQLiteObservationIngestor(store)
    for offset, observation in enumerate((first, second)):
        at = NOW + timedelta(minutes=offset)
        batch = ObservationBatch(
            batch_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            observed_at=at,
            received_at=at,
            facet_observations=(observation,),
            relationship_observations=(
                _membership_relationship(
                    authority,
                    seeded.workspace_root_object_id,
                    observation.target,
                ),
            ),
        )
        assert run(ingestor.ingest(batch)).status.value == "accepted"
    object_value = next(
        item for item in store.list_objects() if item.external_key == "/Shared/report.py"
    )
    assert store.get_facet_sync(object_value.object_id, "metadata").payload == {
        "name": "renamed.py",
        "digest": "abc",
    }


def test_lower_numeric_source_revision_cannot_overwrite_higher_revision(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "source-revision.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    authority = _membership_authority(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    locator = ObjectLocator(
        object_type="file",
        source_kind="databricks.workspace.file",
        external_key="workspace:/Shared/revisioned.py",
        display_name="revisioned.py",
    )

    def revision_batch(revision: str, at: datetime, value: str) -> tuple[ObservationBatch, str]:
        observation = FacetObservation(
            observation_id=uuid4(),
            target=locator,
            facet="metadata",
            facet_version="1",
            update_mode=UpdateMode.SNAPSHOT,
            field_coverage=FieldCoverage.COMPLETE,
            payload={"name": value},
            source_revision=revision,
            authorized_by=(authority,),
        )
        return (
            ObservationBatch(
                batch_id=uuid4(),
                system_id=seeded.system.system_id,
                connection_binding_id=seeded.connection_binding_id,
                adapter_key="databricks",
                adapter_version="1",
                observed_at=at,
                received_at=at,
                facet_observations=(observation,),
                relationship_observations=(
                    _membership_relationship(
                        authority,
                        seeded.workspace_root_object_id,
                        locator,
                    ),
                ),
            ),
            observation.observation_id,
        )

    higher, higher_observation_id = revision_batch("2", NOW, "higher")
    lower, _ = revision_batch("1", NOW + timedelta(minutes=10), "lower")
    assert run(store.ingest(higher)).status.value == "accepted"
    assert run(store.ingest(lower)).status.value == "accepted"

    remote_object = next(
        item for item in store.list_objects() if item.external_key == locator.external_key
    )
    facet = store.get_facet_sync(remote_object.object_id, "metadata")
    assert facet is not None
    assert facet.payload == {"name": "higher"}
    assert facet.source_revision == "2"
    assert facet.supporting_observation_id == higher_observation_id


def test_equal_observed_time_uses_received_order_across_projections(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "received-order.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    authority = _membership_authority(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    declaration = CoverageDeclaration(
        scope=authority,
        completeness=CollectionCoverage.COMPLETE,
        absence_authority=(AbsenceAuthority.RELATIONSHIP,),
    )

    def ordered_batch(label: str, received_at: datetime) -> tuple[ObservationBatch, str, str]:
        locator = ObjectLocator(
            object_type="file",
            source_kind="databricks.workspace.file",
            external_key="workspace:/Shared/ordered.py",
            display_name=label,
        )
        facet = FacetObservation(
            observation_id=uuid4(),
            target=locator,
            facet="metadata",
            facet_version="1",
            update_mode=UpdateMode.SNAPSHOT,
            field_coverage=FieldCoverage.COMPLETE,
            payload={"name": label},
            authorized_by=(authority,),
        )
        relationship = _membership_relationship(
            authority,
            seeded.workspace_root_object_id,
            locator,
        )
        return (
            ObservationBatch(
                batch_id=uuid4(),
                system_id=seeded.system.system_id,
                connection_binding_id=seeded.connection_binding_id,
                adapter_key="databricks",
                adapter_version="1",
                observed_at=NOW,
                received_at=received_at,
                facet_observations=(facet,),
                relationship_observations=(relationship,),
                coverage=(declaration,),
            ),
            facet.observation_id,
            relationship.observation_id,
        )

    newer, newer_facet_id, newer_relationship_id = ordered_batch(
        "newer", NOW + timedelta(minutes=2)
    )
    older, _, _ = ordered_batch("older", NOW + timedelta(minutes=1))
    exact_tie, _, _ = ordered_batch("exact-tie-loses", NOW + timedelta(minutes=2))
    assert run(store.ingest(newer)).status.value == "accepted"
    assert run(store.ingest(older)).status.value == "accepted"
    assert run(store.ingest(exact_tie)).status.value == "accepted"

    remote_object = next(
        item for item in store.list_objects() if item.external_key == "workspace:/Shared/ordered.py"
    )
    facet = store.get_facet_sync(remote_object.object_id, "metadata")
    relationship = store.list_relationships_sync(seeded.workspace_root_object_id)[0]
    latest_credit = store.latest_qualifying_observation(authority)
    expected_coverage_id = store._coverage_observation_id(newer.batch_id, authority)
    watermark = store._connection.execute(
        """
        SELECT supporting_observation_id FROM relationship_coverage_watermarks
        WHERE system_id = ? AND subject_id = ? AND predicate = 'contains'
        """,
        (seeded.system.system_id, seeded.workspace_root_object_id),
    ).fetchone()

    assert remote_object.display_name == "newer"
    assert facet is not None and facet.payload == {"name": "newer"}
    assert facet.supporting_observation_id == newer_facet_id
    assert relationship.presence is PresenceState.PRESENT
    assert relationship.supporting_observation_id == newer_relationship_id, (
        relationship,
        expected_coverage_id,
    )
    assert latest_credit is not None and latest_credit.observation_id == expected_coverage_id
    assert watermark is not None and watermark["supporting_observation_id"] == expected_coverage_id


def test_migration_invalidates_ambiguous_legacy_equal_time_projection(tmp_path) -> None:
    path = tmp_path / "legacy-equal-time.sqlite3"
    with SQLiteStore(path) as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
        )
        authority = _membership_authority(
            seeded.system.system_id, seeded.workspace_root_scope.scope_id
        )
        declaration = CoverageDeclaration(
            scope=authority,
            completeness=CollectionCoverage.COMPLETE,
            absence_authority=(AbsenceAuthority.RELATIONSHIP,),
        )

        def legacy_batch(label: str, received_at: datetime) -> tuple[ObservationBatch, str, str]:
            locator = ObjectLocator(
                object_type="file",
                source_kind="databricks.workspace.file",
                external_key="workspace:/Shared/legacy-ordered.py",
                display_name=label,
            )
            facet = FacetObservation(
                observation_id=uuid4(),
                target=locator,
                facet="metadata",
                facet_version="1",
                update_mode=UpdateMode.SNAPSHOT,
                field_coverage=FieldCoverage.COMPLETE,
                payload={"name": label},
                authorized_by=(authority,),
            )
            relationship = _membership_relationship(
                authority, seeded.workspace_root_object_id, locator
            )
            return (
                ObservationBatch(
                    batch_id=uuid4(),
                    system_id=seeded.system.system_id,
                    connection_binding_id=seeded.connection_binding_id,
                    adapter_key="databricks",
                    adapter_version="1",
                    observed_at=NOW,
                    received_at=received_at,
                    facet_observations=(facet,),
                    relationship_observations=(relationship,),
                    coverage=(declaration,),
                ),
                facet.observation_id,
                relationship.observation_id,
            )

        newer, _, _ = legacy_batch("newer", NOW + timedelta(minutes=2))
        older, older_facet_id, older_relationship_id = legacy_batch(
            "legacy-wrong-winner", NOW + timedelta(minutes=1)
        )
        assert run(store.ingest(newer)).status.value == "accepted"
        assert run(store.ingest(older)).status.value == "accepted"
        remote_object = next(
            item
            for item in store.list_objects()
            if item.external_key == "workspace:/Shared/legacy-ordered.py"
        )
        store._connection.execute(
            """
            UPDATE facets SET payload_json = ?, supporting_observation_id = ?
            WHERE object_id = ? AND facet = 'metadata'
            """,
            ('{"name":"legacy-wrong-winner"}', older_facet_id, remote_object.object_id),
        )
        store._connection.execute(
            """
            UPDATE relationships SET presence = 'present', supporting_observation_id = ?
            WHERE subject_id = ? AND object_id = ? AND predicate = 'contains'
            """,
            (
                older_relationship_id,
                seeded.workspace_root_object_id,
                remote_object.object_id,
            ),
        )
        store._connection.execute(
            """
            UPDATE relationship_coverage_watermarks SET supporting_observation_id = ?
            WHERE system_id = ? AND subject_id = ? AND predicate = 'contains'
            """,
            (
                store._coverage_observation_id(older.batch_id, authority),
                seeded.system.system_id,
                seeded.workspace_root_object_id,
            ),
        )
        _rewind_projection_order_migration(store)
        store._connection.execute(
            "DELETE FROM schema_migrations WHERE version = ?",
            ("0016_projection_received_order",),
        )

    with SQLiteStore(path) as migrated:
        facet = migrated.get_facet_sync(remote_object.object_id, "metadata")
        relationship = migrated.list_relationships_sync(seeded.workspace_root_object_id)[0]
        assert facet is not None
        assert facet.knowledge is KnowledgeState.UNKNOWN
        assert facet.payload == {}
        assert relationship.presence is PresenceState.UNKNOWN
        assert migrated.latest_qualifying_observation(authority) is None
        assert (
            migrated._connection.execute(
                "SELECT COUNT(*) FROM relationship_coverage_watermarks"
            ).fetchone()[0]
            == 0
        )


def test_migration_unknown_object_receipt_rejects_equal_observed_stale_delivery(
    tmp_path,
) -> None:
    path = tmp_path / "legacy-object-receipt.sqlite3"
    with SQLiteStore(path) as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
        )
        authority = _membership_authority(
            seeded.system.system_id, seeded.workspace_root_scope.scope_id
        )

        def object_batch(label: str, received_at: datetime) -> ObservationBatch:
            locator = ObjectLocator(
                object_type="file",
                source_kind="databricks.workspace.file",
                external_key="workspace:/Shared/legacy-receipt.py",
                display_name=label,
            )
            return ObservationBatch(
                batch_id=uuid4(),
                system_id=seeded.system.system_id,
                connection_binding_id=seeded.connection_binding_id,
                adapter_key="databricks",
                adapter_version="1",
                observed_at=NOW,
                received_at=received_at,
                facet_observations=(
                    FacetObservation(
                        observation_id=uuid4(),
                        target=locator,
                        facet="metadata",
                        facet_version="1",
                        update_mode=UpdateMode.SNAPSHOT,
                        field_coverage=FieldCoverage.COMPLETE,
                        payload={"name": label},
                        authorized_by=(authority,),
                    ),
                ),
                relationship_observations=(
                    _membership_relationship(authority, seeded.workspace_root_object_id, locator),
                ),
            )

        original = object_batch("original", NOW + timedelta(minutes=10))
        assert run(store.ingest(original)).status.value == "accepted"
        remote_object = next(
            item
            for item in store.list_objects()
            if item.external_key == "workspace:/Shared/legacy-receipt.py"
        )
        _rewind_projection_order_migration(store)
        store._connection.execute(
            "DELETE FROM schema_migrations WHERE version = ?",
            ("0016_projection_received_order",),
        )

    with SQLiteStore(path) as migrated:
        stale = object_batch("stale", NOW + timedelta(minutes=5))
        assert run(migrated.ingest(stale)).status.value == "accepted"
        unchanged = migrated.get_object_sync(remote_object.object_id)
        facet = migrated.get_facet_sync(remote_object.object_id, "metadata")
        assert unchanged is not None and unchanged.display_name == "original"
        assert facet is not None and facet.payload == {"name": "original"}


def test_migration_does_not_mix_same_external_identity_across_systems(tmp_path) -> None:
    path = tmp_path / "legacy-cross-system.sqlite3"
    with SQLiteStore(path) as store:
        seeded_systems = tuple(
            SystemBootstrapService(store).configure_databricks_workspace(
                display_name=name,
                profile=profile,
                workspace_root="/Shared",
                now=NOW,
            )
            for name, profile in (("one", "ONE"), ("two", "TWO"))
        )
        object_ids: list[str] = []
        for seeded in seeded_systems:
            authority = _membership_authority(
                seeded.system.system_id, seeded.workspace_root_scope.scope_id
            )
            locator = ObjectLocator(
                object_type="file",
                source_kind="databricks.workspace.file",
                external_key="workspace:/Shared/same.py",
                display_name=seeded.system.display_name,
            )
            facet = FacetObservation(
                observation_id=uuid4(),
                target=locator,
                facet="metadata",
                facet_version="1",
                update_mode=UpdateMode.SNAPSHOT,
                field_coverage=FieldCoverage.COMPLETE,
                payload={"name": seeded.system.display_name},
                authorized_by=(authority,),
            )
            batch = ObservationBatch(
                batch_id=uuid4(),
                system_id=seeded.system.system_id,
                connection_binding_id=seeded.connection_binding_id,
                adapter_key="databricks",
                adapter_version="1",
                observed_at=NOW,
                received_at=NOW,
                facet_observations=(facet,),
                relationship_observations=(
                    _membership_relationship(authority, seeded.workspace_root_object_id, locator),
                ),
            )
            assert run(store.ingest(batch)).status.value == "accepted"
            object_ids.append(
                next(
                    item.object_id
                    for item in store.list_objects(system_id=seeded.system.system_id)
                    if item.external_key == locator.external_key
                )
            )
        _rewind_projection_order_migration(store)
        store._connection.execute(
            "DELETE FROM schema_migrations WHERE version = ?",
            ("0016_projection_received_order",),
        )

    with SQLiteStore(path) as migrated:
        for object_id, expected in zip(object_ids, ("one", "two"), strict=True):
            facet = migrated.get_facet_sync(object_id, "metadata")
            assert facet is not None
            assert facet.knowledge is KnowledgeState.KNOWN
            assert facet.payload == {"name": expected}


def test_migration_detects_decimal_revisions_beyond_sqlite_integer_range(tmp_path) -> None:
    path = tmp_path / "legacy-large-revision.sqlite3"
    with SQLiteStore(path) as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
        )
        authority = _membership_authority(
            seeded.system.system_id, seeded.workspace_root_scope.scope_id
        )
        locator = ObjectLocator(
            object_type="file",
            source_kind="databricks.workspace.file",
            external_key="workspace:/Shared/large-revision.py",
            display_name="large-revision.py",
        )
        revisions = ("9" * 100, "1" + "0" * 100)
        for offset, revision in enumerate(revisions):
            facet = FacetObservation(
                observation_id=uuid4(),
                target=locator,
                facet="metadata",
                facet_version="1",
                update_mode=UpdateMode.SNAPSHOT,
                field_coverage=FieldCoverage.COMPLETE,
                payload={"revision": revision},
                source_revision=revision,
                authorized_by=(authority,),
            )
            batch = ObservationBatch(
                batch_id=uuid4(),
                system_id=seeded.system.system_id,
                connection_binding_id=seeded.connection_binding_id,
                adapter_key="databricks",
                adapter_version="1",
                observed_at=NOW + timedelta(minutes=offset),
                received_at=NOW + timedelta(minutes=offset),
                facet_observations=(facet,),
                relationship_observations=(
                    _membership_relationship(authority, seeded.workspace_root_object_id, locator),
                ),
            )
            assert run(store.ingest(batch)).status.value == "accepted"
        remote_object = next(
            item for item in store.list_objects() if item.external_key == locator.external_key
        )
        equal_locator = replace(
            locator,
            external_key="workspace:/Shared/equal-revision.py",
            display_name="equal-revision.py",
        )
        for offset, revision in enumerate(("1", "01"), start=5):
            facet = FacetObservation(
                observation_id=uuid4(),
                target=equal_locator,
                facet="metadata",
                facet_version="1",
                update_mode=UpdateMode.SNAPSHOT,
                field_coverage=FieldCoverage.COMPLETE,
                payload={"revision": revision},
                source_revision=revision,
                authorized_by=(authority,),
            )
            batch = ObservationBatch(
                batch_id=uuid4(),
                system_id=seeded.system.system_id,
                connection_binding_id=seeded.connection_binding_id,
                adapter_key="databricks",
                adapter_version="1",
                observed_at=NOW + timedelta(minutes=offset),
                received_at=NOW + timedelta(minutes=offset),
                facet_observations=(facet,),
                relationship_observations=(
                    _membership_relationship(
                        authority, seeded.workspace_root_object_id, equal_locator
                    ),
                ),
            )
            assert run(store.ingest(batch)).status.value == "accepted"
        equal_object = next(
            item for item in store.list_objects() if item.external_key == equal_locator.external_key
        )
        _rewind_projection_order_migration(store)
        store._connection.execute(
            "DELETE FROM schema_migrations WHERE version = ?",
            ("0016_projection_received_order",),
        )

    with SQLiteStore(path) as migrated:
        facet = migrated.get_facet_sync(remote_object.object_id, "metadata")
        assert facet is not None
        assert facet.knowledge is KnowledgeState.UNKNOWN
        assert facet.payload == {}
        equal_facet = migrated.get_facet_sync(equal_object.object_id, "metadata")
        assert equal_facet is not None
        assert equal_facet.knowledge is KnowledgeState.KNOWN
        assert equal_facet.payload == {"revision": "01"}


def test_object_presence_projection_is_timestamp_monotonic(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local",
        profile="DEFAULT",
        workspace_root="/Shared",
        enabled_capability_keys=(
            "databricks.workspace.children.read",
            "databricks.workspace.metadata.read",
        ),
        now=NOW,
    )
    metadata_capability = store.get_capability_binding_sync(
        seeded.connection_binding_id,
        "databricks.workspace.metadata.read",
        "1",
    )
    assert metadata_capability is not None
    store.upsert_capability_binding(
        replace(
            metadata_capability,
            capability_binding_id=uuid4(),
            capability_version="2",
            coverage_policies=tuple(
                replace(
                    policy,
                    absence_authority=(AbsenceAuthority.OBJECT_PRESENCE,),
                )
                for policy in metadata_capability.coverage_policies
            ),
        ),
        now=NOW,
    )
    ambiguous_scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.OBJECT, seeded.workspace_root_object_id),
        object_type="folder",
        facet="metadata",
        capability_key="databricks.workspace.metadata.read",
    )
    ambiguous = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW,
        received_at=NOW,
        coverage=(CoverageDeclaration(ambiguous_scope, CollectionCoverage.UNKNOWN),),
    )
    assert run(store.ingest(ambiguous)).status.value == "rejected"
    store._connection.execute(
        """
        UPDATE capability_bindings SET enabled = 0
        WHERE capability_binding_id = ?
        """,
        (metadata_capability.capability_binding_id,),
    )
    locator = ObjectLocator(
        object_type="file",
        source_kind="databricks.workspace.file",
        external_key="workspace:/Shared/report.py",
        display_name="report.py",
    )
    membership_authority = _membership_authority(
        seeded.system.system_id, seeded.workspace_root_scope.scope_id
    )

    def present_batch(
        *, observed_at: datetime, target: ObjectLocator = locator
    ) -> ObservationBatch:
        return ObservationBatch(
            batch_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            observed_at=observed_at,
            received_at=max(observed_at, NOW + timedelta(minutes=30)),
            facet_observations=(
                FacetObservation(
                    observation_id=uuid4(),
                    target=target,
                    facet="metadata",
                    facet_version="1",
                    update_mode=UpdateMode.SNAPSHOT,
                    field_coverage=FieldCoverage.COMPLETE,
                    payload={"name": "report.py"},
                    authorized_by=(membership_authority,),
                ),
            ),
            relationship_observations=(
                _membership_relationship(
                    membership_authority,
                    seeded.workspace_root_object_id,
                    target,
                ),
            ),
        )

    newer_present_at = NOW + timedelta(minutes=10)
    assert run(store.ingest(present_batch(observed_at=newer_present_at))).status.value == (
        "accepted"
    )
    remote_object = next(
        item for item in store.list_objects() if item.external_key == locator.external_key
    )
    presence_scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.OBJECT, remote_object.object_id),
        object_type="file",
        facet="metadata",
        capability_key="databricks.workspace.metadata.read",
        coverage=RefreshCoverage.FACET,
    )

    def absence_batch(*, observed_at: datetime) -> ObservationBatch:
        return ObservationBatch(
            batch_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            observed_at=observed_at,
            received_at=NOW + timedelta(minutes=30),
            facet_observations=(
                FacetObservation(
                    observation_id=uuid4(),
                    target=locator,
                    facet="metadata",
                    facet_version="1",
                    update_mode=UpdateMode.ABSENCE,
                    field_coverage=FieldCoverage.COMPLETE,
                    authorized_by=(presence_scope,),
                ),
            ),
            coverage=(
                CoverageDeclaration(
                    scope=presence_scope,
                    completeness=CollectionCoverage.COMPLETE,
                    absence_authority=(AbsenceAuthority.OBJECT_PRESENCE,),
                ),
            ),
        )

    assert run(store.ingest(absence_batch(observed_at=NOW))).status.value == "accepted"
    remote_object = store.get_object_sync(remote_object.object_id)
    assert remote_object is not None
    assert remote_object.presence is PresenceState.PRESENT
    assert remote_object.last_seen_at == newer_present_at

    newer_absence_at = NOW + timedelta(minutes=20)
    assert run(store.ingest(absence_batch(observed_at=newer_absence_at))).status.value == "accepted"
    remote_object = store.get_object_sync(remote_object.object_id)
    assert remote_object is not None
    assert remote_object.presence is PresenceState.ABSENT
    assert remote_object.last_seen_at == newer_absence_at

    canonical_locator = ObjectLocator(object_type="file", object_id=remote_object.object_id)
    assert (
        run(
            store.ingest(
                present_batch(
                    observed_at=NOW + timedelta(minutes=15),
                    target=canonical_locator,
                )
            )
        ).status.value
        == "accepted"
    )
    remote_object = store.get_object_sync(remote_object.object_id)
    assert remote_object is not None
    assert remote_object.presence is PresenceState.ABSENT
    assert remote_object.last_seen_at == newer_absence_at

    latest_present_at = NOW + timedelta(minutes=40)
    assert (
        run(
            store.ingest(present_batch(observed_at=latest_present_at, target=canonical_locator))
        ).status.value
        == "accepted"
    )
    remote_object = store.get_object_sync(remote_object.object_id)
    assert remote_object is not None
    assert remote_object.presence is PresenceState.PRESENT
    assert remote_object.last_seen_at == latest_present_at


def test_metadata_capability_cannot_self_grant_object_presence_authority(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local",
        profile="DEFAULT",
        workspace_root="/Shared",
        enabled_capability_keys=("databricks.workspace.metadata.read",),
        now=NOW,
    )
    remote_object = store.upsert_object(
        RemoteObject(
            object_id=uuid4(),
            system_id=seeded.system.system_id,
            object_type="file",
            object_type_version="1",
            source_kind="databricks.workspace.file",
            external_key="workspace:object_id:101",
            display_name="report.py",
            presence=PresenceState.PRESENT,
            first_seen_at=NOW,
        )
    )
    scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.OBJECT, remote_object.object_id),
        object_type="file",
        facet="metadata",
        capability_key="databricks.workspace.metadata.read",
        coverage=RefreshCoverage.FACET,
    )
    forged = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW + timedelta(seconds=1),
        received_at=NOW + timedelta(seconds=1),
        facet_observations=(
            FacetObservation(
                observation_id=uuid4(),
                target=ObjectLocator(object_type="file", object_id=remote_object.object_id),
                facet="metadata",
                facet_version="1",
                update_mode=UpdateMode.ABSENCE,
                field_coverage=FieldCoverage.COMPLETE,
            ),
        ),
        coverage=(
            CoverageDeclaration(
                scope=scope,
                completeness=CollectionCoverage.COMPLETE,
                absence_authority=(AbsenceAuthority.OBJECT_PRESENCE,),
            ),
        ),
    )

    assert run(store.ingest(forged)).status.value == "rejected"
    unchanged = store.get_object_sync(remote_object.object_id)
    assert unchanged is not None and unchanged.presence is PresenceState.PRESENT


def test_action_coverage_cannot_borrow_another_capability_policy(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local",
        profile="DEFAULT",
        workspace_root="/Shared",
        enabled_capability_keys=(
            "databricks.workspace.children.read",
            "databricks.workspace.metadata.read",
        ),
        now=NOW,
    )
    metadata_scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.OBJECT, seeded.workspace_root_object_id),
        object_type="folder",
        facet="metadata",
        capability_key="databricks.workspace.metadata.read",
    )
    action = AdapterAction(
        action_id=uuid4(),
        correlation_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        capability_key="databricks.workspace.metadata.read",
        capability_version="1",
        target=metadata_scope.target,
        requested_scopes=(metadata_scope,),
    )
    run(store.enqueue(action))
    action_now = datetime.now(UTC)
    lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=action_now))
    assert lease is not None
    run(
        store.mark_running(
            action_id=action.action_id,
            lease_id=lease.lease_id,
            started_at=action_now,
        )
    )
    for capability_key in (None, "databricks.workspace.children.read"):
        borrowed_scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(
                TargetKind.CONFIGURED_SCOPE,
                seeded.workspace_root_scope.scope_id,
            ),
            object_type="folder",
            facet="membership",
            capability_key=capability_key,
        )
        forged = ObservationBatch(
            batch_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            action_id=action.action_id,
            observed_at=NOW + timedelta(seconds=1),
            received_at=NOW + timedelta(seconds=1),
            coverage=(
                CoverageDeclaration(
                    borrowed_scope,
                    CollectionCoverage.COMPLETE,
                    (AbsenceAuthority.RELATIONSHIP,),
                ),
            ),
        )
        assert run(store.ingest(forged, lease_id=lease.lease_id)).status.value == "rejected"


def test_incidental_coverage_requires_enabled_capability_but_running_action_keeps_policy(
    tmp_path,
) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local",
        profile="DEFAULT",
        workspace_root="/Shared",
        enabled_capability_keys=(
            "databricks.workspace.children.read",
            "databricks.workspace.metadata.read",
        ),
        now=NOW,
    )
    membership_scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
        capability_key="databricks.workspace.children.read",
    )
    store._connection.execute(
        """
        UPDATE capability_bindings SET enabled = 0
        WHERE capability_key = 'databricks.workspace.children.read'
        """
    )
    incidental = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW,
        received_at=NOW,
        coverage=(CoverageDeclaration(membership_scope, CollectionCoverage.COMPLETE),),
    )
    assert run(store.ingest(incidental)).status.value == "rejected"

    metadata_scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.OBJECT, seeded.workspace_root_object_id),
        object_type="folder",
        facet="metadata",
        capability_key="databricks.workspace.metadata.read",
    )
    action = AdapterAction(
        action_id=uuid4(),
        correlation_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        capability_key="databricks.workspace.metadata.read",
        capability_version="1",
        target=metadata_scope.target,
        requested_scopes=(metadata_scope,),
    )
    run(store.enqueue(action))
    action_now = datetime.now(UTC)
    lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=action_now))
    assert lease is not None
    run(
        store.mark_running(
            action_id=action.action_id,
            lease_id=lease.lease_id,
            started_at=action_now,
        )
    )
    store._connection.execute(
        """
        UPDATE capability_bindings SET enabled = 0
        WHERE capability_key = 'databricks.workspace.metadata.read'
        """
    )
    running_result = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        action_id=action.action_id,
        observed_at=action_now,
        received_at=action_now,
        coverage=(CoverageDeclaration(metadata_scope, CollectionCoverage.COMPLETE),),
    )
    assert run(store.ingest(running_result, lease_id=lease.lease_id)).status.value == "accepted"


def test_action_linked_ingestion_requires_the_current_running_lease(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local",
        profile="DEFAULT",
        workspace_root="/Shared",
        enabled_capability_keys=("databricks.workspace.metadata.read",),
        now=NOW,
    )
    scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.OBJECT, seeded.workspace_root_object_id),
        object_type="folder",
        facet="metadata",
        capability_key="databricks.workspace.metadata.read",
    )
    action = AdapterAction(
        action_id=uuid4(),
        correlation_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        capability_key="databricks.workspace.metadata.read",
        capability_version="1",
        target=scope.target,
        requested_scopes=(scope,),
    )
    run(store.enqueue(action))
    first_started = datetime.now(UTC)
    first = run(
        store.lease_next(
            adapter_key="databricks",
            worker_id="expired-worker",
            now=first_started,
        )
    )
    assert first is not None
    run(
        store.mark_running(
            action_id=action.action_id,
            lease_id=first.lease_id,
            started_at=first_started,
        )
    )
    reassigned_at = first.leased_until + timedelta(microseconds=1)
    current = run(
        store.lease_next(
            adapter_key="databricks",
            worker_id="current-worker",
            now=reassigned_at,
        )
    )
    assert current is not None
    run(
        store.mark_running(
            action_id=action.action_id,
            lease_id=current.lease_id,
            started_at=reassigned_at,
        )
    )
    batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        action_id=action.action_id,
        observed_at=reassigned_at,
        received_at=reassigned_at,
        coverage=(CoverageDeclaration(scope, CollectionCoverage.COMPLETE),),
    )

    assert run(store.ingest(batch, lease_id=first.lease_id)).status.value == "rejected"
    assert run(store.ingest(batch, lease_id=current.lease_id)).status.value == "accepted"
    assert run(store.ingest(batch)).status.value == "duplicate"


def test_rejected_items_roll_back_identity_and_journal_but_keep_valid_sibling(
    tmp_path,
) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    membership = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
        capability_key="databricks.workspace.children.read",
        coverage=RefreshCoverage.FACET,
    )
    valid = FacetObservation(
        observation_id=uuid4(),
        target=ObjectLocator(
            object_type="file",
            source_kind="databricks.workspace.file",
            external_key="workspace:/Shared/kept.py",
            display_name="kept.py",
        ),
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.SNAPSHOT,
        field_coverage=FieldCoverage.COMPLETE,
        payload={"name": "kept.py"},
    )
    rejected = FacetObservation(
        observation_id=uuid4(),
        target=ObjectLocator(
            object_type="file",
            source_kind="databricks.workspace.file",
            external_key="workspace:/Shared/phantom.py",
            display_name="phantom.py",
        ),
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.ABSENCE,
        field_coverage=FieldCoverage.COMPLETE,
    )
    valid_relationship = _membership_relationship(
        membership,
        seeded.workspace_root_object_id,
        valid.target,
    )
    rejected_relationship = RelationshipObservation(
        observation_id=uuid4(),
        subject=ObjectLocator(
            object_type="folder",
            source_kind="databricks.workspace.folder",
            external_key="workspace:/Shared/transient",
            display_name="transient",
        ),
        predicate="contains",
        object=ObjectLocator(object_type="file", object_id=uuid4()),
        presence=PresenceState.PRESENT,
    )
    batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW,
        received_at=NOW,
        facet_observations=(valid, rejected),
        relationship_observations=(valid_relationship, rejected_relationship),
        coverage=(
            CoverageDeclaration(
                scope=membership,
                completeness=CollectionCoverage.COMPLETE,
                absence_authority=(AbsenceAuthority.RELATIONSHIP,),
            ),
        ),
    )

    result = run(store.ingest(batch))

    assert result.status.value == "partial"
    assert result.accepted_observation_ids == (
        valid.observation_id,
        valid_relationship.observation_id,
    )
    assert result.issue_count == 2
    assert {item.external_key for item in store.list_objects()} == {
        "workspace:/Shared",
        "workspace:/Shared/kept.py",
    }
    journal_ids = {
        row[0]
        for row in store._connection.execute(
            "SELECT observation_id FROM observation_journal WHERE batch_id = ?",
            (batch.batch_id,),
        ).fetchall()
    }
    assert journal_ids == {valid.observation_id, valid_relationship.observation_id}
    assert store.latest_qualifying_observation(membership) is None
    assert (
        store._connection.execute(
            "SELECT COUNT(*) FROM ingestion_issues WHERE batch_id = ?", (batch.batch_id,)
        ).fetchone()[0]
        == 2
    )


def test_merge_time_facet_rejection_rolls_back_identity_journal_and_projection(
    tmp_path,
) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local",
        profile="DEFAULT",
        workspace_root="/Shared",
        enabled_capability_keys=(
            "databricks.workspace.children.read",
            "databricks.workspace.metadata.read",
        ),
        now=NOW,
    )
    authority = _membership_authority(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    target = ObjectLocator(
        object_type="file",
        source_kind="databricks.workspace.file",
        external_key="workspace:/Shared/original.py",
        display_name="original.py",
    )
    first = FacetObservation(
        observation_id=uuid4(),
        target=target,
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.SNAPSHOT,
        field_coverage=FieldCoverage.COMPLETE,
        payload={"left": "a" * 600_000},
        authorized_by=(authority,),
    )
    first_batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW,
        received_at=NOW,
        facet_observations=(first,),
        relationship_observations=(
            _membership_relationship(
                authority,
                seeded.workspace_root_object_id,
                first.target,
            ),
        ),
    )
    assert run(store.ingest(first_batch)).status.value == "accepted"
    original = next(
        item
        for item in store.list_objects()
        if item.external_key == "workspace:/Shared/original.py"
    )
    original_facet = store.get_facet_sync(original.object_id, "metadata")
    assert original_facet is not None
    direct_authority = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.OBJECT, original.object_id),
        object_type="file",
        facet="metadata",
        capability_key="databricks.workspace.metadata.read",
    )

    rejected = FacetObservation(
        observation_id=uuid4(),
        target=ObjectLocator(
            object_type="file",
            source_kind="databricks.workspace.file",
            external_key="workspace:/Shared/original.py",
            display_name="changed.py",
        ),
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.PATCH,
        field_coverage=FieldCoverage.PARTIAL,
        payload={"right": "b" * 600_000},
        field_mask=("right",),
        authorized_by=(direct_authority,),
    )
    rejected_batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW + timedelta(seconds=1),
        received_at=NOW + timedelta(seconds=1),
        facet_observations=(rejected,),
    )

    result = run(store.ingest(rejected_batch))

    assert result.status.value == "partial"
    assert result.accepted_observation_ids == ()
    assert result.issue_count == 1
    unchanged = store.get_object_sync(original.object_id)
    unchanged_facet = store.get_facet_sync(original.object_id, "metadata")
    assert unchanged is not None and unchanged.display_name == "original.py"
    assert unchanged.last_seen_at == NOW
    assert unchanged_facet is not None
    assert unchanged_facet.payload == original_facet.payload
    assert unchanged_facet.supporting_observation_id == first.observation_id
    assert (
        store._connection.execute(
            "SELECT 1 FROM observation_journal WHERE observation_id = ?",
            (rejected.observation_id,),
        ).fetchone()
        is None
    )
    assert run(store.ingest(rejected_batch)).status.value == "duplicate"


def test_typed_external_identity_does_not_rewrite_untyped_legacy_object(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    authority = _membership_authority(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    legacy = store.upsert_object(
        RemoteObject(
            object_id=uuid4(),
            system_id=seeded.system.system_id,
            object_type="file",
            object_type_version="1",
            source_kind="databricks.workspace.file",
            external_key="workspace:101",
            display_name="legacy.py",
            presence=PresenceState.PRESENT,
            first_seen_at=NOW,
        )
    )
    observation = FacetObservation(
        observation_id=uuid4(),
        target=ObjectLocator(
            object_type="file",
            source_kind="databricks.workspace.file",
            external_key="workspace:object_id:101",
            display_name="current.py",
        ),
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.PATCH,
        field_coverage=FieldCoverage.PARTIAL,
        payload={"path": "/Shared/current.py"},
        field_mask=("path",),
        authorized_by=(authority,),
    )
    batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW + timedelta(seconds=1),
        received_at=NOW + timedelta(seconds=1),
        facet_observations=(observation,),
        relationship_observations=(
            _membership_relationship(
                authority,
                seeded.workspace_root_object_id,
                observation.target,
            ),
        ),
    )

    assert run(store.ingest(batch)).status.value == "accepted"
    unchanged = store.get_object_sync(legacy.object_id)
    assert unchanged is not None
    assert unchanged.external_key == "workspace:101"
    assert unchanged.display_name == "legacy.py"
    workspace_files = [
        item for item in store.list_objects() if item.source_kind == "databricks.workspace.file"
    ]
    assert {item.external_key for item in workspace_files} == {
        "workspace:101",
        "workspace:object_id:101",
    }


def test_alternate_typed_witness_cannot_hijack_untyped_legacy_identity(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    authority = _membership_authority(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    legacy = store.upsert_object(
        RemoteObject(
            object_id=uuid4(),
            system_id=seeded.system.system_id,
            object_type="file",
            object_type_version="1",
            source_kind="databricks.workspace.file",
            external_key="workspace:101",
            display_name="original.py",
            presence=PresenceState.PRESENT,
            first_seen_at=NOW,
        )
    )
    incoming = FacetObservation(
        observation_id=uuid4(),
        target=ObjectLocator(
            object_type="file",
            source_kind="databricks.workspace.file",
            external_key="workspace:object_id:202",
            display_name="different.py",
        ),
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.PATCH,
        field_coverage=FieldCoverage.PARTIAL,
        payload={"path": "/Shared/different.py"},
        field_mask=("path",),
        authorized_by=(authority,),
    )
    batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW + timedelta(seconds=1),
        received_at=NOW + timedelta(seconds=1),
        facet_observations=(incoming,),
        relationship_observations=(
            _membership_relationship(
                authority,
                seeded.workspace_root_object_id,
                incoming.target,
            ),
        ),
    )

    assert run(store.ingest(batch)).status.value == "accepted"
    unchanged = store.get_object_sync(legacy.object_id)
    assert unchanged is not None
    assert unchanged.external_key == "workspace:101"
    assert unchanged.display_name == "original.py"
    workspace_files = [
        item for item in store.list_objects() if item.source_kind == "databricks.workspace.file"
    ]
    assert {item.external_key for item in workspace_files} == {
        "workspace:101",
        "workspace:object_id:202",
    }


def test_conflicting_observation_id_target_or_payload_is_not_accepted(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local",
        profile="DEFAULT",
        workspace_root="/Shared",
        enabled_capability_keys=(
            "databricks.workspace.children.read",
            "databricks.workspace.metadata.read",
        ),
        now=NOW,
    )
    authority = _membership_authority(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    observation_id = uuid4()
    original = FacetObservation(
        observation_id=observation_id,
        target=ObjectLocator(
            object_type="file",
            source_kind="databricks.workspace.file",
            external_key="workspace:/Shared/a.py",
            display_name="a.py",
        ),
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.SNAPSHOT,
        field_coverage=FieldCoverage.COMPLETE,
        payload={"name": "a.py", "digest": "one"},
        authorized_by=(authority,),
    )
    ingestor = SQLiteObservationIngestor(store)
    initial_batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW,
        received_at=NOW,
        facet_observations=(original,),
        relationship_observations=(
            _membership_relationship(
                authority,
                seeded.workspace_root_object_id,
                original.target,
            ),
        ),
    )
    assert run(ingestor.ingest(initial_batch)).status.value == "accepted"
    original_object = next(
        item for item in store.list_objects() if item.external_key == "workspace:/Shared/a.py"
    )
    direct_authority = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.OBJECT, original_object.object_id),
        object_type="file",
        facet="metadata",
        capability_key="databricks.workspace.metadata.read",
    )

    target_collision = FacetObservation(
        observation_id=observation_id,
        target=ObjectLocator(
            object_type="file",
            source_kind="databricks.workspace.file",
            external_key="workspace:/Shared/b.py",
            display_name="b.py",
        ),
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.SNAPSHOT,
        field_coverage=FieldCoverage.COMPLETE,
        payload={"name": "b.py", "digest": "one"},
        authorized_by=(direct_authority,),
    )
    payload_collision = FacetObservation(
        observation_id=observation_id,
        target=original.target,
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.SNAPSHOT,
        field_coverage=FieldCoverage.COMPLETE,
        payload={"name": "a.py", "digest": "two"},
        authorized_by=(direct_authority,),
    )
    for observation in (target_collision, payload_collision):
        result = run(
            ingestor.ingest(
                ObservationBatch(
                    batch_id=uuid4(),
                    system_id=seeded.system.system_id,
                    connection_binding_id=seeded.connection_binding_id,
                    adapter_key="databricks",
                    adapter_version="1",
                    observed_at=NOW + timedelta(minutes=1),
                    received_at=NOW + timedelta(minutes=1),
                    facet_observations=(observation,),
                )
            )
        )
        assert result.status.value == "partial"
        assert not result.accepted_observation_ids
    assert {item.external_key for item in store.list_objects()} == {
        "workspace:/Shared",
        "workspace:/Shared/a.py",
    }
    object_value = next(item for item in store.list_objects() if item.external_key.endswith("a.py"))
    assert store.get_facet_sync(object_value.object_id, "metadata").payload["digest"] == "one"


def test_legacy_partial_batch_backfills_only_exact_semantics(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )

    def partial_batch():
        valid = FacetObservation(
            observation_id=uuid4(),
            target=ObjectLocator(object_type="folder", object_id=seeded.workspace_root_object_id),
            facet="membership",
            facet_version="1",
            update_mode=UpdateMode.PATCH,
            field_coverage=FieldCoverage.PARTIAL,
            payload={"member_count": 1},
            field_mask=("member_count",),
        )
        invalid = FacetObservation(
            observation_id=uuid4(),
            target=ObjectLocator(object_type="folder", object_id=uuid4()),
            facet="membership",
            facet_version="1",
            update_mode=UpdateMode.PATCH,
            field_coverage=FieldCoverage.PARTIAL,
            payload={"member_count": 2},
            field_mask=("member_count",),
        )
        batch = ObservationBatch(
            batch_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            observed_at=NOW,
            received_at=NOW,
            facet_observations=(valid, invalid),
        )
        return batch, valid, invalid

    first, valid, invalid = partial_batch()
    assert run(store.ingest(first)).status.value == "partial"
    store._connection.execute(
        "UPDATE observation_batches SET batch_digest = '' WHERE batch_id = ?", (first.batch_id,)
    )
    exact = run(store.ingest(first))
    assert exact.status.value == "rejected"
    digest = store._connection.execute(
        "SELECT batch_digest FROM observation_batches WHERE batch_id = ?", (first.batch_id,)
    ).fetchone()[0]
    assert digest == ""

    changed_payload = ObservationBatch(
        batch_id=first.batch_id,
        system_id=first.system_id,
        connection_binding_id=first.connection_binding_id,
        adapter_key=first.adapter_key,
        adapter_version=first.adapter_version,
        observed_at=first.observed_at,
        received_at=first.received_at,
        facet_observations=(
            valid,
            FacetObservation(
                observation_id=invalid.observation_id,
                target=invalid.target,
                facet="membership",
                facet_version="1",
                update_mode=UpdateMode.PATCH,
                field_coverage=FieldCoverage.PARTIAL,
                payload={"member_count": 99},
                field_mask=("member_count",),
            ),
        ),
    )
    assert run(store.ingest(changed_payload)).status.value == "rejected"

    changed_target = ObservationBatch(
        batch_id=first.batch_id,
        system_id=first.system_id,
        connection_binding_id=first.connection_binding_id,
        adapter_key=first.adapter_key,
        adapter_version=first.adapter_version,
        observed_at=first.observed_at,
        received_at=first.received_at,
        facet_observations=(
            valid,
            FacetObservation(
                observation_id=invalid.observation_id,
                target=ObjectLocator(object_type="folder", object_id=uuid4()),
                facet="membership",
                facet_version="1",
                update_mode=UpdateMode.PATCH,
                field_coverage=FieldCoverage.PARTIAL,
                payload={"member_count": 2},
                field_mask=("member_count",),
            ),
        ),
    )
    assert run(store.ingest(changed_target)).status.value == "rejected"

    subset, _, _ = partial_batch()
    assert run(store.ingest(subset)).status.value == "partial"
    store._connection.execute(
        """
        UPDATE observation_batches
        SET batch_digest = '', accepted_ids_json = '[]'
        WHERE batch_id = ?
        """,
        (subset.batch_id,),
    )
    assert run(store.ingest(subset)).status.value == "rejected"

    incorrect_issue_count, _, _ = partial_batch()
    assert run(store.ingest(incorrect_issue_count)).status.value == "partial"
    store._connection.execute(
        """
        UPDATE observation_batches
        SET batch_digest = '', issue_count = 0
        WHERE batch_id = ?
        """,
        (incorrect_issue_count.batch_id,),
    )
    assert run(store.ingest(incorrect_issue_count)).status.value == "rejected"
