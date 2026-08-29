import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from async_api_view.application import SystemBootstrapService
from async_api_view.contracts import (
    AbsenceAuthority,
    CollectionCoverage,
    CoverageDeclaration,
    FacetObservation,
    FieldCoverage,
    ObjectLocator,
    ObservationBatch,
    PresenceState,
    RefreshCoverage,
    RefreshScope,
    RelationshipObservation,
    TargetKind,
    TargetRef,
    UpdateMode,
)
from async_api_view.ingestion import SQLiteObservationIngestor
from async_api_view.storage import SQLiteStore

NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)


def run(awaitable):
    return asyncio.run(awaitable)


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


def test_partial_facet_patch_never_clears_unobserved_fields(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
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
    )
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
        )
        assert run(ingestor.ingest(batch)).status.value == "accepted"
    object_value = next(
        item for item in store.list_objects() if item.external_key == "/Shared/report.py"
    )
    assert store.get_facet_sync(object_value.object_id, "metadata").payload == {
        "name": "renamed.py",
        "digest": "abc",
    }


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
    locator = ObjectLocator(
        object_type="file",
        source_kind="databricks.workspace.file",
        external_key="workspace:/Shared/report.py",
        display_name="report.py",
    )

    def present_batch(*, observed_at: datetime) -> ObservationBatch:
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
                    target=locator,
                    facet="metadata",
                    facet_version="1",
                    update_mode=UpdateMode.SNAPSHOT,
                    field_coverage=FieldCoverage.COMPLETE,
                    payload={"name": "report.py"},
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

    assert (
        run(store.ingest(present_batch(observed_at=NOW + timedelta(minutes=15)))).status.value
        == "accepted"
    )
    remote_object = store.get_object_sync(remote_object.object_id)
    assert remote_object is not None
    assert remote_object.presence is PresenceState.ABSENT
    assert remote_object.last_seen_at == newer_absence_at

    latest_present_at = NOW + timedelta(minutes=40)
    assert (
        run(store.ingest(present_batch(observed_at=latest_present_at))).status.value == "accepted"
    )
    remote_object = store.get_object_sync(remote_object.object_id)
    assert remote_object is not None
    assert remote_object.presence is PresenceState.PRESENT
    assert remote_object.last_seen_at == latest_present_at


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
        relationship_observations=(rejected_relationship,),
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
    assert result.accepted_observation_ids == (valid.observation_id,)
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
    assert journal_ids == {valid.observation_id}
    assert store.latest_qualifying_observation(membership) is None
    assert (
        store._connection.execute(
            "SELECT COUNT(*) FROM ingestion_issues WHERE batch_id = ?", (batch.batch_id,)
        ).fetchone()[0]
        == 2
    )


def test_conflicting_observation_id_target_or_payload_is_not_accepted(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
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
    )
    assert run(ingestor.ingest(initial_batch)).status.value == "accepted"

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
    )
    payload_collision = FacetObservation(
        observation_id=observation_id,
        target=original.target,
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.SNAPSHOT,
        field_coverage=FieldCoverage.COMPLETE,
        payload={"name": "a.py", "digest": "two"},
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
