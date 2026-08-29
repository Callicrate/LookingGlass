import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from async_api_view.application import DurableCoordinator, SystemBootstrapService
from async_api_view.contracts import (
    ActionCompletion,
    ActionOutcome,
    CollectionCoverage,
    CoverageDeclaration,
    ObservationBatch,
    PresenceState,
    RefreshCoverage,
    RefreshIntent,
    RefreshIntervalOverride,
    RefreshOrigin,
    RefreshScope,
    RemoteObject,
    TargetKind,
    TargetRef,
)
from async_api_view.storage import SQLiteStore

NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)


def run(awaitable):
    return asyncio.run(awaitable)


def _scope(system_id: str, configured_scope_id: str) -> RefreshScope:
    return RefreshScope(
        system_id=system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, configured_scope_id),
        object_type="folder",
        facet="membership",
        coverage=RefreshCoverage.FACET,
    )


def _intent(
    scope: RefreshScope, now: datetime, *, expires_at: datetime | None = None
) -> RefreshIntent:
    return RefreshIntent(
        intent_id=uuid4(),
        idempotency_key=str(uuid4()),
        origin=RefreshOrigin.MANUAL,
        actor_id="local-user",
        scopes=(scope,),
        requested_at=now,
        expires_at=expires_at,
    )


def test_racing_equivalent_intents_elect_one_active_action(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = _scope(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    run(store.submit_refresh(_intent(scope, NOW)))
    run(store.submit_refresh(_intent(scope, NOW)))

    async def coordinate() -> tuple:
        return await asyncio.gather(
            DurableCoordinator(store, worker_id="one").run_once(now=NOW),
            DurableCoordinator(store, worker_id="two").run_once(now=NOW),
        )

    outcomes = run(coordinate())
    assert {outcome.state.value for outcome in outcomes} == {"admitted", "coalesced"}
    assert len(store.list_actions()) == 1


def test_incompatible_persisted_intent_is_rejected_without_blocking_valid_work(
    tmp_path,
) -> None:
    store = SQLiteStore(tmp_path / "intent-poison.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = _scope(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    poisoned = run(store.submit_refresh(_intent(scope, NOW)))
    valid = run(store.submit_refresh(_intent(scope, NOW + timedelta(seconds=1))))
    store._connection.execute(
        "UPDATE refresh_intents SET contract_version = '2' WHERE intent_id = ?",
        (poisoned.intent_id,),
    )

    result = run(
        DurableCoordinator(store, worker_id="coordinator").run_once(now=NOW + timedelta(seconds=1))
    )

    assert result is not None and result.state.value == "admitted"
    assert result.action_id is not None
    poisoned_scope = store.list_intent_scopes(poisoned.intent_id)[0]
    valid_scope = store.list_intent_scopes(valid.intent_id)[0]
    assert poisoned_scope.state.value == "rejected"
    assert poisoned_scope.disposition_reason == "persisted_intent_contract_mismatch"
    assert valid_scope.state.value == "admitted"
    poison_aggregate = store._connection.execute(
        "SELECT aggregate_state FROM refresh_intents WHERE intent_id = ?",
        (poisoned.intent_id,),
    ).fetchone()[0]
    assert poison_aggregate == "complete"
    events = store.list_operational_events(alertable_only=True)
    assert len(events) == 1
    assert events[0].event_type == "refresh.intent.contract_mismatch"
    assert events[0].redacted_summary == "persisted_intent_contract_mismatch"
    assert (
        run(DurableCoordinator(store, worker_id="second").run_once(now=NOW + timedelta(seconds=1)))
        is None
    )
    assert len(store.list_operational_events(alertable_only=True)) == 1


def test_intent_poison_terminalization_operational_error_rolls_back(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SQLiteStore(tmp_path / "intent-poison-rollback.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = _scope(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    poisoned = run(store.submit_refresh(_intent(scope, NOW)))
    store._connection.execute(
        "UPDATE refresh_intents SET contract_version = '2' WHERE intent_id = ?",
        (poisoned.intent_id,),
    )

    def fail_terminalization(*_args: object, **_kwargs: object) -> None:
        raise sqlite3.OperationalError("injected storage failure")

    monkeypatch.setattr(
        store,
        "_terminalize_intent_scope_contract_failure",
        fail_terminalization,
    )

    with pytest.raises(sqlite3.OperationalError, match="injected storage failure"):
        run(DurableCoordinator(store, worker_id="coordinator").run_once(now=NOW))

    assert store.list_intent_scopes(poisoned.intent_id)[0].state.value == "queued"
    assert store.list_operational_events(alertable_only=True) == ()


def test_lease_recovery_revalidates_deferred_scope_and_policy_precedence(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = _scope(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    store.set_refresh_override(
        RefreshIntervalOverride("system", seeded.system.system_id, timedelta(hours=6), "membership")
    )
    store.set_refresh_override(
        RefreshIntervalOverride(
            "object", seeded.workspace_root_object_id, timedelta(hours=2), "membership"
        )
    )
    assert store.effective_interval(scope) == timedelta(hours=2)

    receipt = run(store.submit_refresh(_intent(scope, NOW)))
    leased = run(
        store.lease_next_intent_scope(
            worker_id="crashed", now=NOW, lease_duration=timedelta(seconds=1)
        )
    )
    assert leased is not None
    recovered = run(
        DurableCoordinator(store, worker_id="recovered").run_once(now=NOW + timedelta(seconds=2))
    )
    assert recovered is not None and recovered.state.value == "admitted"
    assert store.list_intent_scopes(receipt.intent_id)[0].state.value == "admitted"


def test_refresh_override_change_wakes_deferred_scope_for_policy_recheck(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "policy-change.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = _scope(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    run(store.submit_refresh(_intent(scope, NOW)))
    initial = run(DurableCoordinator(store, worker_id="initial").run_once(now=NOW))
    assert initial is not None and initial.action_id is not None
    lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=NOW))
    assert lease is not None
    run(store.mark_running(action_id=initial.action_id, lease_id=lease.lease_id, started_at=NOW))
    run(
        store.complete_action(
            ActionCompletion(
                action_id=initial.action_id,
                outcome=ActionOutcome.SUCCEEDED,
                completed_at=NOW + timedelta(seconds=30),
            ),
            lease_id=lease.lease_id,
        )
    )
    requested_at = NOW + timedelta(hours=2)
    deferred_receipt = run(store.submit_refresh(_intent(scope, requested_at)))
    deferred = run(DurableCoordinator(store, worker_id="deferred").run_once(now=requested_at))
    assert deferred is not None and deferred.state.value == "deferred"
    assert deferred.eligible_at == NOW + timedelta(days=1)

    store.set_refresh_override(
        RefreshIntervalOverride("system", seeded.system.system_id, timedelta(days=2), "membership"),
        now=requested_at,
    )
    awakened = store.list_intent_scopes(deferred_receipt.intent_id)[0]
    assert awakened.state.value == "queued"
    assert awakened.disposition_reason == "policy_changed"
    assert awakened.eligible_at is None
    later = run(DurableCoordinator(store, worker_id="later").run_once(now=requested_at))
    assert later is not None and later.state.value == "deferred"
    assert later.eligible_at == NOW + timedelta(days=2)
    store.set_refresh_override(
        RefreshIntervalOverride("system", seeded.system.system_id, timedelta(days=2), "membership"),
        now=requested_at + timedelta(seconds=1),
    )
    unchanged = store.list_intent_scopes(deferred_receipt.intent_id)[0]
    assert unchanged.state.value == "deferred"
    assert unchanged.eligible_at == NOW + timedelta(days=2)

    store.set_refresh_override(
        RefreshIntervalOverride(
            "system", seeded.system.system_id, timedelta(hours=1), "membership"
        ),
        now=requested_at,
    )
    admitted = run(DurableCoordinator(store, worker_id="admitted").run_once(now=requested_at))
    assert admitted is not None and admitted.state.value == "admitted"
    assert admitted.action_id != initial.action_id


def test_owned_core_never_imports_adapter_code() -> None:
    source_root = Path("src/async_api_view")
    for package in ("storage", "application", "ingestion"):
        for path in (source_root / package).rglob("*.py"):
            assert "async_api_view.adapters" not in path.read_text(encoding="utf-8")


def test_capability_hints_select_distinct_uc_actions_and_evidence_does_not_cross(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local",
        profile="DEFAULT",
        workspace_root="/Shared",
        enabled_capability_keys=(
            "databricks.uc.relations.read",
            "databricks.uc.volumes.read",
        ),
        now=NOW,
    )
    schema = store.upsert_object(
        RemoteObject(
            object_id=uuid4(),
            system_id=seeded.system.system_id,
            object_type="generic_object",
            object_type_version="1",
            source_kind="databricks.uc.schema",
            external_key="catalog.schema",
            display_name="schema",
            presence=PresenceState.PRESENT,
            first_seen_at=NOW,
        )
    )
    relations_scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.OBJECT, schema.object_id),
        object_type="generic_object",
        facet="attributes",
        capability_key="databricks.uc.relations.read",
    )
    volumes_scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.OBJECT, schema.object_id),
        object_type="generic_object",
        facet="attributes",
        capability_key="databricks.uc.volumes.read",
    )
    run(store.submit_refresh(_intent(relations_scope, NOW)))
    relations_result = run(DurableCoordinator(store, worker_id="relations").run_once(now=NOW))
    assert relations_result is not None and relations_result.state.value == "admitted"
    assert store.get_action(relations_result.action_id).requested_scopes[0].capability_key == (
        "databricks.uc.relations.read"
    )

    coverage = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW,
        received_at=NOW,
        coverage=(
            CoverageDeclaration(
                scope=relations_scope,
                completeness=CollectionCoverage.PARTIAL,
            ),
        ),
    )
    assert run(store.ingest(coverage)).status.value == "partial"
    assert store.latest_qualifying_observation(volumes_scope) is None

    run(store.submit_refresh(_intent(volumes_scope, NOW)))
    volumes_result = run(DurableCoordinator(store, worker_id="volumes").run_once(now=NOW))
    assert volumes_result is not None and volumes_result.state.value == "admitted"
    actions = store.list_actions()
    assert len(actions) == 2
    assert {action.action.requested_scopes[0].capability_key for action in actions} == {
        "databricks.uc.relations.read",
        "databricks.uc.volumes.read",
    }


def test_partial_collection_does_not_satisfy_follow_up_refresh(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = _scope(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    run(store.submit_refresh(_intent(scope, NOW)))
    admitted = run(DurableCoordinator(store, worker_id="first").run_once(now=NOW))
    assert admitted is not None and admitted.state.value == "admitted"
    lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=NOW))
    assert lease is not None
    run(
        store.mark_running(
            action_id=lease.action.action_id, lease_id=lease.lease_id, started_at=NOW
        )
    )
    partial_batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        action_id=lease.action.action_id,
        observed_at=NOW,
        received_at=NOW,
        coverage=(CoverageDeclaration(scope=scope, completeness=CollectionCoverage.PARTIAL),),
    )
    assert run(store.ingest(partial_batch)).status.value == "partial"
    assert store.latest_qualifying_observation(scope) is None
    run(
        store.complete_action(
            ActionCompletion(
                action_id=lease.action.action_id,
                outcome=ActionOutcome.PARTIAL,
                completed_at=NOW + timedelta(seconds=30),
            ),
            lease_id=lease.lease_id,
        )
    )
    run(store.submit_refresh(_intent(scope, NOW + timedelta(seconds=31))))
    next_result = run(
        DurableCoordinator(store, worker_id="second").run_once(now=NOW + timedelta(seconds=31))
    )
    assert next_result is not None and next_result.state.value == "deferred"


@pytest.mark.parametrize(
    ("reason", "object_target"),
    (
        ("system_disabled", False),
        ("binding_disabled", False),
        ("capability_disabled", False),
        ("configured_scope_disabled", False),
        ("target_not_available", True),
        ("no_live_originating_scope", False),
    ),
)
def test_guard_cancellation_completes_attached_parent_intent(
    tmp_path, reason: str, object_target: bool
) -> None:
    store = SQLiteStore(tmp_path / f"{reason}.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = (
        RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.OBJECT, seeded.workspace_root_object_id),
            object_type="folder",
            facet="membership",
        )
        if object_target
        else _scope(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    )
    receipt = run(store.submit_refresh(_intent(scope, NOW)))
    admitted = run(DurableCoordinator(store, worker_id=reason).run_once(now=NOW))
    assert admitted is not None and admitted.action_id is not None
    lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=NOW))
    assert lease is not None
    if reason == "system_disabled":
        store.set_system_enabled(seeded.system.system_id, enabled=False, now=NOW)
    elif reason == "binding_disabled":
        store._connection.execute(
            "UPDATE connection_bindings SET enabled = 0 WHERE binding_id = ?",
            (seeded.connection_binding_id,),
        )
    elif reason == "capability_disabled":
        store._connection.execute(
            "UPDATE capability_bindings SET enabled = 0 WHERE connection_binding_id = ?",
            (seeded.connection_binding_id,),
        )
    elif reason == "configured_scope_disabled":
        store._connection.execute(
            "UPDATE configured_scopes SET enabled = 0 WHERE scope_id = ?",
            (seeded.workspace_root_scope.scope_id,),
        )
    elif reason == "target_not_available":
        store._connection.execute(
            "UPDATE remote_objects SET presence = 'absent' WHERE object_id = ?",
            (seeded.workspace_root_object_id,),
        )
    else:
        store._connection.execute(
            "UPDATE refresh_intent_scopes SET state = 'cancelled' WHERE intent_id = ?",
            (receipt.intent_id,),
        )
    decision = run(
        store.evaluate(action_id=lease.action.action_id, lease_id=lease.lease_id, now=NOW)
    )
    assert decision.disposition.value == "cancel"
    aggregate = store._connection.execute(
        "SELECT aggregate_state FROM refresh_intents WHERE intent_id = ?", (receipt.intent_id,)
    ).fetchone()[0]
    assert aggregate == "complete"


def test_guard_expires_dead_action_and_requeues_still_live_coalesced_scope(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "deadline.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = _scope(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    expiring = run(store.submit_refresh(_intent(scope, NOW, expires_at=NOW + timedelta(seconds=2))))
    admitted = run(DurableCoordinator(store, worker_id="first").run_once(now=NOW))
    assert admitted is not None and admitted.action_id is not None
    still_live = run(store.submit_refresh(_intent(scope, NOW + timedelta(seconds=1))))
    coalesced = run(
        DurableCoordinator(store, worker_id="coalesced").run_once(now=NOW + timedelta(seconds=1))
    )
    assert coalesced is not None and coalesced.state.value == "coalesced"
    assert coalesced.action_id == admitted.action_id
    lease = run(
        store.lease_next(
            adapter_key="databricks",
            worker_id="worker",
            now=NOW + timedelta(seconds=3),
        )
    )
    assert lease is not None

    decision = run(
        store.evaluate(
            action_id=lease.action.action_id,
            lease_id=lease.lease_id,
            now=NOW + timedelta(seconds=3),
        )
    )

    assert decision.disposition.value == "cancel"
    assert decision.reason == "action_deadline_expired"
    assert store.get_stored_action(admitted.action_id).state.value == "cancelled"
    expired_scope = store.list_intent_scopes(expiring.intent_id)[0]
    live_scope = store.list_intent_scopes(still_live.intent_id)[0]
    assert expired_scope.state.value == "expired"
    assert live_scope.state.value == "queued"
    assert live_scope.linked_action_id is None
    replacement = run(
        DurableCoordinator(store, worker_id="replacement").run_once(now=NOW + timedelta(seconds=3))
    )
    assert replacement is not None and replacement.state.value == "admitted"
    assert replacement.action_id != admitted.action_id


def test_guard_expires_coalesced_scope_without_cancelling_live_action(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "coalesced-expiry.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = _scope(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    live = run(store.submit_refresh(_intent(scope, NOW)))
    admitted = run(DurableCoordinator(store, worker_id="first").run_once(now=NOW))
    assert admitted is not None and admitted.action_id is not None
    expiring = run(
        store.submit_refresh(
            _intent(
                scope,
                NOW + timedelta(seconds=1),
                expires_at=NOW + timedelta(seconds=2),
            )
        )
    )
    coalesced = run(
        DurableCoordinator(store, worker_id="coalesced").run_once(now=NOW + timedelta(seconds=1))
    )
    assert coalesced is not None and coalesced.state.value == "coalesced"
    lease = run(
        store.lease_next(
            adapter_key="databricks",
            worker_id="worker",
            now=NOW + timedelta(seconds=3),
        )
    )
    assert lease is not None

    decision = run(
        store.evaluate(
            action_id=lease.action.action_id,
            lease_id=lease.lease_id,
            now=NOW + timedelta(seconds=3),
        )
    )

    assert decision.disposition.value == "dispatch"
    live_scope = store.list_intent_scopes(live.intent_id)[0]
    expired_scope = store.list_intent_scopes(expiring.intent_id)[0]
    assert live_scope.state.value == "admitted"
    assert expired_scope.state.value == "expired"
    assert expired_scope.linked_action_id == admitted.action_id
    aggregate = store._connection.execute(
        "SELECT aggregate_state FROM refresh_intents WHERE intent_id = ?",
        (expiring.intent_id,),
    ).fetchone()[0]
    assert aggregate == "complete"


def test_guard_satisfaction_completes_attached_parent_intent(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "satisfy.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = _scope(seeded.system.system_id, seeded.workspace_root_scope.scope_id)
    receipt = run(store.submit_refresh(_intent(scope, NOW)))
    admitted = run(DurableCoordinator(store, worker_id="admit").run_once(now=NOW))
    assert admitted is not None and admitted.action_id is not None
    lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=NOW))
    assert lease is not None
    evidence = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW + timedelta(seconds=1),
        received_at=NOW + timedelta(seconds=1),
        coverage=(CoverageDeclaration(scope=scope, completeness=CollectionCoverage.COMPLETE),),
    )
    assert run(store.ingest(evidence)).status.value == "accepted"
    decision = run(
        store.evaluate(
            action_id=lease.action.action_id,
            lease_id=lease.lease_id,
            now=NOW + timedelta(seconds=1),
        )
    )
    assert decision.disposition.value == "satisfy"
    aggregate = store._connection.execute(
        "SELECT aggregate_state FROM refresh_intents WHERE intent_id = ?", (receipt.intent_id,)
    ).fetchone()[0]
    assert aggregate == "complete"
