from dataclasses import fields
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from async_api_view.contracts import (
    PolicyLevel,
    QualifyingObservation,
    RefreshCoverage,
    RefreshDecisionKind,
    RefreshIntent,
    RefreshIntervalOverride,
    RefreshScope,
    ScopePolicyState,
    TargetKind,
    TargetRef,
)
from async_api_view.core import (
    decide_refresh,
    default_interval_map,
    evaluate_eligibility,
    evidence_satisfies,
    resolve_refresh_interval,
)

NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)
SYSTEM_ID = str(uuid4())
OBJECT_ID = str(uuid4())


def scope(
    *,
    facet: str = "metadata",
    target_id: str = OBJECT_ID,
    capability_key: str | None = None,
) -> RefreshScope:
    return RefreshScope(
        system_id=SYSTEM_ID,
        target=TargetRef(TargetKind.OBJECT, target_id),
        object_type="file",
        facet=facet,
        capability_key=capability_key,
        coverage=RefreshCoverage.FACET,
    )


def test_interval_precedence_is_object_then_system_then_type_default() -> None:
    overrides = (
        RefreshIntervalOverride("system", SYSTEM_ID, timedelta(hours=8)),
        RefreshIntervalOverride("system", SYSTEM_ID, timedelta(hours=6), "metadata"),
        RefreshIntervalOverride("object", OBJECT_ID, timedelta(hours=4)),
        RefreshIntervalOverride("object", OBJECT_ID, timedelta(hours=2), "metadata"),
    )

    effective = resolve_refresh_interval(
        object_id=OBJECT_ID,
        system_id=SYSTEM_ID,
        object_type="file",
        facet="metadata",
        overrides=overrides,
    )

    assert effective.interval == timedelta(hours=2)
    assert effective.source_level is PolicyLevel.OBJECT
    assert effective.facet_specific

    without_object = resolve_refresh_interval(
        object_id=OBJECT_ID,
        system_id=SYSTEM_ID,
        object_type="file",
        facet="metadata",
        overrides=overrides[:2],
    )
    assert without_object.interval == timedelta(hours=6)
    assert without_object.source_level is PolicyLevel.SYSTEM

    type_default = resolve_refresh_interval(
        object_id=OBJECT_ID,
        system_id=SYSTEM_ID,
        object_type="file",
        facet="content",
    )
    assert type_default.interval == timedelta(days=7)
    assert type_default.source_level is PolicyLevel.TYPE_DEFAULT


def test_never_observed_and_never_started_scope_is_immediately_eligible() -> None:
    result = evaluate_eligibility(
        now=NOW,
        minimum_interval=timedelta(days=1),
        state=ScopePolicyState(scope()),
    )

    assert result.eligible
    assert result.eligible_at == NOW
    assert result.fresh_until is None


def test_failed_action_start_still_consumes_cooldown_without_freshening_evidence() -> None:
    action_started = NOW - timedelta(hours=1)
    result = evaluate_eligibility(
        now=NOW,
        minimum_interval=timedelta(days=1),
        state=ScopePolicyState(
            scope(),
            latest_qualifying_observation_at=NOW - timedelta(days=10),
            latest_targeted_action_started_at=action_started,
        ),
    )

    assert not result.eligible
    assert result.eligible_at == action_started + timedelta(days=1)
    assert result.fresh_until == NOW - timedelta(days=9)


def test_incidental_evidence_after_request_satisfies_even_during_action_cooldown() -> None:
    requested_at = NOW - timedelta(minutes=15)
    incidental = QualifyingObservation(
        observation_id=uuid4(),
        scope=scope(),
        observed_at=NOW - timedelta(minutes=5),
    )
    state = ScopePolicyState(
        scope(),
        latest_qualifying_observation_at=incidental.observed_at,
        latest_targeted_action_started_at=NOW - timedelta(minutes=10),
    )

    decision = decide_refresh(
        requested_scope=scope(),
        requested_at=requested_at,
        now=NOW,
        minimum_interval=timedelta(days=1),
        state=state,
        evidence=incidental,
    )

    assert not decision.eligibility.eligible
    assert decision.kind is RefreshDecisionKind.SATISFIED
    assert decision.satisfying_observation_id == incidental.observation_id


def test_due_or_wrong_coverage_evidence_does_not_satisfy() -> None:
    requested = scope(facet="content")
    metadata_evidence = QualifyingObservation(
        observation_id=uuid4(),
        scope=scope(facet="metadata"),
        observed_at=NOW,
    )
    old_content = QualifyingObservation(
        observation_id=uuid4(),
        scope=requested,
        observed_at=NOW - timedelta(days=8),
    )

    assert not evidence_satisfies(
        requested_scope=requested,
        requested_at=NOW,
        now=NOW,
        minimum_interval=timedelta(days=7),
        evidence=metadata_evidence,
    )
    assert not evidence_satisfies(
        requested_scope=requested,
        requested_at=NOW,
        now=NOW,
        minimum_interval=timedelta(days=7),
        evidence=old_content,
    )


def test_evidence_from_one_registered_capability_cannot_satisfy_another() -> None:
    relations_scope = scope(capability_key="databricks.uc.relations.read")
    volume_evidence = QualifyingObservation(
        observation_id=uuid4(),
        scope=scope(capability_key="databricks.uc.volumes.read"),
        observed_at=NOW,
    )

    assert not evidence_satisfies(
        requested_scope=relations_scope,
        requested_at=NOW,
        now=NOW,
        minimum_interval=timedelta(days=7),
        evidence=volume_evidence,
    )


def test_no_refresh_bypass_exists_and_decision_has_no_origin_parameter() -> None:
    intent_names = {item.name for item in fields(RefreshIntent)}
    assert "force" not in intent_names

    state = ScopePolicyState(scope(), latest_targeted_action_started_at=NOW - timedelta(hours=1))
    decision = decide_refresh(
        requested_scope=scope(),
        requested_at=NOW,
        now=NOW,
        minimum_interval=timedelta(days=1),
        state=state,
    )
    assert decision.kind is RefreshDecisionKind.DEFER


def test_v1_default_interval_table_matches_accepted_values() -> None:
    defaults = default_interval_map()
    assert defaults == {
        ("folder", "metadata"): timedelta(days=1),
        ("folder", "membership"): timedelta(days=1),
        ("file", "metadata"): timedelta(days=1),
        ("file", "content"): timedelta(days=7),
        ("service", "runtime"): timedelta(days=1),
        ("service", "configuration"): timedelta(days=7),
        ("job", "metadata"): timedelta(days=1),
        ("job", "status"): timedelta(days=1),
        ("job", "run_summary"): timedelta(days=1),
        ("generic_object", "attributes"): timedelta(days=7),
    }
