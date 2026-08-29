import json
from dataclasses import fields
from datetime import UTC, datetime, timedelta, timezone
from inspect import Parameter, signature
from uuid import uuid4

import pytest

from async_api_view.contracts import (
    CONTRACT_VERSION,
    V1_TYPE_DEFINITION_BY_KEY,
    AbsenceAuthority,
    ActionAttempt,
    ActionCompletion,
    ActionLease,
    ActionLifecyclePort,
    ActionOutcome,
    ActionRecord,
    AdapterAction,
    CapabilityBinding,
    CapabilityCoveragePolicy,
    CollectionCoverage,
    ConnectionBinding,
    CoverageDeclaration,
    ErrorClass,
    FacetObservation,
    FacetState,
    FieldCoverage,
    GuardDecision,
    IngestionResult,
    IngestionStatus,
    KnowledgeState,
    ObjectLocator,
    ObjectTypeDefinition,
    ObservationBatch,
    OperationClass,
    RefreshCoverage,
    RefreshIntent,
    RefreshOrigin,
    RefreshScope,
    RelationshipObservation,
    TargetKind,
    TargetRef,
    UpdateMode,
)

NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)


def scope(*, facet: str = "metadata") -> RefreshScope:
    return RefreshScope(
        system_id=uuid4(),
        target=TargetRef(TargetKind.OBJECT, uuid4()),
        object_type="file",
        facet=facet,
        coverage=RefreshCoverage.FACET,
    )


def test_contracts_normalize_aware_times_and_are_json_serializable() -> None:
    refresh_scope = scope()
    observed_at = NOW.astimezone(timezone(timedelta(hours=-4)))
    observation = FacetObservation(
        observation_id=uuid4(),
        target=ObjectLocator(
            object_type="file",
            source_kind="databricks.workspace.file",
            external_key="/Shared/report.py",
            display_name="report.py",
        ),
        facet="metadata",
        facet_version="1",
        update_mode=UpdateMode.PATCH,
        field_coverage=FieldCoverage.PARTIAL,
        payload={"name": "report.py", "byte_size": 42},
        field_mask=("name", "byte_size"),
        satisfies=(refresh_scope,),
    )
    batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=refresh_scope.system_id,
        connection_binding_id=uuid4(),
        adapter_key="databricks",
        adapter_version="0.1.0",
        observed_at=observed_at,
        received_at=NOW,
        facet_observations=(observation,),
        coverage=(
            CoverageDeclaration(
                scope=refresh_scope,
                completeness=CollectionCoverage.PARTIAL,
            ),
        ),
    )

    encoded = json.dumps(batch.to_dict())

    assert batch.observed_at == NOW
    assert '"contract_version": "1"' in encoded
    assert '"observed_at": "2026-08-24T12:00:00Z"' in encoded


def test_refresh_scope_serializes_an_optional_registered_capability_selector() -> None:
    selected = RefreshScope(
        system_id=uuid4(),
        target=TargetRef(TargetKind.OBJECT, uuid4()),
        object_type="generic_object",
        facet="attributes",
        capability_key="databricks.uc.volumes.read",
    )
    defaulted = RefreshScope(
        system_id=uuid4(),
        target=TargetRef(TargetKind.OBJECT, uuid4()),
        object_type="generic_object",
        facet="attributes",
    )

    assert selected.to_dict()["capability_key"] == "databricks.uc.volumes.read"
    assert defaulted.capability_key is None
    assert defaulted.to_dict()["capability_key"] is None

    with pytest.raises(ValueError, match="capability_key"):
        RefreshScope(
            system_id=uuid4(),
            target=TargetRef(TargetKind.OBJECT, uuid4()),
            object_type="generic_object",
            facet="attributes",
            capability_key="databricks api --raw",
        )


def test_contracts_reject_naive_time_and_non_json_payload() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        ObservationBatch(
            batch_id=uuid4(),
            system_id=uuid4(),
            connection_binding_id=uuid4(),
            adapter_key="databricks",
            adapter_version="1",
            observed_at=datetime(2026, 8, 24),
            received_at=NOW,
        )

    with pytest.raises(ValueError, match="JSON-compatible"):
        FacetObservation(
            observation_id=uuid4(),
            target=ObjectLocator(object_type="file", object_id=uuid4()),
            facet="metadata",
            facet_version="1",
            update_mode=UpdateMode.SNAPSHOT,
            field_coverage=FieldCoverage.COMPLETE,
            payload={"unsafe": object()},
        )


def test_partial_observation_requires_explicit_matching_field_mask() -> None:
    with pytest.raises(ValueError, match="field_mask"):
        FacetObservation(
            observation_id=uuid4(),
            target=ObjectLocator(object_type="file", object_id=uuid4()),
            facet="metadata",
            facet_version="1",
            update_mode=UpdateMode.SNAPSHOT,
            field_coverage=FieldCoverage.PARTIAL,
            payload={"name": "report.py"},
        )


def test_contract_models_reject_primitive_enum_values() -> None:
    refresh_scope = scope()
    locator = ObjectLocator(object_type="file", object_id=uuid4())

    with pytest.raises(ValueError, match="TargetKind"):
        TargetRef("object", uuid4())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="RefreshCoverage"):
        RefreshScope(
            system_id=refresh_scope.system_id,
            target=refresh_scope.target,
            object_type="file",
            facet="metadata",
            coverage="facet",  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="RefreshOrigin"):
        RefreshIntent(
            intent_id=uuid4(),
            idempotency_key="primitive-origin",
            origin="automatic",  # type: ignore[arg-type]
            actor_id="local-user",
            scopes=(refresh_scope,),
            requested_at=NOW,
        )
    with pytest.raises(ValueError, match="CollectionCoverage"):
        CoverageDeclaration(
            refresh_scope,
            "complete",  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="AbsenceAuthority"):
        CoverageDeclaration(
            refresh_scope,
            CollectionCoverage.COMPLETE,
            ("relationship",),  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="UpdateMode"):
        FacetObservation(
            observation_id=uuid4(),
            target=locator,
            facet="metadata",
            facet_version="1",
            update_mode="snapshot",  # type: ignore[arg-type]
            field_coverage=FieldCoverage.COMPLETE,
        )
    with pytest.raises(ValueError, match="PresenceState"):
        RelationshipObservation(
            observation_id=uuid4(),
            subject=locator,
            predicate="contains",
            object=locator,
            presence="present",  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="GuardDisposition"):
        GuardDecision("dispatch", "dispatch")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="IngestionStatus"):
        IngestionResult(uuid4(), "accepted")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="OperationClass"):
        CapabilityBinding(
            capability_binding_id=uuid4(),
            connection_binding_id=uuid4(),
            capability_key="example.read",
            capability_version="1",
            operation_class="observe",  # type: ignore[arg-type]
            target_kinds=(TargetKind.OBJECT,),
            produced_facets=("metadata",),
            enabled=True,
        )
    with pytest.raises(ValueError, match="TargetKind"):
        CapabilityBinding(
            capability_binding_id=uuid4(),
            connection_binding_id=uuid4(),
            capability_key="example.read",
            capability_version="1",
            operation_class=OperationClass.OBSERVE,
            target_kinds=("object",),  # type: ignore[arg-type]
            produced_facets=("metadata",),
            enabled=True,
        )
    with pytest.raises(ValueError, match="ActionState"):
        ActionRecord(uuid4(), "ready", NOW)  # type: ignore[arg-type]


def test_contract_models_reject_raw_nested_dto_values() -> None:
    refresh_scope = scope()
    with pytest.raises(ValueError, match="facet_observations must be"):
        ObservationBatch(
            batch_id=uuid4(),
            system_id=refresh_scope.system_id,
            connection_binding_id=uuid4(),
            adapter_key="databricks",
            adapter_version="1",
            observed_at=NOW,
            received_at=NOW,
            facet_observations=({"observation_id": str(uuid4())},),  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="scopes must be"):
        RefreshIntent(
            intent_id=uuid4(),
            idempotency_key="raw-scope",
            origin=RefreshOrigin.MANUAL,
            actor_id="local-user",
            scopes=({"facet": "metadata"},),  # type: ignore[arg-type]
            requested_at=NOW,
        )
    with pytest.raises(ValueError, match="target must be"):
        AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=refresh_scope.system_id,
            connection_binding_id=uuid4(),
            adapter_key="databricks",
            adapter_version="1",
            capability_key="example.read",
            capability_version="1",
            target={"kind": "object"},  # type: ignore[arg-type]
            requested_scopes=(refresh_scope,),
        )
    with pytest.raises(ValueError, match="action must be"):
        ActionLease(  # type: ignore[arg-type]
            action={"action_id": str(uuid4())},
            lease_id=uuid4(),
            leased_until=NOW,
        )
    with pytest.raises(ValueError, match="facets must be"):
        ObjectTypeDefinition(
            type_key="file",
            version="1",
            facets=({"facet": "metadata"},),  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="coverage_policies must be"):
        CapabilityBinding(
            capability_binding_id=uuid4(),
            connection_binding_id=uuid4(),
            capability_key="example.read",
            capability_version="1",
            operation_class=OperationClass.OBSERVE,
            target_kinds=(TargetKind.OBJECT,),
            produced_facets=("metadata",),
            enabled=True,
            coverage_policies=({"facet": "metadata"},),  # type: ignore[arg-type]
        )


def test_coverage_policy_rejects_absence_without_complete_authority() -> None:
    with pytest.raises(ValueError, match="complete maximum coverage"):
        CapabilityCoveragePolicy(
            target_kind=TargetKind.OBJECT,
            facet="metadata",
            coverage=RefreshCoverage.FACET,
            maximum_completeness=CollectionCoverage.PARTIAL,
            absence_authority=(AbsenceAuthority.OBJECT_PRESENCE,),
        )


def test_contract_models_reject_boolean_integer_coercion() -> None:
    refresh_scope = scope()
    with pytest.raises(ValueError, match="priority must be an integer"):
        RefreshIntent(
            intent_id=uuid4(),
            idempotency_key="boolean-priority",
            origin=RefreshOrigin.MANUAL,
            actor_id="local-user",
            scopes=(refresh_scope,),
            requested_at=NOW,
            priority=True,  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="issue_count must be an integer"):
        IngestionResult(uuid4(), status=IngestionStatus.ACCEPTED, issue_count=True)
    with pytest.raises(ValueError, match="enabled must be a boolean"):
        ConnectionBinding(
            binding_id=uuid4(),
            system_id=uuid4(),
            adapter_key="databricks",
            adapter_version="1",
            enabled=1,  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="enabled must be a boolean"):
        CapabilityBinding(
            capability_binding_id=uuid4(),
            connection_binding_id=uuid4(),
            capability_key="example.read",
            capability_version="1",
            operation_class=OperationClass.OBSERVE,
            target_kinds=(TargetKind.OBJECT,),
            produced_facets=("metadata",),
            enabled=1,  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="selection_priority must be an integer"):
        CapabilityBinding(
            capability_binding_id=uuid4(),
            connection_binding_id=uuid4(),
            capability_key="example.read",
            capability_version="1",
            operation_class=OperationClass.OBSERVE,
            target_kinds=(TargetKind.OBJECT,),
            produced_facets=("metadata",),
            enabled=True,
            selection_priority=False,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("invalid", [[], "text", 1])
def test_mapping_contract_fields_require_json_objects(invalid: object) -> None:
    with pytest.raises(ValueError, match="payload must be a JSON object"):
        FacetObservation(
            observation_id=uuid4(),
            target=ObjectLocator(object_type="file", object_id=uuid4()),
            facet="metadata",
            facet_version="1",
            update_mode=UpdateMode.SNAPSHOT,
            field_coverage=FieldCoverage.COMPLETE,
            payload=invalid,  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="payload must be a JSON object"):
        FacetState(
            object_id=uuid4(),
            facet="metadata",
            facet_version="1",
            knowledge=KnowledgeState.KNOWN,
            payload=invalid,  # type: ignore[arg-type]
            observed_at=NOW,
            state_changed_at=NOW,
        )
    with pytest.raises(ValueError, match="non_secret_settings must be a JSON object"):
        ConnectionBinding(
            binding_id=uuid4(),
            system_id=uuid4(),
            adapter_key="databricks",
            adapter_version="1",
            enabled=True,
            non_secret_settings=invalid,  # type: ignore[arg-type]
        )


def test_dispatch_and_intent_have_no_command_secret_or_force_fields() -> None:
    action_fields = {item.name for item in fields(AdapterAction)}
    intent_fields = {item.name for item in fields(RefreshIntent)}

    assert not action_fields & {"command", "arguments", "endpoint", "token", "secret"}
    assert "force" not in intent_fields


def test_action_lifecycle_writes_require_keyword_only_lease_authority() -> None:
    for method in (
        ActionLifecyclePort.record_attempt,
        ActionLifecyclePort.complete_action,
    ):
        parameters = signature(method).parameters
        assert "lease_id" in parameters
        assert parameters["lease_id"].kind is Parameter.KEYWORD_ONLY
        assert parameters["lease_id"].annotation == "str"


def test_action_attempt_rejects_invalid_terminal_and_retry_combinations() -> None:
    attempt_id = uuid4()
    action_id = uuid4()
    ended_at = NOW + timedelta(seconds=1)

    with pytest.raises(ValueError, match="requires ended_at"):
        ActionAttempt(
            attempt_id,
            action_id,
            1,
            NOW,
            outcome=ActionOutcome.SUCCEEDED,
        )
    with pytest.raises(ValueError, match="requires an error_class"):
        ActionAttempt(
            attempt_id,
            action_id,
            1,
            NOW,
            ended_at,
            ActionOutcome.FAILED,
        )
    with pytest.raises(ValueError, match="only a failed attempt"):
        ActionAttempt(
            attempt_id,
            action_id,
            1,
            NOW,
            ended_at,
            ActionOutcome.SUCCEEDED,
            ErrorClass.CONNECTION_TIMEOUT,
        )
    with pytest.raises(ValueError, match="retry_at must follow"):
        ActionAttempt(
            attempt_id,
            action_id,
            1,
            NOW,
            ended_at,
            ActionOutcome.FAILED,
            ErrorClass.CONNECTION_TIMEOUT,
            retry_at=ended_at,
        )
    with pytest.raises(ValueError, match="ended failed attempt"):
        ActionAttempt(
            attempt_id,
            action_id,
            1,
            NOW,
            ended_at,
            ActionOutcome.SUCCEEDED,
            retry_at=ended_at + timedelta(seconds=1),
        )
    with pytest.raises(ValueError, match="ActionOutcome"):
        ActionAttempt(
            attempt_id,
            action_id,
            1,
            NOW,
            ended_at,
            "failed",  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="ErrorClass"):
        ActionAttempt(
            attempt_id,
            action_id,
            1,
            NOW,
            ended_at,
            ActionOutcome.FAILED,
            "connection_timeout",  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="positive integer"):
        ActionAttempt(attempt_id, action_id, True, NOW)  # type: ignore[arg-type]


def test_action_completion_rejects_primitive_enums_and_non_failure_errors() -> None:
    with pytest.raises(ValueError, match="ActionOutcome"):
        ActionCompletion(
            action_id=uuid4(),
            outcome="failed",  # type: ignore[arg-type]
            completed_at=NOW,
        )
    with pytest.raises(ValueError, match="ErrorClass"):
        ActionCompletion(
            action_id=uuid4(),
            outcome=ActionOutcome.FAILED,
            completed_at=NOW,
            error_class="connection_timeout",  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="only a failed action"):
        ActionCompletion(
            action_id=uuid4(),
            outcome=ActionOutcome.SUCCEEDED,
            completed_at=NOW,
            error_class=ErrorClass.CONNECTION_TIMEOUT,
        )
    with pytest.raises(ValueError, match="cannot schedule a retry"):
        ActionCompletion(
            action_id=uuid4(),
            outcome=ActionOutcome.FAILED,
            completed_at=NOW,
            error_class=ErrorClass.CONNECTION_TIMEOUT,
            retry_at=NOW + timedelta(seconds=1),
        )


@pytest.mark.parametrize("ordinal", [True, 1.5, 0])
def test_action_lease_requires_a_positive_integer_attempt_ordinal(
    ordinal: int | float | bool,
) -> None:
    refresh_scope = scope()
    action = AdapterAction(
        action_id=uuid4(),
        correlation_id=uuid4(),
        system_id=refresh_scope.system_id,
        connection_binding_id=uuid4(),
        adapter_key="databricks",
        adapter_version="1",
        capability_key="databricks.workspace.metadata.read",
        capability_version="1",
        target=refresh_scope.target,
        requested_scopes=(refresh_scope,),
    )

    with pytest.raises(ValueError, match="attempt ordinal"):
        ActionLease(action, uuid4(), NOW, attempt_ordinal=ordinal)


def test_manual_and_automatic_requests_share_the_same_versioned_shape() -> None:
    refresh_scope = scope()
    manual = RefreshIntent(
        intent_id=uuid4(),
        idempotency_key="manual-1",
        origin=RefreshOrigin.MANUAL,
        actor_id="local-user",
        scopes=(refresh_scope,),
        requested_at=NOW,
    )
    automatic = RefreshIntent(
        intent_id=uuid4(),
        idempotency_key="auto-1",
        origin=RefreshOrigin.AUTOMATIC,
        actor_id="local-user",
        ui_session_id=uuid4(),
        scopes=(refresh_scope,),
        requested_at=NOW,
    )

    assert manual.contract_version == automatic.contract_version == CONTRACT_VERSION
    assert set(manual.to_dict()) == set(automatic.to_dict())


def test_v1_defaults_cover_workspace_and_unity_catalog_metadata() -> None:
    assert V1_TYPE_DEFINITION_BY_KEY["folder"].facets[0].facet == "metadata"
    assert {facet.facet for facet in V1_TYPE_DEFINITION_BY_KEY["file"].facets} == {
        "metadata",
        "content",
    }
    assert V1_TYPE_DEFINITION_BY_KEY["generic_object"].facets[0].facet == "attributes"
    assert ActionOutcome.FAILED.value == "failed"
