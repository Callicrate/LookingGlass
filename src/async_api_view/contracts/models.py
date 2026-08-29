"""Standard-library-only versioned contracts shared by all application layers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from uuid import UUID

from ._validation import (
    JSONDTO,
    JSONValue,
    normalize_enum_tuple,
    normalize_instance_tuple,
    optional_utc,
    optional_uuid,
    require_bool,
    require_contract_key,
    require_enum,
    require_instance,
    require_int,
    require_positive_duration,
    require_text,
    require_utc,
    require_uuid,
    validate_json_mapping,
)
from .enums import (
    AbsenceAuthority,
    ActionOutcome,
    ActionState,
    CollectionCoverage,
    ErrorClass,
    FieldCoverage,
    GuardDisposition,
    IngestionStatus,
    KnowledgeState,
    OperationClass,
    PresenceState,
    RefreshCoverage,
    RefreshOrigin,
    TargetKind,
    UpdateMode,
)

CONTRACT_VERSION = "1"


def _set_uuid(instance: object, name: str) -> None:
    object.__setattr__(instance, name, require_uuid(getattr(instance, name), name))


def _set_optional_uuid(instance: object, name: str) -> None:
    object.__setattr__(instance, name, optional_uuid(getattr(instance, name), name))


def _set_utc(instance: object, name: str) -> None:
    object.__setattr__(instance, name, require_utc(getattr(instance, name), name))


def _set_optional_utc(instance: object, name: str) -> None:
    object.__setattr__(instance, name, optional_utc(getattr(instance, name), name))


def _check_version(value: str) -> None:
    if value != CONTRACT_VERSION:
        raise ValueError(f"unsupported contract_version {value!r}")


@dataclass(frozen=True, slots=True)
class TargetRef(JSONDTO):
    kind: TargetKind
    target_id: str

    def __post_init__(self) -> None:
        require_enum(self.kind, TargetKind, "kind")
        _set_uuid(self, "target_id")


@dataclass(frozen=True, slots=True)
class ObjectLocator(JSONDTO):
    """Locates an existing object or supplies adapter-owned external identity."""

    object_type: str
    object_type_version: str = CONTRACT_VERSION
    object_id: str | UUID | None = None
    source_kind: str | None = None
    external_key: str | None = None
    display_name: str | None = None

    def __post_init__(self) -> None:
        require_contract_key(self.object_type, "object_type")
        require_text(self.object_type_version, "object_type_version", max_length=32)
        _set_optional_uuid(self, "object_id")
        if self.object_id is None and (self.source_kind is None or self.external_key is None):
            raise ValueError("a locator requires object_id or both source_kind and external_key")
        if self.source_kind is not None:
            require_contract_key(self.source_kind, "source_kind")
        if self.external_key is not None:
            require_text(self.external_key, "external_key", max_length=4096)
        if self.display_name is not None:
            require_text(self.display_name, "display_name", max_length=1024)


@dataclass(frozen=True, slots=True)
class RemoteObject(JSONDTO):
    object_id: str | UUID
    system_id: str | UUID
    object_type: str
    object_type_version: str
    source_kind: str
    external_key: str
    display_name: str
    presence: PresenceState
    first_seen_at: datetime
    last_seen_at: datetime | None = None

    def __post_init__(self) -> None:
        _set_uuid(self, "object_id")
        _set_uuid(self, "system_id")
        require_contract_key(self.object_type, "object_type")
        require_text(self.object_type_version, "object_type_version", max_length=32)
        require_contract_key(self.source_kind, "source_kind")
        require_text(self.external_key, "external_key", max_length=4096)
        require_text(self.display_name, "display_name", max_length=1024)
        require_enum(self.presence, PresenceState, "presence")
        _set_utc(self, "first_seen_at")
        _set_optional_utc(self, "last_seen_at")
        if self.last_seen_at is not None and self.last_seen_at < self.first_seen_at:
            raise ValueError("last_seen_at must not precede first_seen_at")


@dataclass(frozen=True, slots=True)
class FacetState(JSONDTO):
    object_id: str | UUID
    facet: str
    facet_version: str
    knowledge: KnowledgeState
    payload: Mapping[str, JSONValue]
    observed_at: datetime | None
    state_changed_at: datetime
    supporting_observation_id: str | UUID | None = None
    source_revision: str | None = None

    def __post_init__(self) -> None:
        _set_uuid(self, "object_id")
        require_contract_key(self.facet, "facet")
        require_text(self.facet_version, "facet_version", max_length=32)
        require_enum(self.knowledge, KnowledgeState, "knowledge")
        object.__setattr__(self, "payload", validate_json_mapping(self.payload, "payload"))
        _set_optional_utc(self, "observed_at")
        _set_utc(self, "state_changed_at")
        _set_optional_uuid(self, "supporting_observation_id")
        if self.source_revision is not None:
            require_text(self.source_revision, "source_revision", max_length=512)


@dataclass(frozen=True, slots=True)
class RelationshipState(JSONDTO):
    relationship_id: str | UUID
    system_id: str | UUID
    subject_id: str | UUID
    predicate: str
    object_id: str | UUID
    presence: PresenceState
    observed_at: datetime
    supporting_observation_id: str | UUID

    def __post_init__(self) -> None:
        for name in (
            "relationship_id",
            "system_id",
            "subject_id",
            "object_id",
            "supporting_observation_id",
        ):
            _set_uuid(self, name)
        require_contract_key(self.predicate, "predicate")
        require_enum(self.presence, PresenceState, "presence")
        _set_utc(self, "observed_at")


@dataclass(frozen=True, slots=True)
class RefreshScope(JSONDTO):
    system_id: str | UUID
    target: TargetRef
    object_type: str
    facet: str
    capability_key: str | None = None
    coverage: RefreshCoverage = RefreshCoverage.FACET
    field_mask: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _set_uuid(self, "system_id")
        require_instance(self.target, TargetRef, "target")
        require_contract_key(self.object_type, "object_type")
        require_contract_key(self.facet, "facet")
        require_enum(self.coverage, RefreshCoverage, "coverage")
        if self.capability_key is not None:
            require_contract_key(self.capability_key, "capability_key")
        normalized_fields = tuple(dict.fromkeys(self.field_mask))
        for name in normalized_fields:
            require_contract_key(name, "field_mask item")
        object.__setattr__(self, "field_mask", normalized_fields)


@dataclass(frozen=True, slots=True)
class RefreshIntent(JSONDTO):
    """The single request shape used by both manual and automatic refresh."""

    intent_id: str | UUID
    idempotency_key: str
    origin: RefreshOrigin
    actor_id: str
    scopes: tuple[RefreshScope, ...]
    requested_at: datetime
    ui_session_id: str | UUID | None = None
    expires_at: datetime | None = None
    priority: int = 0
    contract_version: str = CONTRACT_VERSION

    def __post_init__(self) -> None:
        _check_version(self.contract_version)
        _set_uuid(self, "intent_id")
        require_text(self.idempotency_key, "idempotency_key", max_length=512)
        require_text(self.actor_id, "actor_id", max_length=512)
        object.__setattr__(
            self,
            "scopes",
            normalize_instance_tuple(self.scopes, RefreshScope, "scopes"),
        )
        require_enum(self.origin, RefreshOrigin, "origin")
        if not self.scopes:
            raise ValueError("scopes must contain at least one refresh scope")
        _set_utc(self, "requested_at")
        _set_optional_uuid(self, "ui_session_id")
        _set_optional_utc(self, "expires_at")
        if self.origin is RefreshOrigin.AUTOMATIC and self.ui_session_id is None:
            raise ValueError("automatic refresh requires a live ui_session_id")
        if self.expires_at is not None and self.expires_at <= self.requested_at:
            raise ValueError("expires_at must be later than requested_at")
        require_int(self.priority, "priority", minimum=0, maximum=100)


@dataclass(frozen=True, slots=True)
class RefreshReceipt(JSONDTO):
    intent_id: str | UUID
    accepted_at: datetime
    scope_ids: tuple[str | UUID, ...]

    def __post_init__(self) -> None:
        _set_uuid(self, "intent_id")
        object.__setattr__(
            self,
            "scope_ids",
            tuple(require_uuid(value, "scope_id") for value in self.scope_ids),
        )
        _set_utc(self, "accepted_at")


@dataclass(frozen=True, slots=True)
class AdapterAction(JSONDTO):
    action_id: str | UUID
    correlation_id: str | UUID
    system_id: str | UUID
    connection_binding_id: str | UUID
    adapter_key: str
    adapter_version: str
    capability_key: str
    capability_version: str
    target: TargetRef
    requested_scopes: tuple[RefreshScope, ...]
    deadline: datetime | None = None
    contract_version: str = CONTRACT_VERSION

    def __post_init__(self) -> None:
        _check_version(self.contract_version)
        for name in (
            "action_id",
            "correlation_id",
            "system_id",
            "connection_binding_id",
        ):
            _set_uuid(self, name)
        require_contract_key(self.adapter_key, "adapter_key")
        require_text(self.adapter_version, "adapter_version", max_length=64)
        require_contract_key(self.capability_key, "capability_key")
        require_text(self.capability_version, "capability_version", max_length=64)
        require_instance(self.target, TargetRef, "target")
        object.__setattr__(
            self,
            "requested_scopes",
            normalize_instance_tuple(
                self.requested_scopes,
                RefreshScope,
                "requested_scopes",
            ),
        )
        if not self.requested_scopes:
            raise ValueError("requested_scopes must not be empty")
        if any(scope.system_id != self.system_id for scope in self.requested_scopes):
            raise ValueError("every requested scope must belong to the action system")
        _set_optional_utc(self, "deadline")


@dataclass(frozen=True, slots=True)
class CoverageDeclaration(JSONDTO):
    scope: RefreshScope
    completeness: CollectionCoverage
    absence_authority: tuple[AbsenceAuthority, ...] = ()

    def __post_init__(self) -> None:
        require_instance(self.scope, RefreshScope, "scope")
        require_enum(self.completeness, CollectionCoverage, "completeness")
        object.__setattr__(
            self,
            "absence_authority",
            normalize_enum_tuple(
                self.absence_authority,
                AbsenceAuthority,
                "absence_authority",
            ),
        )
        if self.completeness is not CollectionCoverage.COMPLETE and self.absence_authority:
            raise ValueError("absence authority requires complete collection coverage")


@dataclass(frozen=True, slots=True)
class FacetObservation(JSONDTO):
    observation_id: str | UUID
    target: ObjectLocator
    facet: str
    facet_version: str
    update_mode: UpdateMode
    field_coverage: FieldCoverage
    payload: Mapping[str, JSONValue] = field(default_factory=dict)
    field_mask: tuple[str, ...] = ()
    satisfies: tuple[RefreshScope, ...] = ()
    remote_as_of: datetime | None = None
    source_revision: str | None = None

    def __post_init__(self) -> None:
        _set_uuid(self, "observation_id")
        require_instance(self.target, ObjectLocator, "target")
        require_contract_key(self.facet, "facet")
        require_text(self.facet_version, "facet_version", max_length=32)
        require_enum(self.update_mode, UpdateMode, "update_mode")
        require_enum(self.field_coverage, FieldCoverage, "field_coverage")
        object.__setattr__(self, "payload", validate_json_mapping(self.payload, "payload"))
        normalized_mask = tuple(dict.fromkeys(self.field_mask))
        for name in normalized_mask:
            require_contract_key(name, "field_mask item")
        object.__setattr__(self, "field_mask", normalized_mask)
        object.__setattr__(
            self,
            "satisfies",
            normalize_instance_tuple(self.satisfies, RefreshScope, "satisfies"),
        )
        if self.update_mode is UpdateMode.PATCH and not self.field_mask:
            raise ValueError("a patch requires an explicit non-empty field_mask")
        if self.field_coverage is FieldCoverage.PARTIAL and not self.field_mask:
            raise ValueError("partial field coverage requires an explicit field_mask")
        if self.update_mode is UpdateMode.ABSENCE and self.payload:
            raise ValueError("an absence observation cannot contain facet payload")
        if self.field_mask and not set(self.field_mask).issubset(self.payload):
            raise ValueError("field_mask entries must be present in payload")
        _set_optional_utc(self, "remote_as_of")
        if self.source_revision is not None:
            require_text(self.source_revision, "source_revision", max_length=512)


@dataclass(frozen=True, slots=True)
class RelationshipObservation(JSONDTO):
    observation_id: str | UUID
    subject: ObjectLocator
    predicate: str
    object: ObjectLocator
    presence: PresenceState

    def __post_init__(self) -> None:
        _set_uuid(self, "observation_id")
        require_instance(self.subject, ObjectLocator, "subject")
        require_instance(self.object, ObjectLocator, "object")
        require_contract_key(self.predicate, "predicate")
        require_enum(self.presence, PresenceState, "presence")
        if self.presence is PresenceState.UNKNOWN:
            raise ValueError("a relationship observation must assert present or absent")


@dataclass(frozen=True, slots=True)
class ObservationBatch(JSONDTO):
    batch_id: str | UUID
    system_id: str | UUID
    connection_binding_id: str | UUID
    adapter_key: str
    adapter_version: str
    observed_at: datetime
    received_at: datetime
    facet_observations: tuple[FacetObservation, ...] = ()
    relationship_observations: tuple[RelationshipObservation, ...] = ()
    coverage: tuple[CoverageDeclaration, ...] = ()
    action_id: str | UUID | None = None
    contract_version: str = CONTRACT_VERSION

    def __post_init__(self) -> None:
        _check_version(self.contract_version)
        for name in ("batch_id", "system_id", "connection_binding_id"):
            _set_uuid(self, name)
        object.__setattr__(
            self,
            "facet_observations",
            normalize_instance_tuple(
                self.facet_observations,
                FacetObservation,
                "facet_observations",
            ),
        )
        object.__setattr__(
            self,
            "relationship_observations",
            normalize_instance_tuple(
                self.relationship_observations,
                RelationshipObservation,
                "relationship_observations",
            ),
        )
        object.__setattr__(
            self,
            "coverage",
            normalize_instance_tuple(self.coverage, CoverageDeclaration, "coverage"),
        )
        _set_optional_uuid(self, "action_id")
        require_contract_key(self.adapter_key, "adapter_key")
        require_text(self.adapter_version, "adapter_version", max_length=64)
        _set_utc(self, "observed_at")
        _set_utc(self, "received_at")
        if self.received_at < self.observed_at:
            raise ValueError("received_at must not precede observed_at")
        for declaration in self.coverage:
            if declaration.scope.system_id != self.system_id:
                raise ValueError("coverage scopes must belong to the batch system")


@dataclass(frozen=True, slots=True)
class ActionAttempt(JSONDTO):
    attempt_id: str | UUID
    action_id: str | UUID
    ordinal: int
    started_at: datetime
    ended_at: datetime | None = None
    outcome: ActionOutcome | None = None
    error_class: ErrorClass | None = None
    retry_at: datetime | None = None
    redacted_diagnostic: str | None = None

    def __post_init__(self) -> None:
        _set_uuid(self, "attempt_id")
        _set_uuid(self, "action_id")
        if isinstance(self.ordinal, bool) or not isinstance(self.ordinal, int) or self.ordinal < 1:
            raise ValueError("ordinal must be a positive integer")
        if self.outcome is not None and not isinstance(self.outcome, ActionOutcome):
            raise ValueError("outcome must be an ActionOutcome")
        if self.error_class is not None and not isinstance(self.error_class, ErrorClass):
            raise ValueError("error_class must be an ErrorClass")
        _set_utc(self, "started_at")
        _set_optional_utc(self, "ended_at")
        _set_optional_utc(self, "retry_at")
        if self.ended_at is not None and self.ended_at < self.started_at:
            raise ValueError("ended_at must not precede started_at")
        if self.outcome is not None and self.ended_at is None:
            raise ValueError("a completed attempt requires ended_at")
        if self.outcome is ActionOutcome.FAILED and self.error_class is None:
            raise ValueError("a failed attempt requires an error_class")
        if self.error_class is not None and self.outcome is not ActionOutcome.FAILED:
            raise ValueError("only a failed attempt may carry an error_class")
        if self.retry_at is not None:
            if self.outcome is not ActionOutcome.FAILED or self.ended_at is None:
                raise ValueError("a retry requires an ended failed attempt")
            if self.retry_at <= self.ended_at:
                raise ValueError("retry_at must follow ended_at")
        if self.redacted_diagnostic is not None:
            require_text(self.redacted_diagnostic, "redacted_diagnostic", max_length=4096)


@dataclass(frozen=True, slots=True)
class ActionCompletion(JSONDTO):
    action_id: str | UUID
    outcome: ActionOutcome
    completed_at: datetime
    error_class: ErrorClass | None = None
    redacted_diagnostic: str | None = None
    retry_at: datetime | None = None

    def __post_init__(self) -> None:
        _set_uuid(self, "action_id")
        if not isinstance(self.outcome, ActionOutcome):
            raise ValueError("outcome must be an ActionOutcome")
        if self.error_class is not None and not isinstance(self.error_class, ErrorClass):
            raise ValueError("error_class must be an ErrorClass")
        _set_utc(self, "completed_at")
        _set_optional_utc(self, "retry_at")
        if self.retry_at is not None:
            raise ValueError("a terminal action completion cannot schedule a retry")
        if self.outcome is ActionOutcome.FAILED and self.error_class is None:
            raise ValueError("a failed action requires an error_class")
        if self.error_class is not None and self.outcome is not ActionOutcome.FAILED:
            raise ValueError("only a failed action may carry an error_class")
        if self.redacted_diagnostic is not None:
            require_text(self.redacted_diagnostic, "redacted_diagnostic", max_length=4096)


@dataclass(frozen=True, slots=True)
class ActionLease(JSONDTO):
    action: AdapterAction
    lease_id: str | UUID
    leased_until: datetime
    attempt_ordinal: int = 1

    def __post_init__(self) -> None:
        require_instance(self.action, AdapterAction, "action")
        _set_uuid(self, "lease_id")
        _set_utc(self, "leased_until")
        if (
            isinstance(self.attempt_ordinal, bool)
            or not isinstance(self.attempt_ordinal, int)
            or self.attempt_ordinal < 1
        ):
            raise ValueError("attempt ordinal must be at least one")


@dataclass(frozen=True, slots=True)
class GuardDecision(JSONDTO):
    disposition: GuardDisposition
    reason: str
    satisfying_observation_ids: tuple[str | UUID, ...] = ()

    def __post_init__(self) -> None:
        require_enum(self.disposition, GuardDisposition, "disposition")
        require_contract_key(self.reason, "reason")
        object.__setattr__(
            self,
            "satisfying_observation_ids",
            tuple(
                require_uuid(value, "satisfying_observation_id")
                for value in self.satisfying_observation_ids
            ),
        )
        if self.disposition is GuardDisposition.SATISFY and not self.satisfying_observation_ids:
            raise ValueError("a satisfy decision requires supporting observation IDs")


@dataclass(frozen=True, slots=True)
class IngestionResult(JSONDTO):
    batch_id: str | UUID
    status: IngestionStatus
    accepted_observation_ids: tuple[str | UUID, ...] = ()
    issue_count: int = 0

    def __post_init__(self) -> None:
        _set_uuid(self, "batch_id")
        require_enum(self.status, IngestionStatus, "status")
        object.__setattr__(
            self,
            "accepted_observation_ids",
            tuple(
                require_uuid(value, "accepted_observation_id")
                for value in self.accepted_observation_ids
            ),
        )
        require_int(self.issue_count, "issue_count", minimum=0)


@dataclass(frozen=True, slots=True)
class ConnectionBinding(JSONDTO):
    binding_id: str | UUID
    system_id: str | UUID
    adapter_key: str
    adapter_version: str
    enabled: bool
    non_secret_settings: Mapping[str, JSONValue] = field(default_factory=dict)
    secret_reference: str | None = None
    revision: str | None = None

    def __post_init__(self) -> None:
        _set_uuid(self, "binding_id")
        _set_uuid(self, "system_id")
        require_bool(self.enabled, "enabled")
        require_contract_key(self.adapter_key, "adapter_key")
        require_text(self.adapter_version, "adapter_version", max_length=64)
        object.__setattr__(
            self,
            "non_secret_settings",
            validate_json_mapping(self.non_secret_settings, "non_secret_settings"),
        )
        if self.secret_reference is not None:
            require_text(self.secret_reference, "secret_reference", max_length=512)
        if self.revision is not None:
            require_text(self.revision, "revision", max_length=64)


@dataclass(frozen=True, slots=True)
class CapabilityCoveragePolicy(JSONDTO):
    target_kind: TargetKind
    facet: str
    coverage: RefreshCoverage
    maximum_completeness: CollectionCoverage
    absence_authority: tuple[AbsenceAuthority, ...] = ()

    def __post_init__(self) -> None:
        require_enum(self.target_kind, TargetKind, "target_kind")
        require_contract_key(self.facet, "facet")
        require_enum(self.coverage, RefreshCoverage, "coverage")
        require_enum(
            self.maximum_completeness,
            CollectionCoverage,
            "maximum_completeness",
        )
        object.__setattr__(
            self,
            "absence_authority",
            normalize_enum_tuple(
                self.absence_authority,
                AbsenceAuthority,
                "absence_authority",
            ),
        )
        if self.maximum_completeness is not CollectionCoverage.COMPLETE and self.absence_authority:
            raise ValueError("absence authority requires complete maximum coverage")


@dataclass(frozen=True, slots=True)
class CapabilityBinding(JSONDTO):
    capability_binding_id: str | UUID
    connection_binding_id: str | UUID
    capability_key: str
    capability_version: str
    operation_class: OperationClass
    target_kinds: tuple[TargetKind, ...]
    produced_facets: tuple[str, ...]
    enabled: bool
    selection_priority: int = 0
    collateral_effects: tuple[str, ...] = ()
    mitigations: tuple[str, ...] = ()
    coverage_policies: tuple[CapabilityCoveragePolicy, ...] = ()

    def __post_init__(self) -> None:
        _set_uuid(self, "capability_binding_id")
        _set_uuid(self, "connection_binding_id")
        require_contract_key(self.capability_key, "capability_key")
        require_text(self.capability_version, "capability_version", max_length=64)
        require_bool(self.enabled, "enabled")
        require_enum(self.operation_class, OperationClass, "operation_class")
        object.__setattr__(
            self,
            "target_kinds",
            normalize_enum_tuple(self.target_kinds, TargetKind, "target_kinds"),
        )
        object.__setattr__(
            self,
            "coverage_policies",
            normalize_instance_tuple(
                self.coverage_policies,
                CapabilityCoveragePolicy,
                "coverage_policies",
            ),
        )
        if self.operation_class is not OperationClass.OBSERVE:
            raise ValueError("version 1 capabilities must be remote-observation-only")
        if not self.target_kinds or not self.produced_facets:
            raise ValueError("a capability requires targets and produced facets")
        for facet in self.produced_facets:
            require_contract_key(facet, "produced facet")
        require_int(self.selection_priority, "selection_priority", minimum=0)
        for effect in self.collateral_effects:
            require_text(effect, "collateral effect", max_length=1024)
        for mitigation in self.mitigations:
            require_text(mitigation, "mitigation", max_length=1024)
        policy_keys = [
            (policy.target_kind, policy.facet, policy.coverage) for policy in self.coverage_policies
        ]
        if len(set(policy_keys)) != len(policy_keys):
            raise ValueError("capability coverage policies must be unique")
        for policy in self.coverage_policies:
            if policy.target_kind not in self.target_kinds:
                raise ValueError("coverage policy target kind is not enabled")
            if policy.facet not in self.produced_facets:
                raise ValueError("coverage policy facet is not produced")


@dataclass(frozen=True, slots=True)
class FacetDefinition(JSONDTO):
    facet: str
    version: str
    minimum_interval: timedelta

    def __post_init__(self) -> None:
        require_contract_key(self.facet, "facet")
        require_text(self.version, "version", max_length=32)
        require_positive_duration(self.minimum_interval, "minimum_interval")


@dataclass(frozen=True, slots=True)
class ObjectTypeDefinition(JSONDTO):
    type_key: str
    version: str
    facets: tuple[FacetDefinition, ...]
    type_minimum_interval: timedelta | None = None

    def __post_init__(self) -> None:
        require_contract_key(self.type_key, "type_key")
        require_text(self.version, "version", max_length=32)
        object.__setattr__(
            self,
            "facets",
            normalize_instance_tuple(self.facets, FacetDefinition, "facets"),
        )
        if not self.facets:
            raise ValueError("an object type requires at least one facet")
        facet_names = [facet.facet for facet in self.facets]
        if len(set(facet_names)) != len(facet_names):
            raise ValueError("facet definitions must be unique")
        if self.type_minimum_interval is not None:
            require_positive_duration(self.type_minimum_interval, "type_minimum_interval")


@dataclass(frozen=True, slots=True)
class RefreshIntervalOverride(JSONDTO):
    level: str
    scope_id: str | UUID
    interval: timedelta
    facet: str | None = None

    def __post_init__(self) -> None:
        if self.level not in {"object", "system"}:
            raise ValueError("level must be 'object' or 'system'")
        _set_uuid(self, "scope_id")
        require_positive_duration(self.interval, "interval")
        if self.facet is not None:
            require_contract_key(self.facet, "facet")


@dataclass(frozen=True, slots=True)
class ScopePolicyState(JSONDTO):
    scope: RefreshScope
    latest_qualifying_observation_at: datetime | None = None
    latest_targeted_action_started_at: datetime | None = None

    def __post_init__(self) -> None:
        require_instance(self.scope, RefreshScope, "scope")
        _set_optional_utc(self, "latest_qualifying_observation_at")
        _set_optional_utc(self, "latest_targeted_action_started_at")


@dataclass(frozen=True, slots=True)
class QualifyingObservation(JSONDTO):
    observation_id: str | UUID
    scope: RefreshScope
    observed_at: datetime

    def __post_init__(self) -> None:
        _set_uuid(self, "observation_id")
        require_instance(self.scope, RefreshScope, "scope")
        _set_utc(self, "observed_at")


@dataclass(frozen=True, slots=True)
class ActionRecord(JSONDTO):
    action_id: str | UUID
    state: ActionState
    started_at: datetime | None

    def __post_init__(self) -> None:
        _set_uuid(self, "action_id")
        require_enum(self.state, ActionState, "state")
        _set_optional_utc(self, "started_at")
