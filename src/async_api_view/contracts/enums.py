"""Version 1 canonical enum vocabulary."""

from enum import StrEnum


class KnowledgeState(StrEnum):
    UNKNOWN = "unknown"
    KNOWN = "known"
    UNSUPPORTED = "unsupported"


class PresenceState(StrEnum):
    UNKNOWN = "unknown"
    PRESENT = "present"
    ABSENT = "absent"


class UpdateMode(StrEnum):
    SNAPSHOT = "snapshot"
    PATCH = "patch"
    ABSENCE = "absence"


class FieldCoverage(StrEnum):
    COMPLETE = "complete"
    PARTIAL = "partial"


class CollectionCoverage(StrEnum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    UNKNOWN = "unknown"


class AbsenceAuthority(StrEnum):
    RELATIONSHIP = "relationship"
    OBJECT_PRESENCE = "object_presence"
    FACET_FIELDS = "facet_fields"


class RefreshCoverage(StrEnum):
    FACET = "facet"
    COLLECTION_MEMBERS = "collection_members"
    OBJECT_PRESENCE = "object_presence"


class TargetKind(StrEnum):
    OBJECT = "object"
    CONFIGURED_SCOPE = "configured_scope"
    SYSTEM = "system"


class RefreshOrigin(StrEnum):
    MANUAL = "manual"
    AUTOMATIC = "automatic"


class OperationClass(StrEnum):
    OBSERVE = "observe"


class IntentScopeState(StrEnum):
    QUEUED = "queued"
    LEASED = "leased"
    DEFERRED = "deferred"
    COALESCED = "coalesced"
    ADMITTED = "admitted"
    SATISFIED = "satisfied"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


class ActionState(StrEnum):
    READY = "ready"
    LEASED = "leased"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    SATISFIED = "satisfied"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ActionOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SATISFIED = "satisfied"


class ErrorClass(StrEnum):
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    CONNECTION_TIMEOUT = "connection_timeout"
    DOWNSTREAM_RATE_LIMIT = "downstream_rate_limit"
    TRANSIENT_DOWNSTREAM = "transient_downstream"
    NOT_FOUND = "not_found"
    UNSUPPORTED = "unsupported"
    INVALID_DOWNSTREAM_RESPONSE = "invalid_downstream_response"
    ADAPTER_CONTRACT_MISMATCH = "adapter_contract_mismatch"
    LOCAL_CANCELLATION = "local_cancellation"
    UNKNOWN_ADAPTER_FAILURE = "unknown_adapter_failure"


class IngestionStatus(StrEnum):
    ACCEPTED = "accepted"
    PARTIAL = "partial"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"


class GuardDisposition(StrEnum):
    DISPATCH = "dispatch"
    SATISFY = "satisfy"
    CANCEL = "cancel"
    FAIL = "fail"


class PolicyLevel(StrEnum):
    OBJECT = "object"
    SYSTEM = "system"
    TYPE_DEFAULT = "type_default"


class RefreshDecisionKind(StrEnum):
    ADMIT = "admit"
    DEFER = "defer"
    SATISFIED = "satisfied"
