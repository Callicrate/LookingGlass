"""Concrete, local-only records returned by the SQLite implementation.

These records deliberately supplement, rather than alter, the stable shared
contracts.  They are the small composition-facing API for configured systems,
registered discovery scopes, durable queue work, and operational events.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from async_api_view.contracts import (
    ActionState,
    AdapterAction,
    IntentScopeState,
    RefreshIntent,
    RefreshScope,
    RelationshipState,
    RemoteObject,
)


@dataclass(frozen=True, slots=True)
class SystemRecord:
    system_id: str
    display_name: str
    system_kind: str
    enabled: bool
    record_created_at: datetime
    record_updated_at: datetime


@dataclass(frozen=True, slots=True)
class ConfiguredScopeRecord:
    scope_id: str
    system_id: str
    object_id: str | None
    object_type: str
    enabled: bool
    display_name: str
    record_created_at: datetime
    record_updated_at: datetime


@dataclass(frozen=True, slots=True)
class IntentScopeWork:
    """One leased atomic refresh scope for the generic coordinator."""

    intent_scope_id: str
    intent: RefreshIntent
    scope: RefreshScope
    state: IntentScopeState
    lease_id: str
    leased_until: datetime


@dataclass(frozen=True, slots=True)
class IntentScopeRecord:
    intent_scope_id: str
    intent_id: str
    scope: RefreshScope
    state: IntentScopeState
    disposition_reason: str | None
    eligible_at: datetime | None
    linked_action_id: str | None
    satisfying_observation_id: str | None


@dataclass(frozen=True, slots=True)
class StoredAction:
    action_id: str
    action: AdapterAction
    state: ActionState
    started_at: datetime | None
    completed_at: datetime | None
    lease_id: str | None
    lease_worker_id: str | None
    leased_until: datetime | None
    retry_at: datetime | None
    error_class: str | None
    redacted_diagnostic: str | None


@dataclass(frozen=True, slots=True)
class ActionActivityRecord:
    action_id: str
    system_id: str
    capability_key: str
    target_kind: str
    target_id: str
    state: str
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    retry_at: datetime | None
    error_class: str | None
    redacted_diagnostic: str | None


@dataclass(frozen=True, slots=True)
class ActionAttemptRecord:
    attempt_id: str
    action_id: str
    ordinal: int
    started_at: datetime
    ended_at: datetime | None
    outcome: str | None
    error_class: str | None
    retry_at: datetime | None
    redacted_diagnostic: str | None


@dataclass(frozen=True, slots=True)
class FacetActionStatusRecord:
    system_id: str
    object_id: str
    facet: str
    action_id: str
    state: str
    occurred_at: datetime
    redacted_diagnostic: str | None


@dataclass(frozen=True, slots=True)
class OperationalEventRecord:
    event_id: str
    event_type: str
    severity: str
    alertable: bool
    system_id: str | None
    action_id: str | None
    error_class: str | None
    redacted_summary: str
    occurred_at: datetime


@dataclass(frozen=True, slots=True)
class RelatedObjectRecord:
    relationship: RelationshipState
    object: RemoteObject
