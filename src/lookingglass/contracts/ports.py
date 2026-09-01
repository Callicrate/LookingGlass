"""Dependency-inversion ports for the local modular monolith."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Protocol, runtime_checkable

from .models import (
    ActionAttempt,
    ActionCompletion,
    ActionLease,
    AdapterAction,
    CapabilityBinding,
    ConnectionBinding,
    FacetState,
    GuardDecision,
    IngestionResult,
    ObservationBatch,
    RefreshIntent,
    RefreshReceipt,
    RelationshipState,
    RemoteObject,
)


class ActionLeaseLost(ValueError):
    """Expected lease-fencing result; callers must not treat it as storage failure."""


@runtime_checkable
class StateQueryPort(Protocol):
    async def get_object(self, object_id: str) -> RemoteObject | None: ...

    async def get_facet(self, object_id: str, facet: str) -> FacetState | None: ...

    async def list_relationships(
        self, subject_id: str, predicate: str | None = None
    ) -> Sequence[RelationshipState]: ...


@runtime_checkable
class RefreshSubmissionPort(Protocol):
    async def submit_refresh(self, intent: RefreshIntent) -> RefreshReceipt: ...


@runtime_checkable
class ActionQueuePort(Protocol):
    async def enqueue(self, action: AdapterAction) -> None: ...

    async def lease_next(
        self, *, adapter_key: str, worker_id: str, now: datetime
    ) -> ActionLease | None: ...


@runtime_checkable
class ActionLifecyclePort(Protocol):
    """Lease-authorized attempt/completion writes after guard-owned logical start."""

    async def record_attempt(self, attempt: ActionAttempt, *, lease_id: str) -> None: ...

    async def complete_action(self, completion: ActionCompletion, *, lease_id: str) -> None: ...

    async def heartbeat(
        self, *, action_id: str, lease_id: str, worker_id: str, at: datetime
    ) -> None: ...


@runtime_checkable
class PreDispatchGuardPort(Protocol):
    async def evaluate(self, *, action_id: str, lease_id: str, now: datetime) -> GuardDecision: ...

    async def authorize_start(
        self, *, action_id: str, lease_id: str, binding_revision: str, now: datetime
    ) -> GuardDecision: ...


@runtime_checkable
class BindingQueryPort(Protocol):
    async def get_connection_binding(self, binding_id: str) -> ConnectionBinding | None: ...

    async def get_capability_binding(
        self, connection_binding_id: str, capability_key: str, capability_version: str
    ) -> CapabilityBinding | None: ...


@runtime_checkable
class ObservationIngestionPort(Protocol):
    """The only canonical-state write surface available to API workers."""

    async def ingest(
        self, batch: ObservationBatch, *, lease_id: str | None = None
    ) -> IngestionResult: ...
