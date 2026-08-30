"""Generic local-only durable refresh coordinator."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from async_api_view.contracts import IntentScopeState, PresenceState, TargetKind
from async_api_view.contracts.defaults import V1_TYPE_DEFINITION_BY_KEY
from async_api_view.core import decide_refresh
from async_api_view.storage import IntentScopeWork, SQLiteStore


@dataclass(frozen=True, slots=True)
class CoordinatorResult:
    """The explainable outcome of one leased atomic scope iteration."""

    intent_scope_id: str
    state: IntentScopeState
    reason: str
    action_id: str | None = None
    satisfying_observation_id: str | None = None
    eligible_at: datetime | None = None


class DurableCoordinator:
    """Lease, validate, coalesce, defer, or admit one local refresh scope.

    The coordinator has no adapter, network, credential, command, or remote
    response dependency. All decisions use local durable state only.
    """

    def __init__(
        self,
        store: SQLiteStore,
        *,
        worker_id: str | None = None,
        lease_duration: timedelta = timedelta(seconds=60),
    ) -> None:
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        self.store = store
        self.worker_id = worker_id or f"coordinator-{uuid4()}"
        self.lease_duration = lease_duration

    def _reject(self, work: IntentScopeWork, reason: str) -> CoordinatorResult:
        self.store.set_intent_scope_disposition(
            intent_scope_id=work.intent_scope_id,
            lease_id=work.lease_id,
            state=IntentScopeState.REJECTED,
            reason=reason,
        )
        return CoordinatorResult(work.intent_scope_id, IntentScopeState.REJECTED, reason)

    def _validate_target(self, work: IntentScopeWork) -> str | None:
        scope = work.scope
        system = self.store.get_system(scope.system_id)
        if system is None:
            return "system_unknown"
        if not system.enabled:
            return "system_disabled"
        definition = V1_TYPE_DEFINITION_BY_KEY.get(scope.object_type)
        if definition is None or scope.facet not in {facet.facet for facet in definition.facets}:
            return "unsupported_facet"
        if scope.target.kind is TargetKind.OBJECT:
            target = self.store.get_object_sync(scope.target.target_id)
            if target is None:
                return "target_unknown"
            if target.system_id != scope.system_id:
                return "target_system_mismatch"
            if target.presence is PresenceState.ABSENT:
                return "target_absent"
            if target.object_type != scope.object_type:
                return "target_type_mismatch"
            return None
        if scope.target.kind is TargetKind.CONFIGURED_SCOPE:
            configured = self.store.get_configured_scope(scope.target.target_id)
            if configured is None:
                return "configured_scope_unknown"
            if configured.system_id != scope.system_id:
                return "configured_scope_system_mismatch"
            if not configured.enabled:
                return "configured_scope_disabled"
            if configured.object_type != scope.object_type:
                return "configured_scope_type_mismatch"
            return None
        return "unsupported_target_kind"

    async def run_once(self, *, now: datetime | None = None) -> CoordinatorResult | None:
        """Process one durable scope, or return ``None`` when no scope is due."""
        record_time = now or datetime.now(UTC)
        authority_time = self.store.authority_time()
        work = await self.store.lease_next_intent_scope(
            worker_id=self.worker_id,
            now=record_time,
            lease_duration=self.lease_duration,
        )
        if work is None:
            return None
        if work.intent.expires_at is not None and work.intent.expires_at <= authority_time:
            self.store.set_intent_scope_disposition(
                intent_scope_id=work.intent_scope_id,
                lease_id=work.lease_id,
                state=IntentScopeState.EXPIRED,
                reason="request_expired",
            )
            return CoordinatorResult(
                work.intent_scope_id, IntentScopeState.EXPIRED, "request_expired"
            )
        invalid_reason = self._validate_target(work)
        if invalid_reason is not None:
            return self._reject(work, invalid_reason)
        selected = self.store.select_capability(
            system_id=work.scope.system_id,
            target_kind=work.scope.target.kind,
            facet=work.scope.facet,
            capability_key=work.scope.capability_key,
        )
        if selected is None:
            return self._reject(work, "capability_unavailable")
        binding, capability = selected
        effective_scope = replace(work.scope, capability_key=capability.capability_key)
        effective_work = replace(work, scope=effective_scope)
        legacy_scope = replace(effective_scope, capability_key=None)
        active = self.store.find_active_action(
            connection_binding_id=binding.binding_id,
            capability=capability,
            scope=effective_scope,
        )
        if active is None and legacy_scope != effective_scope:
            active = self.store.find_active_action(
                connection_binding_id=binding.binding_id,
                capability=capability,
                scope=legacy_scope,
            )
        if active is not None:
            action, _created = self.store.admit_or_coalesce(
                work=effective_work,
                binding=binding,
                capability=capability,
                now=record_time,
            )
            return CoordinatorResult(
                work.intent_scope_id,
                IntentScopeState.COALESCED,
                "active_action_coalesced",
                action_id=action.action_id,
            )
        try:
            interval = self.store.effective_interval(effective_scope)
        except ValueError:
            return self._reject(work, "invalid_scope_policy")
        evidence_candidates = tuple(
            candidate
            for candidate in (
                self.store.latest_qualifying_observation(effective_scope),
                self.store.latest_legacy_observation_for_capability(
                    legacy_scope,
                    connection_binding_id=binding.binding_id,
                    capability_key=capability.capability_key,
                ),
            )
            if candidate is not None
        )
        evidence = max(
            evidence_candidates,
            key=lambda candidate: candidate.observed_at,
            default=None,
        )
        if evidence is not None and evidence.scope != effective_scope:
            evidence = replace(evidence, scope=effective_scope)
        effective_state = self.store.scope_policy_state(effective_scope)
        legacy_started_at = self.store.latest_legacy_action_started_at_for_capability(
            legacy_scope,
            connection_binding_id=binding.binding_id,
            capability_key=capability.capability_key,
        )
        policy_state = replace(
            effective_state,
            latest_qualifying_observation_at=max(
                (
                    value
                    for value in (
                        effective_state.latest_qualifying_observation_at,
                        evidence.observed_at if evidence is not None else None,
                    )
                    if value is not None
                ),
                default=None,
            ),
            latest_targeted_action_started_at=max(
                (
                    value
                    for value in (
                        effective_state.latest_targeted_action_started_at,
                        legacy_started_at,
                    )
                    if value is not None
                ),
                default=None,
            ),
        )
        decision = decide_refresh(
            requested_scope=effective_scope,
            requested_at=work.intent.requested_at,
            now=authority_time,
            minimum_interval=interval,
            state=policy_state,
            evidence=evidence,
        )
        if decision.kind.value == "satisfied":
            self.store.set_intent_scope_disposition(
                intent_scope_id=work.intent_scope_id,
                lease_id=work.lease_id,
                state=IntentScopeState.SATISFIED,
                reason="evidence_satisfied",
                observation_id=decision.satisfying_observation_id,
            )
            return CoordinatorResult(
                work.intent_scope_id,
                IntentScopeState.SATISFIED,
                "evidence_satisfied",
                satisfying_observation_id=decision.satisfying_observation_id,
            )
        if decision.kind.value == "defer":
            self.store.set_intent_scope_disposition(
                intent_scope_id=work.intent_scope_id,
                lease_id=work.lease_id,
                state=IntentScopeState.DEFERRED,
                reason="minimum_interval_not_elapsed",
                eligible_at=decision.eligibility.eligible_at,
            )
            return CoordinatorResult(
                work.intent_scope_id,
                IntentScopeState.DEFERRED,
                "minimum_interval_not_elapsed",
                eligible_at=decision.eligibility.eligible_at,
            )
        action, created = self.store.admit_or_coalesce(
            work=effective_work,
            binding=binding,
            capability=capability,
            now=record_time,
        )
        state = IntentScopeState.ADMITTED if created else IntentScopeState.COALESCED
        return CoordinatorResult(
            work.intent_scope_id,
            state,
            "action_admitted" if created else "active_action_coalesced",
            action_id=action.action_id,
        )
