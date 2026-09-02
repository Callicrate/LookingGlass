"""Pure refresh interval, eligibility, and evidence-satisfaction policy."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta

from lookingglass.contracts._validation import JSONDTO, require_utc
from lookingglass.contracts.defaults import V1_TYPE_DEFINITION_BY_KEY
from lookingglass.contracts.enums import PolicyLevel, RefreshDecisionKind
from lookingglass.contracts.models import (
    QualifyingObservation,
    RefreshIntervalOverride,
    RefreshScope,
    ScopePolicyState,
)


@dataclass(frozen=True, slots=True)
class EffectiveInterval(JSONDTO):
    interval: timedelta
    source_level: PolicyLevel
    source_id: str
    facet_specific: bool


@dataclass(frozen=True, slots=True)
class Eligibility(JSONDTO):
    eligible: bool
    eligible_at: datetime
    fresh_until: datetime | None
    policy_anchor: datetime | None


@dataclass(frozen=True, slots=True)
class RefreshDecision(JSONDTO):
    kind: RefreshDecisionKind
    eligibility: Eligibility
    satisfying_observation_id: str | None = None


def _find_override(
    overrides: Iterable[RefreshIntervalOverride],
    *,
    level: str,
    scope_id: str,
    facet: str | None,
) -> RefreshIntervalOverride | None:
    matches = [
        item
        for item in overrides
        if item.level == level and item.scope_id == scope_id and item.facet == facet
    ]
    if len(matches) > 1:
        raise ValueError(f"multiple {level} overrides exist for scope {scope_id} facet {facet}")
    return matches[0] if matches else None


def resolve_refresh_interval(
    *,
    object_id: str,
    system_id: str,
    object_type: str,
    facet: str,
    overrides: Iterable[RefreshIntervalOverride] = (),
) -> EffectiveInterval:
    """Resolve object facet > object > system facet > system > type defaults."""
    override_items = tuple(overrides)
    precedence = (
        ("object", object_id, facet),
        ("object", object_id, None),
        ("system", system_id, facet),
        ("system", system_id, None),
    )
    for level, scope_id, override_facet in precedence:
        match = _find_override(
            override_items,
            level=level,
            scope_id=scope_id,
            facet=override_facet,
        )
        if match is not None:
            return EffectiveInterval(
                interval=match.interval,
                source_level=PolicyLevel(level),
                source_id=match.scope_id,
                facet_specific=override_facet is not None,
            )

    try:
        definition = V1_TYPE_DEFINITION_BY_KEY[object_type]
    except KeyError as exc:
        raise ValueError(f"unknown object type {object_type!r}") from exc
    facet_defaults = {item.facet: item for item in definition.facets}
    if facet in facet_defaults:
        return EffectiveInterval(
            interval=facet_defaults[facet].minimum_interval,
            source_level=PolicyLevel.TYPE_DEFAULT,
            source_id=f"{object_type}.{facet}",
            facet_specific=True,
        )
    if definition.type_minimum_interval is not None:
        return EffectiveInterval(
            interval=definition.type_minimum_interval,
            source_level=PolicyLevel.TYPE_DEFAULT,
            source_id=object_type,
            facet_specific=False,
        )
    raise ValueError(f"facet {facet!r} is not registered for object type {object_type!r}")


def evaluate_eligibility(
    *, now: datetime, minimum_interval: timedelta, state: ScopePolicyState
) -> Eligibility:
    """Compute eligibility from trusted observation and logical-action anchors.

    The latest action start remains an anchor regardless of its final outcome. This
    deliberately applies cooldown after failure without making old evidence fresh.
    """
    now = require_utc(now, "now")
    observation_at = state.latest_qualifying_observation_at
    action_at = state.latest_targeted_action_started_at
    anchors = tuple(value for value in (observation_at, action_at) if value is not None)
    policy_anchor = max(anchors, default=None)
    fresh_until = observation_at + minimum_interval if observation_at is not None else None
    if policy_anchor is None:
        return Eligibility(
            eligible=True,
            eligible_at=now,
            fresh_until=None,
            policy_anchor=None,
        )
    eligible_at = policy_anchor + minimum_interval
    return Eligibility(
        eligible=now >= eligible_at,
        eligible_at=eligible_at,
        fresh_until=fresh_until,
        policy_anchor=policy_anchor,
    )


def scope_covers(evidence: RefreshScope, requested: RefreshScope) -> bool:
    """Return whether a declared evidence scope satisfies the requested coverage."""
    if (
        evidence.system_id,
        evidence.target,
        evidence.object_type,
        evidence.facet,
        evidence.capability_key,
        evidence.coverage,
    ) != (
        requested.system_id,
        requested.target,
        requested.object_type,
        requested.facet,
        requested.capability_key,
        requested.coverage,
    ):
        return False
    if not evidence.field_mask:
        return True
    return bool(requested.field_mask) and set(requested.field_mask).issubset(evidence.field_mask)


def evidence_satisfies(
    *,
    requested_scope: RefreshScope,
    requested_at: datetime,
    now: datetime,
    minimum_interval: timedelta,
    evidence: QualifyingObservation | None,
) -> bool:
    """Apply the formal coverage and observation-time satisfaction rule."""
    if evidence is None or not scope_covers(evidence.scope, requested_scope):
        return False
    requested_at = require_utc(requested_at, "requested_at")
    now = require_utc(now, "now")
    fresh_until = evidence.observed_at + minimum_interval
    return evidence.observed_at >= requested_at or now < fresh_until


def decide_refresh(
    *,
    requested_scope: RefreshScope,
    requested_at: datetime,
    now: datetime,
    minimum_interval: timedelta,
    state: ScopePolicyState,
    evidence: QualifyingObservation | None = None,
) -> RefreshDecision:
    """Choose satisfaction, deferral, or admission through the one refresh path.

    Request origin is intentionally absent: manual and automatic requests have no
    separate force/bypass semantics. Incidental evidence is evaluated before the
    targeted-action cooldown and therefore can satisfy queued or deferred work.
    """
    eligibility = evaluate_eligibility(
        now=now,
        minimum_interval=minimum_interval,
        state=state,
    )
    if evidence_satisfies(
        requested_scope=requested_scope,
        requested_at=requested_at,
        now=now,
        minimum_interval=minimum_interval,
        evidence=evidence,
    ):
        return RefreshDecision(
            kind=RefreshDecisionKind.SATISFIED,
            eligibility=eligibility,
            satisfying_observation_id=evidence.observation_id if evidence else None,
        )
    return RefreshDecision(
        kind=(RefreshDecisionKind.ADMIT if eligibility.eligible else RefreshDecisionKind.DEFER),
        eligibility=eligibility,
    )


def default_interval_map() -> Mapping[tuple[str, str], timedelta]:
    """Expose the immutable semantic values without leaking registry mutation."""
    return {
        (definition.type_key, facet.facet): facet.minimum_interval
        for definition in V1_TYPE_DEFINITION_BY_KEY.values()
        for facet in definition.facets
    }
