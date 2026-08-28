"""Presentation-only contracts for the loopback web interface."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol

MAX_DISPLAY_LENGTH = 512


def display_text(value: object | None, *, limit: int = MAX_DISPLAY_LENGTH) -> str:
    """Return a bounded, single-line representation of untrusted display data."""

    if value is None:
        return ""
    text = str(value).replace("\x00", "�").replace("\r", " ").replace("\n", " ")
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1]}…"


def timestamp(value: datetime | str | None) -> str:
    """Format exact timestamps without inventing a time for unknown values."""

    if value is None:
        return "Unknown"
    if isinstance(value, datetime):
        return value.isoformat()
    return display_text(value, limit=64)


@dataclass(frozen=True, slots=True)
class RefreshOption:
    system_id: str
    target_kind: Literal["configured_scope", "object"]
    target_id: str
    capability_key: str
    facet: str
    label: str
    collateral_effects: str = "None declared"
    enabled: bool = True
    disabled_reason: str | None = None


@dataclass(frozen=True, slots=True)
class RefreshRequest:
    system_id: str
    target_kind: Literal["configured_scope", "object"]
    target_id: str
    capability_key: str
    facet: str


@dataclass(frozen=True, slots=True)
class FacetView:
    name: str
    knowledge: Literal["known", "unknown", "unsupported"]
    value: str = ""
    known_as_of: datetime | str | None = None
    freshness: Literal[
        "current", "stale", "due", "unobserved", "refreshing", "failed", "unsupported"
    ] = "unobserved"
    effective_interval: str = "Unknown"
    provenance: str = "Unknown"


@dataclass(frozen=True, slots=True)
class ObjectView:
    object_id: str
    system_id: str
    name: str
    object_type: str
    path: str = ""
    presence: str = "unknown"
    facets: tuple[FacetView, ...] = ()


@dataclass(frozen=True, slots=True)
class ActivityView:
    state: str
    occurred_at: datetime | str | None = None
    summary: str = ""
    failure: str | None = None
    intent_id: str | None = None


@dataclass(frozen=True, slots=True)
class SystemView:
    system_id: str
    name: str
    kind: str
    enabled: bool = True
    connection_state: str = "unknown"
    configured_scopes: tuple[str, ...] = ()
    worker_available: bool = True
    last_activity: ActivityView | None = None


@dataclass(frozen=True, slots=True)
class DashboardView:
    systems: tuple[SystemView, ...] = ()
    objects: tuple[ObjectView, ...] = ()
    refresh_options: tuple[RefreshOption, ...] = ()
    loaded_at: datetime | str | None = None
    disconnected: bool = False
    error: str | None = None


@dataclass(frozen=True, slots=True)
class IntentScopeView:
    label: str
    state: str
    system_id: str = ""
    target_kind: Literal["configured_scope", "object"] = "configured_scope"
    target_id: str = ""
    capability_key: str = ""
    facet: str = ""
    action_id: str | None = None
    eligible_at: datetime | str | None = None
    failure: str | None = None
    cached_context: str = "No cached context"


@dataclass(frozen=True, slots=True)
class IntentView:
    intent_id: str
    requested_at: datetime | str | None
    scopes: tuple[IntentScopeView, ...] = ()
    updated_at: datetime | str | None = None
    terminal: bool = False
    error: str | None = None


class WebBackend(Protocol):
    """Narrow application-facing boundary used by the presentation layer."""

    async def dashboard(self) -> DashboardView: ...

    async def submit_refresh(self, request: RefreshRequest) -> str: ...

    async def intent(self, intent_id: str) -> IntentView | None: ...


@dataclass(slots=True)
class UnavailableBackend:
    """Safe default used before application composition supplies real ports."""

    message: str = "Application services are unavailable. Cached state could not be loaded."
    _dashboard: DashboardView = field(init=False)

    def __post_init__(self) -> None:
        self._dashboard = DashboardView(disconnected=True, error=self.message)

    async def dashboard(self) -> DashboardView:
        return self._dashboard

    async def submit_refresh(self, request: RefreshRequest) -> str:
        del request
        raise RuntimeError("refresh worker unavailable")

    async def intent(self, intent_id: str) -> IntentView | None:
        del intent_id
        return None
