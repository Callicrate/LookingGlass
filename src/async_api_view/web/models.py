"""Presentation-only contracts for the loopback web interface."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol
from unicodedata import category
from uuid import UUID

MAX_DISPLAY_LENGTH = 512
DEFAULT_OBJECT_PAGE_SIZE = 50
MAX_OBJECT_QUERY_LENGTH = 128
MAX_OBJECT_PAGE = 1_000_000
MAX_ALERT_PAGE = 10_000
MAX_ACTION_PAGE = 10_000
_BIDI_CONTROLS = frozenset(
    {
        "\u061c",
        "\u200e",
        "\u200f",
        "\u202a",
        "\u202b",
        "\u202c",
        "\u202d",
        "\u202e",
        "\u2066",
        "\u2067",
        "\u2068",
        "\u2069",
    }
)
_CONTRACT_KEY = re.compile(r"[a-z][a-z0-9_.-]{0,127}")
_ALERT_SEVERITIES = frozenset({"", "info", "warning", "error", "critical"})
_ACTION_STATES = frozenset(
    {
        "",
        "ready",
        "leased",
        "running",
        "retry_wait",
        "satisfied",
        "succeeded",
        "partial",
        "failed",
        "cancelled",
    }
)


@dataclass(frozen=True, slots=True)
class DashboardQuery:
    object_query: str = ""
    object_page: int = 1
    object_page_size: int = DEFAULT_OBJECT_PAGE_SIZE

    def __post_init__(self) -> None:
        if len(self.object_query) > MAX_OBJECT_QUERY_LENGTH or any(
            ord(char) < 32 for char in self.object_query
        ):
            raise ValueError("object query is invalid")
        if not 1 <= self.object_page <= MAX_OBJECT_PAGE:
            raise ValueError(f"object page must be between 1 and {MAX_OBJECT_PAGE}")
        if not 1 <= self.object_page_size <= 100:
            raise ValueError("object page size must be between 1 and 100")


@dataclass(frozen=True, slots=True)
class ObjectDetailQuery:
    relationship_page: int = 1
    relationship_page_size: int = DEFAULT_OBJECT_PAGE_SIZE
    object_type: str = ""

    def __post_init__(self) -> None:
        if not 1 <= self.relationship_page <= MAX_OBJECT_PAGE:
            raise ValueError(f"relationship page must be between 1 and {MAX_OBJECT_PAGE}")
        if not 1 <= self.relationship_page_size <= 100:
            raise ValueError("relationship page size must be between 1 and 100")
        if self.object_type and _CONTRACT_KEY.fullmatch(self.object_type) is None:
            raise ValueError("object type filter is invalid")


@dataclass(frozen=True, slots=True)
class AlertHistoryQuery:
    page: int = 1
    page_size: int = DEFAULT_OBJECT_PAGE_SIZE
    event_type: str = ""
    severity: str = ""

    def __post_init__(self) -> None:
        if not 1 <= self.page <= MAX_ALERT_PAGE:
            raise ValueError(f"alert page must be between 1 and {MAX_ALERT_PAGE}")
        if not 1 <= self.page_size <= 100:
            raise ValueError("alert page size must be between 1 and 100")
        if self.event_type and _CONTRACT_KEY.fullmatch(self.event_type) is None:
            raise ValueError("alert event type is invalid")
        if self.severity not in _ALERT_SEVERITIES:
            raise ValueError("alert severity is invalid")


@dataclass(frozen=True, slots=True)
class ActionHistoryQuery:
    page: int = 1
    page_size: int = DEFAULT_OBJECT_PAGE_SIZE
    state: str = ""
    system_id: str = ""
    action_id: str = ""

    def __post_init__(self) -> None:
        if not 1 <= self.page <= MAX_ACTION_PAGE:
            raise ValueError(f"action page must be between 1 and {MAX_ACTION_PAGE}")
        if not 1 <= self.page_size <= 100:
            raise ValueError("action page size must be between 1 and 100")
        if self.state not in _ACTION_STATES:
            raise ValueError("action state is invalid")
        for field_name in ("system_id", "action_id"):
            value = getattr(self, field_name)
            if not value:
                continue
            try:
                normalized = str(UUID(value))
            except (AttributeError, ValueError) as exc:
                raise ValueError(f"{field_name} is invalid") from exc
            object.__setattr__(self, field_name, normalized)
        if self.action_id and (self.system_id or self.state):
            raise ValueError("action ID cannot be combined with activity filters")


def display_text(value: object | None, *, limit: int = MAX_DISPLAY_LENGTH) -> str:
    """Return a bounded, single-line representation of untrusted display data."""

    if value is None:
        return ""
    text = "".join(
        " "
        if char in "\r\n\t"
        else "�"
        if category(char) == "Cc" or char in _BIDI_CONTROLS
        else char
        for char in str(value)
    )
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
    ui_session_id: str | None = None


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
    object_type_version: str = ""
    source_kind: str = ""
    path: str = ""
    presence: str = "unknown"
    first_seen_at: datetime | str | None = None
    last_seen_at: datetime | str | None = None
    facets: tuple[FacetView, ...] = ()


@dataclass(frozen=True, slots=True)
class RelatedObjectView:
    object_id: str
    name: str
    object_type: str
    predicate: str
    relationship_presence: str
    object_presence: str
    observed_at: datetime | str | None


@dataclass(frozen=True, slots=True)
class ObjectDetailView:
    object: ObjectView
    system_name: str = "Unknown system"
    children: tuple[RelatedObjectView, ...] = ()
    refresh_options: tuple[RefreshOption, ...] = ()
    relationship_total: int = 0
    relationship_page: int = 1
    relationship_page_count: int = 1
    relationship_page_start: int = 0
    relationship_page_end: int = 0
    object_type_filter: str = ""
    previous_page_url: str | None = None
    next_page_url: str | None = None
    loaded_at: datetime | str | None = None
    disconnected: bool = False
    error: str | None = None


@dataclass(frozen=True, slots=True)
class ActivityView:
    state: str
    occurred_at: datetime | str | None = None
    summary: str = ""
    failure: str | None = None
    intent_id: str | None = None


@dataclass(frozen=True, slots=True)
class OperationalEventView:
    event_type: str
    severity: str
    summary: str
    occurred_at: datetime | str | None
    system_name: str = "Local runtime"
    error_class: str | None = None
    action_id: str | None = None


@dataclass(frozen=True, slots=True)
class AlertHistoryView:
    alerts: tuple[OperationalEventView, ...] = ()
    total: int = 0
    page: int = 1
    page_count: int = 1
    page_start: int = 0
    page_end: int = 0
    event_type_filter: str = ""
    severity_filter: str = ""
    previous_page_url: str | None = None
    next_page_url: str | None = None
    loaded_at: datetime | str | None = None


@dataclass(frozen=True, slots=True)
class ActionSystemOption:
    system_id: str
    name: str


@dataclass(frozen=True, slots=True)
class ActionActivityView:
    action_id: str
    system_name: str
    capability_key: str
    target_kind: str
    target_id: str
    state: str
    created_at: datetime | str | None
    started_at: datetime | str | None = None
    completed_at: datetime | str | None = None
    retry_at: datetime | str | None = None
    error_class: str | None = None
    diagnostic: str | None = None


@dataclass(frozen=True, slots=True)
class ActionHistoryView:
    actions: tuple[ActionActivityView, ...] = ()
    systems: tuple[ActionSystemOption, ...] = ()
    total: int = 0
    page: int = 1
    page_count: int = 1
    page_start: int = 0
    page_end: int = 0
    state_filter: str = ""
    system_filter: str = ""
    action_filter: str = ""
    previous_page_url: str | None = None
    next_page_url: str | None = None
    loaded_at: datetime | str | None = None


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
    object_total: int = 0
    object_page: int = 1
    object_page_count: int = 1
    object_page_start: int = 0
    object_page_end: int = 0
    object_query: str = ""
    previous_page_url: str | None = None
    next_page_url: str | None = None
    alerts: tuple[OperationalEventView, ...] = ()


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

    async def dashboard(self, query: DashboardQuery | None = None) -> DashboardView: ...

    async def is_refresh_registered(self, request: RefreshRequest) -> bool: ...

    async def object_detail(
        self, object_id: str, query: ObjectDetailQuery | None = None
    ) -> ObjectDetailView | None: ...

    async def alert_history(self, query: AlertHistoryQuery | None = None) -> AlertHistoryView: ...

    async def action_history(
        self, query: ActionHistoryQuery | None = None
    ) -> ActionHistoryView: ...

    async def submit_refresh(self, request: RefreshRequest) -> str: ...

    async def intent(self, intent_id: str) -> IntentView | None: ...


@dataclass(slots=True)
class UnavailableBackend:
    """Safe default used before application composition supplies real ports."""

    message: str = "Application services are unavailable. Cached state could not be loaded."
    _dashboard: DashboardView = field(init=False)

    def __post_init__(self) -> None:
        self._dashboard = DashboardView(disconnected=True, error=self.message)

    async def dashboard(self, query: DashboardQuery | None = None) -> DashboardView:
        del query
        return self._dashboard

    async def is_refresh_registered(self, request: RefreshRequest) -> bool:
        del request
        return False

    async def object_detail(
        self, object_id: str, query: ObjectDetailQuery | None = None
    ) -> ObjectDetailView | None:
        del object_id, query
        return None

    async def alert_history(self, query: AlertHistoryQuery | None = None) -> AlertHistoryView:
        del query
        raise RuntimeError("local state services unavailable")

    async def action_history(self, query: ActionHistoryQuery | None = None) -> ActionHistoryView:
        del query
        raise RuntimeError("local state services unavailable")

    async def submit_refresh(self, request: RefreshRequest) -> str:
        del request
        raise RuntimeError("refresh worker unavailable")

    async def intent(self, intent_id: str) -> IntentView | None:
        del intent_id
        return None
