from __future__ import annotations

import base64
import json
import re
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from async_api_view.web import (
    ActionActivityView,
    ActionAttemptView,
    ActionDetailView,
    ActionHistoryQuery,
    ActionHistoryView,
    ActionSystemOption,
    ActivityView,
    AlertHistoryQuery,
    AlertHistoryView,
    DashboardQuery,
    DashboardView,
    FacetView,
    IntentScopeView,
    IntentView,
    LocalCallerAuthorizer,
    ObjectDetailQuery,
    ObjectDetailView,
    ObjectView,
    OperationalEventView,
    RefreshOption,
    RefreshRequest,
    RelatedObjectView,
    SystemView,
    create_app,
)
from async_api_view.web.models import display_text

NOW = datetime(2026, 8, 24, 14, 35, 9, tzinfo=UTC)
DETAIL_ID = "11111111-1111-4111-8111-111111111111"
CHILD_ID = "33333333-3333-4333-8333-333333333333"
ACTION_ID = "22222222-2222-4222-8222-222222222222"
SYSTEM_ID = "44444444-4444-4444-8444-444444444444"


def page_cursor(*values: str) -> str:
    encoded = json.dumps(values, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(encoded).rstrip(b"=").decode()


OPTION = RefreshOption(
    system_id="system-1",
    target_kind="configured_scope",
    target_id="scope-1",
    capability_key="databricks.workspace.children.read",
    facet="membership",
    label="Workspace root",
    collateral_effects="May create a remote audit record",
)
OBJECT_OPTION = RefreshOption(
    system_id="system-1",
    target_kind="object",
    target_id="object-1",
    capability_key="databricks.workspace.metadata.read",
    facet="metadata",
    label="Workspace object metadata",
)


@pytest.mark.parametrize("disabled_reason", [None, "", "   "])
def test_disabled_refresh_option_requires_accessible_reason(
    disabled_reason: str | None,
) -> None:
    with pytest.raises(ValueError, match="accessible reason"):
        replace(OPTION, enabled=False, disabled_reason=disabled_reason)


def test_enabled_refresh_option_rejects_stale_disabled_reason() -> None:
    with pytest.raises(ValueError, match="enabled refresh option"):
        replace(OPTION, disabled_reason="Worker unavailable")


@dataclass
class FakeBackend:
    dashboard_view: DashboardView = field(default_factory=DashboardView)
    intent_view: IntentView | None = None
    object_view: ObjectDetailView | None = None
    alert_view: AlertHistoryView = field(default_factory=AlertHistoryView)
    action_view: ActionHistoryView = field(default_factory=ActionHistoryView)
    action_detail_view: ActionDetailView | None = None
    intent_id: str = "intent-1"
    dashboard_error: Exception | None = None
    intent_error: Exception | None = None
    object_error: Exception | None = None
    alert_error: Exception | None = None
    action_error: Exception | None = None
    action_detail_error: Exception | None = None
    submitted: list[RefreshRequest] = field(default_factory=list)
    dashboard_queries: list[DashboardQuery] = field(default_factory=list)
    object_queries: list[tuple[str, ObjectDetailQuery]] = field(default_factory=list)
    alert_queries: list[AlertHistoryQuery] = field(default_factory=list)
    action_queries: list[ActionHistoryQuery] = field(default_factory=list)
    action_detail_ids: list[str] = field(default_factory=list)

    async def dashboard(self, query: DashboardQuery | None = None) -> DashboardView:
        if self.dashboard_error:
            raise self.dashboard_error
        self.dashboard_queries.append(query or DashboardQuery())
        return self.dashboard_view

    async def is_refresh_registered(self, request: RefreshRequest) -> bool:
        if self.dashboard_error:
            raise self.dashboard_error
        return any(
            option.enabled
            and (
                option.system_id,
                option.target_kind,
                option.target_id,
                option.capability_key,
                option.facet,
            )
            == (
                request.system_id,
                request.target_kind,
                request.target_id,
                request.capability_key,
                request.facet,
            )
            for option in self.dashboard_view.refresh_options
        )

    async def submit_refresh(self, request: RefreshRequest) -> str:
        self.submitted.append(request)
        return self.intent_id

    async def intent(self, intent_id: str) -> IntentView | None:
        if self.intent_error:
            raise self.intent_error
        if intent_id != self.intent_id:
            return None
        return self.intent_view

    async def object_detail(
        self, object_id: str, query: ObjectDetailQuery | None = None
    ) -> ObjectDetailView | None:
        if self.object_error:
            raise self.object_error
        normalized_query = query or ObjectDetailQuery()
        self.object_queries.append((object_id, normalized_query))
        return (
            replace(
                self.object_view,
                object_type_filter=normalized_query.object_type,
            )
            if self.object_view is not None
            else None
        )

    async def alert_history(self, query: AlertHistoryQuery | None = None) -> AlertHistoryView:
        if self.alert_error:
            raise self.alert_error
        normalized_query = query or AlertHistoryQuery()
        self.alert_queries.append(normalized_query)
        return replace(
            self.alert_view,
            event_type_filter=normalized_query.event_type,
            severity_filter=normalized_query.severity,
        )

    async def action_history(self, query: ActionHistoryQuery | None = None) -> ActionHistoryView:
        if self.action_error:
            raise self.action_error
        normalized_query = query or ActionHistoryQuery()
        self.action_queries.append(normalized_query)
        return replace(
            self.action_view,
            state_filter=normalized_query.state,
            system_filter=normalized_query.system_id,
            action_filter=normalized_query.action_id,
        )

    async def action_detail(self, action_id: str) -> ActionDetailView | None:
        if self.action_detail_error:
            raise self.action_detail_error
        self.action_detail_ids.append(action_id)
        return self.action_detail_view


def authenticated_client(app: FastAPI) -> TestClient:
    client = TestClient(app, base_url="https://testserver")
    token = app.state.local_authorizer.take_bootstrap_token()
    response = client.post(
        "/bootstrap",
        data={"bootstrap_token": token},
        headers={"Origin": "https://testserver"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    return client


def client_for(backend: FakeBackend) -> TestClient:
    app = create_app(
        backend,
        allowed_hosts=("127.0.0.1", "localhost", "testserver"),
    )
    return authenticated_client(app)


def assert_html_error_shell(
    response,
    *,
    status: int,
    heading: str,
    retry: bool,
) -> None:
    assert response.status_code == status
    assert response.headers["content-type"].startswith("text/html")
    assert f"<title>{heading} · Rookery</title>" in response.text
    assert '<main id="main"' in response.text
    assert 'role="alert"' in response.text
    assert heading in response.text
    assert 'href="/">Return to dashboard</a>' in response.text
    assert (">Try again</a>" in response.text) is retry


def test_local_access_denies_every_protected_surface_before_backend_work() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    app = create_app(backend, allowed_hosts=("testserver",))
    client = TestClient(app, base_url="https://testserver")

    responses = (
        client.get("/"),
        client.get("/alerts"),
        client.get("/actions"),
        client.get(f"/actions/{ACTION_ID}"),
        client.get(f"/objects/{DETAIL_ID}"),
        client.get("/intents/intent-1"),
        client.get("/api/intents/intent-1"),
        client.post("/refresh"),
    )

    assert all(response.status_code == 403 for response in responses)
    assert "Unlock this browser" in responses[0].text
    assert responses[6].headers["content-type"].startswith("application/json")
    assert responses[7].headers["content-type"].startswith("text/html")
    assert "Unlock this browser" in responses[7].text
    assert "Data workspace" not in responses[0].text
    assert 'name="csrf_token"' not in responses[0].text
    assert backend.dashboard_queries == []
    assert backend.object_queries == []
    assert backend.alert_queries == []
    assert backend.action_queries == []
    assert backend.action_detail_ids == []
    assert backend.submitted == []


def test_bootstrap_is_public_single_use_and_rotates_a_fixation_cookie() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    app = create_app(backend, allowed_hosts=("testserver",))
    client = TestClient(app, base_url="http://testserver")
    token = app.state.local_authorizer.take_bootstrap_token()
    client.cookies.set("rookery_session", "A" * 43)

    page = client.get("/bootstrap")
    static = client.get("/static/bootstrap.js")
    response = client.post(
        "/bootstrap",
        data={"bootstrap_token": token},
        headers={"Origin": "http://testserver"},
        follow_redirects=False,
    )

    assert page.status_code == 200
    assert static.status_code == 200
    assert token not in page.text + static.text
    assert static.text.index("history.replaceState") < static.text.index("fetch(")
    assert "input.value = token" not in static.text
    assert "localStorage" not in static.text
    assert "sessionStorage" not in static.text
    assert "console." not in static.text
    assert response.status_code == 303
    assert response.headers["location"] == "/"
    assert token not in str(response.headers)
    cookie = response.headers["set-cookie"]
    assert "rookery_session=" in cookie
    assert "HttpOnly" in cookie
    assert "SameSite=strict" in cookie
    assert "; Secure" not in cookie
    assert "Path=/" in cookie
    assert client.get("/").status_code == 200

    replay = TestClient(app, base_url="http://testserver").post(
        "/bootstrap",
        data={"bootstrap_token": token},
        headers={"Origin": "http://testserver"},
        follow_redirects=False,
    )
    assert replay.status_code == 403
    assert "set-cookie" not in replay.headers


def test_process_unique_browser_host_isolates_cookie_from_other_loopback_services() -> None:
    authorizer = LocalCallerAuthorizer()
    app = create_app(
        FakeBackend(dashboard_view=ready_dashboard()),
        allowed_hosts=(authorizer.browser_host,),
        authorizer=authorizer,
    )
    origin = f"http://{authorizer.browser_host}"
    client = TestClient(app, base_url=origin)
    token = authorizer.take_bootstrap_token()

    response = client.post(
        "/bootstrap",
        data={"bootstrap_token": token},
        headers={"Origin": origin},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert client.get("/").status_code == 200
    assert client.get("http://127.0.0.1:8765/").status_code == 400
    assert client.get("http://localhost:8765/").status_code == 400

    def cookie_header(url: str) -> str:
        request = urllib.request.Request(url)
        client.cookies.jar.add_cookie_header(request)
        return request.get_header("Cookie", "")

    assert "rookery_session=" in cookie_header(f"http://{authorizer.browser_host}:9999/")
    assert cookie_header("http://127.0.0.1:9999/") == ""
    assert cookie_header("http://localhost:9999/") == ""
    assert cookie_header("http://unrelated.localhost:9999/") == ""


def test_bootstrap_redemption_is_atomic() -> None:
    authorizer = LocalCallerAuthorizer()
    token = authorizer.take_bootstrap_token()

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(lambda _index: authorizer.redeem(token), range(2)))

    assert sum(result is not None for result in results) == 1


def test_bootstrap_expires_and_restart_rejects_the_prior_session() -> None:
    now = [0.0]
    authorizer = LocalCallerAuthorizer(clock=lambda: now[0])
    token = authorizer.take_bootstrap_token()
    now[0] = 600.0

    assert authorizer.redeem(token) is None

    first = LocalCallerAuthorizer()
    grant = first.redeem(first.take_bootstrap_token())
    assert grant is not None
    restarted = LocalCallerAuthorizer()
    assert restarted.authenticate(grant.cookie_token) is None
    assert first.browser_host != restarted.browser_host
    with pytest.raises(ValueError, match="browser host"):
        LocalCallerAuthorizer(browser_host="localhost")


def test_browser_session_enforces_idle_and_absolute_lifetimes() -> None:
    now = [0.0]
    idle_authorizer = LocalCallerAuthorizer(
        clock=lambda: now[0],
        session_idle_ttl_seconds=10,
        session_absolute_ttl_seconds=30,
    )
    idle_grant = idle_authorizer.redeem(idle_authorizer.take_bootstrap_token())
    assert idle_grant is not None
    now[0] = 10.0
    assert idle_authorizer.authenticate(idle_grant.cookie_token) is None

    now[0] = 0.0
    absolute_authorizer = LocalCallerAuthorizer(
        clock=lambda: now[0],
        session_idle_ttl_seconds=10,
        session_absolute_ttl_seconds=30,
    )
    absolute_grant = absolute_authorizer.redeem(absolute_authorizer.take_bootstrap_token())
    assert absolute_grant is not None
    for current_time in (9.0, 18.0, 27.0, 29.0):
        now[0] = current_time
        assert absolute_authorizer.authenticate(absolute_grant.cookie_token) is not None
    now[0] = 30.0
    assert absolute_authorizer.authenticate(absolute_grant.cookie_token) is None

    with pytest.raises(ValueError, match="idle lifetime"):
        LocalCallerAuthorizer(
            session_idle_ttl_seconds=31,
            session_absolute_ttl_seconds=30,
        )
    with pytest.raises(ValueError, match="absolute lifetime"):
        LocalCallerAuthorizer(session_absolute_ttl_seconds=float("nan"))


def test_expired_browser_session_is_denied_and_cookie_is_cleared() -> None:
    now = [0.0]
    authorizer = LocalCallerAuthorizer(
        clock=lambda: now[0],
        session_idle_ttl_seconds=1,
        session_absolute_ttl_seconds=10,
    )
    backend = FakeBackend(dashboard_view=ready_dashboard())
    app = create_app(
        backend,
        allowed_hosts=("testserver",),
        authorizer=authorizer,
    )
    client = authenticated_client(app)
    now[0] = 2.0

    denied = client.get("/")
    denied_mutation = client.post("/refresh")

    assert denied.status_code == 403
    assert "Unlock this browser" in denied.text
    assert "async-api-view serve" in denied.text
    assert "--config" in denied.text
    assert "Max-Age=0" in denied.headers["set-cookie"]
    assert denied_mutation.status_code == 403
    assert backend.dashboard_queries == []
    assert backend.submitted == []


def test_expired_browser_post_renders_recovery_shell() -> None:
    now = [0.0]
    authorizer = LocalCallerAuthorizer(
        clock=lambda: now[0],
        session_idle_ttl_seconds=1,
        session_absolute_ttl_seconds=10,
    )
    backend = FakeBackend(dashboard_view=ready_dashboard())
    app = create_app(
        backend,
        allowed_hosts=("testserver",),
        authorizer=authorizer,
    )
    client = authenticated_client(app)
    now[0] = 2.0

    denied = client.post("/refresh")

    assert denied.status_code == 403
    assert denied.headers["content-type"].startswith("text/html")
    assert "Unlock this browser" in denied.text
    assert "async-api-view serve" in denied.text
    assert "--config" in denied.text
    assert "Max-Age=0" in denied.headers["set-cookie"]
    assert backend.submitted == []


def test_bootstrap_rejects_cross_origin_and_malformed_requests() -> None:
    app = create_app(FakeBackend(), allowed_hosts=("testserver",))
    client = TestClient(app, base_url="https://testserver")
    token = app.state.local_authorizer.take_bootstrap_token()

    cross_origin = client.post(
        "/bootstrap",
        data={"bootstrap_token": token},
        headers={"Origin": "https://attacker.example"},
    )
    wrong_type = client.post(
        "/bootstrap",
        content="{}",
        headers={"Origin": "https://testserver", "Content-Type": "application/json"},
    )
    unknown_field = client.post(
        "/bootstrap",
        data={"bootstrap_token": token, "next": "https://attacker.example"},
        headers={"Origin": "https://testserver"},
    )

    assert cross_origin.status_code == 403
    assert wrong_type.status_code == 415
    assert unknown_field.status_code == 400


def test_csrf_nonce_is_bound_to_the_authorized_browser_session() -> None:
    first = client_for(FakeBackend(dashboard_view=ready_dashboard()))
    second_backend = FakeBackend(dashboard_view=ready_dashboard())
    second = client_for(second_backend)
    first_token = csrf_from(first.get("/").text)

    response = second.post(
        "/refresh",
        data=valid_form(first_token),
        headers={"Origin": "https://testserver"},
    )

    assert response.status_code == 403
    assert second_backend.submitted == []


def csrf_from(response_text: str) -> str:
    match = re.search(r'name="csrf_token" value="([^"]+)"', response_text)
    assert match
    return match.group(1)


def valid_form(token: str) -> dict[str, str]:
    return {
        "csrf_token": token,
        "system_id": OPTION.system_id,
        "target_kind": OPTION.target_kind,
        "target_id": OPTION.target_id,
        "capability_key": OPTION.capability_key,
        "facet": OPTION.facet,
    }


def ready_dashboard(*, hostile_name: str = "Workspace root") -> DashboardView:
    return DashboardView(
        loaded_at=NOW,
        systems=(
            SystemView(
                system_id="system-1",
                name="Data workspace",
                kind="databricks",
                connection_state="known healthy",
                configured_scopes=("/Workspace",),
                last_activity=ActivityView(state="failed", occurred_at=NOW, failure="timeout"),
            ),
        ),
        objects=(
            ObjectView(
                object_id="object-1",
                system_id="system-1",
                name=hostile_name,
                object_type="folder",
                path="/Workspace",
                facets=(
                    FacetView(
                        name="membership",
                        knowledge="known",
                        value="3 children",
                        known_as_of=NOW,
                        freshness="stale",
                        effective_interval="24 hours",
                        provenance=(
                            "databricks adapter v1 · databricks.workspace.children.read v1"
                        ),
                        provenance_observation_id="observation-1",
                        provenance_action_id=ACTION_ID,
                    ),
                    FacetView(
                        name="content",
                        knowledge="unsupported",
                        value="raw secret file contents",
                        freshness="unsupported",
                    ),
                ),
            ),
        ),
        refresh_options=(OPTION, OBJECT_OPTION),
        object_total=1,
        object_page_start=1,
        object_page_end=1,
    )


def ready_object_detail() -> ObjectDetailView:
    source = ready_dashboard().objects[0]
    object_view = replace(
        source,
        object_id=DETAIL_ID,
        object_type_version="1",
        source_kind="databricks.workspace.folder",
        first_seen_at=NOW,
    )
    return ObjectDetailView(
        object=object_view,
        system_name="Data workspace",
        children=(
            RelatedObjectView(
                object_id=CHILD_ID,
                name="Child notebook",
                object_type="file",
                predicate="contains",
                relationship_presence="present",
                object_presence="present",
                observed_at=NOW,
            ),
        ),
        refresh_options=(replace(OBJECT_OPTION, target_id=DETAIL_ID),),
        relationship_total=1,
        relationship_page_start=1,
        relationship_page_end=1,
        loaded_at=NOW,
    )


def ready_alert_history() -> AlertHistoryView:
    return AlertHistoryView(
        alerts=(
            OperationalEventView(
                event_type="queue.coordinator.failed",
                severity="error",
                summary='<script>alert("x")</script>',
                occurred_at=NOW,
                system_name="Local runtime",
                error_class="unknown_adapter_failure",
                action_id=ACTION_ID,
                system_id=SYSTEM_ID,
            ),
        ),
        total=51,
        page=1,
        page_count=2,
        page_start=1,
        page_end=50,
        next_page_url="/alerts?after=cursor",
        loaded_at=NOW,
    )


def ready_action_history() -> ActionHistoryView:
    return ActionHistoryView(
        actions=(
            ActionActivityView(
                action_id=ACTION_ID,
                system_id=SYSTEM_ID,
                system_name="Data workspace",
                capability_key="databricks.workspace.metadata.read",
                target_kind="object",
                target_id=DETAIL_ID,
                state="retry_wait",
                created_at=NOW,
                started_at=NOW,
                retry_at="2026-08-24T14:40:09+00:00",
                error_class="downstream_rate_limit",
                diagnostic='<script>alert("secret")</script>',
            ),
        ),
        systems=(ActionSystemOption(system_id=SYSTEM_ID, name="Data workspace"),),
        total=51,
        page=1,
        page_count=2,
        page_start=1,
        page_end=50,
        next_page_url="/actions?after=cursor",
        loaded_at=NOW,
    )


def ready_action_detail() -> ActionDetailView:
    return ActionDetailView(
        action=ready_action_history().actions[0],
        attempts=(
            ActionAttemptView(
                ordinal=1,
                started_at=NOW,
                ended_at=NOW,
                outcome="failed",
                error_class="downstream_rate_limit",
                retry_at="2026-08-24T14:40:09+00:00",
                diagnostic='<script>alert("attempt")</script>',
            ),
        ),
        attempt_total=1,
        loaded_at=NOW,
    )


def test_empty_dashboard_explains_unknown_state() -> None:
    response = client_for(FakeBackend()).get("/")

    assert response.status_code == 200
    assert "No systems configured" in response.text
    assert "No cached objects" in response.text
    assert "Refresh unavailable" in response.text
    assert "View history" in response.text
    assert "View activity" in response.text
    assert "Refresh options" in response.text
    assert "<title>Remote state · Rookery</title>" in response.text
    assert "Rookery · local operational view" in response.text
    assert "Async API View" not in response.text


def test_display_text_replaces_terminal_and_bidi_controls() -> None:
    assert display_text("safe\tname\x1b[31m\u202eevil\nnext") == ("safe name�[31m�evil next")
    assert display_text("👩\u200d💻") == "👩\u200d💻"


def test_ready_dashboard_keeps_stale_cached_facts_and_activity_visible() -> None:
    response = client_for(FakeBackend(dashboard_view=ready_dashboard())).get("/")

    assert response.status_code == 200
    assert "3 children" in response.text
    assert NOW.isoformat() in response.text
    assert "stale" in response.text
    assert "timeout" in response.text


def test_dashboard_renders_cached_record_isolation_as_a_nonblocking_warning() -> None:
    view = replace(
        ready_dashboard(),
        integrity_warning=(
            "Rookery isolated malformed cached records; healthy cached state remains available."
        ),
    )

    response = client_for(FakeBackend(dashboard_view=view)).get("/")

    assert response.status_code == 200
    assert "Some cached records were isolated" in response.text
    assert view.integrity_warning in response.text
    assert "Disconnected" not in response.text


def test_duplicate_system_names_render_unique_authority_attribution() -> None:
    dashboard = ready_dashboard()
    enabled = replace(
        dashboard.systems[0],
        system_id=SYSTEM_ID,
        name="TAP",
        enabled=True,
        config_id="tap",
        workspace_root="/",
        authority_label="Verified abcdef123456",
    )
    historical_id = "55555555-5555-4555-8555-555555555555"
    historical = replace(
        enabled,
        system_id=historical_id,
        enabled=False,
        config_id="Legacy / unconfigured",
        authority_label="Legacy / unverified",
    )
    view = replace(dashboard, systems=(enabled, historical))

    response = client_for(FakeBackend(dashboard_view=view)).get("/")

    assert response.status_code == 200
    assert SYSTEM_ID in response.text
    assert historical_id in response.text
    assert "Verified abcdef123456" in response.text
    assert "Legacy / unverified" in response.text
    assert f"System {dashboard.objects[0].system_id}" in response.text
    assert f"System {dashboard.refresh_options[0].system_id}" in response.text


def test_object_containment_is_labeled_as_last_observed_incomplete_evidence() -> None:
    response = client_for(FakeBackend(object_view=ready_object_detail())).get(
        f"/objects/{DETAIL_ID}"
    )

    assert response.status_code == 200
    assert "Last-observed children" in response.text
    assert "last-positive cached evidence" in response.text
    assert "not a complete or live remote listing" in response.text


def test_dashboard_exposes_readable_linked_fact_provenance() -> None:
    response = client_for(FakeBackend(dashboard_view=ready_dashboard())).get("/")

    assert response.status_code == 200
    assert "databricks adapter v1" in response.text
    assert "databricks.workspace.children.read v1" in response.text
    assert "Observation observation-1" in response.text
    assert f'href="/actions/{ACTION_ID}"' in response.text
    assert "Producing action" in response.text
    assert "Request refresh" in response.text
    assert "Raw content is not displayed" in response.text
    assert "raw secret file contents" not in response.text


def test_failed_facet_links_redacted_action_without_hiding_cached_value() -> None:
    dashboard = ready_dashboard()
    remote_object = dashboard.objects[0]
    failed_facet = replace(
        remote_object.facets[0],
        freshness="failed",
        failure="Databricks connection timed out.",
        last_action_id=ACTION_ID,
    )
    view = replace(
        dashboard,
        objects=(replace(remote_object, facets=(failed_facet, *remote_object.facets[1:])),),
    )

    response = client_for(FakeBackend(dashboard_view=view)).get("/")

    assert response.status_code == 200
    assert "3 children" in response.text
    assert "Databricks connection timed out." in response.text
    assert f"/actions/{ACTION_ID}" in response.text


def test_dashboard_passes_bounded_filter_and_cursor_to_backend() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    cursor = page_cursor("folder", DETAIL_ID)

    response = client_for(backend).get(f"/?q=folder&after={cursor}")

    assert response.status_code == 200
    assert backend.dashboard_queries[-1] == DashboardQuery(object_query="folder", cursor=cursor)


def test_dashboard_preserves_boundary_spaces_in_name_prefix() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())

    response = client_for(backend).get("/?q=%20folder%20")

    assert response.status_code == 200
    assert backend.dashboard_queries[-1] == DashboardQuery(object_query=" folder ")


def test_filtered_empty_state_describes_name_prefix_search() -> None:
    dashboard = replace(ready_dashboard(), objects=(), object_query="missing")

    response = client_for(FakeBackend(dashboard_view=dashboard)).get("/?q=missing")

    assert response.status_code == 200
    assert "Try a different name prefix, or clear the filter." in response.text
    assert "Try a different name or type" not in response.text


def test_page_cursors_require_one_canonical_encoding() -> None:
    uppercase_uuid = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"
    with pytest.raises(ValueError, match="cursor"):
        DashboardQuery(cursor=page_cursor(uppercase_uuid))
    with pytest.raises(ValueError, match="cursor"):
        ActionHistoryQuery(cursor=page_cursor(NOW.isoformat(), ACTION_ID))
    noncanonical_json = (
        base64.urlsafe_b64encode(f'[ "{DETAIL_ID}" ]'.encode()).rstrip(b"=").decode()
    )
    with pytest.raises(ValueError, match="cursor"):
        DashboardQuery(cursor=noncanonical_json)


@pytest.mark.parametrize(
    "query",
    [
        "?page=0",
        "?page=1000001",
        f"?page={'9' * 1000}",
        "?page=nan",
        "?page=1&page=2",
        "?after=not-base64",
        "?unknown=value",
        f"?q={'x' * 129}",
        "?q=%20%20%20",
    ],
)
def test_dashboard_rejects_invalid_query_contract(query: str) -> None:
    backend = FakeBackend()

    response = client_for(backend).get(f"/{query}")

    assert response.status_code == 400
    assert backend.dashboard_queries == []


def test_object_page_shows_facets_containment_and_refresh_controls() -> None:
    backend = FakeBackend(object_view=ready_object_detail())

    response = client_for(backend).get(f"/objects/{DETAIL_ID}?type=file")

    assert response.status_code == 200
    assert "Facets and provenance" in response.text
    assert "Child notebook" in response.text
    assert f"/objects/{CHILD_ID}" in response.text
    assert "Data workspace" in response.text
    assert "databricks.workspace.folder" in response.text
    assert "Object refreshes" in response.text
    assert f"System {OBJECT_OPTION.system_id}" in response.text
    assert 'value="file"' in response.text
    assert "Raw content is not displayed" in response.text
    assert csrf_from(response.text)
    assert backend.object_queries == [(DETAIL_ID, ObjectDetailQuery(object_type="file"))]


def test_disabled_refresh_controls_render_matching_accessible_reasons() -> None:
    reason = "Refresh worker is unavailable."
    dashboard_option = replace(OPTION, enabled=False, disabled_reason=reason)
    object_option = replace(
        OBJECT_OPTION,
        target_id=DETAIL_ID,
        enabled=False,
        disabled_reason=reason,
    )
    dashboard = replace(ready_dashboard(), refresh_options=(dashboard_option,))
    object_detail = replace(ready_object_detail(), refresh_options=(object_option,))

    dashboard_response = client_for(FakeBackend(dashboard_view=dashboard)).get("/")
    object_response = client_for(FakeBackend(object_view=object_detail)).get(
        f"/objects/{DETAIL_ID}"
    )

    assert 'disabled aria-describedby="reason-1"' in dashboard_response.text
    assert 'id="reason-1"' in dashboard_response.text
    assert 'disabled aria-describedby="object-reason-1"' in object_response.text
    assert 'id="object-reason-1"' in object_response.text
    assert reason in dashboard_response.text
    assert reason in object_response.text


@pytest.mark.parametrize(
    ("url", "status"),
    [
        ("/objects/not-a-uuid", 404),
        (f"/objects/{DETAIL_ID}?page=0", 400),
        (f"/objects/{DETAIL_ID}?page=1&page=2", 400),
        (f"/objects/{DETAIL_ID}?after=not-base64", 400),
        (f"/objects/{DETAIL_ID}?type=FILE", 400),
        (f"/objects/{DETAIL_ID}?type=a..b", 400),
        (f"/objects/{DETAIL_ID}?type=file&type=folder", 400),
        (f"/objects/{DETAIL_ID}?unknown=x", 400),
    ],
)
def test_object_page_rejects_invalid_path_and_query(url: str, status: int) -> None:
    backend = FakeBackend(object_view=ready_object_detail())

    response = client_for(backend).get(url)

    assert response.status_code == status
    assert backend.object_queries == []


def test_object_page_returns_safe_not_found_and_unavailable_states() -> None:
    missing = client_for(FakeBackend()).get(f"/objects/{DETAIL_ID}")
    failed = client_for(FakeBackend(object_error=RuntimeError("secret profile token"))).get(
        f"/objects/{DETAIL_ID}"
    )

    assert missing.status_code == 404
    assert failed.status_code == 503
    assert "secret profile token" not in failed.text


def test_dashboard_recovers_to_disconnected_error_without_leaking_exception() -> None:
    backend = FakeBackend(dashboard_error=RuntimeError("secret profile token"))
    response = client_for(backend).get("/")

    assert response.status_code == 200
    assert "Disconnected" in response.text
    assert "Try again" in response.text
    assert "Cached snapshot unavailable" in response.text
    assert "No alertable failures recorded" not in response.text
    assert "No systems configured" not in response.text
    assert "No cached objects" not in response.text
    assert "Refresh unavailable" not in response.text
    assert "secret profile token" not in response.text


def test_default_dashboard_does_not_present_unloaded_state_as_empty() -> None:
    app = create_app(allowed_hosts=("testserver",))

    response = authenticated_client(app).get("/")

    assert response.status_code == 200
    assert "Cached snapshot unavailable" in response.text
    assert "No alertable failures recorded" not in response.text
    assert "No systems configured" not in response.text
    assert "No cached objects" not in response.text


def test_dashboard_distinguishes_worker_degradation_from_local_disconnection() -> None:
    view = replace(
        ready_dashboard(),
        refresh_unavailable=True,
        refresh_error="Worker compatibility check is in progress.",
    )

    response = client_for(FakeBackend(dashboard_view=view)).get("/")

    assert response.status_code == 200
    assert "Refresh unavailable" in response.text
    assert "Cached snapshot loaded" in response.text
    assert "Worker compatibility check is in progress." in response.text
    assert "Disconnected" not in response.text


def test_dashboard_shows_bounded_escaped_operational_alerts() -> None:
    view = replace(
        ready_dashboard(),
        alerts=(
            OperationalEventView(
                event_type="refresh.action.failed",
                severity="error",
                summary='<script>alert("x")</script>',
                occurred_at=NOW,
                system_name="Data workspace",
                error_class="connection_timeout",
                action_id=ACTION_ID,
                system_id=SYSTEM_ID,
            ),
        ),
    )

    response = client_for(FakeBackend(dashboard_view=view)).get("/")

    assert response.status_code == 200
    assert "Recent alerts" in response.text
    assert "View history" in response.text
    assert "refresh.action.failed" in response.text
    assert "connection_timeout" in response.text
    assert f"/actions/{ACTION_ID}" in response.text
    assert f"system <code>{SYSTEM_ID}</code>" in response.text
    assert '<script>alert("x")</script>' not in response.text
    assert "&lt;script&gt;alert" in response.text


def test_alert_history_shows_filters_paging_and_escaped_summaries() -> None:
    backend = FakeBackend(alert_view=ready_alert_history())

    response = client_for(backend).get("/alerts?type=queue.coordinator.failed&severity=error")

    assert response.status_code == 200
    assert "Alert history" in response.text
    assert "queue.coordinator.failed" in response.text
    assert "unknown_adapter_failure" in response.text
    assert "/alerts?after=cursor" in response.text
    assert f"/actions/{ACTION_ID}" in response.text
    assert f"system <code>{SYSTEM_ID}</code>" in response.text
    assert '<script>alert("x")</script>' not in response.text
    assert "&lt;script&gt;alert" in response.text
    assert backend.alert_queries == [
        AlertHistoryQuery(
            event_type="queue.coordinator.failed",
            severity="error",
        )
    ]


def test_operational_badges_preserve_semantic_scan_priority() -> None:
    alert = replace(ready_alert_history().alerts[0], severity="critical")
    action = ready_action_history().actions[0]
    backend = FakeBackend(
        alert_view=replace(ready_alert_history(), alerts=(alert,)),
        action_view=replace(
            ready_action_history(),
            actions=(action, replace(action, action_id=CHILD_ID, state="partial")),
        ),
    )
    client = client_for(backend)

    alerts = client.get("/alerts")
    actions = client.get("/actions")
    styles = client.get("/static/style.css")

    assert 'class="badge badge--critical"' in alerts.text
    assert 'class="badge badge--retry_wait"' in actions.text
    assert 'class="badge badge--partial"' in actions.text
    for selector in (
        ".badge--info",
        ".badge--warning",
        ".badge--critical",
        ".badge--retry_wait",
        ".badge--partial",
    ):
        assert selector in styles.text


@pytest.mark.parametrize(
    "query",
    [
        "?page=0",
        "?page=10001",
        "?page=1000001",
        "?after=not-base64",
        "?severity=debug",
        "?type=QUEUE.BAD",
        "?type=a..b",
        "?type=a&type=b",
        "?unknown=value",
    ],
)
def test_alert_history_rejects_invalid_query_before_backend(query: str) -> None:
    backend = FakeBackend(alert_view=ready_alert_history())

    response = client_for(backend).get(f"/alerts{query}")

    assert response.status_code == 400
    assert backend.alert_queries == []


def test_alert_history_returns_safe_unavailable_response() -> None:
    response = client_for(FakeBackend(alert_error=RuntimeError("secret profile token"))).get(
        "/alerts"
    )

    assert response.status_code == 503
    assert "secret profile token" not in response.text


def test_alert_history_default_backend_reports_unavailable() -> None:
    app = create_app(allowed_hosts=("testserver",))
    response = authenticated_client(app).get("/alerts")

    assert_html_error_shell(
        response,
        status=503,
        heading="Local state unavailable",
        retry=True,
    )
    assert "Alert history is unavailable" in response.text


def test_browser_routes_render_accessible_error_shells() -> None:
    cases = (
        (
            "/alerts",
            FakeBackend(alert_error=RuntimeError("secret alert token")),
            503,
            "Local state unavailable",
            True,
        ),
        (
            "/actions",
            FakeBackend(action_error=RuntimeError("secret action token")),
            503,
            "Local state unavailable",
            True,
        ),
        (
            f"/actions/{ACTION_ID}",
            FakeBackend(action_detail_view=None),
            404,
            "Page not found",
            False,
        ),
        (
            f"/objects/{DETAIL_ID}",
            FakeBackend(object_error=RuntimeError("secret object token")),
            503,
            "Local state unavailable",
            True,
        ),
        (
            "/intents/intent-1",
            FakeBackend(intent_error=RuntimeError("secret intent token")),
            503,
            "Local state unavailable",
            True,
        ),
        (
            "/objects/not-a-uuid",
            FakeBackend(),
            404,
            "Page not found",
            False,
        ),
        (
            "/missing",
            FakeBackend(),
            404,
            "Page not found",
            False,
        ),
        (
            "/alerts?page=0",
            FakeBackend(),
            400,
            "Request not accepted",
            False,
        ),
    )
    for path, backend, status, heading, retry in cases:
        response = client_for(backend).get(path)

        assert_html_error_shell(
            response,
            status=status,
            heading=heading,
            retry=retry,
        )
        assert "secret" not in response.text.lower()


def test_action_history_shows_filters_paging_and_escaped_diagnostics() -> None:
    backend = FakeBackend(action_view=ready_action_history())

    response = client_for(backend).get(f"/actions?state=retry_wait&system={SYSTEM_ID}")

    assert response.status_code == 200
    assert "Action activity" in response.text
    assert "databricks.workspace.metadata.read" in response.text
    assert "downstream_rate_limit" in response.text
    assert "/actions?after=cursor" in response.text
    assert f"System {SYSTEM_ID}" in response.text
    assert (
        f"/actions/{ACTION_ID}?return=%2Factions%3Fstate%3Dretry_wait%26system%3D{SYSTEM_ID}"
        in response.text
    )
    assert '<script>alert("secret")</script>' not in response.text
    assert "&lt;script&gt;alert" in response.text
    assert backend.action_queries == [ActionHistoryQuery(state="retry_wait", system_id=SYSTEM_ID)]


@pytest.mark.parametrize(
    "query",
    [
        "?page=0",
        "?page=10001",
        "?after=not-base64",
        "?state=unknown",
        "?system=not-a-uuid",
        "?action=not-a-uuid",
        f"?action={ACTION_ID}&state=failed",
        "?state=ready&state=failed",
        "?unknown=value",
    ],
)
def test_action_history_rejects_invalid_query_before_backend(query: str) -> None:
    backend = FakeBackend(action_view=ready_action_history())

    response = client_for(backend).get(f"/actions{query}")

    assert response.status_code == 400
    assert backend.action_queries == []


def test_action_history_returns_safe_unavailable_response() -> None:
    response = client_for(FakeBackend(action_error=RuntimeError("secret profile token"))).get(
        "/actions"
    )

    assert response.status_code == 503
    assert "secret profile token" not in response.text


def test_action_detail_shows_bounded_escaped_attempts() -> None:
    backend = FakeBackend(
        action_detail_view=replace(
            ready_action_detail(),
            attempt_total=1,
            attempts_truncated=True,
        )
    )

    response = client_for(backend).get(f"/actions/{ACTION_ID}")

    assert response.status_code == 200
    assert "Action detail" in response.text
    assert "Attempt 1" in response.text
    assert "Showing latest 1 · more recorded" in response.text
    assert "downstream_rate_limit" in response.text
    assert '<script>alert("attempt")</script>' not in response.text
    assert "&lt;script&gt;alert" in response.text
    assert backend.action_detail_ids == [ACTION_ID]


def test_action_detail_restores_valid_filtered_return_context() -> None:
    backend = FakeBackend(action_detail_view=ready_action_detail())
    cursor = page_cursor(
        NOW.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        ACTION_ID,
    )

    response = client_for(backend).get(
        f"/actions/{ACTION_ID}?return=%2Factions%3Fstate%3Dfailed%26after%3D{cursor}"
    )

    assert response.status_code == 200
    assert f'href="/actions?state=failed&amp;after={cursor}"' in response.text
    assert backend.action_detail_ids == [ACTION_ID]


@pytest.mark.parametrize(
    "query",
    [
        "?return=https%3A%2F%2Fattacker.example",
        "?return=%2Falerts",
        "?return=%2Factions%23fragment",
        "?return=%2Factions%3Funknown%3Dvalue",
        f"?return=%2Factions%3Faction%3D{ACTION_ID}%26state%3Dfailed",
        "?return=%2Factions&return=%2Factions%3Fafter%3Dcursor_2",
        "?unknown=value",
    ],
)
def test_action_detail_rejects_unsafe_return_context_before_backend(query: str) -> None:
    backend = FakeBackend(action_detail_view=ready_action_detail())

    response = client_for(backend).get(f"/actions/{ACTION_ID}{query}")

    assert response.status_code == 400
    assert backend.action_detail_ids == []


def test_action_detail_returns_safe_not_found_and_unavailable_states() -> None:
    missing_backend = FakeBackend(action_detail_view=None)
    missing = client_for(missing_backend).get(f"/actions/{ACTION_ID}")
    invalid = client_for(FakeBackend()).get("/actions/not-a-uuid")
    unavailable = client_for(
        FakeBackend(action_detail_error=RuntimeError("secret profile token"))
    ).get(f"/actions/{ACTION_ID}")

    assert missing.status_code == 404
    assert invalid.status_code == 404
    assert unavailable.status_code == 503
    assert "secret profile token" not in unavailable.text
    assert missing_backend.action_detail_ids == [ACTION_ID]


def test_remote_markup_is_escaped() -> None:
    hostile = '<img src=x onerror=alert(1)><script>alert("x")</script>'
    response = client_for(FakeBackend(dashboard_view=ready_dashboard(hostile_name=hostile))).get(
        "/"
    )

    assert hostile not in response.text
    assert "&lt;img src=x onerror=alert(1)&gt;" in response.text
    assert response.text.count("<script") == 1


def test_refresh_requires_valid_csrf() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    data = valid_form("wrong-token")

    response = client.post("/refresh", data=data, headers={"Origin": "https://testserver"})

    assert response.status_code == 403
    assert backend.submitted == []


def test_refresh_reports_unavailable_dashboard_without_leaking_error() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    token = csrf_from(client.get("/").text)
    backend.dashboard_error = RuntimeError("secret profile token")

    response = client.post(
        "/refresh", data=valid_form(token), headers={"Origin": "https://testserver"}
    )

    assert response.status_code == 503
    assert response.headers["content-type"].startswith("text/html")
    assert 'role="alert"' in response.text
    assert "Return to dashboard" in response.text
    assert "secret profile token" not in response.text
    assert backend.submitted == []


def test_refresh_requires_same_origin() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    token = csrf_from(client.get("/").text)

    response = client.post(
        "/refresh", data=valid_form(token), headers={"Origin": "https://attacker.example"}
    )

    assert response.status_code == 403
    assert backend.submitted == []


@pytest.mark.parametrize("header", ["Origin", "Referer"])
def test_refresh_rejects_malformed_origin_evidence(header: str) -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    token = csrf_from(client.get("/").text)

    response = client.post(
        "/refresh",
        data=valid_form(token),
        headers={header: "http://[::1"},
    )

    assert response.status_code == 403
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["content-security-policy"].startswith("default-src 'self'")
    assert backend.submitted == []


def test_browser_form_without_origin_uses_same_origin_fetch_metadata() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    token = csrf_from(client.get("/").text)

    response = client.post(
        "/refresh",
        data=valid_form(token),
        headers={"Sec-Fetch-Site": "same-origin"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert len(backend.submitted) == 1


def test_chrome_opaque_origin_uses_same_origin_fetch_metadata() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    token = csrf_from(client.get("/").text)

    response = client.post(
        "/refresh",
        data=valid_form(token),
        headers={
            "Origin": "null",
            "Sec-Fetch-Site": "same-origin",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert len(backend.submitted) == 1


def test_opaque_origin_without_same_origin_fetch_metadata_is_rejected() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    token = csrf_from(client.get("/").text)

    response = client.post(
        "/refresh",
        data=valid_form(token),
        headers={"Origin": "null"},
    )

    assert response.status_code == 403
    assert backend.submitted == []


def test_browser_form_without_origin_accepts_matching_referer() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    token = csrf_from(client.get("/").text)

    response = client.post(
        "/refresh",
        data=valid_form(token),
        headers={"Referer": "https://testserver/dashboard"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert len(backend.submitted) == 1


def test_browser_fallback_rejects_cross_site_fetch_metadata() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    token = csrf_from(client.get("/").text)

    response = client.post(
        "/refresh",
        data=valid_form(token),
        headers={
            "Referer": "https://testserver/",
            "Sec-Fetch-Site": "cross-site",
        },
    )

    assert response.status_code == 403
    assert backend.submitted == []


def test_browser_fallback_still_requires_origin_evidence() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    token = csrf_from(client.get("/").text)

    response = client.post("/refresh", data=valid_form(token))

    assert response.status_code == 403
    assert backend.submitted == []


def test_untrusted_host_is_rejected() -> None:
    response = client_for(FakeBackend()).get("/", headers={"Host": "attacker.example"})

    assert response.status_code == 400
    assert "Invalid host header" in response.text


def test_default_host_allowlist_rejects_test_host() -> None:
    app = create_app(FakeBackend())
    client = TestClient(app, base_url=f"http://{app.state.local_authorizer.browser_host}")

    accepted_host = client.get("/")
    response = client.get("/", headers={"Host": "testserver"})

    assert accepted_host.status_code == 403
    assert response.status_code == 400


def test_registered_refresh_redirects_to_receipt() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    token = csrf_from(client.get("/").text)

    response = client.post(
        "/refresh",
        data=valid_form(token),
        headers={"Origin": "https://testserver"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/intents/intent-1"
    submitted = backend.submitted[0]
    assert UUID(submitted.ui_session_id or "")
    assert replace(submitted, ui_session_id=None) == RefreshRequest(
        system_id="system-1",
        target_kind="configured_scope",
        target_id="scope-1",
        capability_key="databricks.workspace.children.read",
        facet="membership",
    )


def test_arbitrary_fields_paths_and_force_are_rejected() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    token = csrf_from(client.get("/").text)
    base = valid_form(token)

    for extra in (
        {"path": "/etc/shadow"},
        {"force": "true"},
        {"endpoint": "https://attacker.example"},
        {"cli_argument": "--profile=secret"},
    ):
        response = client.post(
            "/refresh", data=base | extra, headers={"Origin": "https://testserver"}
        )
        assert response.status_code == 400
    assert backend.submitted == []


def test_registered_object_target_refresh_is_allowed() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    token = csrf_from(client.get("/").text)
    data = valid_form(token) | {
        "target_kind": OBJECT_OPTION.target_kind,
        "target_id": OBJECT_OPTION.target_id,
        "capability_key": OBJECT_OPTION.capability_key,
        "facet": OBJECT_OPTION.facet,
    }

    response = client.post(
        "/refresh", data=data, headers={"Origin": "https://testserver"}, follow_redirects=False
    )

    assert response.status_code == 303
    submitted = backend.submitted[0]
    assert UUID(submitted.ui_session_id or "")
    assert replace(submitted, ui_session_id=None) == RefreshRequest(
        system_id="system-1",
        target_kind="object",
        target_id="object-1",
        capability_key="databricks.workspace.metadata.read",
        facet="metadata",
    )


def test_unregistered_target_identifier_combination_is_rejected() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    token = csrf_from(client.get("/").text)
    data = valid_form(token) | {"target_id": "../../arbitrary"}

    response = client.post("/refresh", data=data, headers={"Origin": "https://testserver"})

    assert response.status_code == 400
    assert backend.submitted == []


def test_system_and_unknown_target_kinds_are_rejected_before_allowlist_matching() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    token = csrf_from(client.get("/").text)

    for target_kind in ("system", "configured-scope", "path", "OBJECT"):
        response = client.post(
            "/refresh",
            data=valid_form(token) | {"target_kind": target_kind},
            headers={"Origin": "https://testserver"},
        )
        assert response.status_code == 400
    assert backend.submitted == []


def test_intent_page_and_json_show_each_scope_disposition() -> None:
    intent = IntentView(
        intent_id="intent-1",
        requested_at=NOW,
        updated_at=NOW,
        scopes=(
            IntentScopeView(
                label="Folder membership",
                state="deferred",
                target_kind="configured_scope",
                target_id="scope-1",
                capability_key=OPTION.capability_key,
                facet="membership",
                action_id=ACTION_ID,
                eligible_at="2026-08-25T14:35:09+00:00",
                cached_context="3 cached children, stale",
            ),
            IntentScopeView(
                label="Folder metadata",
                state="failed",
                target_kind="object",
                target_id="object-1",
                facet="metadata",
                failure="connection_timeout",
                cached_context="Last known owner: ops",
            ),
        ),
    )
    client = client_for(FakeBackend(intent_view=intent))

    page = client.get("/intents/intent-1")
    poll = client.get("/api/intents/intent-1")

    assert page.status_code == 200
    assert "deferred" in page.text
    assert ACTION_ID in page.text
    assert f"/actions/{ACTION_ID}" in page.text
    assert "configured_scope" in page.text
    assert "scope-1" in page.text
    assert "3 cached children, stale" in page.text
    assert poll.status_code == 200
    assert [scope["state"] for scope in poll.json()["scopes"]] == ["deferred", "failed"]
    assert poll.json()["scopes"][1]["target_kind"] == "object"
    assert poll.json()["scopes"][1]["target_id"] == "object-1"
    assert poll.json()["scopes"][0]["eligible_at"] == "2026-08-25T14:35:09+00:00"


def test_intent_status_failures_are_safe_and_retryable() -> None:
    client = client_for(FakeBackend(intent_error=RuntimeError("secret worker token")))

    page = client.get("/intents/intent-1")
    poll = client.get("/api/intents/intent-1")
    missing_poll = client_for(FakeBackend()).get("/api/intents/intent-1")

    assert page.status_code == 503
    assert poll.status_code == 503
    assert page.headers["content-type"].startswith("text/html")
    assert 'role="alert"' in page.text
    assert poll.headers["content-type"].startswith("application/json")
    assert poll.json() == {"detail": "Intent status is unavailable"}
    assert missing_poll.status_code == 404
    assert missing_poll.headers["content-type"].startswith("application/json")
    assert missing_poll.json() == {"detail": "Intent not found"}
    assert "secret worker token" not in page.text + poll.text


def test_security_headers_are_applied_to_html_json_and_errors() -> None:
    backend = FakeBackend(intent_view=None)
    client = client_for(backend)

    for response in (
        client.get("/"),
        client.get("/api/intents/intent-1"),
        client.get("/missing"),
    ):
        assert response.headers["cache-control"] == "no-store"
        assert response.headers["x-content-type-options"] == "nosniff"
        assert response.headers["x-frame-options"] == "DENY"
        assert response.headers["referrer-policy"] == "no-referrer"
        assert "default-src 'self'" in response.headers["content-security-policy"]


def test_unexpected_authorization_failure_uses_closed_secure_error_shell(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingAuthorizer(LocalCallerAuthorizer):
        def authenticate(self, cookie_token: str | None):
            raise RuntimeError("token=opaque\n\x1b[31m C:\\Users\\person\\session")

    app = create_app(
        FakeBackend(),
        allowed_hosts=("testserver",),
        authorizer=FailingAuthorizer(),
    )
    client = TestClient(
        app,
        base_url="https://testserver",
        raise_server_exceptions=False,
    )

    response = client.get("/")
    api_response = client.get("/api/intents/intent-1")

    assert response.status_code == 500
    assert response.headers["content-type"].startswith("text/html")
    assert "<title>Local access failed · Rookery</title>" in response.text
    assert 'role="alert"' in response.text
    assert 'href="/bootstrap"' in response.text
    assert "Return to dashboard" not in response.text
    assert client.get("/bootstrap").status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-frame-options"] == "DENY"
    assert "default-src 'self'" in response.headers["content-security-policy"]
    assert api_response.status_code == 500
    assert api_response.json() == {"detail": "Local browser authorization is unavailable"}
    assert api_response.headers["cache-control"] == "no-store"
    assert "opaque" not in response.text + api_response.text + caplog.text
    assert "Traceback" not in caplog.text


def test_favicon_is_served_without_browser_log_noise() -> None:
    response = client_for(FakeBackend()).get("/favicon.ico")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("image/svg+xml")


def test_refresh_rejects_wrong_content_type_and_oversize_body() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)

    wrong_type = client.post(
        "/refresh",
        content="{}",
        headers={"Origin": "https://testserver", "Content-Type": "application/json"},
    )
    oversize = client.post(
        "/refresh",
        content="x" * 9000,
        headers={
            "Origin": "https://testserver",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )

    assert wrong_type.status_code == 415
    assert oversize.status_code == 413


def test_first_party_script_avoids_dangerous_dom_sinks() -> None:
    script_path = (
        __import__("pathlib").Path(__file__).parents[1]
        / "src"
        / "async_api_view"
        / "web"
        / "static"
        / "app.js"
    )
    script = script_path.read_text(encoding="utf-8")

    assert "textContent" in script
    assert "element.textContent !== resolved" in script
    assert "response.status === 403" in script
    assert "response.status === 404" in script
    assert "response.status >= 500" in script
    assert "window.location.reload()" in script
    assert 'setPollState("Final state", "final")' in script
    assert 'page.dataset.intentTerminal === "true"' in script
    assert '"disconnected",' in script
    assert '"unavailable",' in script
    assert "Status unavailable · retrying" in script
    style = script_path.with_name("style.css").read_text(encoding="utf-8")
    assert ".pulse--unavailable" in style
    assert ".pulse:not(.pulse--disconnected):not(.pulse--unavailable):not(.pulse--final)" in style
    for sink in ("innerHTML", "outerHTML", "document.write", "eval(", "new Function"):
        assert sink not in script


def test_terminal_intent_page_is_final_before_javascript_runs() -> None:
    terminal = IntentView(
        intent_id="terminal-intent",
        requested_at=NOW,
        updated_at=NOW,
        terminal=True,
        scopes=(IntentScopeView(label="Done", state="succeeded"),),
    )

    response = client_for(FakeBackend(intent_view=terminal, intent_id="terminal-intent")).get(
        "/intents/terminal-intent"
    )

    assert response.status_code == 200
    assert 'data-intent-terminal="true"' in response.text
    assert 'class="pulse pulse--final"' in response.text
    assert "Final state" in response.text
