from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from async_api_view.web import (
    ActivityView,
    DashboardQuery,
    DashboardView,
    FacetView,
    IntentScopeView,
    IntentView,
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


@dataclass
class FakeBackend:
    dashboard_view: DashboardView = field(default_factory=DashboardView)
    intent_view: IntentView | None = None
    object_view: ObjectDetailView | None = None
    intent_id: str = "intent-1"
    dashboard_error: Exception | None = None
    intent_error: Exception | None = None
    object_error: Exception | None = None
    submitted: list[RefreshRequest] = field(default_factory=list)
    dashboard_queries: list[DashboardQuery] = field(default_factory=list)
    object_queries: list[tuple[str, ObjectDetailQuery]] = field(default_factory=list)

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
                relationship_page=normalized_query.relationship_page,
                object_type_filter=normalized_query.object_type,
            )
            if self.object_view is not None
            else None
        )


def client_for(backend: FakeBackend) -> TestClient:
    return TestClient(
        create_app(
            backend,
            allowed_hosts=("127.0.0.1", "localhost", "testserver"),
        )
    )


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
                        provenance="observation-1",
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


def test_empty_dashboard_explains_unknown_state() -> None:
    response = client_for(FakeBackend()).get("/")

    assert response.status_code == 200
    assert "No systems configured" in response.text
    assert "No cached objects" in response.text
    assert "Refresh unsupported" in response.text


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
    assert "Request refresh" in response.text
    assert "Raw content is not displayed" in response.text
    assert "raw secret file contents" not in response.text


def test_dashboard_passes_bounded_filter_and_page_to_backend() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())

    response = client_for(backend).get("/?q=folder&page=2")

    assert response.status_code == 200
    assert backend.dashboard_queries[-1] == DashboardQuery(object_query="folder", object_page=2)


@pytest.mark.parametrize(
    "query",
    [
        "?page=0",
        "?page=1000001",
        f"?page={'9' * 1000}",
        "?page=nan",
        "?page=1&page=2",
        "?unknown=value",
        f"?q={'x' * 129}",
    ],
)
def test_dashboard_rejects_invalid_query_contract(query: str) -> None:
    backend = FakeBackend()

    response = client_for(backend).get(f"/{query}")

    assert response.status_code == 400
    assert backend.dashboard_queries == []


def test_object_page_shows_facets_containment_and_refresh_controls() -> None:
    backend = FakeBackend(object_view=ready_object_detail())

    response = client_for(backend).get(f"/objects/{DETAIL_ID}?page=1&type=file")

    assert response.status_code == 200
    assert "Facets and provenance" in response.text
    assert "Child notebook" in response.text
    assert f"/objects/{CHILD_ID}" in response.text
    assert "Data workspace" in response.text
    assert "databricks.workspace.folder" in response.text
    assert "Object refreshes" in response.text
    assert 'value="file"' in response.text
    assert "Raw content is not displayed" in response.text
    assert csrf_from(response.text)
    assert backend.object_queries == [(DETAIL_ID, ObjectDetailQuery(object_type="file"))]


@pytest.mark.parametrize(
    ("url", "status"),
    [
        ("/objects/not-a-uuid", 404),
        (f"/objects/{DETAIL_ID}?page=0", 400),
        (f"/objects/{DETAIL_ID}?page=1&page=2", 400),
        (f"/objects/{DETAIL_ID}?type=FILE", 400),
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
    assert "secret profile token" not in response.text


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
            ),
        ),
    )

    response = client_for(FakeBackend(dashboard_view=view)).get("/")

    assert response.status_code == 200
    assert "Recent alerts" in response.text
    assert "Latest 1" in response.text
    assert "refresh.action.failed" in response.text
    assert "connection_timeout" in response.text
    assert '<script>alert("x")</script>' not in response.text
    assert "&lt;script&gt;alert" in response.text


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

    response = client.post("/refresh", data=data, headers={"Origin": "http://testserver"})

    assert response.status_code == 403
    assert backend.submitted == []


def test_refresh_reports_unavailable_dashboard_without_leaking_error() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    token = csrf_from(client.get("/").text)
    backend.dashboard_error = RuntimeError("secret profile token")

    response = client.post(
        "/refresh", data=valid_form(token), headers={"Origin": "http://testserver"}
    )

    assert response.status_code == 503
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
        headers={"Referer": "http://testserver/dashboard"},
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
            "Referer": "http://testserver/",
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
    client = TestClient(create_app(FakeBackend()))

    response = client.get("/", headers={"Host": "testserver"})

    assert response.status_code == 400


def test_registered_refresh_redirects_to_receipt() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    token = csrf_from(client.get("/").text)

    response = client.post(
        "/refresh",
        data=valid_form(token),
        headers={"Origin": "http://testserver"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/intents/intent-1"
    assert backend.submitted == [
        RefreshRequest(
            system_id="system-1",
            target_kind="configured_scope",
            target_id="scope-1",
            capability_key="databricks.workspace.children.read",
            facet="membership",
        )
    ]


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
            "/refresh", data=base | extra, headers={"Origin": "http://testserver"}
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
        "/refresh", data=data, headers={"Origin": "http://testserver"}, follow_redirects=False
    )

    assert response.status_code == 303
    assert backend.submitted == [
        RefreshRequest(
            system_id="system-1",
            target_kind="object",
            target_id="object-1",
            capability_key="databricks.workspace.metadata.read",
            facet="metadata",
        )
    ]


def test_unregistered_target_identifier_combination_is_rejected() -> None:
    backend = FakeBackend(dashboard_view=ready_dashboard())
    client = client_for(backend)
    token = csrf_from(client.get("/").text)
    data = valid_form(token) | {"target_id": "../../arbitrary"}

    response = client.post("/refresh", data=data, headers={"Origin": "http://testserver"})

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
            headers={"Origin": "http://testserver"},
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
                action_id="action-1",
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
    assert "action-1" in page.text
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

    assert page.status_code == 503
    assert poll.status_code == 503
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
        headers={"Origin": "http://testserver", "Content-Type": "application/json"},
    )
    oversize = client.post(
        "/refresh",
        content="x" * 9000,
        headers={
            "Origin": "http://testserver",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )

    assert wrong_type.status_code == 415
    assert oversize.status_code == 413


def test_first_party_script_avoids_dangerous_dom_sinks() -> None:
    script = (
        __import__("pathlib").Path(__file__).parents[1]
        / "src"
        / "async_api_view"
        / "web"
        / "static"
        / "app.js"
    ).read_text(encoding="utf-8")

    assert "textContent" in script
    for sink in ("innerHTML", "outerHTML", "document.write", "eval(", "new Function"):
        assert sink not in script
