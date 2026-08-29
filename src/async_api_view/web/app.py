"""FastAPI composition for the local operational dashboard."""

from __future__ import annotations

import hmac
import re
import secrets
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .models import (
    DashboardView,
    IntentView,
    RefreshOption,
    RefreshRequest,
    UnavailableBackend,
    WebBackend,
    display_text,
    timestamp,
)

MAX_FORM_BYTES = 8 * 1024
FORM_FIELDS = frozenset(
    {"csrf_token", "system_id", "target_kind", "target_id", "capability_key", "facet"}
)
UI_TARGET_KINDS = frozenset({"configured_scope", "object"})
SAFE_INTENT_ID = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
SECURITY_HEADERS = {
    "Content-Security-Policy": (
        "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; "
        "connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; "
        "form-action 'self'"
    ),
    "X-Frame-Options": "DENY",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "Cache-Control": "no-store",
    "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
    "Cross-Origin-Opener-Policy": "same-origin",
    "X-Permitted-Cross-Domain-Policies": "none",
}


def _intent_id(value: str) -> str:
    if not SAFE_INTENT_ID.fullmatch(value):
        raise HTTPException(status_code=404, detail="Intent not found")
    return value


def _url_matches_request_origin(
    request: Request,
    value: str,
    *,
    origin_header: bool,
) -> bool:
    host = request.headers.get("host")
    if not host:
        return False
    parsed = urlsplit(value)
    if parsed.username is not None or parsed.password is not None:
        return False
    if parsed.scheme != request.url.scheme or parsed.netloc.lower() != host.lower():
        return False
    return not (origin_header and (parsed.path or parsed.query or parsed.fragment))


def _same_origin(request: Request) -> bool:
    """Validate Origin or the standard same-origin browser fallback headers."""

    origin = request.headers.get("origin")
    opaque_origin = origin is not None and origin.strip().lower() == "null"
    if origin is not None and not opaque_origin:
        return _url_matches_request_origin(request, origin, origin_header=True)

    fetch_site = request.headers.get("sec-fetch-site", "").strip().lower()
    if opaque_origin and fetch_site != "same-origin":
        return False
    if fetch_site == "cross-site":
        return False
    referer = request.headers.get("referer")
    if referer is not None:
        return _url_matches_request_origin(request, referer, origin_header=False)
    return fetch_site == "same-origin"


def _option_matches(option: RefreshOption, submitted: RefreshRequest) -> bool:
    return (
        option.enabled
        and option.target_kind in UI_TARGET_KINDS
        and (
            option.system_id,
            option.target_kind,
            option.target_id,
            option.capability_key,
            option.facet,
        )
        == (
            submitted.system_id,
            submitted.target_kind,
            submitted.target_id,
            submitted.capability_key,
            submitted.facet,
        )
    )


async def _parse_refresh_form(request: Request, csrf_token: str) -> RefreshRequest:
    if not _same_origin(request):
        raise HTTPException(status_code=403, detail="A same-origin request is required")
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "application/x-www-form-urlencoded":
        raise HTTPException(status_code=415, detail="Expected a form-encoded request")
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > MAX_FORM_BYTES:
                raise HTTPException(status_code=413, detail="Request body is too large")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid Content-Length") from exc
    body_buffer = bytearray()
    async for chunk in request.stream():
        if len(body_buffer) + len(chunk) > MAX_FORM_BYTES:
            raise HTTPException(status_code=413, detail="Request body is too large")
        body_buffer.extend(chunk)
    body = bytes(body_buffer)
    try:
        values = parse_qs(
            body.decode("utf-8", errors="strict"),
            keep_blank_values=True,
            strict_parsing=True,
            max_num_fields=len(FORM_FIELDS),
        )
    except (UnicodeDecodeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="Malformed form data") from exc
    if set(values) != FORM_FIELDS or any(len(items) != 1 for items in values.values()):
        raise HTTPException(status_code=400, detail="Unexpected or missing form field")
    if not hmac.compare_digest(values["csrf_token"][0], csrf_token):
        raise HTTPException(status_code=403, detail="Invalid CSRF token")
    fields = {name: values[name][0] for name in FORM_FIELDS - {"csrf_token"}}
    if any(not value or len(value) > 256 for value in fields.values()):
        raise HTTPException(status_code=400, detail="Invalid form value")
    if fields["target_kind"] not in UI_TARGET_KINDS:
        raise HTTPException(status_code=400, detail="Unsupported target kind")
    return RefreshRequest(**fields)


def _intent_payload(view: IntentView) -> dict[str, object]:
    return {
        "intent_id": display_text(view.intent_id, limit=128),
        "requested_at": timestamp(view.requested_at),
        "updated_at": timestamp(view.updated_at),
        "terminal": view.terminal,
        "error": display_text(view.error),
        "scopes": [
            {
                "label": display_text(scope.label),
                "state": display_text(scope.state, limit=32),
                "target_kind": display_text(scope.target_kind, limit=32),
                "target_id": display_text(scope.target_id, limit=256),
                "action_id": display_text(scope.action_id, limit=128),
                "eligible_at": timestamp(scope.eligible_at),
                "failure": display_text(scope.failure),
                "cached_context": display_text(scope.cached_context),
            }
            for scope in view.scopes
        ],
    }


def create_app(
    backend: WebBackend | None = None,
    *,
    allowed_hosts: tuple[str, ...] = ("127.0.0.1", "localhost"),
) -> FastAPI:
    """Create a loopback-oriented app around an injected application facade."""

    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    app.state.backend = backend or UnavailableBackend()
    app.state.csrf_token = secrets.token_urlsafe(32)

    root = Path(__file__).parent
    templates = Environment(
        loader=FileSystemLoader(root / "templates"),
        autoescape=select_autoescape(enabled_extensions=("html", "xml"), default=True),
    )
    templates.filters["display"] = display_text
    templates.filters["timestamp"] = timestamp
    app.mount("/static", StaticFiles(directory=root / "static"), name="static")
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=list(allowed_hosts))

    @app.middleware("http")
    async def secure_responses(request: Request, call_next):  # type: ignore[no-untyped-def]
        response = await call_next(request)
        for name, value in SECURITY_HEADERS.items():
            response.headers[name] = value
        return response

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request) -> HTMLResponse:
        try:
            view = await app.state.backend.dashboard()
        except Exception:
            view = DashboardView(
                disconnected=True,
                error="Local state services are unavailable. Cached facts could not be loaded.",
            )
        content = templates.get_template("index.html").render(
            request=request, view=view, csrf_token=app.state.csrf_token
        )
        return HTMLResponse(content)

    @app.get("/favicon.ico", response_class=FileResponse, include_in_schema=False)
    async def favicon() -> FileResponse:
        return FileResponse(root / "static" / "favicon.svg", media_type="image/svg+xml")

    @app.post("/refresh")
    async def refresh(request: Request) -> Response:
        submitted = await _parse_refresh_form(request, app.state.csrf_token)
        try:
            dashboard_view = await app.state.backend.dashboard()
        except Exception as exc:
            raise HTTPException(
                status_code=503, detail="Local state services are unavailable"
            ) from exc
        if not any(_option_matches(option, submitted) for option in dashboard_view.refresh_options):
            raise HTTPException(status_code=400, detail="Refresh selection is not registered")
        try:
            result = await app.state.backend.submit_refresh(submitted)
        except Exception as exc:
            raise HTTPException(status_code=503, detail="Refresh worker unavailable") from exc
        intent_id = _intent_id(str(getattr(result, "intent_id", result)))
        return RedirectResponse(f"/intents/{intent_id}", status_code=303)

    @app.get("/intents/{intent_id}", response_class=HTMLResponse)
    async def intent_page(request: Request, intent_id: str) -> HTMLResponse:
        try:
            view = await app.state.backend.intent(_intent_id(intent_id))
        except Exception as exc:
            raise HTTPException(status_code=503, detail="Intent status is unavailable") from exc
        if view is None:
            raise HTTPException(status_code=404, detail="Intent not found")
        content = templates.get_template("intent.html").render(request=request, view=view)
        return HTMLResponse(content)

    @app.get("/api/intents/{intent_id}", response_class=JSONResponse)
    async def intent_poll(intent_id: str) -> JSONResponse:
        try:
            view = await app.state.backend.intent(_intent_id(intent_id))
        except Exception as exc:
            raise HTTPException(status_code=503, detail="Intent status is unavailable") from exc
        if view is None:
            raise HTTPException(status_code=404, detail="Intent not found")
        return JSONResponse(_intent_payload(view))

    return app
