"""Ephemeral local-caller authorization for the loopback web interface."""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from uuid import uuid4

BOOTSTRAP_TTL_SECONDS = 10 * 60
SESSION_COOKIE = "rookery_session"
_TOKEN = re.compile(r"\A[A-Za-z0-9_-]{32,128}\Z")
_BROWSER_HOST = re.compile(r"\Arookery-[a-f0-9]{32}\.localhost\Z")


def _digest(token: str) -> bytes:
    return hashlib.sha256(token.encode("ascii")).digest()


@dataclass(frozen=True, slots=True)
class LocalSession:
    session_id: str
    csrf_token: str


@dataclass(frozen=True, slots=True)
class LocalSessionGrant:
    cookie_token: str
    session: LocalSession


class LocalCallerAuthorizer:
    """Exchange one startup capability for one process-local browser session."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        bootstrap_token: str | None = None,
        browser_host: str | None = None,
        bootstrap_ttl_seconds: float = BOOTSTRAP_TTL_SECONDS,
    ) -> None:
        if bootstrap_ttl_seconds <= 0:
            raise ValueError("bootstrap lifetime must be positive")
        token = bootstrap_token or secrets.token_urlsafe(32)
        if _TOKEN.fullmatch(token) is None:
            raise ValueError("bootstrap token is invalid")
        resolved_browser_host = browser_host or f"rookery-{secrets.token_hex(16)}.localhost"
        if _BROWSER_HOST.fullmatch(resolved_browser_host) is None:
            raise ValueError("browser host is invalid")
        self.browser_host = resolved_browser_host
        self._clock = clock
        self._bootstrap_token: str | None = token
        self._bootstrap_digest: bytes | None = _digest(token)
        self._bootstrap_expires_at = clock() + bootstrap_ttl_seconds
        self._session_digest: bytes | None = None
        self._session: LocalSession | None = None
        self._lock = threading.Lock()

    def take_bootstrap_token(self) -> str:
        """Return the startup token once so the trusted launcher can disclose it."""

        with self._lock:
            if self._bootstrap_token is None:
                raise RuntimeError("bootstrap token was already disclosed")
            token = self._bootstrap_token
            self._bootstrap_token = None
            return token

    def redeem(self, token: str) -> LocalSessionGrant | None:
        """Atomically consume a valid startup token and rotate into a session."""

        if _TOKEN.fullmatch(token) is None:
            return None
        candidate = _digest(token)
        with self._lock:
            expected = self._bootstrap_digest
            if (
                expected is None
                or self._clock() > self._bootstrap_expires_at
                or not hmac.compare_digest(candidate, expected)
            ):
                return None
            cookie_token = secrets.token_urlsafe(32)
            session = LocalSession(
                session_id=str(uuid4()),
                csrf_token=secrets.token_urlsafe(32),
            )
            self._bootstrap_digest = None
            self._bootstrap_token = None
            self._session_digest = _digest(cookie_token)
            self._session = session
            return LocalSessionGrant(cookie_token=cookie_token, session=session)

    def authenticate(self, cookie_token: str | None) -> LocalSession | None:
        """Return the server-side session only for the issued opaque cookie."""

        if cookie_token is None or _TOKEN.fullmatch(cookie_token) is None:
            return None
        candidate = _digest(cookie_token)
        with self._lock:
            expected = self._session_digest
            if expected is None or not hmac.compare_digest(candidate, expected):
                return None
            return self._session
