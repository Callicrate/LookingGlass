"""Single-file SQLite implementation of the local durable core.

The class intentionally has no adapter imports and no network surface.  SQL is
static and all data values are bound parameters.  Long-lived work is represented
by leases; the transactions that claim a lease or elect an active action use
``BEGIN IMMEDIATE`` so that separate local processes cannot admit duplicates.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import tempfile
import threading
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import closing, contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from functools import cache
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

from async_api_view.contracts import (
    AbsenceAuthority,
    ActionAttempt,
    ActionCompletion,
    ActionLease,
    ActionLeaseLost,
    ActionOutcome,
    ActionState,
    AdapterAction,
    CapabilityBinding,
    CapabilityCoveragePolicy,
    CollectionCoverage,
    ConnectionBinding,
    CoverageDeclaration,
    ErrorClass,
    FacetObservation,
    FacetState,
    FieldCoverage,
    GuardDecision,
    GuardDisposition,
    IngestionResult,
    IngestionStatus,
    IntentScopeState,
    KnowledgeState,
    ObjectLocator,
    ObservationBatch,
    OperationClass,
    PresenceState,
    QualifyingObservation,
    RefreshCoverage,
    RefreshIntent,
    RefreshIntervalOverride,
    RefreshOrigin,
    RefreshReceipt,
    RefreshScope,
    RelationshipObservation,
    RelationshipState,
    RemoteObject,
    ScopePolicyState,
    TargetKind,
    TargetRef,
    UpdateMode,
    canonical_observation_batch_bytes,
)
from async_api_view.contracts._validation import (
    canonical_json_bytes,
    require_contract_key,
    require_text,
    require_utc,
    require_uuid,
    validate_json,
)
from async_api_view.contracts.defaults import V1_TYPE_DEFINITION_BY_KEY
from async_api_view.core import decide_refresh, resolve_refresh_interval, scope_covers
from async_api_view.local_files import (
    PrivateDirectoryGuard,
    RegularFileGuard,
    absolute_local_path,
    available_bytes,
    harden_private_file,
    prepare_private_directory,
    regular_file_identity,
)

from .models import (
    ActionActivityRecord,
    ActionAttemptRecord,
    AuthorityRecord,
    ConfiguredScopeRecord,
    FacetActionStatusRecord,
    FacetEvidenceRecord,
    IntentScopeRecord,
    IntentScopeWork,
    OperationalEventRecord,
    RelatedObjectRecord,
    StoredAction,
    SystemRecord,
)

_MIGRATIONS_DIR = Path(__file__).with_name("migrations")
_APPLICATION_ID = 0x524F4F4B  # ASCII "ROOK"
_DEFAULT_LEASE = timedelta(seconds=60)
_SQLITE_STARTUP_BUSY_TIMEOUT_MS = 5_000
_SQLITE_BUSY_TIMEOUT_MS = 250
_MAX_JSON_BYTES = 1_048_576
_MAX_DIAGNOSTIC_LENGTH = 1_024
_MAX_DUE_PROMOTIONS_PER_CLAIM = 1_000
_MAX_ACTION_CONTRACT_QUARANTINES_PER_CLAIM = 100
_SQLITE_MAX_INTEGER = (1 << 63) - 1
MIN_WRITE_RESERVE_BYTES = 64 * 1024 * 1024
_HEADROOM_RETRY_DELAY = timedelta(seconds=60)
_TERMINAL_ACTION_STATES = {
    ActionState.SATISFIED,
    ActionState.SUCCEEDED,
    ActionState.PARTIAL,
    ActionState.FAILED,
    ActionState.CANCELLED,
}
_TERMINAL_INTENT_STATES = {
    IntentScopeState.SATISFIED,
    IntentScopeState.REJECTED,
    IntentScopeState.EXPIRED,
    IntentScopeState.CANCELLED,
}


class StorageHeadroomUnavailable(RuntimeError):
    """Raised when a new write would consume the protected local recovery reserve."""


_SECRET_SETTING_PART = re.compile(
    r"(?:password|secret|token|private[_-]?key|authorization|access[_-]?key)", re.IGNORECASE
)
_DIAGNOSTIC_SECRET = re.compile(
    r"(?i)\b((?:[a-z0-9]+[_-])*(?:token|password|secret|authorization|profile|host|"
    r"access[_-]?key|private[_-]?key|api[_-]?key))\b\s*(?:=|:)\s*[^\s,;]+"
)
_BEARER_SECRET = re.compile(r"(?i)\bbearer\s+[^\s,;]+")
_JSON_SECRET = re.compile(
    r'(?i)(["\']?(?:(?:[a-z0-9]+[_-])*(?:token|password|secret|authorization|'
    r"access[_-]?key|private[_-]?key|api[_-]?key))"
    r'["\']?\s*:\s*)(?:"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|[^,}\]\s]+)'
)
_UNSAFE_DIAGNOSTIC_CHARACTER = re.compile(
    "[\x00-\x1f\x7f-\x9f\u061c\u200e\u200f\u202a-\u202e\u2066-\u2069]"
)
_WINDOWS_HOME_PATH = re.compile(
    r"(?i)(?:[A-Z]:\\(?:Users|Documents and Settings)\\[^\\\s\"']+(?:\\[^\s\"']*)*"
    r"|\\\\[^\\\s\"']+\\[^\\\s\"']+(?:\\[^\s\"']*)*)"
)
_POSIX_HOME_PATH = re.compile(r"(?:(?:/home|/Users)/[^/\s\"']+(?:/[^\s\"']*)*|~/(?:[^\s\"']*)?)")
_RUNTIME_EVENT_TYPES = frozenset(
    {
        "queue.coordinator.failed",
        "queue.adapter_worker.failed",
        "observation.ingestion.failed",
    }
)
_RUNTIME_SUMMARY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 .,:;()_-]{0,511}$")
_CONFIG_ID = re.compile(r"\A[a-z0-9._-]{1,128}\Z")
_ALERT_SEVERITIES = frozenset({"info", "warning", "error", "critical"})
_COLLECTION_COVERAGE_RANK = {
    CollectionCoverage.UNKNOWN: 0,
    CollectionCoverage.PARTIAL: 1,
    CollectionCoverage.COMPLETE: 2,
}
_CAPABILITY_KEY_ALTER_TABLES = {
    "ALTER TABLE refresh_credit ADD COLUMN capability_key TEXT;": "refresh_credit",
    "ALTER TABLE refresh_intent_scopes ADD COLUMN capability_key TEXT;": "refresh_intent_scopes",
    "ALTER TABLE adapter_action_scopes ADD COLUMN capability_key TEXT;": "adapter_action_scopes",
}
_REQUIRED_RUNTIME_INDEXES = {
    "ix_observation_batches_received_at": (
        "observation_batches",
        (("received_at", 1, "BINARY"), ("batch_id", 1, "BINARY")),
        0,
        "CREATE INDEX ix_observation_batches_received_at "
        "ON observation_batches (received_at DESC, batch_id DESC)",
    ),
    "ix_adapter_action_scopes_target_facet": (
        "adapter_action_scopes",
        tuple(
            (column, 0, "BINARY") for column in ("target_kind", "target_id", "facet", "action_id")
        ),
        0,
        "CREATE INDEX ix_adapter_action_scopes_target_facet "
        "ON adapter_action_scopes (target_kind, target_id, facet, action_id)",
    ),
    "ix_configured_scopes_object": (
        "configured_scopes",
        (("object_id", 0, "BINARY"), ("scope_id", 0, "BINARY")),
        0,
        "CREATE INDEX ix_configured_scopes_object ON configured_scopes (object_id, scope_id)",
    ),
    "ix_relationships_object_predicate": (
        "relationships",
        tuple(
            (column, 0, "BINARY") for column in ("object_id", "predicate", "presence", "subject_id")
        ),
        0,
        "CREATE INDEX ix_relationships_object_predicate "
        "ON relationships (object_id, predicate, presence, subject_id)",
    ),
    "ix_refresh_intent_scopes_claim_order": (
        "refresh_intent_scopes",
        (
            ("queue_priority", 1, "BINARY"),
            ("queue_requested_at", 0, "BINARY"),
            ("intent_scope_id", 0, "BINARY"),
        ),
        1,
        "CREATE INDEX ix_refresh_intent_scopes_claim_order "
        "ON refresh_intent_scopes ( queue_priority DESC, queue_requested_at, intent_scope_id ) "
        "WHERE state = 'queued'",
    ),
    "ix_refresh_intent_scopes_deferred_due": (
        "refresh_intent_scopes",
        (("eligible_at", 0, "BINARY"), ("intent_scope_id", 0, "BINARY")),
        1,
        "CREATE INDEX ix_refresh_intent_scopes_deferred_due "
        "ON refresh_intent_scopes (eligible_at, intent_scope_id) "
        "WHERE state = 'deferred'",
    ),
    "ix_refresh_intent_scopes_lease_due": (
        "refresh_intent_scopes",
        (("leased_until", 0, "BINARY"), ("intent_scope_id", 0, "BINARY")),
        1,
        "CREATE INDEX ix_refresh_intent_scopes_lease_due "
        "ON refresh_intent_scopes (leased_until, intent_scope_id) "
        "WHERE state = 'leased'",
    ),
    "ix_adapter_actions_claim_order": (
        "adapter_actions",
        (
            ("adapter_key", 0, "BINARY"),
            ("record_created_at", 0, "BINARY"),
            ("action_id", 0, "BINARY"),
        ),
        1,
        "CREATE INDEX ix_adapter_actions_claim_order "
        "ON adapter_actions (adapter_key, record_created_at, action_id) "
        "WHERE state = 'ready'",
    ),
    "ix_adapter_actions_lease_due": (
        "adapter_actions",
        (
            ("adapter_key", 0, "BINARY"),
            ("leased_until", 0, "BINARY"),
            ("action_id", 0, "BINARY"),
        ),
        1,
        "CREATE INDEX ix_adapter_actions_lease_due "
        "ON adapter_actions (adapter_key, leased_until, action_id) "
        "WHERE state IN ('leased', 'running')",
    ),
    "ix_adapter_actions_retry_due": (
        "adapter_actions",
        (
            ("adapter_key", 0, "BINARY"),
            ("retry_at", 0, "BINARY"),
            ("action_id", 0, "BINARY"),
        ),
        1,
        "CREATE INDEX ix_adapter_actions_retry_due "
        "ON adapter_actions (adapter_key, retry_at, action_id) "
        "WHERE state = 'retry_wait'",
    ),
}


@cache
def _schema_signature_through(
    final_version: str,
) -> tuple[tuple[str, frozenset[tuple[str, str, int, int]]], ...]:
    """Derive a table signature by applying authoritative migrations in memory."""

    with closing(sqlite3.connect(":memory:")) as reference:
        for migration in sorted(_MIGRATIONS_DIR.glob("*.sql")):
            reference.executescript(migration.read_text(encoding="utf-8"))
            if migration.stem == final_version:
                break
        else:  # pragma: no cover - source packaging invariant
            raise RuntimeError(f"unknown schema signature version {final_version}")
        table_names = tuple(
            row[0]
            for row in reference.execute(
                """
                SELECT name FROM sqlite_schema
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            ).fetchall()
        )
        return tuple(
            (
                table_name,
                frozenset(
                    (row[1], row[2].upper(), int(row[3]), int(row[5]))
                    for row in reference.execute(
                        "SELECT * FROM pragma_table_info(?)",
                        (table_name,),
                    ).fetchall()
                ),
            )
            for table_name in table_names
        )


def _normalized_schema_sql(value: str) -> str:
    return " ".join(value.rstrip(";").split())


@cache
def _schema_ddl_through(
    final_version: str,
    *,
    include_runtime_repairs: bool = False,
) -> tuple[tuple[str, str, str, str], ...]:
    """Derive authoritative table/index DDL, including constraints and predicates."""

    with closing(sqlite3.connect(":memory:")) as reference:
        reference.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TEXT NOT NULL
            )
            """
        )
        for migration in sorted(_MIGRATIONS_DIR.glob("*.sql")):
            reference.executescript(migration.read_text(encoding="utf-8"))
            if migration.stem == final_version:
                break
        else:  # pragma: no cover - source packaging invariant
            raise RuntimeError(f"unknown schema DDL version {final_version}")
        if include_runtime_repairs:
            for index_name, (
                _table,
                _keys,
                _partial,
                statement,
            ) in _REQUIRED_RUNTIME_INDEXES.items():
                reference.execute(f'DROP INDEX IF EXISTS "{index_name}"')
                reference.execute(statement)
        return tuple(
            (row[0], row[1], row[2], _normalized_schema_sql(row[3]))
            for row in reference.execute(
                """
                SELECT type, name, tbl_name, sql FROM sqlite_schema
                WHERE type IN ('table', 'index', 'trigger', 'view')
                  AND name NOT LIKE 'sqlite_%'
                  AND sql IS NOT NULL
                ORDER BY type, name
                """
            ).fetchall()
        )


def _initial_schema_signature() -> tuple[tuple[str, frozenset[tuple[str, str, int, int]]], ...]:
    return _schema_signature_through("0001_initial")


def _current_schema_signature() -> tuple[tuple[str, frozenset[tuple[str, str, int, int]]], ...]:
    migrations = sorted(_MIGRATIONS_DIR.glob("*.sql"))
    if not migrations:  # pragma: no cover - source packaging invariant
        raise RuntimeError("Rookery migrations are unavailable")
    return _schema_signature_through(migrations[-1].stem)


def _canonical_config_id(value: str) -> str:
    normalized = require_text(value, "config_id", max_length=128).casefold()
    if _CONFIG_ID.fullmatch(normalized) is None:
        raise ValueError(
            "config_id may contain only ASCII letters, digits, periods, underscores, and hyphens"
        )
    return normalized


def _binding_revision(
    *,
    adapter_key: str,
    adapter_version: str,
    non_secret_settings_json: str,
    secret_reference: str | None,
) -> str:
    material = json.dumps(
        [adapter_key, adapter_version, non_secret_settings_json, secret_reference],
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(material.encode()).hexdigest()


class _DatabaseKind(Enum):
    EMPTY = "empty"
    MARKERLESS = "markerless"
    MARKED = "marked"


def _kind_for_application_id(application_id: int) -> _DatabaseKind:
    return _DatabaseKind.MARKED if application_id == _APPLICATION_ID else _DatabaseKind.MARKERLESS


def _schema_ddl(connection: sqlite3.Connection) -> dict[tuple[str, str], tuple[str, str]]:
    return {
        (row["type"], row["name"]): (
            row["tbl_name"],
            _normalized_schema_sql(row["sql"]),
        )
        for row in connection.execute(
            """
            SELECT type, name, tbl_name, sql FROM sqlite_schema
            WHERE type IN ('table', 'index', 'trigger', 'view')
              AND name NOT LIKE 'sqlite_%'
              AND sql IS NOT NULL
            """
        )
    }


def _validate_database_identity(connection: sqlite3.Connection) -> _DatabaseKind:
    """Reject foreign or incompatible SQLite state without mutating it."""

    application_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
    if application_id not in {0, _APPLICATION_ID}:
        raise sqlite3.DatabaseError("SQLite database is not a recognized Rookery store")
    schema_objects = tuple(
        (row["name"], row["type"])
        for row in connection.execute(
            """
            SELECT name, type FROM sqlite_schema
            WHERE name NOT LIKE 'sqlite_%'
            """
        ).fetchall()
    )
    if not schema_objects:
        if application_id == _APPLICATION_ID:
            raise sqlite3.DatabaseError("Rookery database schema is missing")
        return _DatabaseKind.EMPTY
    tables = {name for name, object_type in schema_objects if object_type == "table"}
    if not tables or "schema_migrations" not in tables:
        raise sqlite3.DatabaseError("SQLite database is not a recognized Rookery store")
    ledger_columns = tuple(
        row["name"]
        for row in connection.execute(
            "SELECT name FROM pragma_table_info('schema_migrations') ORDER BY cid"
        ).fetchall()
    )
    if ledger_columns != ("version", "applied_at"):
        raise sqlite3.DatabaseError("Rookery migration ledger is incompatible")
    versions = tuple(
        row["version"]
        for row in connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()
    )
    known_versions = tuple(migration.stem for migration in sorted(_MIGRATIONS_DIR.glob("*.sql")))
    if any(version not in known_versions for version in versions):
        raise sqlite3.DatabaseError("Rookery migration ledger contains an unknown version")
    if not versions:
        if tables != {"schema_migrations"}:
            raise sqlite3.DatabaseError("Rookery database schema is incomplete")
        if set(_schema_ddl(connection)) != {("table", "schema_migrations")}:
            raise sqlite3.DatabaseError("Rookery migration ledger schema is incompatible")
        return _kind_for_application_id(application_id)
    if versions[0] != "0001_initial":
        raise sqlite3.DatabaseError("Rookery initial migration is not recorded")
    initial_schema = dict(_initial_schema_signature())
    if not set(initial_schema).issubset(tables):
        raise sqlite3.DatabaseError("Rookery initial database schema is incomplete")
    for table_name, expected_signature in initial_schema.items():
        actual_signature = {
            (
                row["name"],
                row["type"].upper(),
                int(row["notnull"]),
                int(row["pk"]),
            )
            for row in connection.execute(
                "SELECT * FROM pragma_table_info(?)",
                (table_name,),
            )
        }
        if not expected_signature.issubset(actual_signature):
            raise sqlite3.DatabaseError(f"Rookery {table_name} schema is incompatible")
    expected_ddl = {
        (object_type, name): (table_name, sql)
        for object_type, name, table_name, sql in _schema_ddl_through(versions[-1])
    }
    actual_ddl = _schema_ddl(connection)
    allowed_runtime_indexes = {("index", name) for name in _REQUIRED_RUNTIME_INDEXES}
    unexpected = set(actual_ddl) - set(expected_ddl) - allowed_runtime_indexes
    if unexpected:
        raise sqlite3.DatabaseError("Rookery database contains an unexpected schema object")
    return _kind_for_application_id(application_id)


def _preflight_existing_database(path: Path) -> tuple[RegularFileGuard, _DatabaseKind]:
    """Validate one existing database without opening or changing WAL state."""

    guard = RegularFileGuard(path)
    try:
        source_uri = f"{path.as_uri()}?mode=ro&immutable=1"
        with closing(sqlite3.connect(source_uri, uri=True)) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only = ON")
            kind = _validate_database_identity(connection)
        if kind is not _DatabaseKind.MARKED and any(
            os.path.lexists(sidecar) for sidecar in (Path(f"{path}-wal"), Path(f"{path}-shm"))
        ):
            raise sqlite3.DatabaseError(
                "unmarked Rookery databases with WAL sidecars cannot be adopted safely"
            )
        guard.verify()
        return guard, kind
    except BaseException:
        guard.close()
        raise


def _prepare_state_directory(path: Path) -> Path:
    """Reserve one dedicated private directory without restricting a Git worktree root."""

    if os.path.lexists(path / ".git"):
        raise OSError("Rookery state and backup files require a dedicated private directory")
    return prepare_private_directory(path)


def _create_or_preflight_database(path: Path) -> tuple[RegularFileGuard, _DatabaseKind]:
    if os.path.lexists(path):
        return _preflight_existing_database(path)
    _prepare_state_directory(path.parent)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError:
        return _preflight_existing_database(path)
    else:
        os.close(descriptor)
    harden_private_file(path)
    return RegularFileGuard(path), _DatabaseKind.EMPTY


def _unlink_if_identity(path: Path, identity: tuple[int, int]) -> None:
    if not os.path.lexists(path):
        return
    try:
        matches = regular_file_identity(path, expected_links=None) == identity
    except OSError:
        return
    if matches:
        path.unlink()


def backup_sqlite_database(source_path: str | Path, destination_path: str | Path) -> Path:
    """Publish one validated online SQLite snapshot without overwriting a path."""

    source = absolute_local_path(source_path)
    try:
        source_guard, source_kind = _preflight_existing_database(source)
        if source_kind is _DatabaseKind.EMPTY:
            source_guard.close()
            raise sqlite3.DatabaseError("Rookery backup source is not initialized")
    except sqlite3.Error as exc:
        raise RuntimeError(
            "could not create a consistent SQLite backup from a recognized Rookery store"
        ) from exc
    source_directory_guard: PrivateDirectoryGuard | None = None
    destination_directory_guard: PrivateDirectoryGuard | None = None
    try:
        source_directory = _prepare_state_directory(source.parent)
        source_directory_guard = PrivateDirectoryGuard(source_directory)
        harden_private_file(source)
        for sidecar in (Path(f"{source}-wal"), Path(f"{source}-shm")):
            if os.path.lexists(sidecar):
                harden_private_file(sidecar)
        source_directory_guard.verify()
        source_guard.verify()

        destination = absolute_local_path(destination_path)
        if destination == source:
            raise ValueError("backup destination must differ from the source database")
        if os.path.lexists(destination):
            raise FileExistsError(f"backup destination already exists: {destination}")

        source_uri = f"{source.as_uri()}?mode=ro"
        with closing(sqlite3.connect(source_uri, uri=True)) as source_connection:
            source_connection.row_factory = sqlite3.Row
            source_connection.execute("PRAGMA query_only = ON")
            source_connection.execute("PRAGMA busy_timeout = 5000")
            if _validate_database_identity(source_connection) is not source_kind:
                raise OSError("Rookery backup source changed after immutable preflight")
            source_guard.verify()

            destination_directory = _prepare_state_directory(destination.parent)
            destination_directory_guard = PrivateDirectoryGuard(destination_directory)
            page_count_row = source_connection.execute("PRAGMA page_count").fetchone()
            page_size_row = source_connection.execute("PRAGMA page_size").fetchone()
            if page_count_row is None or page_size_row is None:
                raise RuntimeError("backup source size could not be measured")
            snapshot_bytes = int(page_count_row[0]) * int(page_size_row[0])
            required_bytes = snapshot_bytes + MIN_WRITE_RESERVE_BYTES
            try:
                destination_available = available_bytes(destination.parent)
            except OSError as exc:
                raise StorageHeadroomUnavailable(
                    "Backup capacity could not be confirmed; no snapshot was created"
                ) from exc
            if destination_available < required_bytes:
                raise StorageHeadroomUnavailable(
                    "Backup requires its SQLite snapshot size plus a 64 MiB safety reserve"
                )
            with tempfile.NamedTemporaryFile(
                prefix=".rookery-backup-",
                suffix=".sqlite3.tmp",
                dir=destination.parent,
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
            harden_private_file(temporary_path)
            temporary_guard = RegularFileGuard(temporary_path)
            temporary_identity = temporary_guard.identity
            link_created = False
            published = False
            try:
                with closing(sqlite3.connect(temporary_path)) as destination_connection:
                    source_connection.backup(destination_connection)
                    source_guard.verify()
                    integrity = destination_connection.execute("PRAGMA integrity_check").fetchone()
                    if integrity is None or integrity[0] != "ok":
                        raise RuntimeError("backup failed its SQLite integrity check")
                    destination_connection.row_factory = sqlite3.Row
                    if _validate_database_identity(destination_connection) is not source_kind:
                        raise RuntimeError("backup changed the Rookery database identity")
                temporary_guard.harden()
                temporary_guard.sync()
                destination_directory_guard.verify()
                source_guard.verify()
                os.link(temporary_path, destination)
                link_created = True
                temporary_guard.verify(expected_links=2)
                if regular_file_identity(destination, expected_links=2) != temporary_identity:
                    raise OSError("backup destination does not match the validated snapshot")
                destination_directory_guard.sync()
                temporary_guard.close()
                temporary_path.unlink()
                destination_directory_guard.sync()
                with RegularFileGuard(destination) as destination_guard:
                    if destination_guard.identity != temporary_identity:
                        raise OSError("backup destination changed during publication")
                    destination_guard.harden()
                    destination_guard.sync()
                    destination_directory_guard.sync()
                    destination_guard.verify()
                    published = True
                    return destination
            finally:
                temporary_guard.close()
                _unlink_if_identity(temporary_path, temporary_identity)
                if link_created and not published:
                    _unlink_if_identity(destination, temporary_identity)
    except sqlite3.Error as exc:
        raise RuntimeError("could not create a consistent SQLite backup") from exc
    finally:
        if destination_directory_guard is not None:
            destination_directory_guard.close()
        if source_directory_guard is not None:
            source_directory_guard.close()
        source_guard.close()


def _utc_text(value: datetime) -> str:
    return require_utc(value, "timestamp").isoformat(timespec="microseconds").replace("+00:00", "Z")


def _dt(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return require_utc(parsed, "stored timestamp")


def _stored_timestamp_is_canonical(value: object) -> int:
    """Return one only for the exact UTC format used by durable queue comparisons."""

    if not isinstance(value, str):
        return 0
    try:
        parsed = _dt(value)
    except (TypeError, ValueError):
        return 0
    return int(parsed is not None and _utc_text(parsed) == value)


def _now() -> datetime:
    return datetime.now(UTC)


def _received_order_allows(
    *,
    observed_at: datetime,
    received_at: datetime,
    existing_observed_at: datetime | None,
    existing_received_at: datetime | None,
) -> bool:
    if existing_observed_at is None:
        return True
    if observed_at != existing_observed_at:
        return observed_at > existing_observed_at
    if existing_received_at is None:
        return False
    return received_at > existing_received_at


def _numeric_revision_order(incoming: str | None, existing: str | None) -> int | None:
    if incoming is None or existing is None:
        return None
    if incoming == existing:
        return 0
    if incoming.isascii() and existing.isascii() and incoming.isdecimal() and existing.isdecimal():
        return (int(incoming) > int(existing)) - (int(incoming) < int(existing))
    return None


def _object_search_pattern(query: str) -> str | None:
    if not query:
        return None
    require_text(query, "object_query", max_length=128)
    escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"{escaped}%"


def _json_text(value: object, *, field_name: str) -> str:
    encoded = canonical_json_bytes(value, field_name)
    if len(encoded) > _MAX_JSON_BYTES:
        raise ValueError(f"{field_name} exceeds {_MAX_JSON_BYTES} bytes")
    return encoded.decode("utf-8")


def _json_value(value: str) -> object:
    return json.loads(value)


def _batch_digest(batch: ObservationBatch) -> str:
    """Bind batch identity to its complete canonical envelope without storing raw input."""
    material = canonical_observation_batch_bytes(batch)
    if len(material) > _MAX_JSON_BYTES:
        raise ValueError(f"observation batch exceeds {_MAX_JSON_BYTES} bytes")
    return hashlib.sha256(material).hexdigest()


def _redact(value: str | None) -> str:
    if not value:
        return "no diagnostic supplied"
    cleaned = value.replace("\r", " ").replace("\n", " ")
    cleaned = _UNSAFE_DIAGNOSTIC_CHARACTER.sub("�", cleaned)
    cleaned = _BEARER_SECRET.sub("Bearer <redacted>", cleaned)
    cleaned = _JSON_SECRET.sub(lambda match: f'{match.group(1)}"<redacted>"', cleaned)
    cleaned = _DIAGNOSTIC_SECRET.sub(lambda match: f"{match.group(1)}=<redacted>", cleaned)
    cleaned = _WINDOWS_HOME_PATH.sub("<redacted-path>", cleaned)
    cleaned = _POSIX_HOME_PATH.sub("<redacted-path>", cleaned)
    return cleaned[:_MAX_DIAGNOSTIC_LENGTH]


def _quarantine_event_key(item_kind: str, persisted_id: object) -> str:
    """Identify corrupt rows without revalidating or embedding their persisted IDs."""

    identifier_digest = hashlib.sha256(
        str(persisted_id).encode("utf-8", errors="surrogatepass")
    ).hexdigest()
    return f"{item_kind}-{identifier_digest}-contract-rejected"


def _scope_columns(scope: RefreshScope) -> tuple[str, str, str, str, str, str | None, str, str]:
    return (
        scope.system_id,
        scope.target.kind.value,
        scope.target.target_id,
        scope.object_type,
        scope.facet,
        scope.capability_key,
        scope.coverage.value,
        _json_text(list(scope.field_mask), field_name="scope field_mask"),
    )


def _scope_from_row(row: sqlite3.Row, *, prefix: str = "") -> RefreshScope:
    return RefreshScope(
        system_id=row[f"{prefix}system_id"],
        target=TargetRef(TargetKind(row[f"{prefix}target_kind"]), row[f"{prefix}target_id"]),
        object_type=row[f"{prefix}object_type"],
        facet=row[f"{prefix}facet"],
        capability_key=row[f"{prefix}capability_key"],
        coverage=RefreshCoverage(row[f"{prefix}coverage"]),
        field_mask=tuple(_json_value(row[f"{prefix}field_mask_json"])),
    )


def _coverage_policy_from_value(value: object) -> CapabilityCoveragePolicy:
    if not isinstance(value, dict):
        raise ValueError("stored coverage policy is not an object")
    absence_authority = value.get("absence_authority")
    if not isinstance(absence_authority, list):
        raise ValueError("stored coverage absence authority is not a list")
    return CapabilityCoveragePolicy(
        target_kind=TargetKind(value.get("target_kind")),
        facet=value.get("facet"),  # type: ignore[arg-type]
        coverage=RefreshCoverage(value.get("coverage")),
        maximum_completeness=CollectionCoverage(value.get("maximum_completeness")),
        absence_authority=tuple(AbsenceAuthority(item) for item in absence_authority),
    )


def action_dedupe_key(
    *,
    system_id: str,
    connection_binding_id: str,
    capability_key: str,
    capability_version: str,
    scope: RefreshScope,
) -> str:
    """Return a deterministic digest over only registered, non-secret action data."""
    material = {
        "binding": require_uuid(connection_binding_id, "connection_binding_id"),
        "capability": [require_contract_key(capability_key, "capability_key"), capability_version],
        "scope": {
            "coverage": scope.coverage.value,
            "facet": scope.facet,
            "capability_key": scope.capability_key,
            "field_mask": list(scope.field_mask),
            "object_type": scope.object_type,
            "system_id": scope.system_id,
            "target": [scope.target.kind.value, scope.target.target_id],
        },
        "system_id": require_uuid(system_id, "system_id"),
    }
    encoded = json.dumps(
        material, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class SQLiteStore:
    """Concrete durable store and implementation of all shared local ports.

    The synchronous configuration and list helpers are for trusted local
    composition code.  The asynchronous methods match the shared ports used by
    presentation, coordinator, and adapter-worker layers.
    """

    def __init__(
        self,
        database_path: str | Path,
        *,
        clock: Callable[[], datetime] | None = None,
        available_bytes_probe: Callable[[], int] | None = None,
        minimum_write_headroom_bytes: int = MIN_WRITE_RESERVE_BYTES,
    ) -> None:
        if clock is not None and not callable(clock):
            raise TypeError("clock must be callable")
        if available_bytes_probe is not None and not callable(available_bytes_probe):
            raise TypeError("available_bytes_probe must be callable")
        if (
            isinstance(minimum_write_headroom_bytes, bool)
            or not isinstance(minimum_write_headroom_bytes, int)
            or minimum_write_headroom_bytes < MIN_WRITE_RESERVE_BYTES
        ):
            raise ValueError(
                f"minimum_write_headroom_bytes must be at least {MIN_WRITE_RESERVE_BYTES}"
            )
        if clock is None:
            wall_anchor = _now()
            elapsed_anchor = time.monotonic()

            def elapsed_utc_clock() -> datetime:
                elapsed = max(0.0, time.monotonic() - elapsed_anchor)
                return wall_anchor + timedelta(seconds=elapsed)

            self._clock = elapsed_utc_clock
        else:
            self._clock = clock
        self._clock_guard = threading.Lock()
        self._last_authority_time: datetime | None = None
        self.database_path = absolute_local_path(database_path)
        self.minimum_write_headroom_bytes = minimum_write_headroom_bytes
        self._available_bytes_probe = available_bytes_probe or (
            lambda: available_bytes(self.database_path.parent)
        )
        self._file_guard, preflight_kind = _create_or_preflight_database(self.database_path)
        self._directory_guard: PrivateDirectoryGuard | None = None
        try:
            state_directory = _prepare_state_directory(self.database_path.parent)
            self._directory_guard = PrivateDirectoryGuard(state_directory)
            harden_private_file(self.database_path)
            for sidecar in (
                Path(f"{self.database_path}-wal"),
                Path(f"{self.database_path}-shm"),
            ):
                if os.path.lexists(sidecar):
                    harden_private_file(sidecar)
            self._file_guard.verify()
            database_uri = f"{self.database_path.as_uri()}?mode=rw"
            connection = sqlite3.connect(
                database_uri,
                uri=True,
                isolation_level=None,
                check_same_thread=False,
            )
        except BaseException:
            self._file_guard.close()
            if self._directory_guard is not None:
                self._directory_guard.close()
            raise
        self._connection = connection
        self._connection.row_factory = sqlite3.Row
        self._connection.create_function(
            "rookery_is_canonical_timestamp",
            1,
            _stored_timestamp_is_canonical,
            deterministic=True,
        )
        self._lock = threading.RLock()
        try:
            with self._lock:
                self._connection.execute(f"PRAGMA busy_timeout = {_SQLITE_STARTUP_BUSY_TIMEOUT_MS}")
                live_kind = _validate_database_identity(self._connection)
                if live_kind is not preflight_kind and live_kind is not _DatabaseKind.MARKED:
                    raise OSError("Rookery database identity changed after immutable preflight")
                self._file_guard.verify()
                self._connection.execute("PRAGMA foreign_keys = ON")
                self._connection.execute("PRAGMA synchronous = FULL")
                if live_kind is not _DatabaseKind.MARKED:
                    journal_mode = self._connection.execute(
                        "PRAGMA journal_mode = DELETE"
                    ).fetchone()
                    if journal_mode is None or str(journal_mode[0]).lower() != "delete":
                        raise sqlite3.DatabaseError(
                            "unmarked Rookery database could not enter rollback journal mode"
                        )
                self._harden_storage_files()
            # Validation closes the same transaction that applies every migration and
            # index repair, so incompatible legacy DDL cannot retain partial cleanup.
            with self._immediate_transaction() as connection:
                self._migrate()
                self._repair_required_runtime_indexes()
                self._validate_current_schema()
                self._restore_authority_floor(connection)
                recovery_now = self._current_time()
                self._quarantine_invalid_queue_timing(connection, now=recovery_now)
                disabled_systems = tuple(
                    row["system_id"]
                    for row in connection.execute(
                        "SELECT system_id FROM systems WHERE enabled = 0"
                    ).fetchall()
                )
                self._cancel_authority_work(
                    connection,
                    system_ids=disabled_systems,
                    now_text=_utc_text(recovery_now),
                    reason="authority_legacy_disabled",
                )
                connection.execute(f"PRAGMA application_id = {_APPLICATION_ID}")
                self._harden_storage_files()
            self._enable_wal_mode()
            self._harden_storage_files()
            with self._lock:
                self._connection.execute(f"PRAGMA busy_timeout = {_SQLITE_BUSY_TIMEOUT_MS}")
        except BaseException:
            self._connection.close()
            self._file_guard.close()
            if self._directory_guard is not None:
                self._directory_guard.close()
            raise

    def _current_time(self) -> datetime:
        """Return the store-owned UTC clock used for action-lease authority."""

        sampled = require_utc(self._clock(), "SQLiteStore clock")
        with self._clock_guard:
            if self._last_authority_time is None or sampled > self._last_authority_time:
                self._last_authority_time = sampled
            return self._last_authority_time

    def _advance_authority_floor(self, value: datetime) -> datetime:
        value = require_utc(value, "authority floor")
        with self._clock_guard:
            if self._last_authority_time is None or value > self._last_authority_time:
                self._last_authority_time = value
            return self._last_authority_time

    def _restore_authority_floor(self, connection: sqlite3.Connection) -> None:
        candidates: list[datetime] = []
        lease_queries = (
            """
            SELECT MAX(lease_authority_at) AS lease_authority_at FROM refresh_intent_scopes
            WHERE state = 'leased' AND rookery_is_canonical_timestamp(lease_authority_at) = 1
            """,
            """
            SELECT MAX(leased_until) AS leased_until FROM adapter_actions
            WHERE state IN ('leased', 'running')
              AND rookery_is_canonical_timestamp(leased_until) = 1
            """,
        )
        intent_lease = connection.execute(lease_queries[0]).fetchone()
        if intent_lease is not None:
            value = _dt(intent_lease["lease_authority_at"])
            if value is not None:
                candidates.append(value)
        action_lease = connection.execute(lease_queries[1]).fetchone()
        if action_lease is not None:
            value = _dt(action_lease["leased_until"])
            if value is not None:
                candidates.append(value - _DEFAULT_LEASE)
        if candidates:
            self._advance_authority_floor(max(candidates))

    def _next_receipt_time(
        self, connection: sqlite3.Connection
    ) -> tuple[datetime, datetime | None]:
        prior_row = connection.execute(
            """
            SELECT received_at FROM observation_batches
                INDEXED BY ix_observation_batches_received_at
            ORDER BY received_at DESC, batch_id DESC
            LIMIT 1
            """
        ).fetchone()
        prior = _dt(prior_row["received_at"]) if prior_row is not None else None
        received_at = self._current_time()
        if prior is not None and received_at <= prior:
            received_at = prior + timedelta(microseconds=1)
        return received_at, prior

    def authority_time(self) -> datetime:
        """Expose one store-owned time sample to the local durable coordinator."""

        return self._current_time()

    @property
    def write_headroom_error(self) -> str:
        required_mib = (self.minimum_write_headroom_bytes + (1024**2 - 1)) // 1024**2
        return (
            f"Local storage cannot confirm the required {required_mib} MiB write headroom. "
            "Cached state remains available; free local space before requesting refreshes."
        )

    def write_headroom_available(self) -> bool:
        """Return whether new work can preserve the configured recovery reserve."""

        try:
            measured = self._available_bytes_probe()
        except Exception:
            return False
        return (
            not isinstance(measured, bool)
            and isinstance(measured, int)
            and measured >= self.minimum_write_headroom_bytes
        )

    def require_write_headroom(self) -> None:
        """Reject new work when available capacity cannot preserve recovery writes."""

        if not self.write_headroom_available():
            raise StorageHeadroomUnavailable(self.write_headroom_error)

    def _enable_wal_mode(self) -> None:
        for attempt in range(8):
            try:
                self._connection.execute("PRAGMA journal_mode = WAL")
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or attempt == 7:
                    raise
                time.sleep(min(0.25, 0.01 * (2**attempt)))

    def _harden_storage_files(self) -> None:
        if self._directory_guard is None:  # pragma: no cover - initialization invariant
            raise RuntimeError("Rookery state directory is not guarded")
        self._directory_guard.verify()
        self._file_guard.verify()
        harden_private_file(self.database_path)
        for path in (
            Path(f"{self.database_path}-wal"),
            Path(f"{self.database_path}-shm"),
        ):
            if os.path.lexists(path):
                harden_private_file(path)
        self._file_guard.verify()
        self._directory_guard.verify()

    def close(self) -> None:
        with self._lock:
            self._connection.close()
            self._file_guard.close()
            if self._directory_guard is not None:
                self._directory_guard.close()

    def __enter__(self) -> SQLiteStore:
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.close()

    @contextmanager
    def _immediate_transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            if self._connection.in_transaction:
                savepoint = f"nested_{uuid4().hex}"
                self._connection.execute(f"SAVEPOINT {savepoint}")
                try:
                    yield self._connection
                except BaseException:
                    self._connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                    self._connection.execute(f"RELEASE SAVEPOINT {savepoint}")
                    raise
                else:
                    self._connection.execute(f"RELEASE SAVEPOINT {savepoint}")
                return
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
            except BaseException:
                self._connection.rollback()
                raise
            else:
                self._connection.commit()

    @contextmanager
    def configuration_transaction(self) -> Iterator[None]:
        """Commit one complete desired-configuration application atomically."""

        with self._immediate_transaction():
            yield

    def _quarantine_invalid_queue_timing(
        self,
        connection: sqlite3.Connection,
        *,
        now: datetime,
    ) -> None:
        """Terminalize malformed state-dependent timing once during store recovery."""

        scope_rows = connection.execute(
            """
            SELECT * FROM refresh_intent_scopes
            WHERE (
                state = 'leased' AND (
                    lease_id IS NULL OR lease_worker_id IS NULL
                    OR rookery_is_canonical_timestamp(leased_until) = 0
                )
            ) OR (
                state = 'deferred' AND eligible_at IS NOT NULL
                AND rookery_is_canonical_timestamp(eligible_at) = 0
            )
            ORDER BY queue_requested_at, intent_scope_id
            """
        ).fetchall()
        for row in scope_rows:
            malformed_lease = row["state"] == "leased" and (
                row["lease_id"] is None
                or row["lease_worker_id"] is None
                or not _stored_timestamp_is_canonical(row["leased_until"])
            )
            malformed_deferral = (
                row["state"] == "deferred"
                and row["eligible_at"] is not None
                and not _stored_timestamp_is_canonical(row["eligible_at"])
            )
            if malformed_lease or malformed_deferral:
                self._terminalize_intent_scope_contract_failure(
                    connection,
                    row=row,
                    now=now,
                )

        action_rows = connection.execute(
            """
            SELECT * FROM adapter_actions
            WHERE (
                state IN ('leased', 'running') AND (
                    lease_id IS NULL OR lease_worker_id IS NULL
                    OR rookery_is_canonical_timestamp(leased_until) = 0
                )
            ) OR (
                state = 'retry_wait' AND rookery_is_canonical_timestamp(retry_at) = 0
            )
            ORDER BY record_created_at, action_id
            """
        ).fetchall()
        for row in action_rows:
            malformed_lease = row["state"] in {"leased", "running"} and (
                row["lease_id"] is None
                or row["lease_worker_id"] is None
                or not _stored_timestamp_is_canonical(row["leased_until"])
            )
            malformed_retry = row["state"] == "retry_wait" and not (
                _stored_timestamp_is_canonical(row["retry_at"])
            )
            if malformed_lease or malformed_retry:
                self._terminalize_action_contract_failure(
                    connection,
                    action_id=row["action_id"],
                    authority_now=now,
                    reason="persisted_action_timing_mismatch",
                )

    @staticmethod
    @contextmanager
    def _ingestion_item_savepoint(connection: sqlite3.Connection) -> Iterator[None]:
        """Roll back one rejected item without discarding valid batch siblings."""

        connection.execute("SAVEPOINT ingestion_item")
        try:
            yield
        except BaseException:
            connection.execute("ROLLBACK TO SAVEPOINT ingestion_item")
            connection.execute("RELEASE SAVEPOINT ingestion_item")
            raise
        else:
            connection.execute("RELEASE SAVEPOINT ingestion_item")

    def _migrate(self) -> None:
        if not self._connection.in_transaction:  # pragma: no cover - private invariant
            raise RuntimeError("schema migration requires an initialization transaction")
        with self._lock:
            _validate_database_identity(self._connection)
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version TEXT PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """
            )
            for migration in sorted(_MIGRATIONS_DIR.glob("*.sql")):
                script = migration.read_text(encoding="utf-8")
                applied = self._connection.execute(
                    "SELECT 1 FROM schema_migrations WHERE version = ?",
                    (migration.stem,),
                ).fetchone()
                if applied is None:
                    for statement in self._migration_statements(script):
                        self._execute_migration_statement(statement)
                    self._connection.execute(
                        "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                        (migration.stem, _utc_text(_now())),
                    )

    def _validate_current_schema(self) -> None:
        current_schema = dict(_current_schema_signature())
        tables = {
            row["name"]
            for row in self._connection.execute(
                """
                SELECT name FROM sqlite_schema
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                """
            )
        }
        if not set(current_schema).issubset(tables):
            raise sqlite3.DatabaseError("Rookery current database schema is incomplete")
        for table_name, expected_signature in current_schema.items():
            actual_signature = {
                (
                    row["name"],
                    row["type"].upper(),
                    int(row["notnull"]),
                    int(row["pk"]),
                )
                for row in self._connection.execute(
                    "SELECT * FROM pragma_table_info(?)",
                    (table_name,),
                )
            }
            if not expected_signature.issubset(actual_signature):
                raise sqlite3.DatabaseError(f"Rookery current {table_name} schema is incompatible")
        migrations = sorted(_MIGRATIONS_DIR.glob("*.sql"))
        if not migrations:  # pragma: no cover - source packaging invariant
            raise RuntimeError("Rookery migrations are unavailable")
        expected_ddl = {
            (object_type, name): (table_name, sql)
            for object_type, name, table_name, sql in _schema_ddl_through(
                migrations[-1].stem,
                include_runtime_repairs=True,
            )
        }
        actual_ddl = _schema_ddl(self._connection)
        expected_ddl.pop(("table", "schema_migrations"), None)
        actual_ddl.pop(("table", "schema_migrations"), None)
        unexpected = set(actual_ddl) - set(expected_ddl)
        if unexpected:
            raise sqlite3.DatabaseError("Rookery current schema has an unexpected object")
        for identity, expected in expected_ddl.items():
            if actual_ddl.get(identity) != expected:
                raise sqlite3.DatabaseError(
                    f"Rookery current schema object {identity[1]} is incompatible"
                )

    def _repair_required_runtime_indexes(self) -> None:
        """Reassert indexes required by forced runtime query plans after restore."""

        with self._immediate_transaction() as connection:
            for index_name, (
                table_name,
                expected_keys,
                expected_partial,
                statement,
            ) in _REQUIRED_RUNTIME_INDEXES.items():
                schema_row = connection.execute(
                    "SELECT tbl_name, sql FROM sqlite_schema WHERE type = 'index' AND name = ?",
                    (index_name,),
                ).fetchone()
                properties = connection.execute(
                    """
                    SELECT "unique", origin, partial
                    FROM pragma_index_list(?)
                    WHERE name = ?
                    """,
                    (table_name, index_name),
                ).fetchone()
                key_columns = tuple(
                    (row["name"], row["desc"], row["coll"])
                    for row in connection.execute(
                        """
                        SELECT name, "desc", coll
                        FROM pragma_index_xinfo(?)
                        WHERE key = 1
                        ORDER BY seqno
                        """,
                        (index_name,),
                    ).fetchall()
                )
                valid = (
                    schema_row is not None
                    and schema_row["tbl_name"] == table_name
                    and _normalized_schema_sql(schema_row["sql"])
                    == _normalized_schema_sql(statement)
                    and properties is not None
                    and properties["unique"] == 0
                    and properties["origin"] == "c"
                    and properties["partial"] == expected_partial
                    and key_columns == expected_keys
                )
                if valid:
                    continue
                if schema_row is not None:
                    connection.execute(f'DROP INDEX "{index_name}"')
                self._execute_required_runtime_index_statement(connection, statement)

    @staticmethod
    def _execute_required_runtime_index_statement(
        connection: sqlite3.Connection, statement: str
    ) -> None:
        """Execute canonical repair DDL; isolated for atomic-failure testing."""

        connection.execute(statement)

    @staticmethod
    def _migration_statements(script: str) -> Iterator[str]:
        """Split trusted migration SQL without letting ``executescript`` commit.

        Migration files are static package resources, but each statement still
        executes under the caller's explicit transaction. ``complete_statement``
        understands SQLite quoting and comments better than newline splitting.
        """
        buffer = ""
        for line in script.splitlines(keepends=True):
            buffer += line
            if not sqlite3.complete_statement(buffer):
                continue
            statement = buffer.strip()
            if statement:
                yield statement
            buffer = ""
        if buffer.strip():
            raise ValueError("migration SQL ends with an incomplete statement")

    def _execute_migration_statement(self, statement: str) -> None:
        """Execute one static migration statement; isolated for failure testing."""
        try:
            self._connection.execute(statement)
        except sqlite3.OperationalError as exc:
            table = _CAPABILITY_KEY_ALTER_TABLES.get(statement.strip())
            if table is None or "duplicate column name" not in str(exc).lower():
                raise
            columns = self._migration_table_columns(table)
            if columns.get("capability_key", "").upper() != "TEXT":
                raise RuntimeError(
                    "partially applied capability migration has incompatible schema"
                ) from exc

    def _migration_table_columns(self, table: str) -> dict[str, str]:
        if table == "refresh_credit":
            rows = self._connection.execute("PRAGMA table_info(refresh_credit)").fetchall()
        elif table == "refresh_intent_scopes":
            rows = self._connection.execute("PRAGMA table_info(refresh_intent_scopes)").fetchall()
        elif table == "adapter_action_scopes":
            rows = self._connection.execute("PRAGMA table_info(adapter_action_scopes)").fetchall()
        else:  # pragma: no cover - fixed migration table map
            raise ValueError("unsupported migration table")
        return {row["name"]: row["type"] for row in rows}

    @staticmethod
    def _validate_non_secret_settings(settings: object) -> str:
        validated = validate_json(settings, "non_secret_settings")

        def check(value: object, path: str = "") -> None:
            if isinstance(value, dict):
                for key, nested in value.items():
                    child_path = f"{path}.{key}" if path else key
                    if _SECRET_SETTING_PART.search(key):
                        raise ValueError(
                            f"non_secret_settings must not contain credential field {child_path!r}"
                        )
                    check(nested, child_path)
            elif isinstance(value, list):
                for nested in value:
                    check(nested, path)

        check(validated)
        return _json_text(validated, field_name="non_secret_settings")

    def create_system(
        self,
        *,
        system_id: str,
        display_name: str,
        system_kind: str,
        enabled: bool = True,
        now: datetime | None = None,
    ) -> SystemRecord:
        system_id = require_uuid(system_id, "system_id")
        require_text(display_name, "display_name", max_length=1024)
        require_contract_key(system_kind, "system_kind")
        timestamp = _utc_text(now or _now())
        with self._immediate_transaction() as connection:
            connection.execute(
                """
                INSERT INTO systems (
                    system_id, display_name, system_kind, enabled,
                    record_created_at, record_updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(system_id) DO UPDATE SET
                    display_name = excluded.display_name,
                    system_kind = excluded.system_kind,
                    enabled = excluded.enabled,
                    record_updated_at = excluded.record_updated_at
                """,
                (system_id, display_name, system_kind, int(enabled), timestamp, timestamp),
            )
        result = self.get_system(system_id)
        if result is None:  # pragma: no cover - transaction invariant
            raise RuntimeError("created system was not found")
        return result

    def get_system(self, system_id: str) -> SystemRecord | None:
        system_id = require_uuid(system_id, "system_id")
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM systems WHERE system_id = ?", (system_id,)
            ).fetchone()
        return self._system_from_row(row) if row else None

    def list_systems(self) -> tuple[SystemRecord, ...]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM systems ORDER BY display_name, system_id"
            ).fetchall()
        return tuple(self._system_from_row(row) for row in rows)

    @staticmethod
    def _system_from_row(row: sqlite3.Row) -> SystemRecord:
        return SystemRecord(
            system_id=row["system_id"],
            display_name=row["display_name"],
            system_kind=row["system_kind"],
            enabled=bool(row["enabled"]),
            record_created_at=_dt(row["record_created_at"]),  # type: ignore[arg-type]
            record_updated_at=_dt(row["record_updated_at"]),  # type: ignore[arg-type]
        )

    def set_system_enabled(
        self, system_id: str, *, enabled: bool, now: datetime | None = None
    ) -> None:
        system_id = require_uuid(system_id, "system_id")
        timestamp = _utc_text(now or _now())
        with self._immediate_transaction() as connection:
            result = connection.execute(
                "UPDATE systems SET enabled = ?, record_updated_at = ? WHERE system_id = ?",
                (int(enabled), timestamp, system_id),
            )
            if result.rowcount != 1:
                raise ValueError("unknown system")

    def get_configured_system_identity(
        self, *, system_kind: str, config_id: str, authority_key: str
    ) -> str | None:
        """Return the earliest durable mapping across ASCII-case aliases."""

        require_contract_key(system_kind, "system_kind")
        config_id = _canonical_config_id(config_id)
        require_text(authority_key, "authority_key", max_length=2048)
        with self._lock:
            row = self._connection.execute(
                """
                SELECT system_id
                FROM configured_system_identities
                WHERE system_kind = ? AND config_id = ? COLLATE NOCASE AND authority_key = ?
                ORDER BY record_created_at, config_id, system_id
                LIMIT 1
                """,
                (system_kind, config_id, authority_key),
            ).fetchone()
        return row["system_id"] if row is not None else None

    def get_configured_identity_for_system(self, system_id: str) -> tuple[str, str] | None:
        """Return the non-secret config ID and authority key for one local system."""

        system_id = require_uuid(system_id, "system_id")
        with self._lock:
            row = self._connection.execute(
                """
                SELECT config_id, authority_key FROM configured_system_identities
                WHERE system_id = ?
                """,
                (system_id,),
            ).fetchone()
        return (row["config_id"], row["authority_key"]) if row is not None else None

    def is_system_authority_retired(self, system_id: str) -> bool:
        system_id = require_uuid(system_id, "system_id")
        with self._lock:
            row = self._connection.execute(
                "SELECT 1 FROM retired_system_authorities WHERE system_id = ?",
                (system_id,),
            ).fetchone()
        return row is not None

    def list_authorities(self) -> tuple[AuthorityRecord, ...]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT system.system_id, system.display_name, system.enabled,
                       retired.system_id IS NOT NULL AS retired,
                       identity.config_id,
                       json_extract(binding.non_secret_settings_json, '$.workspace_root')
                           AS workspace_root,
                       json_extract(binding.non_secret_settings_json, '$.authority_fingerprint')
                           AS authority_fingerprint,
                       (
                           SELECT MAX(
                               COALESCE(action.completed_at, action.started_at,
                                        action.record_created_at)
                           )
                           FROM adapter_actions AS action
                           WHERE action.system_id = system.system_id
                       ) AS last_activity_at
                FROM systems AS system
                LEFT JOIN retired_system_authorities AS retired
                    ON retired.system_id = system.system_id
                LEFT JOIN configured_system_identities AS identity
                    ON identity.system_id = system.system_id
                LEFT JOIN connection_bindings AS binding
                    ON binding.system_id = system.system_id
                WHERE system.system_kind = 'databricks.workspace'
                ORDER BY system.enabled DESC, system.display_name, system.system_id
                """
            ).fetchall()
        return tuple(
            AuthorityRecord(
                system_id=row["system_id"],
                display_name=row["display_name"],
                enabled=bool(row["enabled"]),
                retired=bool(row["retired"]),
                config_id=row["config_id"],
                workspace_root=row["workspace_root"],
                authority_fingerprint=row["authority_fingerprint"],
                last_activity_at=_dt(row["last_activity_at"]),
            )
            for row in rows
        )

    def _cancel_authority_work(
        self,
        connection: sqlite3.Connection,
        *,
        system_ids: tuple[str, ...],
        now_text: str,
        reason: str,
    ) -> None:
        if not system_ids:
            return
        encoded_system_ids = _json_text(list(system_ids), field_name="authority system IDs")
        connection.execute(
            """
            UPDATE refresh_intent_scopes
            SET state = 'cancelled', disposition_reason = ?, eligible_at = NULL,
                lease_id = NULL, lease_worker_id = NULL, leased_until = NULL
            WHERE intent_scope_id IN (
                SELECT link.intent_scope_id
                FROM action_intent_scopes AS link
                JOIN adapter_actions AS action ON action.action_id = link.action_id
                WHERE action.system_id IN (SELECT value FROM json_each(?))
                  AND action.state IN ('ready', 'leased', 'running', 'retry_wait')
            ) AND state IN ('admitted', 'coalesced')
            """,
            (reason, encoded_system_ids),
        )
        connection.execute(
            """
            UPDATE adapter_actions
            SET state = 'cancelled', completed_at = ?, lease_id = NULL,
                lease_worker_id = NULL, leased_until = NULL, retry_at = NULL,
                error_class = ?, redacted_diagnostic = ?
            WHERE system_id IN (SELECT value FROM json_each(?))
              AND state IN ('ready', 'leased', 'running', 'retry_wait')
            """,
            (
                now_text,
                ErrorClass.LOCAL_CANCELLATION.value,
                reason,
                encoded_system_ids,
            ),
        )
        connection.execute(
            """
            UPDATE refresh_intent_scopes
            SET state = 'cancelled', disposition_reason = ?, eligible_at = NULL,
                lease_id = NULL, lease_worker_id = NULL, leased_until = NULL
            WHERE system_id IN (SELECT value FROM json_each(?))
              AND state IN ('queued', 'leased', 'deferred')
            """,
            (reason, encoded_system_ids),
        )
        connection.execute(
            """
            UPDATE refresh_intents AS intent
            SET aggregate_state = CASE WHEN EXISTS (
                SELECT 1
                FROM refresh_intent_scopes AS scope
                    INDEXED BY ix_refresh_intent_scopes_intent_system
                LEFT JOIN adapter_actions AS action
                    ON action.action_id = scope.linked_action_id
                WHERE scope.intent_id = intent.intent_id
                  AND scope.state NOT IN ('satisfied', 'rejected', 'expired', 'cancelled')
                  AND COALESCE(action.state, '') NOT IN (
                      'satisfied', 'succeeded', 'partial', 'failed', 'cancelled'
                  )
            ) THEN 'open' ELSE 'complete' END
            WHERE EXISTS (
                SELECT 1
                FROM refresh_intent_scopes AS affected
                    INDEXED BY ix_refresh_intent_scopes_intent_system
                WHERE affected.intent_id = intent.intent_id
                  AND affected.system_id IN (SELECT value FROM json_each(?))
            )
            """,
            (encoded_system_ids,),
        )

    def set_authority_retired(
        self,
        system_id: str,
        *,
        retired: bool,
        now: datetime | None = None,
    ) -> None:
        system_id = require_uuid(system_id, "system_id")
        timestamp = _utc_text(now or _now())
        with self._immediate_transaction() as connection:
            system = connection.execute(
                "SELECT system_kind FROM systems WHERE system_id = ?",
                (system_id,),
            ).fetchone()
            if system is None or system["system_kind"] != "databricks.workspace":
                raise ValueError("unknown Databricks authority")
            if retired:
                connection.execute(
                    """
                    INSERT INTO retired_system_authorities (system_id, retired_at)
                    VALUES (?, ?)
                    ON CONFLICT(system_id) DO NOTHING
                    """,
                    (system_id, timestamp),
                )
                self._cancel_authority_work(
                    connection,
                    system_ids=(system_id,),
                    now_text=timestamp,
                    reason="authority_retired",
                )
                connection.execute(
                    "UPDATE systems SET enabled = 0, record_updated_at = ? WHERE system_id = ?",
                    (timestamp, system_id),
                )
                connection.execute(
                    "UPDATE connection_bindings SET enabled = 0, record_updated_at = ? "
                    "WHERE system_id = ?",
                    (timestamp, system_id),
                )
                connection.execute(
                    "UPDATE capability_bindings SET enabled = 0, record_updated_at = ? "
                    "WHERE connection_binding_id IN ("
                    "SELECT binding_id FROM connection_bindings WHERE system_id = ?)",
                    (timestamp, system_id),
                )
                connection.execute(
                    "UPDATE configured_scopes SET enabled = 0, record_updated_at = ? "
                    "WHERE system_id = ?",
                    (timestamp, system_id),
                )
            else:
                connection.execute(
                    "DELETE FROM retired_system_authorities WHERE system_id = ?",
                    (system_id,),
                )

    def upsert_configured_system_identity(
        self,
        *,
        system_kind: str,
        config_id: str,
        authority_key: str,
        system_id: str,
        now: datetime | None = None,
    ) -> None:
        require_contract_key(system_kind, "system_kind")
        config_id = _canonical_config_id(config_id)
        require_text(authority_key, "authority_key", max_length=2048)
        system_id = require_uuid(system_id, "system_id")
        timestamp = _utc_text(now or _now())
        with self._immediate_transaction() as connection:
            system = connection.execute(
                "SELECT system_kind FROM systems WHERE system_id = ?",
                (system_id,),
            ).fetchone()
            if system is None or system["system_kind"] != system_kind:
                raise ValueError("configured identity references an incompatible system")
            aliases = connection.execute(
                """
                SELECT record_created_at
                FROM configured_system_identities
                WHERE system_kind = ? AND config_id = ? COLLATE NOCASE AND authority_key = ?
                ORDER BY record_created_at, config_id, system_id
                """,
                (system_kind, config_id, authority_key),
            ).fetchall()
            created_at = aliases[0]["record_created_at"] if aliases else timestamp
            connection.execute(
                """
                DELETE FROM configured_system_identities
                WHERE system_kind = ? AND (
                    (config_id = ? COLLATE NOCASE AND authority_key = ?)
                    OR system_id = ?
                )
                """,
                (system_kind, config_id, authority_key, system_id),
            )
            connection.execute(
                """
                INSERT INTO configured_system_identities (
                    system_kind, config_id, authority_key, system_id,
                    record_created_at, record_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (system_kind, config_id, authority_key, system_id, created_at, timestamp),
            )

    def reconcile_configured_resources(
        self,
        *,
        system_kind: str,
        system_ids: Iterable[str],
        connection_binding_ids: Iterable[str],
        capability_binding_ids: Iterable[str],
        scope_ids: Iterable[str],
        now: datetime | None = None,
    ) -> None:
        """Apply one configuration source as desired enabled state without deleting cache."""
        require_contract_key(system_kind, "system_kind")
        desired_systems = tuple(require_uuid(value, "system_id") for value in system_ids)
        desired_bindings = tuple(
            require_uuid(value, "connection_binding_id") for value in connection_binding_ids
        )
        desired_capabilities = tuple(
            require_uuid(value, "capability_binding_id") for value in capability_binding_ids
        )
        desired_scopes = tuple(require_uuid(value, "scope_id") for value in scope_ids)
        timestamp = _utc_text(now or _now())
        with self._immediate_transaction() as connection:
            active_systems = tuple(
                row["system_id"]
                for row in connection.execute(
                    "SELECT system_id FROM systems WHERE system_kind = ? AND enabled = 1",
                    (system_kind,),
                ).fetchall()
            )
            retired_systems = {
                row["system_id"]
                for row in connection.execute(
                    "SELECT system_id FROM retired_system_authorities"
                ).fetchall()
            }
            revoked_systems = tuple(
                system_id
                for system_id in active_systems
                if system_id not in desired_systems or system_id in retired_systems
            )
            self._cancel_authority_work(
                connection,
                system_ids=revoked_systems,
                now_text=timestamp,
                reason="authority_removed",
            )
            connection.execute(
                "UPDATE capability_bindings SET enabled = 0, record_updated_at = ? "
                "WHERE connection_binding_id IN ("
                "SELECT binding_id FROM connection_bindings WHERE system_id IN ("
                "SELECT system_id FROM systems WHERE system_kind = ?))",
                (timestamp, system_kind),
            )
            connection.execute(
                "UPDATE connection_bindings SET enabled = 0, record_updated_at = ? "
                "WHERE system_id IN ("
                "SELECT system_id FROM systems WHERE system_kind = ?)",
                (timestamp, system_kind),
            )
            connection.execute(
                "UPDATE configured_scopes SET enabled = 0, record_updated_at = ? "
                "WHERE system_id IN ("
                "SELECT system_id FROM systems WHERE system_kind = ?)",
                (timestamp, system_kind),
            )
            connection.execute(
                "UPDATE systems SET enabled = 0, record_updated_at = ? WHERE system_kind = ?",
                (timestamp, system_kind),
            )
            connection.executemany(
                "UPDATE systems SET enabled = 1, record_updated_at = ? "
                "WHERE system_id = ? AND system_kind = ? AND NOT EXISTS ("
                "SELECT 1 FROM retired_system_authorities AS retired "
                "WHERE retired.system_id = systems.system_id)",
                ((timestamp, value, system_kind) for value in desired_systems),
            )
            connection.executemany(
                "UPDATE connection_bindings SET enabled = 1, record_updated_at = ? "
                "WHERE binding_id = ? AND system_id IN ("
                "SELECT system_id FROM systems WHERE system_kind = ? AND enabled = 1)",
                ((timestamp, value, system_kind) for value in desired_bindings),
            )
            connection.executemany(
                "UPDATE capability_bindings SET enabled = 1, record_updated_at = ? "
                "WHERE capability_binding_id = ? AND connection_binding_id IN ("
                "SELECT binding_id FROM connection_bindings WHERE system_id IN ("
                "SELECT system_id FROM systems WHERE system_kind = ? AND enabled = 1))",
                ((timestamp, value, system_kind) for value in desired_capabilities),
            )
            connection.executemany(
                "UPDATE configured_scopes SET enabled = 1, record_updated_at = ? "
                "WHERE scope_id = ? AND system_id IN ("
                "SELECT system_id FROM systems WHERE system_kind = ? AND enabled = 1)",
                ((timestamp, value, system_kind) for value in desired_scopes),
            )

    def upsert_connection_binding(
        self, binding: ConnectionBinding, *, now: datetime | None = None
    ) -> None:
        settings = self._validate_non_secret_settings(binding.non_secret_settings)
        timestamp = _utc_text(now or _now())
        with self._immediate_transaction() as connection:
            if (
                connection.execute(
                    "SELECT 1 FROM systems WHERE system_id = ?", (binding.system_id,)
                ).fetchone()
                is None
            ):
                raise ValueError("connection binding references an unknown system")
            connection.execute(
                """
                INSERT INTO connection_bindings (
                    binding_id, system_id, adapter_key, adapter_version, enabled,
                    non_secret_settings_json, secret_reference, record_created_at, record_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(binding_id) DO UPDATE SET
                    system_id = excluded.system_id,
                    adapter_key = excluded.adapter_key,
                    adapter_version = excluded.adapter_version,
                    enabled = excluded.enabled,
                    non_secret_settings_json = excluded.non_secret_settings_json,
                    secret_reference = excluded.secret_reference,
                    record_updated_at = excluded.record_updated_at
                """,
                (
                    binding.binding_id,
                    binding.system_id,
                    binding.adapter_key,
                    binding.adapter_version,
                    int(binding.enabled),
                    settings,
                    binding.secret_reference,
                    timestamp,
                    timestamp,
                ),
            )

    def upsert_capability_binding(
        self, capability: CapabilityBinding, *, now: datetime | None = None
    ) -> None:
        timestamp = _utc_text(now or _now())
        target_kinds_json = _json_text(
            [item.value for item in capability.target_kinds], field_name="target_kinds"
        )
        produced_facets_json = _json_text(
            list(capability.produced_facets), field_name="produced_facets"
        )
        coverage_policies_json = _json_text(
            [policy.to_dict() for policy in capability.coverage_policies],
            field_name="coverage_policies",
        )
        with self._immediate_transaction() as connection:
            if (
                connection.execute(
                    "SELECT 1 FROM connection_bindings WHERE binding_id = ?",
                    (capability.connection_binding_id,),
                ).fetchone()
                is None
            ):
                raise ValueError("capability binding references an unknown connection binding")
            existing = connection.execute(
                """
                SELECT connection_binding_id, capability_key, capability_version,
                       operation_class, target_kinds_json, produced_facets_json,
                       coverage_policies_json, coverage_policy_initialized
                FROM capability_bindings WHERE capability_binding_id = ?
                """,
                (capability.capability_binding_id,),
            ).fetchone()
            if existing is not None:
                immutable_values = (
                    (existing["connection_binding_id"], capability.connection_binding_id),
                    (existing["capability_key"], capability.capability_key),
                    (existing["capability_version"], capability.capability_version),
                    (existing["operation_class"], capability.operation_class.value),
                    (existing["target_kinds_json"], target_kinds_json),
                    (existing["produced_facets_json"], produced_facets_json),
                )
                if any(stored != requested for stored, requested in immutable_values):
                    raise ValueError("capability version contract is immutable")
                stored_policy = existing["coverage_policies_json"]
                if (
                    existing["coverage_policy_initialized"]
                    and stored_policy != coverage_policies_json
                ):
                    raise ValueError("capability coverage policy requires a version change")
            connection.execute(
                """
                INSERT INTO capability_bindings (
                    capability_binding_id, connection_binding_id, capability_key,
                    capability_version, operation_class, target_kinds_json,
                    produced_facets_json, coverage_policies_json,
                    coverage_policy_initialized, enabled, selection_priority,
                    collateral_effects_json, mitigations_json, record_created_at,
                    record_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(capability_binding_id) DO UPDATE SET
                    connection_binding_id = excluded.connection_binding_id,
                    capability_key = excluded.capability_key,
                    capability_version = excluded.capability_version,
                    operation_class = excluded.operation_class,
                    target_kinds_json = excluded.target_kinds_json,
                    produced_facets_json = excluded.produced_facets_json,
                    coverage_policies_json = excluded.coverage_policies_json,
                    coverage_policy_initialized = 1,
                    enabled = excluded.enabled,
                    selection_priority = excluded.selection_priority,
                    collateral_effects_json = excluded.collateral_effects_json,
                    mitigations_json = excluded.mitigations_json,
                    record_updated_at = excluded.record_updated_at
                """,
                (
                    capability.capability_binding_id,
                    capability.connection_binding_id,
                    capability.capability_key,
                    capability.capability_version,
                    capability.operation_class.value,
                    target_kinds_json,
                    produced_facets_json,
                    coverage_policies_json,
                    int(capability.enabled),
                    capability.selection_priority,
                    _json_text(
                        list(capability.collateral_effects), field_name="collateral_effects"
                    ),
                    _json_text(list(capability.mitigations), field_name="mitigations"),
                    timestamp,
                    timestamp,
                ),
            )

    def create_configured_scope(
        self,
        *,
        scope_id: str,
        system_id: str,
        object_type: str,
        display_name: str,
        object_id: str | None = None,
        enabled: bool = True,
        now: datetime | None = None,
    ) -> ConfiguredScopeRecord:
        scope_id = require_uuid(scope_id, "scope_id")
        system_id = require_uuid(system_id, "system_id")
        object_id = require_uuid(object_id, "object_id") if object_id else None
        require_contract_key(object_type, "object_type")
        require_text(display_name, "display_name", max_length=1024)
        timestamp = _utc_text(now or _now())
        with self._immediate_transaction() as connection:
            if (
                connection.execute(
                    "SELECT 1 FROM systems WHERE system_id = ?", (system_id,)
                ).fetchone()
                is None
            ):
                raise ValueError("configured scope references an unknown system")
            if (
                object_id
                and connection.execute(
                    "SELECT 1 FROM remote_objects WHERE object_id = ? AND system_id = ?",
                    (object_id, system_id),
                ).fetchone()
                is None
            ):
                raise ValueError("configured scope object does not belong to its system")
            connection.execute(
                """
                INSERT INTO configured_scopes (
                    scope_id, system_id, object_id, object_type, enabled, display_name,
                    record_created_at, record_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(scope_id) DO UPDATE SET
                    system_id = excluded.system_id,
                    object_id = excluded.object_id,
                    object_type = excluded.object_type,
                    enabled = excluded.enabled,
                    display_name = excluded.display_name,
                    record_updated_at = excluded.record_updated_at
                """,
                (
                    scope_id,
                    system_id,
                    object_id,
                    object_type,
                    int(enabled),
                    display_name,
                    timestamp,
                    timestamp,
                ),
            )
        result = self.get_configured_scope(scope_id)
        if result is None:  # pragma: no cover - transaction invariant
            raise RuntimeError("created configured scope was not found")
        return result

    def get_configured_scope(self, scope_id: str) -> ConfiguredScopeRecord | None:
        scope_id = require_uuid(scope_id, "scope_id")
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM configured_scopes WHERE scope_id = ?", (scope_id,)
            ).fetchone()
        return self._configured_scope_from_row(row) if row else None

    def list_configured_scopes(
        self, *, system_id: str | None = None
    ) -> tuple[ConfiguredScopeRecord, ...]:
        """List registered local discovery scopes after restart."""
        if system_id is None:
            statement = "SELECT * FROM configured_scopes ORDER BY display_name, scope_id"
            parameters: tuple[object, ...] = ()
        else:
            statement = (
                "SELECT * FROM configured_scopes WHERE system_id = ? "
                "ORDER BY display_name, scope_id"
            )
            parameters = (require_uuid(system_id, "system_id"),)
        with self._lock:
            rows = self._connection.execute(statement, parameters).fetchall()
        return tuple(self._configured_scope_from_row(row) for row in rows)

    @staticmethod
    def _configured_scope_from_row(row: sqlite3.Row) -> ConfiguredScopeRecord:
        return ConfiguredScopeRecord(
            scope_id=row["scope_id"],
            system_id=row["system_id"],
            object_id=row["object_id"],
            object_type=row["object_type"],
            enabled=bool(row["enabled"]),
            display_name=row["display_name"],
            record_created_at=_dt(row["record_created_at"]),  # type: ignore[arg-type]
            record_updated_at=_dt(row["record_updated_at"]),  # type: ignore[arg-type]
        )

    def upsert_object(self, remote_object: RemoteObject) -> RemoteObject:
        """Create or update a configured object without manufacturing presence."""
        with self._immediate_transaction() as connection:
            if (
                connection.execute(
                    "SELECT 1 FROM systems WHERE system_id = ?", (remote_object.system_id,)
                ).fetchone()
                is None
            ):
                raise ValueError("remote object references an unknown system")
            connection.execute(
                """
                INSERT INTO remote_objects (
                    object_id, system_id, object_type, object_type_version,
                    source_kind, external_key, display_name, presence,
                    first_seen_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(system_id, source_kind, external_key) DO UPDATE SET
                    display_name = excluded.display_name,
                    presence = CASE
                        WHEN remote_objects.presence = 'unknown' THEN excluded.presence
                        ELSE remote_objects.presence
                    END,
                    last_seen_at = CASE
                        WHEN remote_objects.last_seen_at IS NULL THEN excluded.last_seen_at
                        WHEN excluded.last_seen_at IS NULL THEN remote_objects.last_seen_at
                        WHEN excluded.last_seen_at > remote_objects.last_seen_at
                            THEN excluded.last_seen_at
                        ELSE remote_objects.last_seen_at
                    END
                """,
                (
                    remote_object.object_id,
                    remote_object.system_id,
                    remote_object.object_type,
                    remote_object.object_type_version,
                    remote_object.source_kind,
                    remote_object.external_key,
                    remote_object.display_name,
                    remote_object.presence.value,
                    _utc_text(remote_object.first_seen_at),
                    _utc_text(remote_object.last_seen_at) if remote_object.last_seen_at else None,
                ),
            )
            row = connection.execute(
                """
                SELECT * FROM remote_objects
                WHERE system_id = ? AND source_kind = ? AND external_key = ?
                """,
                (remote_object.system_id, remote_object.source_kind, remote_object.external_key),
            ).fetchone()
        if row is None:  # pragma: no cover - transaction invariant
            raise RuntimeError("upserted object was not found")
        return self._object_from_row(row)

    @staticmethod
    def _object_from_row(row: sqlite3.Row) -> RemoteObject:
        return RemoteObject(
            object_id=row["object_id"],
            system_id=row["system_id"],
            object_type=row["object_type"],
            object_type_version=row["object_type_version"],
            source_kind=row["source_kind"],
            external_key=row["external_key"],
            display_name=row["display_name"],
            presence=PresenceState(row["presence"]),
            first_seen_at=_dt(row["first_seen_at"]),  # type: ignore[arg-type]
            last_seen_at=_dt(row["last_seen_at"]),
        )

    def _get_object_row(self, object_id: str) -> sqlite3.Row | None:
        return self._connection.execute(
            "SELECT * FROM remote_objects WHERE object_id = ?", (object_id,)
        ).fetchone()

    def get_object_sync(self, object_id: str) -> RemoteObject | None:
        object_id = require_uuid(object_id, "object_id")
        with self._lock:
            row = self._get_object_row(object_id)
        return self._object_from_row(row) if row else None

    def list_objects(self, *, system_id: str | None = None) -> tuple[RemoteObject, ...]:
        if system_id is None:
            statement = "SELECT * FROM remote_objects ORDER BY display_name, object_id"
            parameters: tuple[object, ...] = ()
        else:
            statement = (
                "SELECT * FROM remote_objects WHERE system_id = ? ORDER BY display_name, object_id"
            )
            parameters = (require_uuid(system_id, "system_id"),)
        with self._lock:
            rows = self._connection.execute(statement, parameters).fetchall()
        return tuple(self._object_from_row(row) for row in rows)

    def count_objects(self, *, query: str = "") -> int:
        pattern = _object_search_pattern(query)
        statement = "SELECT COUNT(*) FROM remote_objects"
        parameters: tuple[object, ...] = ()
        if pattern is not None:
            statement += (
                " WHERE display_name LIKE ? ESCAPE '\\'"
                " OR object_type LIKE ? ESCAPE '\\'"
                " OR source_kind LIKE ? ESCAPE '\\'"
            )
            parameters = (pattern, pattern, pattern)
        with self._lock:
            row = self._connection.execute(statement, parameters).fetchone()
        return int(row[0]) if row is not None else 0

    def list_objects_page(
        self,
        *,
        offset: int,
        limit: int,
        query: str = "",
    ) -> tuple[RemoteObject, ...]:
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("object offset must be a non-negative integer")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("object limit must be between 1 and 100")
        pattern = _object_search_pattern(query)
        statement = "SELECT * FROM remote_objects"
        parameters: tuple[object, ...]
        if pattern is None:
            parameters = (limit, offset)
        else:
            statement += (
                " WHERE display_name LIKE ? ESCAPE '\\'"
                " OR object_type LIKE ? ESCAPE '\\'"
                " OR source_kind LIKE ? ESCAPE '\\'"
            )
            parameters = (pattern, pattern, pattern, limit, offset)
        statement += " ORDER BY display_name, object_id LIMIT ? OFFSET ?"
        with self._lock:
            rows = self._connection.execute(statement, parameters).fetchall()
        return tuple(self._object_from_row(row) for row in rows)

    def list_objects_after(
        self,
        *,
        after_name: str | None,
        after_id: str | None,
        limit: int,
        query: str = "",
    ) -> tuple[RemoteObject, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 101:
            raise ValueError("object cursor limit must be between 1 and 101")
        if query and (after_name is None) != (after_id is None):
            raise ValueError("filtered object cursor is incomplete")
        if not query and after_name is not None:
            raise ValueError("unfiltered object cursor has an unexpected name")
        if after_name is not None:
            require_text(after_name, "object cursor name", max_length=512)
        if after_id is not None:
            after_id = require_uuid(after_id, "object cursor ID")
        pattern = _object_search_pattern(query)
        conditions: list[str] = []
        parameters: list[object] = []
        if pattern is not None:
            conditions.append("display_name LIKE ? ESCAPE '\\' COLLATE NOCASE")
            parameters.append(pattern)
            if after_name is not None:
                conditions.append(
                    "(display_name COLLATE NOCASE, object_id) > (? COLLATE NOCASE, ?)"
                )
                parameters.extend((after_name, after_id))
        if after_id is not None and pattern is None:
            conditions.append("object_id > ?")
            parameters.append(after_id)
        statement = "SELECT * FROM remote_objects"
        if conditions:
            statement += " WHERE " + " AND ".join(conditions)
        statement += (
            " ORDER BY display_name COLLATE NOCASE, object_id LIMIT ?"
            if pattern is not None
            else " ORDER BY object_id LIMIT ?"
        )
        parameters.append(limit)
        with self._lock:
            rows = self._connection.execute(statement, tuple(parameters)).fetchall()
        return tuple(self._object_from_row(row) for row in rows)

    def _get_facet_row(self, object_id: str, facet: str) -> sqlite3.Row | None:
        return self._connection.execute(
            "SELECT * FROM facets WHERE object_id = ? AND facet = ?", (object_id, facet)
        ).fetchone()

    @staticmethod
    def _facet_from_row(row: sqlite3.Row) -> FacetState:
        return FacetState(
            object_id=row["object_id"],
            facet=row["facet"],
            facet_version=row["facet_version"],
            knowledge=KnowledgeState(row["knowledge"]),
            payload=_json_value(row["payload_json"]),  # type: ignore[arg-type]
            observed_at=_dt(row["observed_at"]),
            state_changed_at=_dt(row["state_changed_at"]),  # type: ignore[arg-type]
            supporting_observation_id=row["supporting_observation_id"],
            source_revision=row["source_revision"],
        )

    def get_facet_sync(self, object_id: str, facet: str) -> FacetState | None:
        object_id = require_uuid(object_id, "object_id")
        require_contract_key(facet, "facet")
        with self._lock:
            row = self._get_facet_row(object_id, facet)
        return self._facet_from_row(row) if row else None

    def list_facets(self, object_id: str) -> tuple[FacetState, ...]:
        object_id = require_uuid(object_id, "object_id")
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM facets WHERE object_id = ? ORDER BY facet", (object_id,)
            ).fetchall()
        return tuple(self._facet_from_row(row) for row in rows)

    def list_facet_evidence(self, object_id: str) -> tuple[FacetEvidenceRecord, ...]:
        object_id = require_uuid(object_id, "object_id")
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT facet.*,
                       journal.observation_id AS provenance_observation_id,
                       batch.batch_id AS provenance_batch_id,
                       batch.adapter_key AS provenance_adapter_key,
                       batch.adapter_version AS provenance_adapter_version,
                       action.action_id AS provenance_action_id,
                       action.capability_key AS provenance_capability_key,
                       action.capability_version AS provenance_capability_version
                FROM facets AS facet
                LEFT JOIN observation_journal AS journal
                  ON journal.observation_id = facet.supporting_observation_id
                LEFT JOIN observation_batches AS batch ON batch.batch_id = journal.batch_id
                LEFT JOIN adapter_actions AS action
                  ON action.action_id = batch.action_id
                 AND action.system_id = batch.system_id
                 AND action.connection_binding_id = batch.connection_binding_id
                 AND action.adapter_key = batch.adapter_key
                 AND action.adapter_version = batch.adapter_version
                WHERE facet.object_id = ?
                ORDER BY facet.facet
                """,
                (object_id,),
            ).fetchall()
        return tuple(
            FacetEvidenceRecord(
                facet=self._facet_from_row(row),
                observation_id=row["provenance_observation_id"],
                batch_id=row["provenance_batch_id"],
                adapter_key=row["provenance_adapter_key"],
                adapter_version=row["provenance_adapter_version"],
                action_id=row["provenance_action_id"],
                capability_key=row["provenance_capability_key"],
                capability_version=row["provenance_capability_version"],
            )
            for row in rows
        )

    @staticmethod
    def _relationship_from_row(row: sqlite3.Row) -> RelationshipState:
        return RelationshipState(
            relationship_id=row["relationship_id"],
            system_id=row["system_id"],
            subject_id=row["subject_id"],
            predicate=row["predicate"],
            object_id=row["object_id"],
            presence=PresenceState(row["presence"]),
            observed_at=_dt(row["observed_at"]),  # type: ignore[arg-type]
            supporting_observation_id=row["supporting_observation_id"],
        )

    def list_relationships_sync(
        self, subject_id: str, predicate: str | None = None
    ) -> tuple[RelationshipState, ...]:
        subject_id = require_uuid(subject_id, "subject_id")
        if predicate is None:
            statement = (
                "SELECT * FROM relationships WHERE subject_id = ? ORDER BY predicate, object_id"
            )
            parameters: tuple[object, ...] = (subject_id,)
        else:
            require_contract_key(predicate, "predicate")
            statement = (
                "SELECT * FROM relationships WHERE subject_id = ? AND predicate = ? "
                "ORDER BY object_id"
            )
            parameters = (subject_id, predicate)
        with self._lock:
            rows = self._connection.execute(statement, parameters).fetchall()
        return tuple(self._relationship_from_row(row) for row in rows)

    def get_present_parent_sync(
        self,
        object_id: str,
        *,
        predicate: str = "contains",
    ) -> RemoteObject | None:
        object_id = require_uuid(object_id, "object_id")
        require_contract_key(predicate, "predicate")
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT parent.*
                FROM relationships AS relationship
                    INDEXED BY ix_relationships_object_predicate
                JOIN remote_objects AS parent ON parent.object_id = relationship.subject_id
                WHERE relationship.object_id = ? AND relationship.predicate = ?
                  AND relationship.presence = 'present'
                  AND parent.system_id = relationship.system_id
                  AND parent.presence != 'absent'
                LIMIT 2
                """,
                (object_id, predicate),
            ).fetchall()
        return self._object_from_row(rows[0]) if len(rows) == 1 else None

    def count_related_objects_sync(
        self,
        subject_id: str,
        *,
        predicate: str = "contains",
        object_type: str | None = None,
    ) -> int:
        subject_id = require_uuid(subject_id, "subject_id")
        require_contract_key(predicate, "predicate")
        if object_type is None:
            statement = (
                "SELECT COUNT(*) FROM relationships AS relationships "
                "INNER JOIN remote_objects AS objects "
                "ON objects.object_id = relationships.object_id "
                "WHERE relationships.subject_id = ? AND relationships.predicate = ? "
                "AND relationships.presence = 'present'"
            )
            parameters: tuple[object, ...] = (subject_id, predicate)
        else:
            require_contract_key(object_type, "object_type")
            statement = (
                "SELECT COUNT(*) FROM relationships AS relationships "
                "INNER JOIN remote_objects AS objects "
                "ON objects.object_id = relationships.object_id "
                "WHERE relationships.subject_id = ? AND relationships.predicate = ? "
                "AND relationships.presence = 'present' AND objects.object_type = ?"
            )
            parameters = (subject_id, predicate, object_type)
        with self._lock:
            row = self._connection.execute(statement, parameters).fetchone()
        return int(row[0]) if row is not None else 0

    def list_related_objects_page_sync(
        self,
        subject_id: str,
        *,
        offset: int,
        limit: int,
        predicate: str = "contains",
        object_type: str | None = None,
    ) -> tuple[RelatedObjectRecord, ...]:
        subject_id = require_uuid(subject_id, "subject_id")
        require_contract_key(predicate, "predicate")
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("relationship offset must be a non-negative integer")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("relationship limit must be between 1 and 100")
        statement = (
            "SELECT relationships.relationship_id AS r_relationship_id, "
            "relationships.system_id AS r_system_id, "
            "relationships.subject_id AS r_subject_id, "
            "relationships.predicate AS r_predicate, "
            "relationships.object_id AS r_object_id, "
            "relationships.presence AS r_presence, "
            "relationships.observed_at AS r_observed_at, "
            "relationships.supporting_observation_id AS r_supporting_observation_id, "
            "relationships.supporting_observation_id AS r_supporting_observation_id, "
            "objects.* FROM relationships AS relationships "
            "INNER JOIN remote_objects AS objects "
            "ON objects.object_id = relationships.object_id "
            "WHERE relationships.subject_id = ? AND relationships.predicate = ? "
            "AND relationships.presence = 'present'"
        )
        if object_type is None:
            parameters: tuple[object, ...] = (subject_id, predicate, limit, offset)
        else:
            require_contract_key(object_type, "object_type")
            statement += " AND objects.object_type = ?"
            parameters = (subject_id, predicate, object_type, limit, offset)
        statement += " ORDER BY relationships.object_id LIMIT ? OFFSET ?"
        with self._lock:
            rows = self._connection.execute(statement, parameters).fetchall()
        return tuple(
            RelatedObjectRecord(
                relationship=RelationshipState(
                    relationship_id=row["r_relationship_id"],
                    system_id=row["r_system_id"],
                    subject_id=row["r_subject_id"],
                    predicate=row["r_predicate"],
                    object_id=row["r_object_id"],
                    presence=PresenceState(row["r_presence"]),
                    observed_at=_dt(row["r_observed_at"]),  # type: ignore[arg-type]
                    supporting_observation_id=row["r_supporting_observation_id"],
                ),
                object=self._object_from_row(row),
            )
            for row in rows
        )

    def list_related_objects_after_sync(
        self,
        subject_id: str,
        *,
        after_id: str | None,
        limit: int,
        predicate: str = "contains",
        object_type: str | None = None,
    ) -> tuple[RelatedObjectRecord, ...]:
        subject_id = require_uuid(subject_id, "subject_id")
        require_contract_key(predicate, "predicate")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 101:
            raise ValueError("relationship cursor limit must be between 1 and 101")
        if after_id is not None:
            after_id = require_uuid(after_id, "relationship cursor ID")
        statement = (
            "SELECT relationships.relationship_id AS r_relationship_id, "
            "relationships.system_id AS r_system_id, "
            "relationships.subject_id AS r_subject_id, "
            "relationships.predicate AS r_predicate, "
            "relationships.object_id AS r_object_id, "
            "relationships.presence AS r_presence, "
            "relationships.observed_at AS r_observed_at, "
            "relationships.supporting_observation_id AS r_supporting_observation_id, "
            "objects.* FROM relationships AS relationships "
            "INDEXED BY ix_relationships_subject_predicate "
            "INNER JOIN remote_objects AS objects ON objects.object_id = relationships.object_id "
            "WHERE relationships.subject_id = ? AND relationships.predicate = ? "
            "AND relationships.presence = 'present'"
        )
        parameters: list[object] = [subject_id, predicate]
        if object_type is not None:
            require_contract_key(object_type, "object_type")
            statement += " AND objects.object_type = ?"
            parameters.append(object_type)
        if after_id is not None:
            statement += " AND relationships.object_id > ?"
            parameters.append(after_id)
        statement += " ORDER BY relationships.object_id LIMIT ?"
        parameters.append(limit)
        with self._lock:
            rows = self._connection.execute(statement, tuple(parameters)).fetchall()
        return tuple(
            RelatedObjectRecord(
                relationship=RelationshipState(
                    relationship_id=row["r_relationship_id"],
                    system_id=row["r_system_id"],
                    subject_id=row["r_subject_id"],
                    predicate=row["r_predicate"],
                    object_id=row["r_object_id"],
                    presence=PresenceState(row["r_presence"]),
                    observed_at=_dt(row["r_observed_at"]),  # type: ignore[arg-type]
                    supporting_observation_id=row["r_supporting_observation_id"],
                ),
                object=self._object_from_row(row),
            )
            for row in rows
        )

    async def get_object(self, object_id: str) -> RemoteObject | None:
        return self.get_object_sync(object_id)

    async def get_facet(self, object_id: str, facet: str) -> FacetState | None:
        return self.get_facet_sync(object_id, facet)

    async def list_relationships(
        self, subject_id: str, predicate: str | None = None
    ) -> Sequence[RelationshipState]:
        return self.list_relationships_sync(subject_id, predicate)

    def get_connection_binding_sync(self, binding_id: str) -> ConnectionBinding | None:
        binding_id = require_uuid(binding_id, "binding_id")
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM connection_bindings WHERE binding_id = ?", (binding_id,)
            ).fetchone()
        return self._binding_from_row(row) if row else None

    @staticmethod
    def _binding_from_row(row: sqlite3.Row) -> ConnectionBinding:
        return ConnectionBinding(
            binding_id=row["binding_id"],
            system_id=row["system_id"],
            adapter_key=row["adapter_key"],
            adapter_version=row["adapter_version"],
            enabled=bool(row["enabled"]),
            non_secret_settings=_json_value(row["non_secret_settings_json"]),  # type: ignore[arg-type]
            secret_reference=row["secret_reference"],
            revision=_binding_revision(
                adapter_key=row["adapter_key"],
                adapter_version=row["adapter_version"],
                non_secret_settings_json=row["non_secret_settings_json"],
                secret_reference=row["secret_reference"],
            ),
        )

    def get_capability_binding_sync(
        self, connection_binding_id: str, capability_key: str, capability_version: str
    ) -> CapabilityBinding | None:
        connection_binding_id = require_uuid(connection_binding_id, "connection_binding_id")
        require_contract_key(capability_key, "capability_key")
        require_text(capability_version, "capability_version", max_length=64)
        with self._lock:
            row = self._connection.execute(
                """
                SELECT * FROM capability_bindings
                WHERE connection_binding_id = ? AND capability_key = ? AND capability_version = ?
                """,
                (connection_binding_id, capability_key, capability_version),
            ).fetchone()
        return self._capability_from_row(row) if row else None

    def list_connection_bindings(
        self, *, system_id: str | None = None
    ) -> tuple[ConnectionBinding, ...]:
        """List bounded binding records; secret values are never stored here."""
        if system_id is None:
            statement = "SELECT * FROM connection_bindings ORDER BY binding_id"
            parameters: tuple[object, ...] = ()
        else:
            statement = "SELECT * FROM connection_bindings WHERE system_id = ? ORDER BY binding_id"
            parameters = (require_uuid(system_id, "system_id"),)
        with self._lock:
            rows = self._connection.execute(statement, parameters).fetchall()
        return tuple(self._binding_from_row(row) for row in rows)

    def list_capability_bindings(
        self,
        *,
        system_id: str | None = None,
        connection_binding_id: str | None = None,
    ) -> tuple[CapabilityBinding, ...]:
        """List registered capability declarations without exposing connection settings."""
        if system_id is None and connection_binding_id is None:
            statement = (
                "SELECT * FROM capability_bindings ORDER BY capability_key, capability_version"
            )
            parameters: tuple[object, ...] = ()
        elif system_id is None:
            statement = (
                "SELECT * FROM capability_bindings WHERE connection_binding_id = ? "
                "ORDER BY capability_key, capability_version"
            )
            parameters = (require_uuid(connection_binding_id, "connection_binding_id"),)
        elif connection_binding_id is None:
            statement = (
                "SELECT capability.* FROM capability_bindings AS capability "
                "JOIN connection_bindings AS binding "
                "ON binding.binding_id = capability.connection_binding_id "
                "WHERE binding.system_id = ? "
                "ORDER BY capability.capability_key, capability.capability_version"
            )
            parameters = (require_uuid(system_id, "system_id"),)
        else:
            statement = (
                "SELECT capability.* FROM capability_bindings AS capability "
                "JOIN connection_bindings AS binding "
                "ON binding.binding_id = capability.connection_binding_id "
                "WHERE binding.system_id = ? AND capability.connection_binding_id = ? "
                "ORDER BY capability.capability_key, capability.capability_version"
            )
            parameters = (
                require_uuid(system_id, "system_id"),
                require_uuid(connection_binding_id, "connection_binding_id"),
            )
        with self._lock:
            rows = self._connection.execute(statement, parameters).fetchall()
        return tuple(self._capability_from_row(row) for row in rows)

    @staticmethod
    def _capability_from_row(row: sqlite3.Row) -> CapabilityBinding:
        coverage_policies = tuple(
            _coverage_policy_from_value(value)
            for value in _json_value(row["coverage_policies_json"])
        )
        return CapabilityBinding(
            capability_binding_id=row["capability_binding_id"],
            connection_binding_id=row["connection_binding_id"],
            capability_key=row["capability_key"],
            capability_version=row["capability_version"],
            operation_class=OperationClass(row["operation_class"]),
            target_kinds=tuple(TargetKind(item) for item in _json_value(row["target_kinds_json"])),
            produced_facets=tuple(_json_value(row["produced_facets_json"])),
            enabled=bool(row["enabled"]),
            selection_priority=row["selection_priority"],
            collateral_effects=tuple(_json_value(row["collateral_effects_json"])),
            mitigations=tuple(_json_value(row["mitigations_json"])),
            coverage_policies=coverage_policies,
        )

    async def get_connection_binding(self, binding_id: str) -> ConnectionBinding | None:
        return self.get_connection_binding_sync(binding_id)

    async def get_capability_binding(
        self, connection_binding_id: str, capability_key: str, capability_version: str
    ) -> CapabilityBinding | None:
        return self.get_capability_binding_sync(
            connection_binding_id, capability_key, capability_version
        )

    def select_capability(
        self,
        *,
        system_id: str,
        target_kind: TargetKind,
        facet: str,
        capability_key: str | None = None,
    ) -> tuple[ConnectionBinding, CapabilityBinding] | None:
        """Select an enabled local observe capability without changing its contract.

        A scope hint pins selection to that registered capability key. Without a
        hint, priority remains the deterministic legacy fallback.
        """
        system_id = require_uuid(system_id, "system_id")
        require_contract_key(facet, "facet")
        if capability_key is not None:
            require_contract_key(capability_key, "capability_key")
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT cb.*, b.binding_id AS b_binding_id, b.system_id AS b_system_id,
                       b.adapter_key AS b_adapter_key, b.adapter_version AS b_adapter_version,
                       b.enabled AS b_enabled, b.non_secret_settings_json AS b_settings,
                       b.secret_reference AS b_secret_reference
                FROM capability_bindings AS cb
                JOIN connection_bindings AS b ON b.binding_id = cb.connection_binding_id
                JOIN systems AS s ON s.system_id = b.system_id
                WHERE b.system_id = ? AND s.enabled = 1 AND b.enabled = 1 AND cb.enabled = 1
                  AND cb.operation_class = 'observe'
                ORDER BY cb.selection_priority DESC, cb.capability_key, cb.capability_version,
                         cb.capability_binding_id
                """,
                (system_id,),
            ).fetchall()
        for row in rows:
            capability = self._capability_from_row(row)
            if capability_key is not None and capability.capability_key != capability_key:
                continue
            if (
                target_kind not in capability.target_kinds
                or facet not in capability.produced_facets
            ):
                continue
            binding = ConnectionBinding(
                binding_id=row["b_binding_id"],
                system_id=row["b_system_id"],
                adapter_key=row["b_adapter_key"],
                adapter_version=row["b_adapter_version"],
                enabled=bool(row["b_enabled"]),
                non_secret_settings=_json_value(row["b_settings"]),  # type: ignore[arg-type]
                secret_reference=row["b_secret_reference"],
                revision=_binding_revision(
                    adapter_key=row["b_adapter_key"],
                    adapter_version=row["b_adapter_version"],
                    non_secret_settings_json=row["b_settings"],
                    secret_reference=row["b_secret_reference"],
                ),
            )
            return binding, capability
        return None

    def set_refresh_override(
        self, override: RefreshIntervalOverride, *, now: datetime | None = None
    ) -> None:
        interval_seconds = int(override.interval.total_seconds())
        if interval_seconds <= 0 or timedelta(seconds=interval_seconds) != override.interval:
            raise ValueError("refresh interval must be an integral positive number of seconds")
        with self._immediate_transaction() as connection:
            previous = connection.execute(
                """
                SELECT interval_seconds
                FROM refresh_overrides
                WHERE level = ? AND scope_id = ? AND facet IS ?
                """,
                (override.level, override.scope_id, override.facet),
            ).fetchall()
            changed = len(previous) != 1 or previous[0]["interval_seconds"] != interval_seconds
            connection.execute(
                """
                DELETE FROM refresh_overrides
                WHERE level = ? AND scope_id = ? AND facet IS ?
                """,
                (override.level, override.scope_id, override.facet),
            )
            connection.execute(
                """
                INSERT INTO refresh_overrides (
                    level, scope_id, facet, interval_seconds, record_updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    override.level,
                    override.scope_id,
                    override.facet,
                    interval_seconds,
                    _utc_text(now or _now()),
                ),
            )
            if changed:
                parameters = (override.facet, override.facet)
                if override.level == "system":
                    connection.execute(
                        """
                        UPDATE refresh_intent_scopes
                        SET state = 'queued', disposition_reason = 'policy_changed',
                            eligible_at = NULL, lease_id = NULL, lease_worker_id = NULL,
                            leased_until = NULL
                        WHERE state = 'deferred' AND system_id = ?
                          AND (? IS NULL OR facet = ?)
                        """,
                        (override.scope_id, *parameters),
                    )
                else:
                    connection.execute(
                        """
                        UPDATE refresh_intent_scopes
                        SET state = 'queued', disposition_reason = 'policy_changed',
                            eligible_at = NULL, lease_id = NULL, lease_worker_id = NULL,
                            leased_until = NULL
                        WHERE state = 'deferred' AND target_kind = 'object' AND target_id = ?
                          AND (? IS NULL OR facet = ?)
                        """,
                        (override.scope_id, *parameters),
                    )
                    connection.execute(
                        """
                        UPDATE refresh_intent_scopes
                        SET state = 'queued', disposition_reason = 'policy_changed',
                            eligible_at = NULL, lease_id = NULL, lease_worker_id = NULL,
                            leased_until = NULL
                        WHERE state = 'deferred' AND target_kind = 'configured_scope'
                          AND target_id IN (
                              SELECT scope_id FROM configured_scopes WHERE object_id = ?
                          )
                          AND (? IS NULL OR facet = ?)
                        """,
                        (override.scope_id, *parameters),
                    )

    def _overrides_for(
        self, *, object_id: str, system_id: str
    ) -> tuple[RefreshIntervalOverride, ...]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT level, scope_id, facet, interval_seconds FROM refresh_overrides
                WHERE (level = 'object' AND scope_id = ?) OR (level = 'system' AND scope_id = ?)
                """,
                (object_id, system_id),
            ).fetchall()
        return tuple(
            RefreshIntervalOverride(
                level=row["level"],
                scope_id=row["scope_id"],
                facet=row["facet"],
                interval=timedelta(seconds=row["interval_seconds"]),
            )
            for row in rows
        )

    def resolve_target_object_id(self, scope: RefreshScope) -> str | None:
        if scope.target.kind is TargetKind.OBJECT:
            return scope.target.target_id
        if scope.target.kind is TargetKind.CONFIGURED_SCOPE:
            configured = self.get_configured_scope(scope.target.target_id)
            return configured.object_id if configured else None
        return None

    def effective_interval(self, scope: RefreshScope) -> timedelta:
        object_id = self.resolve_target_object_id(scope)
        if object_id is None:
            raise ValueError("refresh scope has no canonical object policy target")
        return resolve_refresh_interval(
            object_id=object_id,
            system_id=scope.system_id,
            object_type=scope.object_type,
            facet=scope.facet,
            overrides=self._overrides_for(object_id=object_id, system_id=scope.system_id),
        ).interval

    async def submit_refresh(self, intent: RefreshIntent) -> RefreshReceipt:
        """Durably record the one generic intent shape, idempotently by caller key."""
        accepted_at = _now()
        with self._immediate_transaction() as connection:
            duplicate = connection.execute(
                "SELECT intent_id, accepted_at FROM refresh_intents WHERE idempotency_key = ?",
                (intent.idempotency_key,),
            ).fetchone()
            if duplicate is not None:
                rows = connection.execute(
                    """
                    SELECT intent_scope_id FROM refresh_intent_scopes
                    WHERE intent_id = ?
                    ORDER BY intent_scope_id
                    """,
                    (duplicate["intent_id"],),
                ).fetchall()
                return RefreshReceipt(
                    intent_id=duplicate["intent_id"],
                    accepted_at=_dt(duplicate["accepted_at"]),  # type: ignore[arg-type]
                    scope_ids=tuple(row["intent_scope_id"] for row in rows),
                )
            self.require_write_headroom()
            connection.execute(
                """
                INSERT INTO refresh_intents (
                    intent_id, idempotency_key, origin, actor_id, ui_session_id, requested_at,
                    expires_at, priority, aggregate_state, contract_version, accepted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)
                """,
                (
                    intent.intent_id,
                    intent.idempotency_key,
                    intent.origin.value,
                    intent.actor_id,
                    intent.ui_session_id,
                    _utc_text(intent.requested_at),
                    _utc_text(intent.expires_at) if intent.expires_at else None,
                    intent.priority,
                    intent.contract_version,
                    _utc_text(accepted_at),
                ),
            )
            scope_ids: list[str] = []
            for scope in intent.scopes:
                scope_id = str(uuid4())
                scope_ids.append(scope_id)
                connection.execute(
                    """
                    INSERT INTO refresh_intent_scopes (
                        intent_scope_id, intent_id, system_id, target_kind, target_id,
                        object_type, facet, capability_key, coverage, field_mask_json, state,
                        queue_priority, queue_requested_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)
                    """,
                    (
                        scope_id,
                        intent.intent_id,
                        *_scope_columns(scope),
                        intent.priority,
                        _utc_text(intent.requested_at),
                    ),
                )
        return RefreshReceipt(
            intent_id=intent.intent_id, accepted_at=accepted_at, scope_ids=tuple(scope_ids)
        )

    def _intent_from_row(self, row: sqlite3.Row) -> RefreshIntent:
        scope_rows = self._connection.execute(
            "SELECT * FROM refresh_intent_scopes WHERE intent_id = ? ORDER BY intent_scope_id",
            (row["intent_id"],),
        ).fetchall()
        return RefreshIntent(
            intent_id=row["intent_id"],
            idempotency_key=row["idempotency_key"],
            origin=RefreshOrigin(row["origin"]),
            actor_id=row["actor_id"],
            scopes=tuple(_scope_from_row(scope_row) for scope_row in scope_rows),
            requested_at=_dt(row["requested_at"]),  # type: ignore[arg-type]
            ui_session_id=row["ui_session_id"],
            expires_at=_dt(row["expires_at"]),
            priority=row["priority"],
            contract_version=row["contract_version"],
        )

    def list_intent_scopes(self, intent_id: str) -> tuple[IntentScopeRecord, ...]:
        intent_id = require_uuid(intent_id, "intent_id")
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM refresh_intent_scopes WHERE intent_id = ? ORDER BY intent_scope_id",
                (intent_id,),
            ).fetchall()
        return tuple(self._intent_scope_record(row) for row in rows)

    @staticmethod
    def _intent_scope_record(row: sqlite3.Row) -> IntentScopeRecord:
        return IntentScopeRecord(
            intent_scope_id=row["intent_scope_id"],
            intent_id=row["intent_id"],
            scope=_scope_from_row(row),
            state=IntentScopeState(row["state"]),
            disposition_reason=row["disposition_reason"],
            eligible_at=_dt(row["eligible_at"]),
            linked_action_id=row["linked_action_id"],
            satisfying_observation_id=row["satisfying_observation_id"],
        )

    def list_refresh_intents(self) -> tuple[RefreshIntent, ...]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM refresh_intents ORDER BY requested_at DESC, intent_id"
            ).fetchall()
            return tuple(self._intent_from_row(row) for row in rows)

    def get_refresh_intent(self, intent_id: str) -> RefreshIntent | None:
        """Return the durable request shape; use ``list_intent_scopes`` for status."""
        intent_id = require_uuid(intent_id, "intent_id")
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM refresh_intents WHERE intent_id = ?", (intent_id,)
            ).fetchone()
            return self._intent_from_row(row) if row else None

    def get_refresh_intent_requested_at(self, intent_id: str) -> datetime | None:
        intent_id = require_uuid(intent_id, "intent_id")
        with self._lock:
            row = self._connection.execute(
                "SELECT requested_at FROM refresh_intents WHERE intent_id = ?", (intent_id,)
            ).fetchone()
        return _dt(row["requested_at"]) if row is not None else None

    def _terminalize_intent_scope_contract_failure(
        self,
        connection: sqlite3.Connection,
        *,
        row: sqlite3.Row,
        now: datetime,
    ) -> None:
        reason = "persisted_intent_contract_mismatch"
        connection.execute(
            """
            UPDATE refresh_intent_scopes
            SET state = 'rejected', disposition_reason = ?, eligible_at = NULL,
                lease_id = NULL, lease_worker_id = NULL, leased_until = NULL
            WHERE intent_scope_id = ?
            """,
            (reason, row["intent_scope_id"]),
        )
        self._insert_event(
            connection,
            idempotency_key=_quarantine_event_key(
                "intent-scope",
                row["intent_scope_id"],
            ),
            event_type="refresh.intent.contract_mismatch",
            severity="error",
            alertable=True,
            system_id=self._known_event_system_id(connection, row["system_id"]),
            action_id=None,
            error_class=ErrorClass.ADAPTER_CONTRACT_MISMATCH.value,
            summary=reason,
            occurred_at=now,
        )
        self._refresh_intent_aggregate(
            connection,
            intent_scope_id=row["intent_scope_id"],
        )

    def _promote_due_intent_scopes(self) -> bool:
        with self._immediate_transaction() as connection:
            authority_now = self._current_time()
            now_text = _utc_text(authority_now)
            promoted = 0
            for priority in range(100, -1, -1):
                remaining = _MAX_DUE_PROMOTIONS_PER_CLAIM - promoted
                if remaining == 0:
                    break
                result = connection.execute(
                    """
                    UPDATE refresh_intent_scopes
                    SET state = 'queued', eligible_at = NULL
                    WHERE intent_scope_id IN (
                        SELECT intent_scope_id FROM refresh_intent_scopes
                            INDEXED BY ix_refresh_intent_scopes_deferred_priority_due
                        WHERE state = 'deferred' AND queue_priority = ?
                          AND eligible_at IS NULL
                        ORDER BY eligible_at, queue_requested_at, intent_scope_id
                        LIMIT ?
                    )
                    """,
                    (priority, remaining),
                )
                promoted += result.rowcount
                remaining = _MAX_DUE_PROMOTIONS_PER_CLAIM - promoted
                if remaining == 0:
                    break
                result = connection.execute(
                    """
                    UPDATE refresh_intent_scopes
                    SET state = 'queued', eligible_at = NULL
                    WHERE intent_scope_id IN (
                        SELECT intent_scope_id FROM refresh_intent_scopes
                            INDEXED BY ix_refresh_intent_scopes_deferred_priority_due
                        WHERE state = 'deferred' AND queue_priority = ?
                          AND eligible_at <= ?
                        ORDER BY eligible_at, queue_requested_at, intent_scope_id
                        LIMIT ?
                    )
                    """,
                    (priority, now_text, remaining),
                )
                promoted += result.rowcount
            remaining = _MAX_DUE_PROMOTIONS_PER_CLAIM - promoted
            result = connection.execute(
                """
                UPDATE refresh_intent_scopes
                SET state = 'queued', lease_id = NULL, lease_worker_id = NULL,
                    leased_until = NULL
                WHERE intent_scope_id IN (
                    SELECT intent_scope_id FROM refresh_intent_scopes
                        INDEXED BY ix_refresh_intent_scopes_lease_due
                    WHERE state = 'leased' AND leased_until <= ?
                    LIMIT ?
                )
                """,
                (now_text, remaining),
            )
            promoted += result.rowcount
        return promoted >= _MAX_DUE_PROMOTIONS_PER_CLAIM

    async def lease_next_intent_scope(
        self,
        *,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta = _DEFAULT_LEASE,
    ) -> IntentScopeWork | None:
        require_text(worker_id, "worker_id", max_length=256)
        require_utc(now, "now")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        self._promote_due_intent_scopes()
        with self._immediate_transaction() as connection:
            authority_now = self._current_time()
            leased_until = authority_now + lease_duration
            while True:
                row = connection.execute(
                    """
                    SELECT scope.*, intent.idempotency_key, intent.origin, intent.actor_id,
                           intent.ui_session_id, intent.requested_at, intent.expires_at,
                           intent.priority, intent.contract_version
                    FROM refresh_intent_scopes AS scope
                        INDEXED BY ix_refresh_intent_scopes_claim_order
                    JOIN refresh_intents AS intent ON intent.intent_id = scope.intent_id
                    WHERE scope.state = 'queued'
                    ORDER BY scope.queue_priority DESC, scope.queue_requested_at,
                             scope.intent_scope_id
                    LIMIT 1
                    """,
                ).fetchone()
                if row is None:
                    return None
                try:
                    scope = _scope_from_row(row)
                    intent = RefreshIntent(
                        intent_id=row["intent_id"],
                        idempotency_key=row["idempotency_key"],
                        origin=RefreshOrigin(row["origin"]),
                        actor_id=row["actor_id"],
                        scopes=(scope,),
                        requested_at=_dt(row["requested_at"]),  # type: ignore[arg-type]
                        ui_session_id=row["ui_session_id"],
                        expires_at=_dt(row["expires_at"]),
                        priority=row["priority"],
                        contract_version=row["contract_version"],
                    )
                except (TypeError, ValueError, json.JSONDecodeError):
                    self._terminalize_intent_scope_contract_failure(
                        connection,
                        row=row,
                        now=authority_now,
                    )
                    continue
                break
            lease_id = str(uuid4())
            result = connection.execute(
                """
                UPDATE refresh_intent_scopes
                SET state = 'leased', lease_id = ?, lease_worker_id = ?, leased_until = ?,
                    lease_authority_at = ?
                WHERE intent_scope_id = ? AND state = 'queued'
                """,
                (
                    lease_id,
                    worker_id,
                    _utc_text(leased_until),
                    _utc_text(authority_now),
                    row["intent_scope_id"],
                ),
            )
            if (
                result.rowcount != 1
            ):  # pragma: no cover - lock and transaction make this unreachable
                return None
        return IntentScopeWork(
            intent_scope_id=row["intent_scope_id"],
            intent=intent,
            scope=scope,
            state=IntentScopeState.LEASED,
            lease_id=lease_id,
            leased_until=leased_until,
        )

    def _claim_is_current(
        self,
        connection: sqlite3.Connection,
        *,
        intent_scope_id: str,
        lease_id: str,
        authority_now: datetime,
    ) -> bool:
        return (
            connection.execute(
                """
                SELECT 1 FROM refresh_intent_scopes
                WHERE intent_scope_id = ? AND state = 'leased' AND lease_id = ?
                  AND leased_until > ?
                """,
                (intent_scope_id, lease_id, _utc_text(authority_now)),
            ).fetchone()
            is not None
        )

    def _expire_intent_scope_if_due(
        self,
        connection: sqlite3.Connection,
        *,
        intent_scope_id: str,
        authority_now: datetime,
    ) -> bool:
        row = connection.execute(
            """
            SELECT intent.expires_at
            FROM refresh_intent_scopes AS scope
            JOIN refresh_intents AS intent ON intent.intent_id = scope.intent_id
            WHERE scope.intent_scope_id = ?
            """,
            (intent_scope_id,),
        ).fetchone()
        expires_at = _dt(row["expires_at"]) if row is not None else None
        if expires_at is None or expires_at > authority_now:
            return False
        connection.execute(
            """
            UPDATE refresh_intent_scopes
            SET state = 'expired', disposition_reason = 'request_expired',
                eligible_at = NULL, linked_action_id = NULL,
                satisfying_observation_id = NULL, lease_id = NULL,
                lease_worker_id = NULL, leased_until = NULL, lease_authority_at = NULL
            WHERE intent_scope_id = ?
            """,
            (intent_scope_id,),
        )
        self._refresh_intent_aggregate(connection, intent_scope_id=intent_scope_id)
        return True

    def set_intent_scope_disposition(
        self,
        *,
        intent_scope_id: str,
        lease_id: str,
        state: IntentScopeState,
        reason: str,
        eligible_at: datetime | None = None,
        action_id: str | None = None,
        observation_id: str | None = None,
    ) -> IntentScopeState:
        if state is IntentScopeState.LEASED:
            raise ValueError("leased is not a final coordinator disposition")
        require_contract_key(reason, "reason")
        if action_id is not None:
            action_id = require_uuid(action_id, "action_id")
        if observation_id is not None:
            observation_id = require_uuid(observation_id, "observation_id")
        with self._immediate_transaction() as connection:
            authority_now = self._current_time()
            if not self._claim_is_current(
                connection,
                intent_scope_id=intent_scope_id,
                lease_id=lease_id,
                authority_now=authority_now,
            ):
                raise ValueError("intent scope lease is no longer current")
            if state is not IntentScopeState.EXPIRED and self._expire_intent_scope_if_due(
                connection,
                intent_scope_id=intent_scope_id,
                authority_now=authority_now,
            ):
                return IntentScopeState.EXPIRED
            connection.execute(
                """
                UPDATE refresh_intent_scopes
                SET state = ?, disposition_reason = ?, eligible_at = ?, linked_action_id = ?,
                    satisfying_observation_id = ?, lease_id = NULL,
                    lease_worker_id = NULL, leased_until = NULL, lease_authority_at = NULL
                WHERE intent_scope_id = ?
                """,
                (
                    state.value,
                    reason,
                    _utc_text(eligible_at) if eligible_at else None,
                    action_id,
                    observation_id,
                    intent_scope_id,
                ),
            )
            self._refresh_intent_aggregate(connection, intent_scope_id=intent_scope_id)
        return state

    @staticmethod
    def _refresh_intent_aggregate(connection: sqlite3.Connection, *, intent_scope_id: str) -> None:
        row = connection.execute(
            "SELECT intent_id FROM refresh_intent_scopes WHERE intent_scope_id = ?",
            (intent_scope_id,),
        ).fetchone()
        if row is None:
            return
        scope_rows = connection.execute(
            """
            SELECT scope.state, action.state AS action_state
            FROM refresh_intent_scopes AS scope
            LEFT JOIN adapter_actions AS action ON action.action_id = scope.linked_action_id
            WHERE scope.intent_id = ?
            """,
            (row["intent_id"],),
        ).fetchall()
        complete = all(
            scope_row["state"] in {item.value for item in _TERMINAL_INTENT_STATES}
            or scope_row["action_state"] in {item.value for item in _TERMINAL_ACTION_STATES}
            for scope_row in scope_rows
        )
        if complete:
            connection.execute(
                "UPDATE refresh_intents SET aggregate_state = 'complete' WHERE intent_id = ?",
                (row["intent_id"],),
            )

    def _refresh_action_parent_aggregates(
        self, connection: sqlite3.Connection, *, action_id: str
    ) -> None:
        linked_scopes = connection.execute(
            "SELECT intent_scope_id FROM action_intent_scopes WHERE action_id = ?",
            (action_id,),
        ).fetchall()
        for linked_scope in linked_scopes:
            self._refresh_intent_aggregate(
                connection, intent_scope_id=linked_scope["intent_scope_id"]
            )

    def latest_qualifying_observation(self, scope: RefreshScope) -> QualifyingObservation | None:
        base = _scope_columns(scope)
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM refresh_credit
                WHERE system_id = ? AND target_kind = ? AND target_id = ? AND object_type = ?
                  AND facet = ? AND capability_key IS ? AND coverage = ?
                ORDER BY observed_at DESC, received_at DESC, rowid ASC
                """,
                base[:7],
            )
            for row in rows:
                evidence_scope = _scope_from_row(row)
                if scope_covers(evidence_scope, scope):
                    return QualifyingObservation(
                        observation_id=row["observation_id"],
                        scope=evidence_scope,
                        observed_at=_dt(row["observed_at"]),  # type: ignore[arg-type]
                    )
        return None

    def latest_legacy_observation_for_capability(
        self,
        scope: RefreshScope,
        *,
        connection_binding_id: str,
        capability_key: str,
    ) -> QualifyingObservation | None:
        """Read only action-linked NULL-selector evidence from the selected capability."""

        if scope.capability_key is not None:
            raise ValueError("legacy observation lookup requires a NULL capability selector")
        connection_binding_id = require_uuid(
            connection_binding_id,
            "connection_binding_id",
        )
        capability_key = require_contract_key(capability_key, "capability_key")
        base = _scope_columns(scope)
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT credit.*
                FROM refresh_credit AS credit
                JOIN observation_journal AS journal
                  ON journal.observation_id = credit.observation_id
                JOIN observation_batches AS batch
                  ON batch.batch_id = journal.batch_id AND batch.system_id = credit.system_id
                 AND batch.connection_binding_id = ?
                JOIN adapter_actions AS action
                  ON action.action_id = batch.action_id AND action.capability_key = ?
                 AND action.connection_binding_id = ?
                WHERE credit.system_id = ? AND credit.target_kind = ?
                  AND credit.target_id = ? AND credit.object_type = ?
                  AND credit.facet = ? AND credit.capability_key IS NULL
                  AND credit.coverage = ?
                ORDER BY credit.observed_at DESC, credit.received_at DESC, credit.rowid ASC
                """,
                (
                    connection_binding_id,
                    capability_key,
                    connection_binding_id,
                    *base[:5],
                    base[6],
                ),
            )
            for row in rows:
                evidence_scope = _scope_from_row(row)
                if scope_covers(evidence_scope, scope):
                    return QualifyingObservation(
                        observation_id=row["observation_id"],
                        scope=evidence_scope,
                        observed_at=_dt(row["observed_at"]),  # type: ignore[arg-type]
                    )
        return None

    def latest_legacy_action_started_at_for_capability(
        self,
        scope: RefreshScope,
        *,
        connection_binding_id: str,
        capability_key: str,
    ) -> datetime | None:
        """Read the rollout cooldown anchor without crossing producer capabilities."""

        if scope.capability_key is not None:
            raise ValueError("legacy action lookup requires a NULL capability selector")
        connection_binding_id = require_uuid(
            connection_binding_id,
            "connection_binding_id",
        )
        capability_key = require_contract_key(capability_key, "capability_key")
        columns = _scope_columns(scope)
        with self._lock:
            row = self._connection.execute(
                """
                SELECT MAX(action.started_at) AS started_at
                FROM adapter_action_scopes AS scope
                JOIN adapter_actions AS action ON action.action_id = scope.action_id
                WHERE scope.system_id = ? AND scope.target_kind = ? AND scope.target_id = ?
                  AND scope.object_type = ? AND scope.facet = ?
                  AND scope.capability_key IS NULL AND scope.coverage = ?
                  AND scope.field_mask_json = ? AND action.capability_key = ?
                  AND action.connection_binding_id = ?
                  AND action.started_at IS NOT NULL
                """,
                (
                    *columns[:5],
                    *columns[6:],
                    capability_key,
                    connection_binding_id,
                ),
            ).fetchone()
        return _dt(row["started_at"]) if row is not None else None

    def scope_policy_state(self, scope: RefreshScope) -> ScopePolicyState:
        evidence = self.latest_qualifying_observation(scope)
        columns = _scope_columns(scope)
        with self._lock:
            row = self._connection.execute(
                """
                SELECT latest_started_at
                FROM action_scope_cooldown
                WHERE system_id = ? AND target_kind = ? AND target_id = ?
                  AND object_type = ? AND facet = ?
                  AND capability_key_is_null = ? AND capability_key = ?
                  AND coverage = ? AND field_mask_json = ?
                """,
                (
                    *columns[:5],
                    int(columns[5] is None),
                    columns[5] or "",
                    *columns[6:],
                ),
            ).fetchone()
        return ScopePolicyState(
            scope=scope,
            latest_qualifying_observation_at=evidence.observed_at if evidence else None,
            latest_targeted_action_started_at=_dt(row["latest_started_at"]) if row else None,
        )

    def _action_from_row(self, row: sqlite3.Row) -> AdapterAction:
        scope_rows = self._connection.execute(
            """
            SELECT * FROM adapter_action_scopes
            WHERE action_id = ?
            ORDER BY facet, field_mask_json
            """,
            (row["action_id"],),
        ).fetchall()
        return AdapterAction(
            action_id=row["action_id"],
            correlation_id=row["correlation_id"],
            system_id=row["system_id"],
            connection_binding_id=row["connection_binding_id"],
            adapter_key=row["adapter_key"],
            adapter_version=row["adapter_version"],
            capability_key=row["capability_key"],
            capability_version=row["capability_version"],
            target=TargetRef(TargetKind(row["target_kind"]), row["target_id"]),
            requested_scopes=tuple(_scope_from_row(scope_row) for scope_row in scope_rows),
            deadline=_dt(row["deadline"]),
            contract_version=row["contract_version"],
        )

    def _stored_action_from_row(self, row: sqlite3.Row) -> StoredAction:
        return StoredAction(
            action_id=row["action_id"],
            action=self._action_from_row(row),
            state=ActionState(row["state"]),
            started_at=_dt(row["started_at"]),
            completed_at=_dt(row["completed_at"]),
            lease_id=row["lease_id"],
            lease_worker_id=row["lease_worker_id"],
            leased_until=_dt(row["leased_until"]),
            retry_at=_dt(row["retry_at"]),
            error_class=row["error_class"],
            redacted_diagnostic=row["redacted_diagnostic"],
        )

    def get_action(self, action_id: str) -> AdapterAction | None:
        action_id = require_uuid(action_id, "action_id")
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM adapter_actions WHERE action_id = ?", (action_id,)
            ).fetchone()
            return self._action_from_row(row) if row else None

    def get_stored_action(self, action_id: str) -> StoredAction | None:
        action_id = require_uuid(action_id, "action_id")
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM adapter_actions WHERE action_id = ?", (action_id,)
            ).fetchone()
        return self._stored_action_from_row(row) if row else None

    def list_actions(self, *, system_id: str | None = None) -> tuple[StoredAction, ...]:
        if system_id is None:
            statement = "SELECT * FROM adapter_actions ORDER BY record_created_at DESC, action_id"
            parameters: tuple[object, ...] = ()
        else:
            statement = (
                "SELECT * FROM adapter_actions WHERE system_id = ? "
                "ORDER BY record_created_at DESC, action_id"
            )
            parameters = (require_uuid(system_id, "system_id"),)
        with self._lock:
            rows = self._connection.execute(statement, parameters).fetchall()
        return tuple(self._stored_action_from_row(row) for row in rows)

    @staticmethod
    def _validate_action_activity_filters(
        *,
        system_id: str | None,
        state: str | None,
        action_id: str | None,
    ) -> tuple[str | None, str | None, str | None]:
        normalized_system = require_uuid(system_id, "system_id") if system_id else None
        normalized_action = require_uuid(action_id, "action_id") if action_id else None
        if state is not None:
            try:
                ActionState(state)
            except ValueError as exc:
                raise ValueError("action state is not registered") from exc
        if normalized_action is not None and (normalized_system is not None or state is not None):
            raise ValueError("action ID cannot be combined with activity filters")
        return normalized_system, state, normalized_action

    @staticmethod
    def _action_activity_from_row(row: sqlite3.Row) -> ActionActivityRecord:
        return ActionActivityRecord(
            action_id=row["action_id"],
            system_id=row["system_id"],
            capability_key=row["capability_key"],
            target_kind=row["target_kind"],
            target_id=row["target_id"],
            state=row["state"],
            created_at=_dt(row["record_created_at"]),  # type: ignore[arg-type]
            started_at=_dt(row["started_at"]),
            completed_at=_dt(row["completed_at"]),
            retry_at=_dt(row["retry_at"]),
            error_class=row["error_class"],
            redacted_diagnostic=row["redacted_diagnostic"],
        )

    @staticmethod
    def _action_attempt_from_row(row: sqlite3.Row) -> ActionAttemptRecord:
        return ActionAttemptRecord(
            attempt_id=row["attempt_id"],
            action_id=row["action_id"],
            ordinal=row["ordinal"],
            started_at=_dt(row["started_at"]),  # type: ignore[arg-type]
            ended_at=_dt(row["ended_at"]),
            outcome=row["outcome"],
            error_class=row["error_class"],
            retry_at=_dt(row["retry_at"]),
            redacted_diagnostic=row["redacted_diagnostic"],
        )

    def get_action_activity(self, action_id: str) -> ActionActivityRecord | None:
        action_id = require_uuid(action_id, "action_id")
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM adapter_actions WHERE action_id = ?", (action_id,)
            ).fetchone()
        return self._action_activity_from_row(row) if row is not None else None

    def list_action_attempts(
        self, action_id: str, *, limit: int = 100
    ) -> tuple[ActionAttemptRecord, ...]:
        action_id = require_uuid(action_id, "action_id")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 101:
            raise ValueError("attempt limit must be between 1 and 101")
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM action_attempts
                WHERE action_id = ?
                ORDER BY ordinal DESC
                LIMIT ?
                """,
                (action_id, limit),
            ).fetchall()
        return tuple(reversed(tuple(self._action_attempt_from_row(row) for row in rows)))

    def count_action_attempts(self, action_id: str) -> int:
        action_id = require_uuid(action_id, "action_id")
        with self._lock:
            row = self._connection.execute(
                "SELECT COUNT(*) FROM action_attempts WHERE action_id = ?", (action_id,)
            ).fetchone()
        return int(row[0]) if row is not None else 0

    def list_latest_facet_actions(
        self, object_ids: Sequence[str]
    ) -> tuple[FacetActionStatusRecord, ...]:
        """Return one active-preferred/latest action per visible object facet.

        The caller supplies at most 100 object IDs. Both branches are driven from
        that bounded JSON set through target-ID indexes; grouping is therefore
        limited to action history attached to the visible objects.
        """

        if len(object_ids) > 100:
            raise ValueError("at most 100 object IDs may be queried")
        normalized_ids = tuple(
            dict.fromkeys(require_uuid(value, "object_id") for value in object_ids)
        )
        if not normalized_ids:
            return ()
        object_statement = """
            WITH visible AS (
                SELECT value AS object_id FROM json_each(?)
            )
            SELECT
                status.system_id,
                status.target_id AS object_id,
                status.facet,
                status.action_id,
                status.state,
                status.occurred_at,
                status.redacted_diagnostic
            FROM visible
            CROSS JOIN facet_action_status AS status
                INDEXED BY ix_facet_action_status_target
            WHERE status.target_kind = 'object'
              AND status.target_id = visible.object_id
        """
        configured_statement = """
            WITH visible AS (
                SELECT value AS object_id FROM json_each(?)
            )
            SELECT
                status.system_id,
                configured.object_id,
                status.facet,
                status.action_id,
                status.state,
                status.occurred_at,
                status.redacted_diagnostic
            FROM visible
            CROSS JOIN configured_scopes AS configured
                INDEXED BY ix_configured_scopes_object
            CROSS JOIN facet_action_status AS status
                INDEXED BY ix_facet_action_status_target
            WHERE configured.object_id = visible.object_id
              AND status.target_kind = 'configured_scope'
              AND status.target_id = configured.scope_id
        """
        encoded_ids = json.dumps(normalized_ids)
        with self._lock:
            rows = (
                *self._connection.execute(object_statement, (encoded_ids,)).fetchall(),
                *self._connection.execute(configured_statement, (encoded_ids,)).fetchall(),
            )
        active_states = {"ready", "leased", "running", "retry_wait"}
        records: dict[tuple[str, str, str], FacetActionStatusRecord] = {}
        for row in rows:
            record = FacetActionStatusRecord(
                system_id=row["system_id"],
                object_id=row["object_id"],
                facet=row["facet"],
                action_id=row["action_id"],
                state=row["state"],
                occurred_at=_dt(row["occurred_at"]),  # type: ignore[arg-type]
                redacted_diagnostic=row["redacted_diagnostic"],
            )
            key = (record.system_id, record.object_id, record.facet)
            existing = records.get(key)
            if existing is None or (
                record.state in active_states,
                record.occurred_at,
                record.action_id,
            ) > (
                existing.state in active_states,
                existing.occurred_at,
                existing.action_id,
            ):
                records[key] = record
        return tuple(records[key] for key in sorted(records))

    def count_action_activity(
        self,
        *,
        system_id: str | None = None,
        state: str | None = None,
        action_id: str | None = None,
    ) -> int:
        system_id, state, action_id = self._validate_action_activity_filters(
            system_id=system_id,
            state=state,
            action_id=action_id,
        )
        if action_id is not None:
            statement = "SELECT COUNT(*) FROM adapter_actions WHERE action_id = ?"
            parameters: tuple[object, ...] = (action_id,)
        elif system_id is None and state is None:
            statement = "SELECT COUNT(*) FROM adapter_actions"
            parameters = ()
        elif system_id is not None and state is None:
            statement = "SELECT COUNT(*) FROM adapter_actions WHERE system_id = ?"
            parameters = (system_id,)
        elif system_id is None:
            statement = "SELECT COUNT(*) FROM adapter_actions WHERE state = ?"
            parameters = (state,)
        else:
            statement = "SELECT COUNT(*) FROM adapter_actions WHERE system_id = ? AND state = ?"
            parameters = (system_id, state)
        with self._lock:
            row = self._connection.execute(statement, parameters).fetchone()
        return int(row[0]) if row is not None else 0

    def list_action_activity_page(
        self,
        *,
        offset: int,
        limit: int,
        system_id: str | None = None,
        state: str | None = None,
        action_id: str | None = None,
    ) -> tuple[ActionActivityRecord, ...]:
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("action offset must be a non-negative integer")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("action limit must be between 1 and 100")
        system_id, state, action_id = self._validate_action_activity_filters(
            system_id=system_id,
            state=state,
            action_id=action_id,
        )
        if action_id is not None:
            statement = "SELECT * FROM adapter_actions WHERE action_id = ? LIMIT ? OFFSET ?"
            parameters: tuple[object, ...] = (action_id, limit, offset)
        elif system_id is None and state is None:
            statement = (
                "SELECT * FROM adapter_actions "
                "ORDER BY record_created_at DESC, action_id LIMIT ? OFFSET ?"
            )
            parameters = (limit, offset)
        elif system_id is not None and state is None:
            statement = (
                "SELECT * FROM adapter_actions WHERE system_id = ? "
                "ORDER BY record_created_at DESC, action_id LIMIT ? OFFSET ?"
            )
            parameters = (system_id, limit, offset)
        elif system_id is None:
            statement = (
                "SELECT * FROM adapter_actions WHERE state = ? "
                "ORDER BY record_created_at DESC, action_id LIMIT ? OFFSET ?"
            )
            parameters = (state, limit, offset)
        else:
            statement = (
                "SELECT * FROM adapter_actions WHERE system_id = ? AND state = ? "
                "ORDER BY record_created_at DESC, action_id LIMIT ? OFFSET ?"
            )
            parameters = (system_id, state, limit, offset)
        with self._lock:
            rows = self._connection.execute(statement, parameters).fetchall()
        return tuple(self._action_activity_from_row(row) for row in rows)

    def list_action_activity_after(
        self,
        *,
        after_created_at: datetime | None,
        after_action_id: str | None,
        limit: int,
        system_id: str | None = None,
        state: str | None = None,
        action_id: str | None = None,
    ) -> tuple[ActionActivityRecord, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 101:
            raise ValueError("action cursor limit must be between 1 and 101")
        if (after_created_at is None) != (after_action_id is None):
            raise ValueError("action cursor is incomplete")
        system_id, state, action_id = self._validate_action_activity_filters(
            system_id=system_id,
            state=state,
            action_id=action_id,
        )
        conditions: list[str] = []
        parameters: list[object] = []
        if action_id is not None:
            conditions.append("action_id = ?")
            parameters.append(action_id)
        else:
            if system_id is not None:
                conditions.append("system_id = ?")
                parameters.append(system_id)
            if state is not None:
                conditions.append("state = ?")
                parameters.append(state)
            if after_created_at is not None:
                after_action_id = require_uuid(after_action_id, "action cursor ID")  # type: ignore[arg-type]
                conditions.append(
                    "(record_created_at < ? OR (record_created_at = ? AND action_id > ?))"
                )
                timestamp = _utc_text(require_utc(after_created_at, "action cursor time"))
                parameters.extend((timestamp, timestamp, after_action_id))
        statement = "SELECT * FROM adapter_actions"
        if conditions:
            statement += " WHERE " + " AND ".join(conditions)
        statement += " ORDER BY record_created_at DESC, action_id LIMIT ?"
        parameters.append(limit)
        with self._lock:
            rows = self._connection.execute(statement, tuple(parameters)).fetchall()
        return tuple(self._action_activity_from_row(row) for row in rows)

    def list_dashboard_actions(self) -> tuple[StoredAction, ...]:
        """Return active actions plus the latest recorded action for each system."""
        active_states = (
            ActionState.READY.value,
            ActionState.LEASED.value,
            ActionState.RUNNING.value,
            ActionState.RETRY_WAIT.value,
        )
        with self._lock:
            rows = self._connection.execute(
                """
                WITH ranked AS (
                    SELECT
                        action_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY system_id
                            ORDER BY record_created_at DESC, action_id
                        ) AS recency_rank
                    FROM adapter_actions
                ),
                selected AS (
                    SELECT action_id FROM ranked WHERE recency_rank = 1
                    UNION
                    SELECT action_id FROM adapter_actions
                    WHERE state IN (?, ?, ?, ?)
                )
                SELECT actions.*
                FROM adapter_actions AS actions
                INNER JOIN selected ON selected.action_id = actions.action_id
                ORDER BY actions.record_created_at DESC, actions.action_id
                """,
                active_states,
            ).fetchall()
        return tuple(self._stored_action_from_row(row) for row in rows)

    def list_latest_system_activity(self) -> tuple[ActionActivityRecord, ...]:
        """Return active-preferred system summaries without contract reconstruction."""

        with self._lock:
            rows = self._connection.execute(
                """
                SELECT action.*
                FROM systems AS system
                JOIN adapter_actions AS action
                  ON action.action_id = COALESCE(
                      (
                          SELECT active.action_id
                          FROM adapter_actions AS active
                          WHERE active.system_id = system.system_id
                            AND active.state IN ('ready', 'leased', 'running', 'retry_wait')
                          ORDER BY active.record_created_at DESC, active.action_id
                          LIMIT 1
                      ),
                      (
                          SELECT latest.action_id
                          FROM adapter_actions AS latest
                          WHERE latest.system_id = system.system_id
                          ORDER BY latest.record_created_at DESC, latest.action_id
                          LIMIT 1
                      )
                  )
                ORDER BY action.record_created_at DESC, action.action_id
                """
            ).fetchall()
        return tuple(self._action_activity_from_row(row) for row in rows)

    def _insert_action(
        self,
        connection: sqlite3.Connection,
        *,
        action: AdapterAction,
        dedupe_key: str,
        created_at: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT INTO adapter_actions (
                action_id, correlation_id, system_id, connection_binding_id,
                adapter_key, adapter_version, capability_key, capability_version,
                target_kind, target_id, deadline, contract_version, dedupe_key,
                state, record_created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ready', ?)
            """,
            (
                action.action_id,
                action.correlation_id,
                action.system_id,
                action.connection_binding_id,
                action.adapter_key,
                action.adapter_version,
                action.capability_key,
                action.capability_version,
                action.target.kind.value,
                action.target.target_id,
                _utc_text(action.deadline) if action.deadline else None,
                action.contract_version,
                dedupe_key,
                _utc_text(created_at),
            ),
        )
        for scope in action.requested_scopes:
            connection.execute(
                """
                INSERT INTO adapter_action_scopes (
                    action_id, system_id, target_kind, target_id, object_type, facet,
                    capability_key, coverage, field_mask_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (action.action_id, *_scope_columns(scope)),
            )

    def _active_action_by_dedupe(
        self, connection: sqlite3.Connection, dedupe_key: str
    ) -> sqlite3.Row | None:
        return connection.execute(
            """
            SELECT * FROM adapter_actions
            WHERE dedupe_key = ? AND state IN ('ready', 'leased', 'running', 'retry_wait')
            """,
            (dedupe_key,),
        ).fetchone()

    def find_active_action(
        self, *, connection_binding_id: str, capability: CapabilityBinding, scope: RefreshScope
    ) -> AdapterAction | None:
        dedupe_key = action_dedupe_key(
            system_id=scope.system_id,
            connection_binding_id=connection_binding_id,
            capability_key=capability.capability_key,
            capability_version=capability.capability_version,
            scope=scope,
        )
        with self._lock:
            row = self._active_action_by_dedupe(self._connection, dedupe_key)
            return self._action_from_row(row) if row else None

    def _attach_scope_to_action(
        self,
        connection: sqlite3.Connection,
        *,
        intent_scope_id: str,
        lease_id: str,
        action_id: str,
        state: IntentScopeState,
        authority_now: datetime,
    ) -> None:
        if not self._claim_is_current(
            connection,
            intent_scope_id=intent_scope_id,
            lease_id=lease_id,
            authority_now=authority_now,
        ):
            raise ValueError("intent scope lease is no longer current")
        connection.execute(
            "INSERT OR IGNORE INTO action_intent_scopes (action_id, intent_scope_id) VALUES (?, ?)",
            (action_id, intent_scope_id),
        )
        connection.execute(
            """
            UPDATE refresh_intent_scopes
            SET state = ?, disposition_reason = ?, linked_action_id = ?, lease_id = NULL,
                lease_worker_id = NULL, leased_until = NULL
            WHERE intent_scope_id = ?
            """,
            (
                state.value,
                "action_admitted"
                if state is IntentScopeState.ADMITTED
                else "active_action_coalesced",
                action_id,
                intent_scope_id,
            ),
        )
        self._refresh_intent_aggregate(connection, intent_scope_id=intent_scope_id)

    def admit_or_coalesce(
        self,
        *,
        work: IntentScopeWork,
        binding: ConnectionBinding,
        capability: CapabilityBinding,
        now: datetime,
    ) -> tuple[AdapterAction | None, bool]:
        """Atomically elect one active dedupe winner and attach this intent scope."""
        effective_scope = replace(work.scope, capability_key=capability.capability_key)
        legacy_scope = replace(effective_scope, capability_key=None)
        dedupe_key = action_dedupe_key(
            system_id=effective_scope.system_id,
            connection_binding_id=binding.binding_id,
            capability_key=capability.capability_key,
            capability_version=capability.capability_version,
            scope=effective_scope,
        )
        legacy_dedupe_key = (
            action_dedupe_key(
                system_id=legacy_scope.system_id,
                connection_binding_id=binding.binding_id,
                capability_key=capability.capability_key,
                capability_version=capability.capability_version,
                scope=legacy_scope,
            )
            if legacy_scope != effective_scope
            else None
        )
        action = AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=effective_scope.system_id,
            connection_binding_id=binding.binding_id,
            adapter_key=binding.adapter_key,
            adapter_version=binding.adapter_version,
            capability_key=capability.capability_key,
            capability_version=capability.capability_version,
            target=effective_scope.target,
            requested_scopes=(effective_scope,),
            deadline=work.intent.expires_at,
        )
        with self._immediate_transaction() as connection:
            authority_now = self._current_time()
            if not self._claim_is_current(
                connection,
                intent_scope_id=work.intent_scope_id,
                lease_id=work.lease_id,
                authority_now=authority_now,
            ):
                raise ValueError("intent scope lease is no longer current")
            if self._expire_intent_scope_if_due(
                connection,
                intent_scope_id=work.intent_scope_id,
                authority_now=authority_now,
            ):
                return None, False
            existing = self._active_action_by_dedupe(connection, dedupe_key)
            if existing is None and legacy_dedupe_key is not None:
                existing = self._active_action_by_dedupe(connection, legacy_dedupe_key)
            if existing is not None:
                self._attach_scope_to_action(
                    connection,
                    intent_scope_id=work.intent_scope_id,
                    lease_id=work.lease_id,
                    action_id=existing["action_id"],
                    state=IntentScopeState.COALESCED,
                    authority_now=authority_now,
                )
                return self._action_from_row(existing), False
            self.require_write_headroom()
            try:
                self._insert_action(
                    connection, action=action, dedupe_key=dedupe_key, created_at=now
                )
            except sqlite3.IntegrityError:
                existing = self._active_action_by_dedupe(connection, dedupe_key)
                if existing is None and legacy_dedupe_key is not None:
                    existing = self._active_action_by_dedupe(connection, legacy_dedupe_key)
                if existing is None:
                    raise
                self._attach_scope_to_action(
                    connection,
                    intent_scope_id=work.intent_scope_id,
                    lease_id=work.lease_id,
                    action_id=existing["action_id"],
                    state=IntentScopeState.COALESCED,
                    authority_now=authority_now,
                )
                return self._action_from_row(existing), False
            self._attach_scope_to_action(
                connection,
                intent_scope_id=work.intent_scope_id,
                lease_id=work.lease_id,
                action_id=action.action_id,
                state=IntentScopeState.ADMITTED,
                authority_now=authority_now,
            )
        return action, True

    async def enqueue(self, action: AdapterAction) -> None:
        """Persist a direct adapter action for local queue tests/composition.

        Normal application flow uses :meth:`admit_or_coalesce`, which also
        attaches durable intent-scope receipts.
        """
        dedupe_key = action_dedupe_key(
            system_id=action.system_id,
            connection_binding_id=action.connection_binding_id,
            capability_key=action.capability_key,
            capability_version=action.capability_version,
            scope=action.requested_scopes[0],
        )
        with self._immediate_transaction() as connection:
            row = connection.execute(
                "SELECT 1 FROM adapter_actions WHERE action_id = ?", (action.action_id,)
            ).fetchone()
            if row is None:
                self._insert_action(
                    connection, action=action, dedupe_key=dedupe_key, created_at=_now()
                )

    def _promote_due_actions(self, *, adapter_key: str, now: datetime) -> bool:
        now_text = _utc_text(now)
        with self._immediate_transaction() as connection:
            promoted = 0
            result = connection.execute(
                """
                UPDATE adapter_actions
                SET state = 'ready', lease_id = NULL, lease_worker_id = NULL,
                    leased_until = NULL
                WHERE action_id IN (
                    SELECT action_id FROM adapter_actions
                        INDEXED BY ix_adapter_actions_lease_due
                    WHERE adapter_key = ? AND state IN ('leased', 'running')
                      AND leased_until <= ?
                    LIMIT ?
                )
                """,
                (adapter_key, now_text, _MAX_DUE_PROMOTIONS_PER_CLAIM),
            )
            promoted += result.rowcount
            remaining = _MAX_DUE_PROMOTIONS_PER_CLAIM - promoted
            result = connection.execute(
                """
                UPDATE adapter_actions
                SET state = 'ready', retry_at = NULL, lease_id = NULL,
                    lease_worker_id = NULL, leased_until = NULL
                WHERE action_id IN (
                    SELECT action_id FROM adapter_actions
                        INDEXED BY ix_adapter_actions_retry_due
                    WHERE adapter_key = ? AND state = 'retry_wait' AND retry_at <= ?
                    LIMIT ?
                )
                """,
                (adapter_key, now_text, remaining),
            )
            promoted += result.rowcount
        return promoted >= _MAX_DUE_PROMOTIONS_PER_CLAIM

    async def lease_next(
        self, *, adapter_key: str, worker_id: str, now: datetime
    ) -> ActionLease | None:
        require_contract_key(adapter_key, "adapter_key")
        require_text(worker_id, "worker_id", max_length=256)
        require_utc(now, "now")
        lease_id = str(uuid4())
        self._promote_due_actions(adapter_key=adapter_key, now=self._current_time())
        with self._immediate_transaction() as connection:
            lease_now = self._current_time()
            leased_until = lease_now + _DEFAULT_LEASE
            quarantined = 0
            while True:
                row = connection.execute(
                    """
                    SELECT * FROM adapter_actions INDEXED BY ix_adapter_actions_claim_order
                    WHERE adapter_key = ? AND state = 'ready'
                    ORDER BY record_created_at, action_id
                    LIMIT 1
                    """,
                    (adapter_key,),
                ).fetchone()
                if row is None:
                    return None
                try:
                    action = self._action_from_row(row)
                except (TypeError, ValueError, json.JSONDecodeError):
                    self._terminalize_action_contract_failure(
                        connection,
                        action_id=row["action_id"],
                        authority_now=lease_now,
                    )
                    quarantined += 1
                    if quarantined >= _MAX_ACTION_CONTRACT_QUARANTINES_PER_CLAIM:
                        return None
                    continue
                attempt_summary = connection.execute(
                    """
                    SELECT
                        COALESCE(MAX(CASE WHEN typeof(ordinal) != 'integer' OR ordinal < 1
                                          THEN 1 ELSE 0 END), 0),
                        COALESCE(MAX(CASE WHEN typeof(ordinal) = 'integer' AND ordinal >= 1
                                          THEN ordinal ELSE 0 END), 0)
                    FROM action_attempts
                    WHERE action_id = ?
                    """,
                    (row["action_id"],),
                ).fetchone()
                invalid_attempt = bool(attempt_summary[0])
                max_ordinal = attempt_summary[1]
                if (
                    invalid_attempt
                    or not isinstance(max_ordinal, int)
                    or max_ordinal >= _SQLITE_MAX_INTEGER
                ):
                    self._terminalize_action_contract_failure(
                        connection,
                        action_id=row["action_id"],
                        authority_now=lease_now,
                        reason="malformed_attempt_contract",
                    )
                    quarantined += 1
                    if quarantined >= _MAX_ACTION_CONTRACT_QUARANTINES_PER_CLAIM:
                        return None
                    continue
                attempt_ordinal = max_ordinal + 1
                break
            result = connection.execute(
                """
                UPDATE adapter_actions
                SET state = 'leased', lease_id = ?, lease_worker_id = ?, leased_until = ?
                WHERE action_id = ? AND state = 'ready'
                """,
                (
                    lease_id,
                    worker_id,
                    _utc_text(leased_until),
                    row["action_id"],
                ),
            )
            if result.rowcount != 1:  # pragma: no cover - transaction invariant
                return None
        return ActionLease(
            action=action,
            lease_id=lease_id,
            leased_until=leased_until,
            attempt_ordinal=attempt_ordinal,
        )

    async def mark_running(self, *, action_id: str, lease_id: str, started_at: datetime) -> None:
        action_id = require_uuid(action_id, "action_id")
        lease_id = require_uuid(lease_id, "lease_id")
        started_at = require_utc(started_at, "started_at")
        with self._immediate_transaction() as connection:
            lease_now = self._current_time()
            row = connection.execute(
                "SELECT state, lease_id, leased_until FROM adapter_actions WHERE action_id = ?",
                (action_id,),
            ).fetchone()
            if row is None:
                raise ValueError("unknown adapter action")
            if (
                row["state"] != ActionState.LEASED.value
                or row["lease_id"] != lease_id
                or _dt(row["leased_until"]) is None
                or _dt(row["leased_until"]) <= lease_now
            ):
                raise ValueError("adapter action lease is not valid for logical start")
            connection.execute(
                """
                UPDATE adapter_actions
                SET state = 'running', started_at = COALESCE(started_at, ?), error_class = NULL,
                    redacted_diagnostic = NULL, retry_at = NULL
                WHERE action_id = ?
                """,
                (_utc_text(lease_now), action_id),
            )

    def _require_live_action_lease(
        self,
        connection: sqlite3.Connection,
        *,
        action_id: str,
        lease_id: str,
        allowed_states: set[ActionState],
    ) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT * FROM adapter_actions
            WHERE action_id = ? AND lease_id = ?
            """,
            (action_id, lease_id),
        ).fetchone()
        if row is None:
            raise ActionLeaseLost("adapter action lease is no longer current")
        if ActionState(row["state"]) not in allowed_states:
            raise ActionLeaseLost("adapter action is not in a lease-authorized state")
        leased_until = _dt(row["leased_until"])
        if leased_until is None or leased_until <= self._current_time():
            raise ActionLeaseLost("adapter action lease has expired")
        return row

    async def record_attempt(self, attempt: ActionAttempt, *, lease_id: str) -> None:
        lease_id = require_uuid(lease_id, "lease_id")
        attempt_at = attempt.ended_at or attempt.started_at
        with self._immediate_transaction() as connection:
            action_row = self._require_live_action_lease(
                connection,
                action_id=attempt.action_id,
                lease_id=lease_id,
                allowed_states={ActionState.RUNNING},
            )
            result = connection.execute(
                """
                INSERT INTO action_attempts (
                    attempt_id, action_id, ordinal, started_at, ended_at,
                    outcome, error_class, retry_at, redacted_diagnostic
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(attempt_id) DO NOTHING
                """,
                (
                    attempt.attempt_id,
                    attempt.action_id,
                    attempt.ordinal,
                    _utc_text(attempt.started_at),
                    _utc_text(attempt.ended_at) if attempt.ended_at else None,
                    attempt.outcome.value if attempt.outcome else None,
                    attempt.error_class.value if attempt.error_class else None,
                    _utc_text(attempt.retry_at) if attempt.retry_at else None,
                    (
                        _redact(attempt.redacted_diagnostic)
                        if attempt.redacted_diagnostic is not None
                        else None
                    ),
                ),
            )
            if result.rowcount != 1:
                return
            if attempt.outcome is ActionOutcome.FAILED:
                self._insert_event(
                    connection,
                    idempotency_key=f"attempt-{attempt.attempt_id}-failed",
                    event_type="refresh.action.attempt_failed",
                    severity="warning",
                    alertable=False,
                    system_id=action_row["system_id"],
                    action_id=attempt.action_id,
                    attempt_id=attempt.attempt_id,
                    error_class=(attempt.error_class.value if attempt.error_class else None),
                    summary=attempt.redacted_diagnostic,
                    occurred_at=attempt_at,
                )
            if attempt.retry_at is not None:
                connection.execute(
                    """
                    UPDATE adapter_actions
                    SET state = 'retry_wait', retry_at = ?
                    WHERE action_id = ? AND state = 'running'
                    """,
                    (_utc_text(attempt.retry_at), attempt.action_id),
                )

    async def heartbeat(
        self, *, action_id: str, lease_id: str, worker_id: str, at: datetime
    ) -> None:
        action_id = require_uuid(action_id, "action_id")
        lease_id = require_uuid(lease_id, "lease_id")
        require_text(worker_id, "worker_id", max_length=256)
        require_utc(at, "at")
        with self._immediate_transaction() as connection:
            lease_now = self._current_time()
            result = connection.execute(
                """
                UPDATE adapter_actions
                SET leased_until = ?
                WHERE action_id = ? AND lease_id = ? AND lease_worker_id = ?
                  AND state IN ('leased', 'running', 'retry_wait')
                  AND leased_until > ?
                """,
                (
                    _utc_text(lease_now + _DEFAULT_LEASE),
                    action_id,
                    lease_id,
                    worker_id,
                    _utc_text(lease_now),
                ),
            )
            if result.rowcount != 1:
                raise ActionLeaseLost("adapter action lease is no longer current")

    @staticmethod
    def _known_event_system_id(
        connection: sqlite3.Connection,
        persisted_system_id: object,
    ) -> str | None:
        row = connection.execute(
            "SELECT system_id FROM systems WHERE system_id = ?",
            (persisted_system_id,),
        ).fetchone()
        return row["system_id"] if row is not None else None

    def _insert_event(
        self,
        connection: sqlite3.Connection,
        *,
        idempotency_key: str,
        event_type: str,
        severity: str,
        alertable: bool,
        system_id: str | None,
        action_id: str | None,
        attempt_id: str | None = None,
        error_class: str | None,
        summary: str | None,
        occurred_at: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT OR IGNORE INTO operational_events (
                event_id, idempotency_key, event_type, severity, alertable, system_id, action_id,
                attempt_id, error_class, redacted_summary, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid4()),
                require_text(idempotency_key, "event idempotency_key", max_length=512),
                require_contract_key(event_type, "event_type"),
                require_contract_key(severity, "severity"),
                int(alertable),
                system_id,
                action_id,
                attempt_id,
                error_class,
                _redact(summary),
                _utc_text(occurred_at),
            ),
        )

    async def complete_action(self, completion: ActionCompletion, *, lease_id: str) -> None:
        """Persist a terminal outcome and atomically create its one failure event."""
        lease_id = require_uuid(lease_id, "lease_id")
        destination_state = ActionState(completion.outcome.value)
        with self._immediate_transaction() as connection:
            row = self._require_live_action_lease(
                connection,
                action_id=completion.action_id,
                lease_id=lease_id,
                allowed_states={ActionState.LEASED, ActionState.RUNNING, ActionState.RETRY_WAIT},
            )
            connection.execute(
                """
                UPDATE adapter_actions
                SET state = ?, completed_at = ?, lease_id = NULL, lease_worker_id = NULL,
                    leased_until = NULL, error_class = ?, redacted_diagnostic = ?, retry_at = ?
                WHERE action_id = ?
                """,
                (
                    destination_state.value,
                    _utc_text(completion.completed_at),
                    completion.error_class.value if completion.error_class else None,
                    (
                        _redact(completion.redacted_diagnostic)
                        if completion.redacted_diagnostic is not None
                        else None
                    ),
                    _utc_text(completion.retry_at) if completion.retry_at else None,
                    completion.action_id,
                ),
            )
            if completion.outcome is ActionOutcome.FAILED:
                self._insert_event(
                    connection,
                    idempotency_key=f"action-{completion.action_id}-terminal-failed",
                    event_type="refresh.action.failed",
                    severity="error",
                    alertable=True,
                    system_id=row["system_id"],
                    action_id=completion.action_id,
                    error_class=completion.error_class.value if completion.error_class else None,
                    summary=completion.redacted_diagnostic,
                    occurred_at=completion.completed_at,
                )
            self._refresh_action_parent_aggregates(connection, action_id=completion.action_id)

    @staticmethod
    def _operational_event_from_row(row: sqlite3.Row) -> OperationalEventRecord:
        return OperationalEventRecord(
            event_id=row["event_id"],
            event_type=row["event_type"],
            severity=row["severity"],
            alertable=bool(row["alertable"]),
            system_id=row["system_id"],
            action_id=row["action_id"],
            attempt_id=row["attempt_id"],
            error_class=row["error_class"],
            redacted_summary=row["redacted_summary"],
            occurred_at=_dt(row["occurred_at"]),  # type: ignore[arg-type]
        )

    def list_operational_events(
        self,
        *,
        system_id: str | None = None,
        alertable_only: bool = False,
        limit: int | None = None,
    ) -> tuple[OperationalEventRecord, ...]:
        if limit is not None and (
            isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100
        ):
            raise ValueError("operational event limit must be between 1 and 100")
        if system_id is None and not alertable_only:
            statement = "SELECT * FROM operational_events ORDER BY occurred_at DESC, event_id"
            parameters: tuple[object, ...] = ()
        elif system_id is None:
            statement = (
                "SELECT * FROM operational_events WHERE alertable = 1 "
                "ORDER BY occurred_at DESC, event_id"
            )
            parameters = ()
        elif not alertable_only:
            statement = (
                "SELECT * FROM operational_events WHERE system_id = ? "
                "ORDER BY occurred_at DESC, event_id"
            )
            parameters = (require_uuid(system_id, "system_id"),)
        else:
            statement = (
                "SELECT * FROM operational_events WHERE system_id = ? AND alertable = 1 "
                "ORDER BY occurred_at DESC, event_id"
            )
            parameters = (require_uuid(system_id, "system_id"),)
        if limit is not None:
            statement += " LIMIT ?"
            parameters = (*parameters, limit)
        with self._lock:
            rows = self._connection.execute(statement, parameters).fetchall()
        return tuple(self._operational_event_from_row(row) for row in rows)

    def count_alertable_events(
        self, *, event_type: str | None = None, severity: str | None = None
    ) -> int:
        if event_type is not None:
            require_contract_key(event_type, "event_type")
        if severity is not None and severity not in _ALERT_SEVERITIES:
            raise ValueError("severity is not registered")
        if event_type is None and severity is None:
            statement = "SELECT COUNT(*) FROM operational_events WHERE alertable = 1"
            parameters: tuple[object, ...] = ()
        elif event_type is not None and severity is None:
            statement = (
                "SELECT COUNT(*) FROM operational_events WHERE alertable = 1 AND event_type = ?"
            )
            parameters = (event_type,)
        elif event_type is None:
            statement = (
                "SELECT COUNT(*) FROM operational_events WHERE alertable = 1 AND severity = ?"
            )
            parameters = (severity,)
        else:
            statement = (
                "SELECT COUNT(*) FROM operational_events "
                "WHERE alertable = 1 AND event_type = ? AND severity = ?"
            )
            parameters = (event_type, severity)
        with self._lock:
            row = self._connection.execute(statement, parameters).fetchone()
        return int(row[0]) if row is not None else 0

    def list_alertable_events_page(
        self,
        *,
        offset: int,
        limit: int,
        event_type: str | None = None,
        severity: str | None = None,
    ) -> tuple[OperationalEventRecord, ...]:
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("alert offset must be a non-negative integer")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("alert limit must be between 1 and 100")
        if event_type is not None:
            require_contract_key(event_type, "event_type")
        if severity is not None and severity not in _ALERT_SEVERITIES:
            raise ValueError("severity is not registered")
        if event_type is None and severity is None:
            statement = (
                "SELECT * FROM operational_events WHERE alertable = 1 "
                "ORDER BY occurred_at DESC, event_id LIMIT ? OFFSET ?"
            )
            parameters: tuple[object, ...] = (limit, offset)
        elif event_type is not None and severity is None:
            statement = (
                "SELECT * FROM operational_events "
                "WHERE alertable = 1 AND event_type = ? "
                "ORDER BY occurred_at DESC, event_id LIMIT ? OFFSET ?"
            )
            parameters = (event_type, limit, offset)
        elif event_type is None:
            statement = (
                "SELECT * FROM operational_events "
                "WHERE alertable = 1 AND severity = ? "
                "ORDER BY occurred_at DESC, event_id LIMIT ? OFFSET ?"
            )
            parameters = (severity, limit, offset)
        else:
            statement = (
                "SELECT * FROM operational_events "
                "WHERE alertable = 1 AND event_type = ? AND severity = ? "
                "ORDER BY occurred_at DESC, event_id LIMIT ? OFFSET ?"
            )
            parameters = (event_type, severity, limit, offset)
        with self._lock:
            rows = self._connection.execute(statement, parameters).fetchall()
        return tuple(self._operational_event_from_row(row) for row in rows)

    def list_alertable_events_after(
        self,
        *,
        after_occurred_at: datetime | None,
        after_event_id: str | None,
        limit: int,
        event_type: str | None = None,
        severity: str | None = None,
    ) -> tuple[OperationalEventRecord, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 101:
            raise ValueError("alert cursor limit must be between 1 and 101")
        if (after_occurred_at is None) != (after_event_id is None):
            raise ValueError("alert cursor is incomplete")
        if event_type is not None:
            require_contract_key(event_type, "event_type")
        if severity is not None and severity not in _ALERT_SEVERITIES:
            raise ValueError("severity is not registered")
        timestamp: str | None = None
        if after_occurred_at is not None:
            after_event_id = require_uuid(after_event_id, "alert cursor ID")  # type: ignore[arg-type]
            timestamp = _utc_text(require_utc(after_occurred_at, "alert cursor time"))
        if event_type is None and severity is None and timestamp is None:
            statement = (
                "SELECT * FROM operational_events WHERE alertable = 1 "
                "ORDER BY occurred_at DESC, event_id LIMIT ?"
            )
            parameters: tuple[object, ...] = (limit,)
        elif event_type is None and severity is None:
            statement = (
                "SELECT * FROM operational_events WHERE alertable = 1 "
                "AND (occurred_at < ? OR (occurred_at = ? AND event_id > ?)) "
                "ORDER BY occurred_at DESC, event_id LIMIT ?"
            )
            parameters = (timestamp, timestamp, after_event_id, limit)
        elif event_type is not None and severity is None and timestamp is None:
            statement = (
                "SELECT * FROM operational_events WHERE alertable = 1 AND event_type = ? "
                "ORDER BY occurred_at DESC, event_id LIMIT ?"
            )
            parameters = (event_type, limit)
        elif event_type is not None and severity is None:
            statement = (
                "SELECT * FROM operational_events WHERE alertable = 1 AND event_type = ? "
                "AND (occurred_at < ? OR (occurred_at = ? AND event_id > ?)) "
                "ORDER BY occurred_at DESC, event_id LIMIT ?"
            )
            parameters = (event_type, timestamp, timestamp, after_event_id, limit)
        elif event_type is None and timestamp is None:
            statement = (
                "SELECT * FROM operational_events WHERE alertable = 1 AND severity = ? "
                "ORDER BY occurred_at DESC, event_id LIMIT ?"
            )
            parameters = (severity, limit)
        elif event_type is None:
            statement = (
                "SELECT * FROM operational_events WHERE alertable = 1 AND severity = ? "
                "AND (occurred_at < ? OR (occurred_at = ? AND event_id > ?)) "
                "ORDER BY occurred_at DESC, event_id LIMIT ?"
            )
            parameters = (severity, timestamp, timestamp, after_event_id, limit)
        elif timestamp is None:
            statement = (
                "SELECT * FROM operational_events "
                "WHERE alertable = 1 AND event_type = ? AND severity = ? "
                "ORDER BY occurred_at DESC, event_id LIMIT ?"
            )
            parameters = (event_type, severity, limit)
        else:
            statement = (
                "SELECT * FROM operational_events "
                "WHERE alertable = 1 AND event_type = ? AND severity = ? "
                "AND (occurred_at < ? OR (occurred_at = ? AND event_id > ?)) "
                "ORDER BY occurred_at DESC, event_id LIMIT ?"
            )
            parameters = (
                event_type,
                severity,
                timestamp,
                timestamp,
                after_event_id,
                limit,
            )
        with self._lock:
            rows = self._connection.execute(statement, parameters).fetchall()
        return tuple(self._operational_event_from_row(row) for row in rows)

    def record_runtime_failure(
        self, *, event_type: str, summary: str, occurred_at: datetime
    ) -> OperationalEventRecord:
        """Persist one bounded, redacted infrastructure-failure event.

        Callers supply a short application-authored summary, not an exception or
        downstream payload. The narrow character set makes this safe to expose
        in later local status views without retaining a traceback.
        """
        if event_type not in _RUNTIME_EVENT_TYPES:
            raise ValueError("runtime event type is not registered")
        require_text(summary, "runtime failure summary", max_length=512)
        if _RUNTIME_SUMMARY.fullmatch(summary) is None:
            raise ValueError("runtime failure summary must be a bounded safe message")
        occurred_at = require_utc(occurred_at, "occurred_at")
        key_material = f"{event_type}:{_utc_text(occurred_at)}:{summary}".encode()
        idempotency_key = f"runtime-{hashlib.sha256(key_material).hexdigest()}"
        with self._immediate_transaction() as connection:
            self._insert_event(
                connection,
                idempotency_key=idempotency_key,
                event_type=event_type,
                severity="error",
                alertable=True,
                system_id=None,
                action_id=None,
                error_class=ErrorClass.UNKNOWN_ADAPTER_FAILURE.value,
                summary=summary,
                occurred_at=occurred_at,
            )
            row = connection.execute(
                "SELECT * FROM operational_events WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        if row is None:  # pragma: no cover - transaction invariant
            raise RuntimeError("runtime failure event was not recorded")
        return self._operational_event_from_row(row)

    def _guard_cancel(
        self,
        connection: sqlite3.Connection,
        *,
        action_id: str,
        reason: str,
        authority_now: datetime,
    ) -> GuardDecision:
        connection.execute(
            """
            UPDATE adapter_actions
            SET state = 'cancelled', completed_at = ?, lease_id = NULL, lease_worker_id = NULL,
                leased_until = NULL, error_class = ?, redacted_diagnostic = ?
            WHERE action_id = ?
            """,
            (
                _utc_text(authority_now),
                ErrorClass.LOCAL_CANCELLATION.value,
                _redact(reason),
                action_id,
            ),
        )
        self._refresh_action_parent_aggregates(connection, action_id=action_id)
        return GuardDecision(GuardDisposition.CANCEL, reason)

    def _guard_defer_for_headroom(
        self,
        connection: sqlite3.Connection,
        *,
        action_id: str,
        authority_now: datetime,
    ) -> GuardDecision:
        connection.execute(
            """
            UPDATE adapter_actions
            SET state = 'retry_wait', retry_at = ?, lease_id = NULL,
                lease_worker_id = NULL, leased_until = NULL,
                error_class = NULL, redacted_diagnostic = ?
            WHERE action_id = ?
            """,
            (
                _utc_text(authority_now + _HEADROOM_RETRY_DELAY),
                _redact("storage_headroom_low"),
                action_id,
            ),
        )
        self._refresh_action_parent_aggregates(connection, action_id=action_id)
        return GuardDecision(GuardDisposition.FAIL, "storage_headroom_low")

    def _terminalize_action_contract_failure(
        self,
        connection: sqlite3.Connection,
        *,
        action_id: str,
        authority_now: datetime,
        reason: str = "malformed_action_contract",
    ) -> GuardDecision:
        row = connection.execute(
            "SELECT system_id FROM adapter_actions WHERE action_id = ?", (action_id,)
        ).fetchone()
        if row is None:
            return GuardDecision(GuardDisposition.FAIL, "unknown_action")
        connection.execute(
            """
            UPDATE refresh_intent_scopes
            SET state = 'rejected', disposition_reason = ?, eligible_at = NULL,
                lease_id = NULL, lease_worker_id = NULL, leased_until = NULL
            WHERE intent_scope_id IN (
                SELECT intent_scope_id FROM action_intent_scopes WHERE action_id = ?
            ) AND state IN ('admitted', 'coalesced')
            """,
            (reason, action_id),
        )
        connection.execute(
            """
            UPDATE adapter_actions
            SET state = 'failed', completed_at = ?, lease_id = NULL, lease_worker_id = NULL,
                leased_until = NULL, retry_at = NULL, error_class = ?, redacted_diagnostic = ?
            WHERE action_id = ?
            """,
            (
                _utc_text(authority_now),
                ErrorClass.ADAPTER_CONTRACT_MISMATCH.value,
                _redact(reason),
                action_id,
            ),
        )
        self._insert_event(
            connection,
            idempotency_key=_quarantine_event_key("action", action_id),
            event_type="refresh.action.failed",
            severity="error",
            alertable=True,
            system_id=self._known_event_system_id(connection, row["system_id"]),
            action_id=action_id,
            error_class=ErrorClass.ADAPTER_CONTRACT_MISMATCH.value,
            summary=reason,
            occurred_at=authority_now,
        )
        self._refresh_action_parent_aggregates(connection, action_id=action_id)
        return GuardDecision(GuardDisposition.FAIL, reason)

    def _guard_expire_action(
        self,
        connection: sqlite3.Connection,
        *,
        action_id: str,
        authority_now: datetime,
    ) -> GuardDecision:
        reason = "action_deadline_expired"
        linked_scopes = connection.execute(
            """
            SELECT scope.intent_scope_id, intent.expires_at
            FROM action_intent_scopes AS link
            JOIN refresh_intent_scopes AS scope ON scope.intent_scope_id = link.intent_scope_id
            JOIN refresh_intents AS intent ON intent.intent_id = scope.intent_id
            WHERE link.action_id = ? AND scope.state IN ('admitted', 'coalesced')
            """,
            (action_id,),
        ).fetchall()
        expired_scope_ids = tuple(
            row["intent_scope_id"]
            for row in linked_scopes
            if _dt(row["expires_at"]) is not None and _dt(row["expires_at"]) <= authority_now
        )
        live_scope_ids = tuple(
            row["intent_scope_id"]
            for row in linked_scopes
            if row["intent_scope_id"] not in expired_scope_ids
        )
        for intent_scope_id in expired_scope_ids:
            connection.execute(
                """
                UPDATE refresh_intent_scopes
                SET state = 'expired', disposition_reason = ?, eligible_at = NULL,
                    lease_id = NULL, lease_worker_id = NULL, leased_until = NULL
                WHERE intent_scope_id = ?
                """,
                (reason, intent_scope_id),
            )
        for intent_scope_id in live_scope_ids:
            connection.execute(
                "DELETE FROM action_intent_scopes WHERE intent_scope_id = ?",
                (intent_scope_id,),
            )
            connection.execute(
                """
                UPDATE refresh_intent_scopes
                SET state = 'queued', disposition_reason = ?, eligible_at = ?,
                    linked_action_id = NULL, lease_id = NULL,
                    lease_worker_id = NULL, leased_until = NULL
                WHERE intent_scope_id = ?
                """,
                (reason, _utc_text(authority_now), intent_scope_id),
            )
        connection.execute(
            """
            UPDATE adapter_actions
            SET state = 'cancelled', completed_at = ?, lease_id = NULL, lease_worker_id = NULL,
                leased_until = NULL, retry_at = NULL, error_class = ?, redacted_diagnostic = ?
            WHERE action_id = ?
            """,
            (
                _utc_text(authority_now),
                ErrorClass.LOCAL_CANCELLATION.value,
                _redact(reason),
                action_id,
            ),
        )
        self._refresh_action_parent_aggregates(connection, action_id=action_id)
        return GuardDecision(GuardDisposition.CANCEL, reason)

    def _prune_expired_action_scopes(
        self,
        connection: sqlite3.Connection,
        *,
        action_id: str,
        authority_now: datetime,
    ) -> tuple[sqlite3.Row, ...]:
        scope_rows = connection.execute(
            """
            SELECT scope.*, intent.requested_at, intent.expires_at
            FROM action_intent_scopes AS link
            JOIN refresh_intent_scopes AS scope ON scope.intent_scope_id = link.intent_scope_id
            JOIN refresh_intents AS intent ON intent.intent_id = scope.intent_id
            WHERE link.action_id = ? AND scope.state IN ('admitted', 'coalesced')
            """,
            (action_id,),
        ).fetchall()
        live_rows: list[sqlite3.Row] = []
        for scope_row in scope_rows:
            expires_at = _dt(scope_row["expires_at"])
            if expires_at is None or expires_at > authority_now:
                live_rows.append(scope_row)
                continue
            connection.execute(
                """
                UPDATE refresh_intent_scopes
                SET state = 'expired', disposition_reason = 'request_expired',
                    eligible_at = NULL, lease_id = NULL, lease_worker_id = NULL,
                    leased_until = NULL
                WHERE intent_scope_id = ?
                """,
                (scope_row["intent_scope_id"],),
            )
            self._refresh_intent_aggregate(
                connection,
                intent_scope_id=scope_row["intent_scope_id"],
            )
        return tuple(live_rows)

    async def evaluate(self, *, action_id: str, lease_id: str, now: datetime) -> GuardDecision:
        """Run the generic local pre-dispatch guard against a current lease."""
        return self._evaluate_action_guard(
            action_id=action_id,
            lease_id=lease_id,
            now=now,
            authorize_start=False,
        )

    async def authorize_start(
        self, *, action_id: str, lease_id: str, binding_revision: str, now: datetime
    ) -> GuardDecision:
        """Atomically revalidate authority and transition an allowed lease to running."""
        return self._evaluate_action_guard(
            action_id=action_id,
            lease_id=lease_id,
            now=now,
            authorize_start=True,
            binding_revision=require_text(binding_revision, "binding_revision", max_length=64),
        )

    def _evaluate_action_guard(
        self,
        *,
        action_id: str,
        lease_id: str,
        now: datetime,
        authorize_start: bool,
        binding_revision: str | None = None,
    ) -> GuardDecision:
        action_id = require_uuid(action_id, "action_id")
        lease_id = require_uuid(lease_id, "lease_id")
        now = require_utc(now, "now")
        with self._immediate_transaction() as connection:
            authority_now = self._current_time()
            row = connection.execute(
                """
                SELECT action.*, system.enabled AS system_enabled,
                       binding.enabled AS binding_enabled,
                       binding.adapter_key AS binding_adapter_key,
                       binding.adapter_version AS binding_adapter_version,
                       binding.non_secret_settings_json AS binding_settings_json,
                       binding.secret_reference AS binding_secret_reference,
                       capability.enabled AS capability_enabled,
                       capability.operation_class
                FROM adapter_actions AS action
                JOIN systems AS system ON system.system_id = action.system_id
                JOIN connection_bindings AS binding
                  ON binding.binding_id = action.connection_binding_id
                LEFT JOIN capability_bindings AS capability
                  ON capability.connection_binding_id = action.connection_binding_id
                 AND capability.capability_key = action.capability_key
                 AND capability.capability_version = action.capability_version
                WHERE action.action_id = ?
                """,
                (action_id,),
            ).fetchone()
            if row is None:
                return GuardDecision(GuardDisposition.FAIL, "unknown_action")
            if row["state"] != ActionState.LEASED.value or row["lease_id"] != lease_id:
                return GuardDecision(GuardDisposition.FAIL, "invalid_action_lease")
            try:
                leased_until = _dt(row["leased_until"])
                deadline = _dt(row["deadline"])
            except (TypeError, ValueError):
                return self._terminalize_action_contract_failure(
                    connection,
                    action_id=action_id,
                    authority_now=authority_now,
                )
            if leased_until is None or leased_until <= authority_now:
                return GuardDecision(GuardDisposition.FAIL, "expired_action_lease")
            if deadline is not None and deadline <= authority_now:
                try:
                    return self._guard_expire_action(
                        connection,
                        action_id=action_id,
                        authority_now=authority_now,
                    )
                except (TypeError, ValueError):
                    return self._terminalize_action_contract_failure(
                        connection,
                        action_id=action_id,
                        authority_now=authority_now,
                    )
            try:
                active_scopes = self._prune_expired_action_scopes(
                    connection,
                    action_id=action_id,
                    authority_now=authority_now,
                )
            except (TypeError, ValueError):
                return self._terminalize_action_contract_failure(
                    connection,
                    action_id=action_id,
                    authority_now=authority_now,
                )
            if not active_scopes:
                return self._guard_cancel(
                    connection,
                    action_id=action_id,
                    reason="no_live_originating_scope",
                    authority_now=authority_now,
                )
            if not row["system_enabled"]:
                return self._guard_cancel(
                    connection,
                    action_id=action_id,
                    reason="system_disabled",
                    authority_now=authority_now,
                )
            if not row["binding_enabled"]:
                return self._guard_cancel(
                    connection,
                    action_id=action_id,
                    reason="binding_disabled",
                    authority_now=authority_now,
                )
            if authorize_start:
                current_binding_revision = _binding_revision(
                    adapter_key=row["binding_adapter_key"],
                    adapter_version=row["binding_adapter_version"],
                    non_secret_settings_json=row["binding_settings_json"],
                    secret_reference=row["binding_secret_reference"],
                )
                if current_binding_revision != binding_revision:
                    return self._guard_cancel(
                        connection,
                        action_id=action_id,
                        reason="binding_changed",
                        authority_now=authority_now,
                    )
            if row["capability_enabled"] is None:
                return self._guard_cancel(
                    connection,
                    action_id=action_id,
                    reason="capability_missing",
                    authority_now=authority_now,
                )
            if not row["capability_enabled"]:
                return self._guard_cancel(
                    connection,
                    action_id=action_id,
                    reason="capability_disabled",
                    authority_now=authority_now,
                )
            if row["operation_class"] != OperationClass.OBSERVE.value:
                return GuardDecision(GuardDisposition.FAIL, "capability_not_observe")
            satisfying_ids: list[str] = []
            for scope_row in active_scopes:
                scope = _scope_from_row(scope_row)
                if scope.target.kind is TargetKind.OBJECT:
                    target = connection.execute(
                        "SELECT presence FROM remote_objects WHERE object_id = ? AND system_id = ?",
                        (scope.target.target_id, scope.system_id),
                    ).fetchone()
                    if target is None or target["presence"] == PresenceState.ABSENT.value:
                        return self._guard_cancel(
                            connection,
                            action_id=action_id,
                            reason="target_not_available",
                            authority_now=authority_now,
                        )
                elif scope.target.kind is TargetKind.CONFIGURED_SCOPE:
                    configured = connection.execute(
                        """
                        SELECT enabled FROM configured_scopes
                        WHERE scope_id = ? AND system_id = ?
                        """,
                        (scope.target.target_id, scope.system_id),
                    ).fetchone()
                    if configured is None or not configured["enabled"]:
                        return self._guard_cancel(
                            connection,
                            action_id=action_id,
                            reason="configured_scope_disabled",
                            authority_now=authority_now,
                        )
                try:
                    interval = self.effective_interval(scope)
                except ValueError:
                    return GuardDecision(GuardDisposition.FAIL, "invalid_scope_policy")
                evidence = self.latest_qualifying_observation(scope)
                decision = decide_refresh(
                    requested_scope=scope,
                    requested_at=_dt(scope_row["requested_at"]),  # type: ignore[arg-type]
                    now=authority_now,
                    minimum_interval=interval,
                    state=self.scope_policy_state(scope),
                    evidence=evidence,
                )
                if decision.kind.value != "satisfied" or decision.satisfying_observation_id is None:
                    if authorize_start:
                        if not self.write_headroom_available():
                            return self._guard_defer_for_headroom(
                                connection,
                                action_id=action_id,
                                authority_now=authority_now,
                            )
                        result = connection.execute(
                            """
                            UPDATE adapter_actions
                            SET state = 'running', started_at = COALESCE(started_at, ?),
                                leased_until = ?, error_class = NULL,
                                redacted_diagnostic = NULL, retry_at = NULL
                            WHERE action_id = ? AND state = 'leased' AND lease_id = ?
                              AND leased_until > ?
                            """,
                            (
                                _utc_text(authority_now),
                                _utc_text(authority_now + _DEFAULT_LEASE),
                                action_id,
                                lease_id,
                                _utc_text(authority_now),
                            ),
                        )
                        if result.rowcount != 1:  # pragma: no cover - transaction invariant
                            return GuardDecision(GuardDisposition.FAIL, "invalid_action_lease")
                    return GuardDecision(GuardDisposition.DISPATCH, "dispatch")
                satisfying_ids.append(decision.satisfying_observation_id)
            result = connection.execute(
                """
                UPDATE adapter_actions
                SET state = 'satisfied', completed_at = ?, lease_id = NULL, lease_worker_id = NULL,
                    leased_until = NULL
                WHERE action_id = ? AND state = 'leased' AND lease_id = ? AND leased_until > ?
                """,
                (_utc_text(now), action_id, lease_id, _utc_text(authority_now)),
            )
            if result.rowcount != 1:
                return GuardDecision(GuardDisposition.FAIL, "expired_action_lease")
            for scope_row, observation_id in zip(active_scopes, satisfying_ids, strict=True):
                connection.execute(
                    """
                    UPDATE refresh_intent_scopes
                    SET state = 'satisfied', disposition_reason = 'evidence_satisfied',
                        satisfying_observation_id = ?
                    WHERE intent_scope_id = ?
                    """,
                    (observation_id, scope_row["intent_scope_id"]),
                )
            self._refresh_action_parent_aggregates(connection, action_id=action_id)
        return GuardDecision(
            GuardDisposition.SATISFY,
            "evidence_satisfied",
            tuple(satisfying_ids),
        )

    def _validate_batch_context(
        self,
        connection: sqlite3.Connection,
        batch: ObservationBatch,
        *,
        lease_id: str | None,
        require_action_lease: bool = True,
    ) -> None:
        binding = connection.execute(
            """
            SELECT system_id, adapter_key, adapter_version FROM connection_bindings
            WHERE binding_id = ?
            """,
            (batch.connection_binding_id,),
        ).fetchone()
        if binding is None:
            raise ValueError("unknown_connection_binding")
        if (
            binding["system_id"] != batch.system_id
            or binding["adapter_key"] != batch.adapter_key
            or binding["adapter_version"] != batch.adapter_version
        ):
            raise ValueError("batch_binding_contract_mismatch")
        if batch.action_id is not None:
            action = connection.execute(
                """
                SELECT system_id, connection_binding_id, adapter_key,
                       adapter_version
                FROM adapter_actions
                WHERE action_id = ?
                """,
                (batch.action_id,),
            ).fetchone()
            if action is None or any(
                (
                    action["system_id"] != batch.system_id,
                    action["connection_binding_id"] != batch.connection_binding_id,
                    action["adapter_key"] != batch.adapter_key,
                    action["adapter_version"] != batch.adapter_version,
                )
            ):
                raise ValueError("batch_action_contract_mismatch")
            if require_action_lease:
                if lease_id is None:
                    raise ValueError("action_linked_batch_requires_lease")
                self._require_live_action_lease(
                    connection,
                    action_id=batch.action_id,
                    lease_id=require_uuid(lease_id, "lease_id"),
                    allowed_states={ActionState.RUNNING},
                )

    def _authority_capability_rows(
        self,
        connection: sqlite3.Connection,
        *,
        batch: ObservationBatch,
        scope: RefreshScope,
    ) -> tuple[sqlite3.Row, ...]:
        if batch.action_id is not None:
            action = connection.execute(
                """
                SELECT capability_key, capability_version
                FROM adapter_actions WHERE action_id = ?
                """,
                (batch.action_id,),
            ).fetchone()
            if action is None or scope.capability_key != action["capability_key"]:
                return ()
            action_scope = connection.execute(
                """
                SELECT 1 FROM adapter_action_scopes
                WHERE action_id = ? AND system_id = ? AND target_kind = ?
                  AND target_id = ? AND object_type = ? AND facet = ?
                  AND (capability_key IS ? OR capability_key IS NULL)
                  AND coverage = ? AND field_mask_json = ?
                """,
                (batch.action_id, *_scope_columns(scope)),
            ).fetchone()
            if action_scope is None:
                return ()
            return tuple(
                connection.execute(
                    """
                    SELECT target_kinds_json, produced_facets_json, coverage_policies_json
                    FROM capability_bindings
                    WHERE connection_binding_id = ? AND capability_key = ?
                      AND capability_version = ? AND operation_class = 'observe'
                      AND coverage_policy_initialized = 1
                    """,
                    (
                        batch.connection_binding_id,
                        action["capability_key"],
                        action["capability_version"],
                    ),
                ).fetchall()
            )
        if scope.capability_key is None:
            return ()
        rows = tuple(
            connection.execute(
                """
                SELECT target_kinds_json, produced_facets_json, coverage_policies_json
                FROM capability_bindings
                WHERE connection_binding_id = ? AND capability_key = ?
                  AND operation_class = 'observe' AND enabled = 1
                  AND coverage_policy_initialized = 1
                """,
                (batch.connection_binding_id, scope.capability_key),
            ).fetchall()
        )
        return rows if len(rows) == 1 else ()

    @staticmethod
    def _capability_authorizes_scope(
        capability: sqlite3.Row,
        scope: RefreshScope,
        *,
        relationship: bool,
    ) -> bool:
        if scope.target.kind.value not in _json_value(capability["target_kinds_json"]):
            return False
        return any(
            policy.target_kind is scope.target.kind
            and policy.facet == scope.facet
            and policy.coverage is scope.coverage
            and (not relationship or AbsenceAuthority.RELATIONSHIP in policy.absence_authority)
            for policy in (
                _coverage_policy_from_value(value)
                for value in _json_value(capability["coverage_policies_json"])
            )
        )

    @staticmethod
    def _authority_target_object_id(
        connection: sqlite3.Connection,
        scope: RefreshScope,
    ) -> str | None:
        if scope.target.kind is TargetKind.OBJECT:
            row = connection.execute(
                """
                SELECT object_id FROM remote_objects
                WHERE object_id = ? AND system_id = ? AND object_type = ?
                """,
                (scope.target.target_id, scope.system_id, scope.object_type),
            ).fetchone()
            return row["object_id"] if row is not None else None
        row = connection.execute(
            """
            SELECT configured.object_id
            FROM configured_scopes AS configured
            JOIN remote_objects AS object ON object.object_id = configured.object_id
            WHERE configured.scope_id = ? AND configured.system_id = ?
              AND configured.object_type = ? AND object.system_id = configured.system_id
              AND object.object_type = configured.object_type
            """,
            (scope.target.target_id, scope.system_id, scope.object_type),
        ).fetchone()
        return row["object_id"] if row is not None else None

    def _cached_authority_capability_rows(
        self,
        connection: sqlite3.Connection,
        *,
        batch: ObservationBatch,
        scope: RefreshScope,
        cache: dict[tuple[object, ...], tuple[sqlite3.Row, ...]],
    ) -> tuple[sqlite3.Row, ...]:
        key = (batch.action_id, *_scope_columns(scope))
        if key not in cache:
            cache[key] = self._authority_capability_rows(
                connection,
                batch=batch,
                scope=scope,
            )
        return cache[key]

    def _cached_authority_target_object_id(
        self,
        connection: sqlite3.Connection,
        *,
        scope: RefreshScope,
        cache: dict[tuple[object, ...], str | None],
    ) -> str | None:
        key = _scope_columns(scope)
        if key not in cache:
            cache[key] = self._authority_target_object_id(connection, scope)
        return cache[key]

    def _facet_item_is_authorized(
        self,
        connection: sqlite3.Connection,
        *,
        batch: ObservationBatch,
        observation: FacetObservation,
        object_id: str,
        fallback_scopes: tuple[RefreshScope, ...],
        capability_cache: dict[tuple[object, ...], tuple[sqlite3.Row, ...]],
        target_cache: dict[tuple[object, ...], str | None],
        authorized_contains_locators: set[tuple[RefreshScope, tuple[object, ...]]],
        authorized_contains_objects: set[tuple[RefreshScope, str]],
    ) -> bool:
        if not self._adapter_owns_object(
            connection,
            object_id=object_id,
            adapter_key=batch.adapter_key,
        ):
            return False
        for scope in observation.authorized_by or fallback_scopes:
            for capability in self._cached_authority_capability_rows(
                connection,
                batch=batch,
                scope=scope,
                cache=capability_cache,
            ):
                if (
                    self._capability_authorizes_scope(
                        capability,
                        scope,
                        relationship=False,
                    )
                    and observation.facet in _json_value(capability["produced_facets_json"])
                    and (
                        self._cached_authority_target_object_id(
                            connection,
                            scope=scope,
                            cache=target_cache,
                        )
                        == object_id
                        or (scope, self._locator_identity_key(observation.target))
                        in authorized_contains_locators
                        or (scope, object_id) in authorized_contains_objects
                    )
                ):
                    return True
        return False

    @staticmethod
    def _locator_identity_key(locator: ObjectLocator) -> tuple[object, ...]:
        if locator.object_id is not None:
            return ("object", locator.object_type, locator.object_id)
        return (
            "external",
            locator.object_type,
            locator.source_kind,
            locator.external_key,
        )

    @staticmethod
    def _existing_locator_object_id(
        connection: sqlite3.Connection,
        *,
        system_id: str,
        locator: ObjectLocator,
    ) -> str | None:
        if locator.object_id is not None:
            row = connection.execute(
                """
                SELECT object_id FROM remote_objects
                WHERE object_id = ? AND system_id = ? AND object_type = ?
                """,
                (locator.object_id, system_id, locator.object_type),
            ).fetchone()
        else:
            row = connection.execute(
                """
                SELECT object_id FROM remote_objects
                WHERE system_id = ? AND source_kind = ? AND external_key = ?
                  AND object_type = ?
                """,
                (
                    system_id,
                    locator.source_kind,
                    locator.external_key,
                    locator.object_type,
                ),
            ).fetchone()
        return row["object_id"] if row is not None else None

    def _collection_authority_index(
        self,
        connection: sqlite3.Connection,
        *,
        batch: ObservationBatch,
        fallback_scopes: tuple[RefreshScope, ...],
        capability_cache: dict[tuple[object, ...], tuple[sqlite3.Row, ...]],
        target_cache: dict[tuple[object, ...], str | None],
        conflicting_observation_ids: frozenset[str],
    ) -> tuple[
        set[tuple[RefreshScope, tuple[object, ...]]],
        set[tuple[RefreshScope, str]],
    ]:
        locators: set[tuple[RefreshScope, tuple[object, ...]]] = set()
        objects: set[tuple[RefreshScope, str]] = set()
        locator_object_cache: dict[tuple[object, ...], str | None] = {}

        def existing_object_id(locator: ObjectLocator) -> str | None:
            key = self._locator_identity_key(locator)
            if key not in locator_object_cache:
                locator_object_cache[key] = self._existing_locator_object_id(
                    connection,
                    system_id=batch.system_id,
                    locator=locator,
                )
            return locator_object_cache[key]

        for observation in batch.relationship_observations:
            if (
                observation.predicate != "contains"
                or observation.presence is not PresenceState.PRESENT
                or observation.observation_id in conflicting_observation_ids
                or self._journal_item_status(
                    connection,
                    observation_id=observation.observation_id,
                    batch=batch,
                    item_kind="relationship",
                    item=observation,
                )
                == "conflict"
            ):
                continue
            subject_id = existing_object_id(observation.subject)
            if subject_id is None or not self._adapter_owns_object(
                connection,
                object_id=subject_id,
                adapter_key=batch.adapter_key,
            ):
                continue
            object_id = existing_object_id(observation.object)
            for scope in observation.authorized_by or fallback_scopes:
                if self._cached_authority_target_object_id(
                    connection,
                    scope=scope,
                    cache=target_cache,
                ) != subject_id or not any(
                    self._capability_authorizes_scope(
                        capability,
                        scope,
                        relationship=True,
                    )
                    for capability in self._cached_authority_capability_rows(
                        connection,
                        batch=batch,
                        scope=scope,
                        cache=capability_cache,
                    )
                ):
                    continue
                locators.add((scope, self._locator_identity_key(observation.object)))
                if object_id is not None:
                    objects.add((scope, object_id))
        return locators, objects

    def _relationship_item_is_authorized(
        self,
        connection: sqlite3.Connection,
        *,
        batch: ObservationBatch,
        declarations: tuple[CoverageDeclaration, ...],
        observation: RelationshipObservation,
        subject_id: str,
        object_id: str,
        fallback_scopes: tuple[RefreshScope, ...],
        capability_cache: dict[tuple[object, ...], tuple[sqlite3.Row, ...]],
        target_cache: dict[tuple[object, ...], str | None],
    ) -> bool:
        if observation.predicate != "contains" or not all(
            self._adapter_owns_object(
                connection,
                object_id=candidate,
                adapter_key=batch.adapter_key,
            )
            for candidate in (subject_id, object_id)
        ):
            return False
        for scope in observation.authorized_by or fallback_scopes:
            if (
                self._cached_authority_target_object_id(
                    connection,
                    scope=scope,
                    cache=target_cache,
                )
                != subject_id
            ):
                continue
            if observation.presence is PresenceState.ABSENT and not any(
                declaration.scope == scope
                and declaration.completeness is CollectionCoverage.COMPLETE
                and AbsenceAuthority.RELATIONSHIP in declaration.absence_authority
                for declaration in declarations
            ):
                continue
            if any(
                self._capability_authorizes_scope(
                    capability,
                    scope,
                    relationship=True,
                )
                for capability in self._cached_authority_capability_rows(
                    connection,
                    batch=batch,
                    scope=scope,
                    cache=capability_cache,
                )
            ):
                return True
        return False

    @staticmethod
    def _adapter_owns_object(
        connection: sqlite3.Connection,
        *,
        object_id: str,
        adapter_key: str,
    ) -> bool:
        row = connection.execute(
            "SELECT source_kind FROM remote_objects WHERE object_id = ?",
            (object_id,),
        ).fetchone()
        return row is not None and row["source_kind"].startswith(f"{adapter_key}.")

    @staticmethod
    def _item_authority_scopes(
        connection: sqlite3.Connection,
        *,
        batch: ObservationBatch,
        explicit: tuple[RefreshScope, ...],
        declarations: tuple[CoverageDeclaration, ...],
    ) -> tuple[RefreshScope, ...]:
        if explicit:
            return explicit
        if batch.action_id is not None:
            return tuple(
                _scope_from_row(row)
                for row in connection.execute(
                    "SELECT * FROM adapter_action_scopes WHERE action_id = ?",
                    (batch.action_id,),
                ).fetchall()
            )
        return tuple(declaration.scope for declaration in declarations)

    def _validate_coverage(
        self, connection: sqlite3.Connection, batch: ObservationBatch
    ) -> tuple[CoverageDeclaration, ...]:
        declarations = tuple(batch.coverage)
        action_capability = (
            connection.execute(
                """
                SELECT capability_key, capability_version
                FROM adapter_actions WHERE action_id = ?
                """,
                (batch.action_id,),
            ).fetchone()
            if batch.action_id is not None
            else None
        )
        seen: set[tuple[str, str, str, str, str, str, str]] = set()
        for declaration in declarations:
            scope = declaration.scope
            if scope.system_id != batch.system_id:
                raise ValueError("coverage_system_mismatch")
            if scope.target.kind is TargetKind.OBJECT:
                target_exists = connection.execute(
                    """
                    SELECT 1 FROM remote_objects
                    WHERE object_id = ? AND system_id = ? AND object_type = ?
                    """,
                    (scope.target.target_id, batch.system_id, scope.object_type),
                ).fetchone()
                if target_exists is None:
                    raise ValueError("coverage_object_target_unavailable")
            elif scope.target.kind is TargetKind.CONFIGURED_SCOPE:
                configured_target = connection.execute(
                    """
                    SELECT configured.object_id, configured.object_type,
                           object.system_id AS object_system_id,
                           object.object_type AS canonical_object_type
                    FROM configured_scopes AS configured
                    LEFT JOIN remote_objects AS object
                      ON object.object_id = configured.object_id
                    WHERE configured.scope_id = ? AND configured.system_id = ?
                    """,
                    (scope.target.target_id, batch.system_id),
                ).fetchone()
                if configured_target is None or (
                    configured_target["object_type"] != scope.object_type
                    or (
                        configured_target["object_id"] is not None
                        and (
                            configured_target["object_system_id"] != batch.system_id
                            or configured_target["canonical_object_type"] != scope.object_type
                        )
                    )
                ):
                    raise ValueError("coverage_configured_target_unavailable")
                if (
                    scope.facet == "membership"
                    and AbsenceAuthority.RELATIONSHIP in declaration.absence_authority
                    and configured_target["object_id"] is None
                ):
                    raise ValueError("coverage_relationship_target_unavailable")
            definition = V1_TYPE_DEFINITION_BY_KEY.get(scope.object_type)
            if definition is None or scope.facet not in {item.facet for item in definition.facets}:
                raise ValueError("coverage_unsupported_facet")
            if action_capability is not None:
                if scope.capability_key != action_capability["capability_key"]:
                    raise ValueError("coverage_action_capability_mismatch")
                capability_rows = connection.execute(
                    """
                    SELECT capability_key, target_kinds_json, produced_facets_json,
                           coverage_policies_json
                    FROM capability_bindings
                    WHERE connection_binding_id = ? AND capability_key = ?
                      AND capability_version = ? AND operation_class = 'observe'
                      AND coverage_policy_initialized = 1
                    """,
                    (
                        batch.connection_binding_id,
                        action_capability["capability_key"],
                        action_capability["capability_version"],
                    ),
                ).fetchall()
            else:
                if scope.capability_key is None:
                    raise ValueError("incidental_coverage_requires_capability")
                capability_rows = connection.execute(
                    """
                    SELECT capability_key, target_kinds_json, produced_facets_json,
                           coverage_policies_json
                    FROM capability_bindings
                    WHERE connection_binding_id = ? AND capability_key = ?
                      AND operation_class = 'observe' AND enabled = 1
                      AND coverage_policy_initialized = 1
                    """,
                    (batch.connection_binding_id, scope.capability_key),
                ).fetchall()
                if len(capability_rows) > 1:
                    raise ValueError("incidental_coverage_ambiguous_capability_version")
            matching_rows = tuple(
                row
                for row in capability_rows
                if scope.target.kind.value in _json_value(row["target_kinds_json"])
                and scope.facet in _json_value(row["produced_facets_json"])
            )
            if not matching_rows:
                raise ValueError("coverage_not_supported_by_binding")
            authorized = any(
                any(
                    policy.target_kind is scope.target.kind
                    and policy.facet == scope.facet
                    and policy.coverage is scope.coverage
                    and _COLLECTION_COVERAGE_RANK[declaration.completeness]
                    <= _COLLECTION_COVERAGE_RANK[policy.maximum_completeness]
                    and set(declaration.absence_authority).issubset(policy.absence_authority)
                    for policy in (
                        _coverage_policy_from_value(value)
                        for value in _json_value(row["coverage_policies_json"])
                    )
                )
                for row in matching_rows
            )
            if not authorized:
                raise ValueError("coverage_exceeds_capability_policy")
            columns = _scope_columns(scope)
            if columns in seen:
                raise ValueError("duplicate_coverage_declaration")
            seen.add(columns)
        return declarations

    def _coverage_contains(
        self, declarations: Iterable[CoverageDeclaration], scope: RefreshScope
    ) -> bool:
        return any(declaration.scope == scope for declaration in declarations)

    def _resolve_locator(
        self,
        connection: sqlite3.Connection,
        *,
        locator: ObjectLocator,
        system_id: str,
        observed_at: datetime,
        received_at: datetime,
        mark_present: bool = True,
    ) -> RemoteObject:
        if locator.object_id is not None:
            row = connection.execute(
                "SELECT * FROM remote_objects WHERE object_id = ? AND system_id = ?",
                (locator.object_id, system_id),
            ).fetchone()
            if row is None:
                # A discovery worker may use its configured-scope ID as the
                # root locator. Resolve that local alias before rejecting it;
                # this preserves the configured canonical root identity.
                configured = connection.execute(
                    """
                    SELECT object_id FROM configured_scopes
                    WHERE scope_id = ? AND system_id = ? AND object_type = ?
                    """,
                    (locator.object_id, system_id, locator.object_type),
                ).fetchone()
                if configured is not None and configured["object_id"] is not None:
                    row = connection.execute(
                        "SELECT * FROM remote_objects WHERE object_id = ? AND system_id = ?",
                        (configured["object_id"], system_id),
                    ).fetchone()
            if row is None:
                raise ValueError("unknown_object_locator")
            if row["object_type"] != locator.object_type:
                raise ValueError("object_locator_type_mismatch")
            previous_seen = _dt(row["last_seen_at"])
            previous_received = _dt(row["last_seen_received_at"])
            if mark_present and _received_order_allows(
                observed_at=observed_at,
                received_at=received_at,
                existing_observed_at=previous_seen,
                existing_received_at=previous_received,
            ):
                connection.execute(
                    """
                    UPDATE remote_objects
                    SET first_seen_at = MIN(first_seen_at, ?), presence = 'present',
                        last_seen_at = ?, last_seen_received_at = ?
                    WHERE object_id = ?
                    """,
                    (
                        _utc_text(observed_at),
                        _utc_text(observed_at),
                        _utc_text(received_at),
                        row["object_id"],
                    ),
                )
                row = connection.execute(
                    "SELECT * FROM remote_objects WHERE object_id = ?", (row["object_id"],)
                ).fetchone()
            return self._object_from_row(row)
        if (
            locator.source_kind is None
            or locator.external_key is None
            or locator.display_name is None
        ):
            raise ValueError("incomplete_external_object_locator")
        row = connection.execute(
            """
            SELECT * FROM remote_objects
            WHERE system_id = ? AND source_kind = ? AND external_key = ?
            """,
            (system_id, locator.source_kind, locator.external_key),
        ).fetchone()
        if row is None:
            if not mark_present:
                raise ValueError("unknown_absence_object_locator")
            object_id = str(uuid4())
            connection.execute(
                """
                INSERT INTO remote_objects (
                    object_id, system_id, object_type, object_type_version,
                    source_kind, external_key, display_name, presence,
                    first_seen_at, last_seen_at, last_seen_received_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'present', ?, ?, ?)
                """,
                (
                    object_id,
                    system_id,
                    locator.object_type,
                    locator.object_type_version,
                    locator.source_kind,
                    locator.external_key,
                    locator.display_name,
                    _utc_text(observed_at),
                    _utc_text(observed_at),
                    _utc_text(received_at),
                ),
            )
            row = connection.execute(
                "SELECT * FROM remote_objects WHERE object_id = ?", (object_id,)
            ).fetchone()
        else:
            if row["object_type"] != locator.object_type:
                raise ValueError("external_identity_type_mismatch")
            previous_seen = _dt(row["last_seen_at"])
            previous_received = _dt(row["last_seen_received_at"])
            if mark_present and _received_order_allows(
                observed_at=observed_at,
                received_at=received_at,
                existing_observed_at=previous_seen,
                existing_received_at=previous_received,
            ):
                connection.execute(
                    """
                    UPDATE remote_objects
                    SET display_name = ?, first_seen_at = MIN(first_seen_at, ?),
                        presence = 'present', last_seen_at = ?, last_seen_received_at = ?
                    WHERE object_id = ?
                    """,
                    (
                        locator.display_name,
                        _utc_text(observed_at),
                        _utc_text(observed_at),
                        _utc_text(received_at),
                        row["object_id"],
                    ),
                )
                row = connection.execute(
                    "SELECT * FROM remote_objects WHERE object_id = ?", (row["object_id"],)
                ).fetchone()
        if row is None:  # pragma: no cover - transaction invariant
            raise RuntimeError("resolved object was not found")
        return self._object_from_row(row)

    @staticmethod
    def _journal_item_json(item: object) -> str:
        item_value = item.to_dict() if hasattr(item, "to_dict") else item  # type: ignore[union-attr]
        if isinstance(item_value, dict) and not item_value.get("authorized_by"):
            item_value.pop("authorized_by", None)
        return _json_text(item_value, field_name="observation journal item")

    def _journal_item_status(
        self,
        connection: sqlite3.Connection,
        *,
        observation_id: str,
        batch: ObservationBatch,
        item_kind: str,
        item: object,
    ) -> str:
        existing = connection.execute(
            """
            SELECT batch_id, item_kind, item_json FROM observation_journal
            WHERE observation_id = ?
            """,
            (observation_id,),
        ).fetchone()
        if existing is None:
            return "new"
        item_json = self._journal_item_json(item)
        if (
            existing["batch_id"] == batch.batch_id
            and existing["item_kind"] == item_kind
            and existing["item_json"] == item_json
        ):
            return "duplicate"
        return "conflict"

    def _conflicting_batch_observation_ids(
        self,
        batch: ObservationBatch,
    ) -> frozenset[str]:
        identities_by_id: dict[str, set[tuple[str, bytes]]] = {}
        for item_kind, observations in (
            ("facet", batch.facet_observations),
            ("relationship", batch.relationship_observations),
        ):
            for observation in observations:
                digest = hashlib.sha256(self._journal_item_json(observation).encode()).digest()
                identities_by_id.setdefault(observation.observation_id, set()).add(
                    (item_kind, digest)
                )
        return frozenset(
            observation_id
            for observation_id, identities in identities_by_id.items()
            if len(identities) > 1
        )

    def _record_journal_item(
        self,
        connection: sqlite3.Connection,
        *,
        observation_id: str,
        batch: ObservationBatch,
        item_kind: str,
        item: object,
    ) -> str:
        status = self._journal_item_status(
            connection,
            observation_id=observation_id,
            batch=batch,
            item_kind=item_kind,
            item=item,
        )
        if status != "new":
            return status
        connection.execute(
            """
            INSERT INTO observation_journal (
                observation_id, batch_id, item_kind, item_json, observed_at, received_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                observation_id,
                batch.batch_id,
                item_kind,
                self._journal_item_json(item),
                _utc_text(batch.observed_at),
                _utc_text(batch.received_at),
            ),
        )
        return "recorded"

    def _record_ingestion_issue(
        self,
        connection: sqlite3.Connection,
        *,
        batch: ObservationBatch,
        item_kind: str,
        detail: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO ingestion_issues (
                issue_id, batch_id, action_id, item_kind, error_class, redacted_detail, recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid4()),
                batch.batch_id,
                batch.action_id,
                item_kind,
                ErrorClass.INVALID_DOWNSTREAM_RESPONSE.value,
                _redact(detail),
                _utc_text(_now()),
            ),
        )

    def _merge_facet_observation(
        self,
        connection: sqlite3.Connection,
        *,
        batch: ObservationBatch,
        observation: FacetObservation,
        object_id: str,
    ) -> None:
        existing = self._get_facet_row(object_id, observation.facet)
        existing_observed = _dt(existing["observed_at"]) if existing else None
        revision_order = _numeric_revision_order(
            observation.source_revision,
            existing["source_revision"] if existing else None,
        )
        if revision_order is not None and revision_order < 0:
            return
        if revision_order in {None, 0} and not _received_order_allows(
            observed_at=batch.observed_at,
            received_at=batch.received_at,
            existing_observed_at=existing_observed,
            existing_received_at=_dt(existing["received_at"]) if existing else None,
        ):
            return
        if observation.update_mode is UpdateMode.ABSENCE:
            return
        prior_payload = _json_value(existing["payload_json"]) if existing else {}
        if not isinstance(prior_payload, dict):  # pragma: no cover - migration invariant
            raise ValueError("stored facet payload is not an object")
        # Even a complete snapshot is merged field-wise in this first durable slice.
        # This is deliberately conservative: no capability contract currently grants
        # field-complete clearing authority, so omission never clears a known value.
        merged = dict(prior_payload)
        update_payload = (
            {name: observation.payload[name] for name in observation.field_mask}
            if observation.update_mode is UpdateMode.PATCH
            or observation.field_coverage is FieldCoverage.PARTIAL
            else observation.payload
        )
        merged.update(update_payload)
        changed = existing is None or merged != prior_payload
        state_changed_at = batch.observed_at if changed else _dt(existing["state_changed_at"])
        connection.execute(
            """
            INSERT INTO facets (
                object_id, facet, facet_version, knowledge, payload_json,
                observed_at, state_changed_at, supporting_observation_id,
                source_revision, received_at
            ) VALUES (?, ?, ?, 'known', ?, ?, ?, ?, ?, ?)
            ON CONFLICT(object_id, facet) DO UPDATE SET
                facet_version = excluded.facet_version,
                knowledge = excluded.knowledge,
                payload_json = excluded.payload_json,
                observed_at = excluded.observed_at,
                state_changed_at = excluded.state_changed_at,
                supporting_observation_id = excluded.supporting_observation_id,
                source_revision = excluded.source_revision,
                received_at = excluded.received_at
            """,
            (
                object_id,
                observation.facet,
                observation.facet_version,
                _json_text(merged, field_name="merged facet payload"),
                _utc_text(batch.observed_at),
                _utc_text(state_changed_at),  # type: ignore[arg-type]
                observation.observation_id,
                observation.source_revision,
                _utc_text(batch.received_at),
            ),
        )

    def _absence_is_authorized(
        self,
        declarations: Iterable[CoverageDeclaration],
        *,
        object_id: str,
        observation: FacetObservation,
    ) -> bool:
        return any(
            declaration.completeness is CollectionCoverage.COMPLETE
            and declaration.scope.target.kind is TargetKind.OBJECT
            and declaration.scope.target.target_id == object_id
            and declaration.scope.facet == observation.facet
            and any(
                authority.value == "object_presence" for authority in declaration.absence_authority
            )
            for declaration in declarations
        )

    def _merge_relationship_observation(
        self,
        connection: sqlite3.Connection,
        *,
        batch: ObservationBatch,
        observation: RelationshipObservation,
        subject_id: str,
        object_id: str,
    ) -> None:
        projected_presence = observation.presence
        projected_at = batch.observed_at
        projected_received_at = batch.received_at
        projected_support = observation.observation_id
        projected_from_watermark = False
        if observation.predicate == "contains":
            watermark = connection.execute(
                """
                SELECT observed_at, received_at, supporting_observation_id
                FROM relationship_coverage_watermarks
                WHERE system_id = ? AND subject_id = ? AND predicate = ?
                """,
                (batch.system_id, subject_id, observation.predicate),
            ).fetchone()
            if watermark is not None:
                watermark_at = _dt(watermark["observed_at"])
                if watermark_at is None:  # pragma: no cover - schema invariant
                    raise ValueError("relationship coverage watermark lacks observed time")
                watermark_received_at = _dt(watermark["received_at"]) or watermark_at
                if not _received_order_allows(
                    observed_at=batch.observed_at,
                    received_at=batch.received_at,
                    existing_observed_at=watermark_at,
                    existing_received_at=watermark_received_at,
                ):
                    projected_presence = PresenceState.ABSENT
                    projected_at = watermark_at
                    projected_received_at = watermark_received_at
                    projected_support = watermark["supporting_observation_id"]
                    projected_from_watermark = True
        existing = connection.execute(
            """
            SELECT * FROM relationships
            WHERE system_id = ? AND subject_id = ? AND predicate = ? AND object_id = ?
            """,
            (batch.system_id, subject_id, observation.predicate, object_id),
        ).fetchone()
        if (
            existing is not None
            and projected_from_watermark
            and projected_at == _dt(existing["observed_at"])
            and projected_received_at
            <= (_dt(existing["received_at"]) or _dt(existing["observed_at"]))
        ):
            return
        if existing is not None and not _received_order_allows(
            observed_at=projected_at,
            received_at=projected_received_at,
            existing_observed_at=_dt(existing["observed_at"]),
            existing_received_at=_dt(existing["received_at"]),
        ):
            return
        relationship_id = (
            existing["relationship_id"]
            if existing is not None
            else str(
                uuid5(
                    NAMESPACE_URL,
                    f"{batch.system_id}:{subject_id}:{observation.predicate}:{object_id}",
                )
            )
        )
        connection.execute(
            """
            INSERT INTO relationships (
                relationship_id, system_id, subject_id, predicate, object_id, presence, observed_at,
                supporting_observation_id, received_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(system_id, subject_id, predicate, object_id) DO UPDATE SET
                presence = excluded.presence,
                observed_at = excluded.observed_at,
                supporting_observation_id = excluded.supporting_observation_id,
                received_at = excluded.received_at
            """,
            (
                relationship_id,
                batch.system_id,
                subject_id,
                observation.predicate,
                object_id,
                projected_presence.value,
                _utc_text(projected_at),
                projected_support,
                _utc_text(projected_received_at),
            ),
        )

    def _coverage_observation_id(self, batch_id: str, scope: RefreshScope) -> str:
        return str(uuid5(NAMESPACE_URL, f"coverage:{batch_id}:{_scope_columns(scope)}"))

    @staticmethod
    def _coverage_journal_item(declaration: CoverageDeclaration) -> dict[str, object]:
        return {
            "scope": declaration.scope.to_dict(),
            "completeness": declaration.completeness.value,
            "absence_authority": [authority.value for authority in declaration.absence_authority],
        }

    def _legacy_batch_matches(
        self,
        connection: sqlite3.Connection,
        *,
        prior: sqlite3.Row,
        batch: ObservationBatch,
    ) -> bool:
        """Verify a pre-digest accepted or partial batch before backfilling."""
        try:
            prior_status = IngestionStatus(prior["status"])
        except ValueError:
            return False
        if prior_status not in {IngestionStatus.ACCEPTED, IngestionStatus.PARTIAL}:
            return False
        issue_count = prior["issue_count"]
        if not isinstance(issue_count, int) or issue_count < 0:
            return False
        if prior_status is IngestionStatus.PARTIAL and issue_count > 0:
            # Pre-0003 partial batches journaled only accepted items. Their
            # rejected-item target/body is irretrievably absent, so replay
            # equivalence cannot be proven. This is a legacy pre-release
            # safety boundary, not a general duplicate-delivery behavior.
            return False
        has_incomplete_coverage = any(
            declaration.completeness is not CollectionCoverage.COMPLETE
            for declaration in batch.coverage
        )
        expected_status = (
            IngestionStatus.PARTIAL
            if issue_count or has_incomplete_coverage
            else IngestionStatus.ACCEPTED
        )
        if (
            prior["system_id"],
            prior["connection_binding_id"],
            prior["adapter_key"],
            prior["adapter_version"],
            prior["action_id"],
            prior["observed_at"],
            prior["received_at"],
            prior_status,
            issue_count,
        ) != (
            batch.system_id,
            batch.connection_binding_id,
            batch.adapter_key,
            batch.adapter_version,
            batch.action_id,
            _utc_text(batch.observed_at),
            _utc_text(batch.received_at),
            expected_status,
            issue_count,
        ):
            return False
        try:
            accepted_ids = tuple(_json_value(prior["accepted_ids_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        if not all(isinstance(item, str) for item in accepted_ids):
            return False
        accepted_id_set = set(accepted_ids)
        input_items = [
            (item.observation_id, "facet", self._journal_item_json(item))
            for item in batch.facet_observations
        ]
        input_items.extend(
            (item.observation_id, "relationship", self._journal_item_json(item))
            for item in batch.relationship_observations
        )
        input_ids = [item[0] for item in input_items]
        if (
            len(accepted_ids) != len(accepted_id_set)
            or not accepted_id_set.issubset(input_ids)
            or len(input_items) - len(accepted_ids) != issue_count
        ):
            return False
        issue_rows = connection.execute(
            "SELECT item_kind FROM ingestion_issues WHERE batch_id = ? ORDER BY item_kind",
            (batch.batch_id,),
        ).fetchall()
        if len(issue_rows) != issue_count or [row["item_kind"] for row in issue_rows] != sorted(
            item[1] for item in input_items if item[0] not in accepted_id_set
        ):
            return False
        expected_items = [item for item in input_items if item[0] in accepted_id_set]
        if issue_count == 0:
            expected_items.extend(
                (
                    self._coverage_observation_id(batch.batch_id, declaration.scope),
                    "coverage",
                    self._journal_item_json(self._coverage_journal_item(declaration)),
                )
                for declaration in batch.coverage
                if declaration.completeness is CollectionCoverage.COMPLETE
            )
        stored_items = connection.execute(
            """
            SELECT observation_id, item_kind, item_json FROM observation_journal
            WHERE batch_id = ?
            """,
            (batch.batch_id,),
        ).fetchall()
        return sorted(expected_items) == sorted(
            (row["observation_id"], row["item_kind"], row["item_json"]) for row in stored_items
        )

    def _grant_coverage_credit(
        self,
        connection: sqlite3.Connection,
        *,
        batch: ObservationBatch,
        declaration: CoverageDeclaration,
    ) -> str:
        observation_id = self._coverage_observation_id(batch.batch_id, declaration.scope)
        journal_status = self._record_journal_item(
            connection,
            observation_id=observation_id,
            batch=batch,
            item_kind="coverage",
            item=self._coverage_journal_item(declaration),
        )
        if journal_status == "conflict":
            raise ValueError("coverage_observation_id_collision")
        connection.execute(
            """
            INSERT OR IGNORE INTO refresh_credit (
                credit_id, observation_id, system_id, target_kind, target_id, object_type, facet,
                capability_key, coverage, field_mask_json, observed_at, received_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid5(NAMESPACE_URL, f"credit:{observation_id}")),
                observation_id,
                *_scope_columns(declaration.scope),
                _utc_text(batch.observed_at),
                _utc_text(batch.received_at),
            ),
        )
        return observation_id

    def _reconcile_complete_membership(
        self,
        connection: sqlite3.Connection,
        *,
        batch: ObservationBatch,
        declaration: CoverageDeclaration,
        positive_contains: set[tuple[str, str]],
        coverage_observation_id: str,
    ) -> None:
        scope = declaration.scope
        if (
            declaration.completeness is not CollectionCoverage.COMPLETE
            or scope.facet != "membership"
            or not any(
                authority.value == "relationship" for authority in declaration.absence_authority
            )
        ):
            return
        subject_id = self.resolve_target_object_id(scope)
        if subject_id is None:
            return
        connection.execute(
            """
            INSERT INTO relationship_coverage_watermarks (
                system_id, subject_id, predicate, observed_at, supporting_observation_id,
                received_at
            ) VALUES (?, ?, 'contains', ?, ?, ?)
            ON CONFLICT(system_id, subject_id, predicate) DO UPDATE SET
                observed_at = excluded.observed_at,
                supporting_observation_id = excluded.supporting_observation_id,
                received_at = excluded.received_at
            WHERE excluded.observed_at > relationship_coverage_watermarks.observed_at
               OR (
                   excluded.observed_at = relationship_coverage_watermarks.observed_at
                   AND excluded.received_at > COALESCE(
                       relationship_coverage_watermarks.received_at,
                       relationship_coverage_watermarks.observed_at
                   )
               )
            """,
            (
                batch.system_id,
                subject_id,
                _utc_text(batch.observed_at),
                coverage_observation_id,
                _utc_text(batch.received_at),
            ),
        )
        effective_watermark = connection.execute(
            """
            SELECT supporting_observation_id FROM relationship_coverage_watermarks
            WHERE system_id = ? AND subject_id = ? AND predicate = 'contains'
            """,
            (batch.system_id, subject_id),
        ).fetchone()
        if (
            effective_watermark is None
            or effective_watermark["supporting_observation_id"] != coverage_observation_id
        ):
            return
        rows = connection.execute(
            """
            SELECT relationship_id, object_id, observed_at, received_at FROM relationships
            WHERE system_id = ? AND subject_id = ?
              AND predicate = 'contains' AND presence = 'present'
            """,
            (batch.system_id, subject_id),
        ).fetchall()
        for row in rows:
            if (subject_id, row["object_id"]) in positive_contains:
                continue
            if not _received_order_allows(
                observed_at=batch.observed_at,
                received_at=batch.received_at,
                existing_observed_at=_dt(row["observed_at"]),
                existing_received_at=_dt(row["received_at"]),
            ):
                continue
            connection.execute(
                """
                UPDATE relationships
                SET presence = 'absent', observed_at = ?, supporting_observation_id = ?,
                    received_at = ?
                WHERE relationship_id = ?
                """,
                (
                    _utc_text(batch.observed_at),
                    coverage_observation_id,
                    _utc_text(batch.received_at),
                    row["relationship_id"],
                ),
            )

    async def ingest(
        self, batch: ObservationBatch, *, lease_id: str | None = None
    ) -> IngestionResult:
        """Validate, journal, and conservatively project normalized evidence.

        The accepted journal is intentionally uncompacted in this slice.  A bad
        item becomes a redacted ingestion issue; it never clears known facts or
        enables absence reconciliation for the batch.
        """
        local_observation_time = batch.observed_at_is_local
        with self._immediate_transaction() as connection:
            prior = connection.execute(
                """
                SELECT system_id, connection_binding_id, adapter_key, adapter_version,
                       action_id, observed_at, received_at, status, accepted_ids_json,
                       issue_count, batch_digest, observed_at_is_local
                FROM observation_batches WHERE batch_id = ?
                """,
                (batch.batch_id,),
            ).fetchone()
            if prior is not None and bool(prior["observed_at_is_local"]) != local_observation_time:
                return IngestionResult(batch_id=batch.batch_id, status=IngestionStatus.REJECTED)
            if prior is not None:
                received_at = _dt(prior["received_at"])
                observed_at = (
                    _dt(prior["observed_at"]) if local_observation_time else batch.observed_at
                )
            else:
                if local_observation_time:
                    received_at, _latest_receipt = self._next_receipt_time(connection)
                    observed_at = received_at
                else:
                    observed_at = batch.observed_at
                    received_at = batch.received_at
            if received_at is None:
                raise ValueError("stored batch receipt time is unavailable")
            if observed_at is None:
                raise ValueError("stored batch observation time is unavailable")
            batch = replace(batch, observed_at=observed_at, received_at=received_at)
            try:
                batch_digest = _batch_digest(batch)
            except ValueError:
                return IngestionResult(batch_id=batch.batch_id, status=IngestionStatus.REJECTED)
            if prior is not None:
                if prior["batch_digest"] == batch_digest:
                    return IngestionResult(
                        batch_id=batch.batch_id,
                        status=IngestionStatus.DUPLICATE,
                        accepted_observation_ids=tuple(_json_value(prior["accepted_ids_json"])),
                        issue_count=prior["issue_count"],
                    )
                if prior["batch_digest"] == "":
                    try:
                        self._validate_batch_context(
                            connection,
                            batch,
                            lease_id=None,
                            require_action_lease=False,
                        )
                        self._validate_coverage(connection, batch)
                    except ValueError:
                        return IngestionResult(
                            batch_id=batch.batch_id, status=IngestionStatus.REJECTED
                        )
                    if self._legacy_batch_matches(connection, prior=prior, batch=batch):
                        connection.execute(
                            """
                            UPDATE observation_batches
                            SET batch_digest = ?
                            WHERE batch_id = ? AND batch_digest = ''
                            """,
                            (batch_digest, batch.batch_id),
                        )
                        return IngestionResult(
                            batch_id=batch.batch_id,
                            status=IngestionStatus.DUPLICATE,
                            accepted_observation_ids=tuple(_json_value(prior["accepted_ids_json"])),
                            issue_count=prior["issue_count"],
                        )
                return IngestionResult(batch_id=batch.batch_id, status=IngestionStatus.REJECTED)
            try:
                self._validate_batch_context(connection, batch, lease_id=lease_id)
                declarations = self._validate_coverage(connection, batch)
            except ValueError:
                return IngestionResult(batch_id=batch.batch_id, status=IngestionStatus.REJECTED)
            connection.execute(
                """
                INSERT INTO observation_batches (
                    batch_id, system_id, connection_binding_id, adapter_key,
                    adapter_version, action_id, observed_at, received_at,
                    status, accepted_ids_json, issue_count, batch_digest,
                    observed_at_is_local
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'accepted', '[]', 0, ?, ?)
                """,
                (
                    batch.batch_id,
                    batch.system_id,
                    batch.connection_binding_id,
                    batch.adapter_key,
                    batch.adapter_version,
                    batch.action_id,
                    _utc_text(batch.observed_at),
                    _utc_text(batch.received_at),
                    batch_digest,
                    int(batch.observed_at_is_local),
                ),
            )
            facet_accepted_ids: list[str] = []
            relationship_accepted_ids: list[str] = []
            issue_count = 0
            positive_contains: set[tuple[str, str]] = set()
            fallback_authority_scopes = self._item_authority_scopes(
                connection,
                batch=batch,
                explicit=(),
                declarations=declarations,
            )
            authority_capability_cache: dict[tuple[object, ...], tuple[sqlite3.Row, ...]] = {}
            authority_target_cache: dict[tuple[object, ...], str | None] = {}
            conflicting_observation_ids = self._conflicting_batch_observation_ids(batch)
            authorized_contains_locators, authorized_contains_objects = (
                self._collection_authority_index(
                    connection,
                    batch=batch,
                    fallback_scopes=fallback_authority_scopes,
                    capability_cache=authority_capability_cache,
                    target_cache=authority_target_cache,
                    conflicting_observation_ids=conflicting_observation_ids,
                )
            )
            for observation in batch.facet_observations:
                try:
                    with self._ingestion_item_savepoint(connection):
                        if observation.observation_id in conflicting_observation_ids:
                            raise ValueError("observation_id_collision")
                        definition = V1_TYPE_DEFINITION_BY_KEY.get(observation.target.object_type)
                        if definition is None or observation.facet not in {
                            item.facet for item in definition.facets
                        }:
                            raise ValueError("unsupported_facet_observation")
                        if any(
                            satisfaction.system_id != batch.system_id
                            or not self._coverage_contains(declarations, satisfaction)
                            for satisfaction in observation.satisfies
                        ):
                            raise ValueError("undeclared_freshness_claim")
                        journal_status = self._journal_item_status(
                            connection,
                            observation_id=observation.observation_id,
                            batch=batch,
                            item_kind="facet",
                            item=observation,
                        )
                        if journal_status == "duplicate":
                            continue
                        if journal_status == "conflict":
                            raise ValueError("observation_id_collision")
                        target = self._resolve_locator(
                            connection,
                            locator=observation.target,
                            system_id=batch.system_id,
                            observed_at=batch.observed_at,
                            received_at=batch.received_at,
                            mark_present=observation.update_mode is not UpdateMode.ABSENCE,
                        )
                        if not self._facet_item_is_authorized(
                            connection,
                            batch=batch,
                            observation=observation,
                            object_id=target.object_id,
                            fallback_scopes=fallback_authority_scopes,
                            capability_cache=authority_capability_cache,
                            target_cache=authority_target_cache,
                            authorized_contains_locators=authorized_contains_locators,
                            authorized_contains_objects=authorized_contains_objects,
                        ):
                            raise ValueError("facet_observation_not_authorized")
                        recorded_status = self._record_journal_item(
                            connection,
                            observation_id=observation.observation_id,
                            batch=batch,
                            item_kind="facet",
                            item=observation,
                        )
                        if recorded_status != "recorded":  # pragma: no cover - invariant
                            raise RuntimeError(
                                "observation journal status changed during ingestion"
                            )
                        if observation.update_mode is UpdateMode.ABSENCE:
                            if not self._absence_is_authorized(
                                declarations,
                                object_id=target.object_id,
                                observation=observation,
                            ):
                                raise ValueError("unauthorized_object_absence")
                            connection.execute(
                                """
                                UPDATE remote_objects
                                SET presence = 'absent', last_seen_at = ?,
                                    last_seen_received_at = ?
                                WHERE object_id = ?
                                  AND (
                                      last_seen_at IS NULL OR last_seen_at < ?
                                      OR (
                                          last_seen_at = ?
                                          AND COALESCE(last_seen_received_at, last_seen_at) < ?
                                      )
                                  )
                                """,
                                (
                                    _utc_text(batch.observed_at),
                                    _utc_text(batch.received_at),
                                    target.object_id,
                                    _utc_text(batch.observed_at),
                                    _utc_text(batch.observed_at),
                                    _utc_text(batch.received_at),
                                ),
                            )
                        else:
                            self._merge_facet_observation(
                                connection,
                                batch=batch,
                                observation=observation,
                                object_id=target.object_id,
                            )
                        facet_accepted_ids.append(observation.observation_id)
                except ValueError as error:
                    issue_count += 1
                    self._record_ingestion_issue(
                        connection,
                        batch=batch,
                        item_kind="facet",
                        detail=str(error),
                    )
            for observation in batch.relationship_observations:
                try:
                    with self._ingestion_item_savepoint(connection):
                        if observation.observation_id in conflicting_observation_ids:
                            raise ValueError("observation_id_collision")
                        journal_status = self._journal_item_status(
                            connection,
                            observation_id=observation.observation_id,
                            batch=batch,
                            item_kind="relationship",
                            item=observation,
                        )
                        if journal_status == "duplicate":
                            continue
                        if journal_status == "conflict":
                            raise ValueError("observation_id_collision")
                        subject = self._resolve_locator(
                            connection,
                            locator=observation.subject,
                            system_id=batch.system_id,
                            observed_at=batch.observed_at,
                            received_at=batch.received_at,
                        )
                        object_value = self._resolve_locator(
                            connection,
                            locator=observation.object,
                            system_id=batch.system_id,
                            observed_at=batch.observed_at,
                            received_at=batch.received_at,
                        )
                        if not self._relationship_item_is_authorized(
                            connection,
                            batch=batch,
                            declarations=declarations,
                            observation=observation,
                            subject_id=subject.object_id,
                            object_id=object_value.object_id,
                            fallback_scopes=fallback_authority_scopes,
                            capability_cache=authority_capability_cache,
                            target_cache=authority_target_cache,
                        ):
                            raise ValueError("relationship_observation_not_authorized")
                        recorded_status = self._record_journal_item(
                            connection,
                            observation_id=observation.observation_id,
                            batch=batch,
                            item_kind="relationship",
                            item=observation,
                        )
                        if recorded_status != "recorded":  # pragma: no cover - invariant
                            raise RuntimeError(
                                "observation journal status changed during ingestion"
                            )
                        self._merge_relationship_observation(
                            connection,
                            batch=batch,
                            observation=observation,
                            subject_id=subject.object_id,
                            object_id=object_value.object_id,
                        )
                        if (
                            observation.predicate == "contains"
                            and observation.presence is PresenceState.PRESENT
                        ):
                            positive_contains.add((subject.object_id, object_value.object_id))
                        relationship_accepted_ids.append(observation.observation_id)
                except ValueError as error:
                    issue_count += 1
                    self._record_ingestion_issue(
                        connection,
                        batch=batch,
                        item_kind="relationship",
                        detail=str(error),
                    )
            coverage_observations: list[tuple[CoverageDeclaration, str]] = []
            if issue_count == 0:
                for declaration in declarations:
                    if declaration.completeness is not CollectionCoverage.COMPLETE:
                        continue
                    coverage_observations.append(
                        (
                            declaration,
                            self._grant_coverage_credit(
                                connection, batch=batch, declaration=declaration
                            ),
                        )
                    )
                for declaration, coverage_observation_id in coverage_observations:
                    self._reconcile_complete_membership(
                        connection,
                        batch=batch,
                        declaration=declaration,
                        positive_contains=positive_contains,
                        coverage_observation_id=coverage_observation_id,
                    )
            has_incomplete_coverage = any(
                declaration.completeness is not CollectionCoverage.COMPLETE
                for declaration in declarations
            )
            status = (
                IngestionStatus.PARTIAL
                if issue_count or has_incomplete_coverage
                else IngestionStatus.ACCEPTED
            )
            accepted_ids = facet_accepted_ids + relationship_accepted_ids
            connection.execute(
                """
                UPDATE observation_batches
                SET status = ?, accepted_ids_json = ?, issue_count = ?
                WHERE batch_id = ?
                """,
                (
                    status.value,
                    _json_text(accepted_ids, field_name="accepted observation IDs"),
                    issue_count,
                    batch.batch_id,
                ),
            )
        return IngestionResult(
            batch_id=batch.batch_id,
            status=status,
            accepted_observation_ids=tuple(accepted_ids),
            issue_count=issue_count,
        )
