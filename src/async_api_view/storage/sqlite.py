"""Single-file SQLite implementation of the local durable core.

The class intentionally has no adapter imports and no network surface.  SQL is
static and all data values are bound parameters.  Long-lived work is represented
by leases; the transactions that claim a lease or elect an active action use
``BEGIN IMMEDIATE`` so that separate local processes cannot admit duplicates.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
import time
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

from async_api_view.contracts import (
    ActionAttempt,
    ActionCompletion,
    ActionLease,
    ActionOutcome,
    ActionState,
    AdapterAction,
    CapabilityBinding,
    CollectionCoverage,
    ConnectionBinding,
    CoverageDeclaration,
    ErrorClass,
    FacetObservation,
    FacetState,
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
    RefreshReceipt,
    RefreshScope,
    RelationshipObservation,
    RelationshipState,
    RemoteObject,
    ScopePolicyState,
    TargetKind,
    TargetRef,
    UpdateMode,
)
from async_api_view.contracts._validation import (
    require_contract_key,
    require_text,
    require_utc,
    require_uuid,
    validate_json,
)
from async_api_view.contracts.defaults import V1_TYPE_DEFINITION_BY_KEY
from async_api_view.core import decide_refresh, resolve_refresh_interval, scope_covers

from .models import (
    ConfiguredScopeRecord,
    IntentScopeRecord,
    IntentScopeWork,
    OperationalEventRecord,
    RelatedObjectRecord,
    StoredAction,
    SystemRecord,
)

_MIGRATIONS_DIR = Path(__file__).with_name("migrations")
_DEFAULT_LEASE = timedelta(seconds=60)
_MAX_JSON_BYTES = 1_048_576
_MAX_DIAGNOSTIC_LENGTH = 1_024
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
_SECRET_SETTING_PART = re.compile(
    r"(?:password|secret|token|private[_-]?key|authorization|access[_-]?key)", re.IGNORECASE
)
_DIAGNOSTIC_SECRET = re.compile(
    r"(?i)\b(token|password|secret|authorization|profile|host)\b\s*(?:=|:)\s*[^\s,;]+"
)
_BEARER_SECRET = re.compile(r"(?i)\bbearer\s+[^\s,;]+")
_JSON_SECRET = re.compile(
    r'(?i)(["\']?(?:token|password|secret|authorization|access[_-]?key|private[_-]?key)'
    r'["\']?\s*:\s*)(?:"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|[^,}\]\s]+)'
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
_ALERT_SEVERITIES = frozenset({"info", "warning", "error", "critical"})
_CAPABILITY_KEY_ALTER_TABLES = {
    "ALTER TABLE refresh_credit ADD COLUMN capability_key TEXT;": "refresh_credit",
    "ALTER TABLE refresh_intent_scopes ADD COLUMN capability_key TEXT;": "refresh_intent_scopes",
    "ALTER TABLE adapter_action_scopes ADD COLUMN capability_key TEXT;": "adapter_action_scopes",
}


def _utc_text(value: datetime) -> str:
    return require_utc(value, "timestamp").isoformat(timespec="microseconds").replace("+00:00", "Z")


def _dt(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return require_utc(parsed, "stored timestamp")


def _now() -> datetime:
    return datetime.now(UTC)


def _object_search_pattern(query: str) -> str | None:
    if not query:
        return None
    require_text(query, "object_query", max_length=128)
    escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def _json_text(value: object, *, field_name: str) -> str:
    validated = validate_json(value, field_name)
    encoded = json.dumps(validated, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    if len(encoded.encode("utf-8")) > _MAX_JSON_BYTES:
        raise ValueError(f"{field_name} exceeds {_MAX_JSON_BYTES} bytes")
    return encoded


def _json_value(value: str) -> object:
    return json.loads(value)


def _batch_digest(batch: ObservationBatch) -> str:
    """Bind batch identity to its complete canonical envelope without storing raw input."""
    material = _json_text(batch.to_dict(), field_name="observation batch")
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _redact(value: str | None) -> str:
    if not value:
        return "no diagnostic supplied"
    cleaned = value.replace("\x00", "").replace("\r", " ").replace("\n", " ")
    cleaned = _BEARER_SECRET.sub("Bearer <redacted>", cleaned)
    cleaned = _JSON_SECRET.sub(lambda match: f'{match.group(1)}"<redacted>"', cleaned)
    cleaned = _DIAGNOSTIC_SECRET.sub(lambda match: f"{match.group(1)}=<redacted>", cleaned)
    cleaned = _WINDOWS_HOME_PATH.sub("<redacted-path>", cleaned)
    cleaned = _POSIX_HOME_PATH.sub("<redacted-path>", cleaned)
    return cleaned[:_MAX_DIAGNOSTIC_LENGTH]


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

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(
            self.database_path,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        try:
            with self._lock:
                self._connection.execute("PRAGMA foreign_keys = ON")
                self._connection.execute("PRAGMA busy_timeout = 5000")
                self._enable_wal_mode()
                self._connection.execute("PRAGMA synchronous = FULL")
            self._migrate()
        except BaseException:
            self._connection.close()
            raise

    def _enable_wal_mode(self) -> None:
        for attempt in range(8):
            try:
                self._connection.execute("PRAGMA journal_mode = WAL")
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or attempt == 7:
                    raise
                time.sleep(min(0.25, 0.01 * (2**attempt)))

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> SQLiteStore:
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.close()

    @contextmanager
    def _immediate_transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
            except BaseException:
                self._connection.rollback()
                raise
            else:
                self._connection.commit()

    def _migrate(self) -> None:
        with self._lock:
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
                self._connection.execute("BEGIN IMMEDIATE")
                try:
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
                except BaseException:
                    self._connection.rollback()
                    raise
                else:
                    self._connection.commit()

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
        require_contract_key(system_kind, "system_kind")
        require_text(config_id, "config_id", max_length=128)
        require_text(authority_key, "authority_key", max_length=2048)
        with self._lock:
            row = self._connection.execute(
                """
                SELECT system_id
                FROM configured_system_identities
                WHERE system_kind = ? AND config_id = ? AND authority_key = ?
                """,
                (system_kind, config_id, authority_key),
            ).fetchone()
        return row["system_id"] if row is not None else None

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
        require_text(config_id, "config_id", max_length=128)
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
            connection.execute(
                """
                INSERT INTO configured_system_identities (
                    system_kind, config_id, authority_key, system_id,
                    record_created_at, record_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(system_kind, config_id, authority_key) DO UPDATE SET
                    system_id = excluded.system_id,
                    record_updated_at = excluded.record_updated_at
                """,
                (system_kind, config_id, authority_key, system_id, timestamp, timestamp),
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
                "WHERE system_id = ? AND system_kind = ?",
                ((timestamp, value, system_kind) for value in desired_systems),
            )
            connection.executemany(
                "UPDATE connection_bindings SET enabled = 1, record_updated_at = ? "
                "WHERE binding_id = ? AND system_id IN ("
                "SELECT system_id FROM systems WHERE system_kind = ?)",
                ((timestamp, value, system_kind) for value in desired_bindings),
            )
            connection.executemany(
                "UPDATE capability_bindings SET enabled = 1, record_updated_at = ? "
                "WHERE capability_binding_id = ? AND connection_binding_id IN ("
                "SELECT binding_id FROM connection_bindings WHERE system_id IN ("
                "SELECT system_id FROM systems WHERE system_kind = ?))",
                ((timestamp, value, system_kind) for value in desired_capabilities),
            )
            connection.executemany(
                "UPDATE configured_scopes SET enabled = 1, record_updated_at = ? "
                "WHERE scope_id = ? AND system_id IN ("
                "SELECT system_id FROM systems WHERE system_kind = ?)",
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
        with self._immediate_transaction() as connection:
            if (
                connection.execute(
                    "SELECT 1 FROM connection_bindings WHERE binding_id = ?",
                    (capability.connection_binding_id,),
                ).fetchone()
                is None
            ):
                raise ValueError("capability binding references an unknown connection binding")
            connection.execute(
                """
                INSERT INTO capability_bindings (
                    capability_binding_id, connection_binding_id, capability_key,
                    capability_version, operation_class, target_kinds_json,
                    produced_facets_json, enabled, selection_priority,
                    collateral_effects_json, mitigations_json, record_created_at,
                    record_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(capability_binding_id) DO UPDATE SET
                    connection_binding_id = excluded.connection_binding_id,
                    capability_key = excluded.capability_key,
                    capability_version = excluded.capability_version,
                    operation_class = excluded.operation_class,
                    target_kinds_json = excluded.target_kinds_json,
                    produced_facets_json = excluded.produced_facets_json,
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
                    _json_text(
                        [item.value for item in capability.target_kinds], field_name="target_kinds"
                    ),
                    _json_text(list(capability.produced_facets), field_name="produced_facets"),
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
            connection.execute(
                """
                INSERT INTO refresh_overrides (
                    level, scope_id, facet, interval_seconds, record_updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(level, scope_id, facet) DO UPDATE SET
                    interval_seconds = excluded.interval_seconds,
                    record_updated_at = excluded.record_updated_at
                """,
                (
                    override.level,
                    override.scope_id,
                    override.facet,
                    interval_seconds,
                    _utc_text(now or _now()),
                ),
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
                        object_type, facet, capability_key, coverage, field_mask_json, state
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued')
                    """,
                    (scope_id, intent.intent_id, *_scope_columns(scope)),
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
            origin=row["origin"],
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

    async def lease_next_intent_scope(
        self,
        *,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta = _DEFAULT_LEASE,
    ) -> IntentScopeWork | None:
        require_text(worker_id, "worker_id", max_length=256)
        now = require_utc(now, "now")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        now_text = _utc_text(now)
        leased_until = now + lease_duration
        with self._immediate_transaction() as connection:
            row = connection.execute(
                """
                SELECT scope.*, intent.idempotency_key, intent.origin, intent.actor_id,
                       intent.ui_session_id, intent.requested_at, intent.expires_at,
                       intent.priority, intent.contract_version
                FROM refresh_intent_scopes AS scope
                JOIN refresh_intents AS intent ON intent.intent_id = scope.intent_id
                WHERE scope.state = 'queued'
                   OR (
                       scope.state = 'deferred'
                       AND (scope.eligible_at IS NULL OR scope.eligible_at <= ?)
                   )
                   OR (scope.state = 'leased' AND scope.leased_until <= ?)
                ORDER BY intent.priority DESC, intent.requested_at, scope.intent_scope_id
                LIMIT 1
                """,
                (now_text, now_text),
            ).fetchone()
            if row is None:
                return None
            lease_id = str(uuid4())
            result = connection.execute(
                """
                UPDATE refresh_intent_scopes
                SET state = 'leased', lease_id = ?, lease_worker_id = ?, leased_until = ?
                WHERE intent_scope_id = ?
                  AND (state = 'queued'
                       OR (state = 'deferred' AND (eligible_at IS NULL OR eligible_at <= ?))
                       OR (state = 'leased' AND leased_until <= ?))
                """,
                (
                    lease_id,
                    worker_id,
                    _utc_text(leased_until),
                    row["intent_scope_id"],
                    now_text,
                    now_text,
                ),
            )
            if (
                result.rowcount != 1
            ):  # pragma: no cover - lock and transaction make this unreachable
                return None
            intent = RefreshIntent(
                intent_id=row["intent_id"],
                idempotency_key=row["idempotency_key"],
                origin=row["origin"],
                actor_id=row["actor_id"],
                scopes=(_scope_from_row(row),),
                requested_at=_dt(row["requested_at"]),  # type: ignore[arg-type]
                ui_session_id=row["ui_session_id"],
                expires_at=_dt(row["expires_at"]),
                priority=row["priority"],
                contract_version=row["contract_version"],
            )
        return IntentScopeWork(
            intent_scope_id=row["intent_scope_id"],
            intent=intent,
            scope=_scope_from_row(row),
            state=IntentScopeState.LEASED,
            lease_id=lease_id,
            leased_until=leased_until,
        )

    def _claim_is_current(
        self, connection: sqlite3.Connection, *, intent_scope_id: str, lease_id: str
    ) -> bool:
        return (
            connection.execute(
                """
                SELECT 1 FROM refresh_intent_scopes
                WHERE intent_scope_id = ? AND state = 'leased' AND lease_id = ?
                """,
                (intent_scope_id, lease_id),
            ).fetchone()
            is not None
        )

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
    ) -> None:
        if state is IntentScopeState.LEASED:
            raise ValueError("leased is not a final coordinator disposition")
        require_contract_key(reason, "reason")
        if action_id is not None:
            action_id = require_uuid(action_id, "action_id")
        if observation_id is not None:
            observation_id = require_uuid(observation_id, "observation_id")
        with self._immediate_transaction() as connection:
            if not self._claim_is_current(
                connection, intent_scope_id=intent_scope_id, lease_id=lease_id
            ):
                raise ValueError("intent scope lease is no longer current")
            connection.execute(
                """
                UPDATE refresh_intent_scopes
                SET state = ?, disposition_reason = ?, eligible_at = ?, linked_action_id = ?,
                    satisfying_observation_id = ?, lease_id = NULL,
                    lease_worker_id = NULL, leased_until = NULL
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
                ORDER BY observed_at DESC, credit_id DESC
                """,
                base[:7],
            ).fetchall()
        for row in rows:
            evidence_scope = _scope_from_row(row)
            if scope_covers(evidence_scope, scope):
                return QualifyingObservation(
                    observation_id=row["observation_id"],
                    scope=evidence_scope,
                    observed_at=_dt(row["observed_at"]),  # type: ignore[arg-type]
                )
        return None

    def scope_policy_state(self, scope: RefreshScope) -> ScopePolicyState:
        evidence = self.latest_qualifying_observation(scope)
        columns = _scope_columns(scope)
        with self._lock:
            row = self._connection.execute(
                """
                SELECT MAX(action.started_at) AS started_at
                FROM adapter_actions AS action
                JOIN adapter_action_scopes AS scope ON scope.action_id = action.action_id
                WHERE scope.system_id = ? AND scope.target_kind = ? AND scope.target_id = ?
                  AND scope.object_type = ? AND scope.facet = ? AND scope.capability_key IS ?
                  AND scope.coverage = ?
                  AND scope.field_mask_json = ? AND action.started_at IS NOT NULL
                """,
                columns,
            ).fetchone()
        return ScopePolicyState(
            scope=scope,
            latest_qualifying_observation_at=evidence.observed_at if evidence else None,
            latest_targeted_action_started_at=_dt(row["started_at"]) if row else None,
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
    ) -> None:
        if not self._claim_is_current(
            connection, intent_scope_id=intent_scope_id, lease_id=lease_id
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
    ) -> tuple[AdapterAction, bool]:
        """Atomically elect one active dedupe winner and attach this intent scope."""
        dedupe_key = action_dedupe_key(
            system_id=work.scope.system_id,
            connection_binding_id=binding.binding_id,
            capability_key=capability.capability_key,
            capability_version=capability.capability_version,
            scope=work.scope,
        )
        action = AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=work.scope.system_id,
            connection_binding_id=binding.binding_id,
            adapter_key=binding.adapter_key,
            adapter_version=binding.adapter_version,
            capability_key=capability.capability_key,
            capability_version=capability.capability_version,
            target=work.scope.target,
            requested_scopes=(work.scope,),
            deadline=work.intent.expires_at,
        )
        with self._immediate_transaction() as connection:
            existing = self._active_action_by_dedupe(connection, dedupe_key)
            if existing is not None:
                self._attach_scope_to_action(
                    connection,
                    intent_scope_id=work.intent_scope_id,
                    lease_id=work.lease_id,
                    action_id=existing["action_id"],
                    state=IntentScopeState.COALESCED,
                )
                return self._action_from_row(existing), False
            try:
                self._insert_action(
                    connection, action=action, dedupe_key=dedupe_key, created_at=now
                )
            except sqlite3.IntegrityError:
                existing = self._active_action_by_dedupe(connection, dedupe_key)
                if existing is None:
                    raise
                self._attach_scope_to_action(
                    connection,
                    intent_scope_id=work.intent_scope_id,
                    lease_id=work.lease_id,
                    action_id=existing["action_id"],
                    state=IntentScopeState.COALESCED,
                )
                return self._action_from_row(existing), False
            self._attach_scope_to_action(
                connection,
                intent_scope_id=work.intent_scope_id,
                lease_id=work.lease_id,
                action_id=action.action_id,
                state=IntentScopeState.ADMITTED,
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

    async def lease_next(
        self, *, adapter_key: str, worker_id: str, now: datetime
    ) -> ActionLease | None:
        require_contract_key(adapter_key, "adapter_key")
        require_text(worker_id, "worker_id", max_length=256)
        now = require_utc(now, "now")
        lease_id = str(uuid4())
        leased_until = now + _DEFAULT_LEASE
        now_text = _utc_text(now)
        with self._immediate_transaction() as connection:
            row = connection.execute(
                """
                SELECT * FROM adapter_actions
                WHERE adapter_key = ?
                  AND (
                      state = 'ready'
                      OR (state = 'leased' AND leased_until <= ?)
                      OR (state = 'running' AND leased_until <= ?)
                      OR (state = 'retry_wait' AND retry_at <= ?)
                  )
                ORDER BY record_created_at, action_id
                LIMIT 1
                """,
                (adapter_key, now_text, now_text, now_text),
            ).fetchone()
            if row is None:
                return None
            result = connection.execute(
                """
                UPDATE adapter_actions
                SET state = 'leased', lease_id = ?, lease_worker_id = ?, leased_until = ?
                WHERE action_id = ?
                  AND (
                      state = 'ready'
                      OR (state = 'leased' AND leased_until <= ?)
                      OR (state = 'running' AND leased_until <= ?)
                      OR (state = 'retry_wait' AND retry_at <= ?)
                  )
                """,
                (
                    lease_id,
                    worker_id,
                    _utc_text(leased_until),
                    row["action_id"],
                    now_text,
                    now_text,
                    now_text,
                ),
            )
            if result.rowcount != 1:  # pragma: no cover - transaction invariant
                return None
            action = self._action_from_row(row)
        return ActionLease(action=action, lease_id=lease_id, leased_until=leased_until)

    async def mark_running(self, *, action_id: str, lease_id: str, started_at: datetime) -> None:
        action_id = require_uuid(action_id, "action_id")
        lease_id = require_uuid(lease_id, "lease_id")
        started_at = require_utc(started_at, "started_at")
        with self._immediate_transaction() as connection:
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
                or _dt(row["leased_until"]) < started_at
            ):
                raise ValueError("adapter action lease is not valid for logical start")
            connection.execute(
                """
                UPDATE adapter_actions
                SET state = 'running', started_at = COALESCE(started_at, ?), error_class = NULL,
                    redacted_diagnostic = NULL
                WHERE action_id = ?
                """,
                (_utc_text(started_at), action_id),
            )

    def _require_live_action_lease(
        self,
        connection: sqlite3.Connection,
        *,
        action_id: str,
        lease_id: str,
        allowed_states: set[ActionState],
        at: datetime,
    ) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT * FROM adapter_actions
            WHERE action_id = ? AND lease_id = ?
            """,
            (action_id, lease_id),
        ).fetchone()
        if row is None:
            raise ValueError("adapter action lease is no longer current")
        if ActionState(row["state"]) not in allowed_states:
            raise ValueError("adapter action is not in a lease-authorized state")
        leased_until = _dt(row["leased_until"])
        if leased_until is None or leased_until < at:
            raise ValueError("adapter action lease has expired")
        return row

    async def record_attempt(self, attempt: ActionAttempt, *, lease_id: str) -> None:
        lease_id = require_uuid(lease_id, "lease_id")
        attempt_at = attempt.ended_at or attempt.started_at
        with self._immediate_transaction() as connection:
            self._require_live_action_lease(
                connection,
                action_id=attempt.action_id,
                lease_id=lease_id,
                allowed_states={ActionState.RUNNING},
                at=attempt_at,
            )
            connection.execute(
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
                    _redact(attempt.redacted_diagnostic),
                ),
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
        at = require_utc(at, "at")
        with self._immediate_transaction() as connection:
            result = connection.execute(
                """
                UPDATE adapter_actions
                SET leased_until = ?
                WHERE action_id = ? AND lease_id = ? AND lease_worker_id = ?
                  AND state IN ('leased', 'running', 'retry_wait')
                  AND leased_until >= ?
                """,
                (_utc_text(at + _DEFAULT_LEASE), action_id, lease_id, worker_id, _utc_text(at)),
            )
            if result.rowcount != 1:
                raise ValueError("adapter action lease is no longer current")

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
        error_class: str | None,
        summary: str | None,
        occurred_at: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT OR IGNORE INTO operational_events (
                event_id, idempotency_key, event_type, severity, alertable, system_id, action_id,
                error_class, redacted_summary, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid4()),
                require_text(idempotency_key, "event idempotency_key", max_length=512),
                require_contract_key(event_type, "event_type"),
                require_contract_key(severity, "severity"),
                int(alertable),
                system_id,
                action_id,
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
                at=completion.completed_at,
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
                    _redact(completion.redacted_diagnostic),
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
        self, connection: sqlite3.Connection, *, action_id: str, reason: str
    ) -> GuardDecision:
        connection.execute(
            """
            UPDATE adapter_actions
            SET state = 'cancelled', completed_at = ?, lease_id = NULL, lease_worker_id = NULL,
                leased_until = NULL, error_class = ?, redacted_diagnostic = ?
            WHERE action_id = ?
            """,
            (_utc_text(_now()), ErrorClass.LOCAL_CANCELLATION.value, _redact(reason), action_id),
        )
        self._refresh_action_parent_aggregates(connection, action_id=action_id)
        return GuardDecision(GuardDisposition.CANCEL, reason)

    async def evaluate(self, *, action_id: str, lease_id: str, now: datetime) -> GuardDecision:
        """Run the generic local pre-dispatch guard against a current lease."""
        action_id = require_uuid(action_id, "action_id")
        lease_id = require_uuid(lease_id, "lease_id")
        now = require_utc(now, "now")
        with self._immediate_transaction() as connection:
            row = connection.execute(
                """
                SELECT action.*, system.enabled AS system_enabled,
                       binding.enabled AS binding_enabled,
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
            if _dt(row["leased_until"]) is None or _dt(row["leased_until"]) < now:
                return GuardDecision(GuardDisposition.FAIL, "expired_action_lease")
            if not row["system_enabled"]:
                return self._guard_cancel(connection, action_id=action_id, reason="system_disabled")
            if not row["binding_enabled"]:
                return self._guard_cancel(
                    connection, action_id=action_id, reason="binding_disabled"
                )
            if row["capability_enabled"] is None:
                return self._guard_cancel(
                    connection, action_id=action_id, reason="capability_missing"
                )
            if not row["capability_enabled"]:
                return self._guard_cancel(
                    connection, action_id=action_id, reason="capability_disabled"
                )
            if row["operation_class"] != OperationClass.OBSERVE.value:
                return GuardDecision(GuardDisposition.FAIL, "capability_not_observe")
            active_scopes = connection.execute(
                """
                SELECT scope.*, intent.requested_at
                FROM action_intent_scopes AS link
                JOIN refresh_intent_scopes AS scope ON scope.intent_scope_id = link.intent_scope_id
                JOIN refresh_intents AS intent ON intent.intent_id = scope.intent_id
                WHERE link.action_id = ? AND scope.state IN ('admitted', 'coalesced')
                """,
                (action_id,),
            ).fetchall()
            if not active_scopes:
                return self._guard_cancel(
                    connection, action_id=action_id, reason="no_live_originating_scope"
                )
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
                            connection, action_id=action_id, reason="target_not_available"
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
                            connection, action_id=action_id, reason="configured_scope_disabled"
                        )
                try:
                    interval = self.effective_interval(scope)
                except ValueError:
                    return GuardDecision(GuardDisposition.FAIL, "invalid_scope_policy")
                evidence = self.latest_qualifying_observation(scope)
                decision = decide_refresh(
                    requested_scope=scope,
                    requested_at=_dt(scope_row["requested_at"]),  # type: ignore[arg-type]
                    now=now,
                    minimum_interval=interval,
                    state=self.scope_policy_state(scope),
                    evidence=evidence,
                )
                if decision.kind.value != "satisfied" or decision.satisfying_observation_id is None:
                    return GuardDecision(GuardDisposition.DISPATCH, "dispatch")
                satisfying_ids.append(decision.satisfying_observation_id)
            connection.execute(
                """
                UPDATE adapter_actions
                SET state = 'satisfied', completed_at = ?, lease_id = NULL, lease_worker_id = NULL,
                    leased_until = NULL
                WHERE action_id = ?
                """,
                (_utc_text(now), action_id),
            )
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
        self, connection: sqlite3.Connection, batch: ObservationBatch
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

    def _validate_coverage(
        self, connection: sqlite3.Connection, batch: ObservationBatch
    ) -> tuple[CoverageDeclaration, ...]:
        declarations = tuple(batch.coverage)
        capability_rows = connection.execute(
            """
            SELECT capability_key, target_kinds_json, produced_facets_json
            FROM capability_bindings
            WHERE connection_binding_id = ? AND operation_class = 'observe'
            """,
            (batch.connection_binding_id,),
        ).fetchall()
        seen: set[tuple[str, str, str, str, str, str, str]] = set()
        for declaration in declarations:
            scope = declaration.scope
            if scope.system_id != batch.system_id:
                raise ValueError("coverage_system_mismatch")
            definition = V1_TYPE_DEFINITION_BY_KEY.get(scope.object_type)
            if definition is None or scope.facet not in {item.facet for item in definition.facets}:
                raise ValueError("coverage_unsupported_facet")
            supported = any(
                (scope.capability_key is None or row["capability_key"] == scope.capability_key)
                and scope.target.kind.value in _json_value(row["target_kinds_json"])
                and scope.facet in _json_value(row["produced_facets_json"])
                for row in capability_rows
            )
            if not supported:
                raise ValueError("coverage_not_supported_by_binding")
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
            object_id = str(uuid4())
            connection.execute(
                """
                INSERT INTO remote_objects (
                    object_id, system_id, object_type, object_type_version,
                    source_kind, external_key, display_name, presence,
                    first_seen_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'present', ?, ?)
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
                ),
            )
            row = connection.execute(
                "SELECT * FROM remote_objects WHERE object_id = ?", (object_id,)
            ).fetchone()
        else:
            if row["object_type"] != locator.object_type:
                raise ValueError("external_identity_type_mismatch")
            previous_seen = _dt(row["last_seen_at"])
            if previous_seen is None or observed_at >= previous_seen:
                connection.execute(
                    """
                    UPDATE remote_objects
                    SET display_name = ?, presence = 'present', last_seen_at = ?
                    WHERE object_id = ?
                    """,
                    (locator.display_name, _utc_text(observed_at), row["object_id"]),
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
        if existing_observed is not None and batch.observed_at < existing_observed:
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
        merged.update(observation.payload)
        changed = existing is None or merged != prior_payload
        state_changed_at = batch.observed_at if changed else _dt(existing["state_changed_at"])
        connection.execute(
            """
            INSERT INTO facets (
                object_id, facet, facet_version, knowledge, payload_json,
                observed_at, state_changed_at, supporting_observation_id,
                source_revision
            ) VALUES (?, ?, ?, 'known', ?, ?, ?, ?, ?)
            ON CONFLICT(object_id, facet) DO UPDATE SET
                facet_version = excluded.facet_version,
                knowledge = excluded.knowledge,
                payload_json = excluded.payload_json,
                observed_at = excluded.observed_at,
                state_changed_at = excluded.state_changed_at,
                supporting_observation_id = excluded.supporting_observation_id,
                source_revision = excluded.source_revision
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
        existing = connection.execute(
            """
            SELECT * FROM relationships
            WHERE system_id = ? AND subject_id = ? AND predicate = ? AND object_id = ?
            """,
            (batch.system_id, subject_id, observation.predicate, object_id),
        ).fetchone()
        if existing is not None and batch.observed_at < _dt(existing["observed_at"]):
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
                supporting_observation_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(system_id, subject_id, predicate, object_id) DO UPDATE SET
                presence = excluded.presence,
                observed_at = excluded.observed_at,
                supporting_observation_id = excluded.supporting_observation_id
            """,
            (
                relationship_id,
                batch.system_id,
                subject_id,
                observation.predicate,
                object_id,
                observation.presence.value,
                _utc_text(batch.observed_at),
                observation.observation_id,
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
                capability_key, coverage, field_mask_json, observed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid5(NAMESPACE_URL, f"credit:{observation_id}")),
                observation_id,
                *_scope_columns(declaration.scope),
                _utc_text(batch.observed_at),
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
        rows = connection.execute(
            """
            SELECT relationship_id, object_id FROM relationships
            WHERE system_id = ? AND subject_id = ?
              AND predicate = 'contains' AND presence = 'present'
            """,
            (batch.system_id, subject_id),
        ).fetchall()
        for row in rows:
            if (subject_id, row["object_id"]) in positive_contains:
                continue
            connection.execute(
                """
                UPDATE relationships
                SET presence = 'absent', observed_at = ?, supporting_observation_id = ?
                WHERE relationship_id = ?
                """,
                (_utc_text(batch.observed_at), coverage_observation_id, row["relationship_id"]),
            )

    async def ingest(self, batch: ObservationBatch) -> IngestionResult:
        """Validate, journal, and conservatively project normalized evidence.

        The accepted journal is intentionally uncompacted in this slice.  A bad
        item becomes a redacted ingestion issue; it never clears known facts or
        enables absence reconciliation for the batch.
        """
        batch_digest = _batch_digest(batch)
        with self._immediate_transaction() as connection:
            prior = connection.execute(
                """
                SELECT system_id, connection_binding_id, adapter_key, adapter_version,
                       action_id, observed_at, received_at, status, accepted_ids_json,
                       issue_count, batch_digest
                FROM observation_batches WHERE batch_id = ?
                """,
                (batch.batch_id,),
            ).fetchone()
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
                        self._validate_batch_context(connection, batch)
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
                self._validate_batch_context(connection, batch)
                declarations = self._validate_coverage(connection, batch)
            except ValueError:
                return IngestionResult(batch_id=batch.batch_id, status=IngestionStatus.REJECTED)
            connection.execute(
                """
                INSERT INTO observation_batches (
                    batch_id, system_id, connection_binding_id, adapter_key,
                    adapter_version, action_id, observed_at, received_at,
                    status, accepted_ids_json, issue_count, batch_digest
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'accepted', '[]', 0, ?)
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
                ),
            )
            accepted_ids: list[str] = []
            issue_count = 0
            positive_contains: set[tuple[str, str]] = set()
            for observation in batch.facet_observations:
                try:
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
                    )
                    recorded_status = self._record_journal_item(
                        connection,
                        observation_id=observation.observation_id,
                        batch=batch,
                        item_kind="facet",
                        item=observation,
                    )
                    if recorded_status != "recorded":  # pragma: no cover - transaction invariant
                        raise RuntimeError("observation journal status changed during ingestion")
                    if observation.update_mode is UpdateMode.ABSENCE:
                        if not self._absence_is_authorized(
                            declarations, object_id=target.object_id, observation=observation
                        ):
                            raise ValueError("unauthorized_object_absence")
                        connection.execute(
                            "UPDATE remote_objects SET presence = 'absent' WHERE object_id = ?",
                            (target.object_id,),
                        )
                    else:
                        self._merge_facet_observation(
                            connection,
                            batch=batch,
                            observation=observation,
                            object_id=target.object_id,
                        )
                    accepted_ids.append(observation.observation_id)
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
                    )
                    object_value = self._resolve_locator(
                        connection,
                        locator=observation.object,
                        system_id=batch.system_id,
                        observed_at=batch.observed_at,
                    )
                    recorded_status = self._record_journal_item(
                        connection,
                        observation_id=observation.observation_id,
                        batch=batch,
                        item_kind="relationship",
                        item=observation,
                    )
                    if recorded_status != "recorded":  # pragma: no cover - transaction invariant
                        raise RuntimeError("observation journal status changed during ingestion")
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
                    accepted_ids.append(observation.observation_id)
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
