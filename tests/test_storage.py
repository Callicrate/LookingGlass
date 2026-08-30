import asyncio
import os
import sqlite3
import stat
import subprocess
import threading
import time
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from async_api_view.application import DurableCoordinator, SystemBootstrapService
from async_api_view.contracts import (
    ActionAttempt,
    ActionCompletion,
    ActionLeaseLost,
    ActionOutcome,
    ActionState,
    AdapterAction,
    CapabilityCoveragePolicy,
    CollectionCoverage,
    ErrorClass,
    FacetObservation,
    FieldCoverage,
    GuardDisposition,
    IngestionStatus,
    IntentScopeState,
    ObjectLocator,
    ObservationBatch,
    PresenceState,
    RefreshCoverage,
    RefreshIntent,
    RefreshIntervalOverride,
    RefreshOrigin,
    RefreshScope,
    RemoteObject,
    TargetKind,
    TargetRef,
    UpdateMode,
)
from async_api_view.storage import SQLiteStore, backup_sqlite_database
from async_api_view.storage import sqlite as sqlite_storage
from async_api_view.storage.sqlite import _redact

NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)


def run(awaitable):
    return asyncio.run(awaitable)


def test_migrations_reopen_with_durable_wal_state(tmp_path) -> None:
    path = tmp_path / "state.sqlite3"
    with SQLiteStore(path) as store:
        system = SystemBootstrapService(store).create_system(
            display_name="local", system_kind="databricks.workspace", now=NOW
        )
        assert store._connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert store._connection.execute("PRAGMA application_id").fetchone()[0] == 0x524F4F4B
        assert store.get_system(system.system_id) is not None

    with SQLiteStore(path) as reopened:
        assert [system.display_name for system in reopened.list_systems()] == ["local"]
        versions = reopened._connection.execute("SELECT version FROM schema_migrations").fetchall()
        assert [row[0] for row in versions] == [
            "0001_initial",
            "0002_refresh_scope_capability_key",
            "0003_observation_batch_digest",
            "0004_configured_system_identities",
            "0005_operational_event_recency",
            "0006_relationship_navigation",
            "0007_operational_event_filters",
            "0008_action_activity",
            "0009_facet_action_status",
            "0010_dashboard_active_action",
            "0011_refresh_override_identity",
            "0012_deferred_scope_policy_indexes",
            "0013_capability_coverage_policy",
            "0014_coverage_policy_initialization",
            "0015_relationship_coverage_watermarks",
            "0016_projection_received_order",
            "0017_queue_claim_order",
            "0018_action_state_projections",
            "0019_web_cursor_indexes",
            "0020_retired_authorities",
            "0021_intent_aggregate_indexes",
            "0022_observation_receipt_order",
            "0023_time_authority",
        ]
        child_plan = reopened._connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT relationships.object_id, objects.display_name
            FROM relationships AS relationships
            INNER JOIN remote_objects AS objects
                ON objects.object_id = relationships.object_id
            WHERE relationships.subject_id = ?
                AND relationships.predicate = ?
                AND relationships.presence = 'present'
                AND objects.object_type = ?
            ORDER BY relationships.object_id
            LIMIT 50
            """,
            (str(uuid4()), "contains", "file"),
        ).fetchall()
        assert any("ix_relationships_subject_predicate" in row[3] for row in child_plan)
        assert not any("TEMP B-TREE" in row[3] for row in child_plan)


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE TABLE unexpected_state (value TEXT)",
        "CREATE INDEX unexpected_system_name ON systems(display_name)",
        "CREATE VIEW unexpected_systems AS SELECT * FROM systems",
        """
        CREATE TRIGGER suppress_runtime_events
        AFTER INSERT ON operational_events
        BEGIN
            DELETE FROM operational_events
            WHERE idempotency_key = NEW.idempotency_key;
        END
        """,
    ],
)
def test_unexpected_schema_objects_fail_immutable_preflight_without_mutation(
    tmp_path: Path,
    statement: str,
) -> None:
    path = tmp_path / "unexpected-schema.sqlite3"
    with SQLiteStore(path):
        pass
    with sqlite3.connect(path) as connection:
        connection.execute(statement)
        connection.commit()
    original = path.read_bytes()
    sidecars = {suffix: Path(f"{path}{suffix}").exists() for suffix in ("-wal", "-shm")}

    with pytest.raises(sqlite3.DatabaseError, match=r"schema object|schema objects"):
        SQLiteStore(path)

    assert path.read_bytes() == original
    assert {suffix: Path(f"{path}{suffix}").exists() for suffix in ("-wal", "-shm")} == sidecars


def test_existing_empty_database_file_initializes_after_read_only_preflight(tmp_path) -> None:
    path = tmp_path / "state" / "rookery.sqlite3"
    path.parent.mkdir()
    path.touch()

    with SQLiteStore(path) as store:
        assert store._connection.execute("PRAGMA application_id").fetchone()[0] == 0x524F4F4B
        assert (
            store._connection.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] == 23
        )


def test_new_database_is_migrated_and_marked_before_wal_activation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "state" / "rookery.sqlite3"
    original = SQLiteStore._enable_wal_mode

    def verify_before_wal(store: SQLiteStore) -> None:
        assert store._connection.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
        assert store._connection.execute("PRAGMA application_id").fetchone()[0] == 0x524F4F4B
        assert (
            store._connection.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] == 23
        )
        original(store)

    monkeypatch.setattr(SQLiteStore, "_enable_wal_mode", verify_before_wal)

    with SQLiteStore(path) as store:
        assert store._connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


def test_unrelated_sqlite_schema_is_rejected_before_ledger_mutation(tmp_path) -> None:
    path = tmp_path / "unrelated.sqlite3"
    unrelated = sqlite3.connect(path)
    unrelated.execute("CREATE TABLE systems (unrelated TEXT)")
    unrelated.commit()
    unrelated.close()

    with pytest.raises(sqlite3.DatabaseError, match="not a recognized Rookery store"):
        SQLiteStore(path)

    check = sqlite3.connect(path)
    try:
        assert [row[1] for row in check.execute("PRAGMA table_info(systems)")] == ["unrelated"]
        assert (
            check.execute(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?",
                ("schema_migrations",),
            ).fetchone()[0]
            == 0
        )
        assert check.execute("PRAGMA application_id").fetchone()[0] == 0
    finally:
        check.close()


def test_foreign_sqlite_rejection_preserves_delete_mode_bytes_and_sidecar_absence(
    tmp_path: Path,
) -> None:
    path = tmp_path / "foreign-delete.sqlite3"
    foreign = sqlite3.connect(path)
    try:
        foreign.execute("CREATE TABLE foreign_state (value TEXT NOT NULL)")
        foreign.execute("INSERT INTO foreign_state VALUES ('preserve me')")
        foreign.commit()
        assert foreign.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    finally:
        foreign.close()
    original = path.read_bytes()
    original_mode = stat.S_IMODE(path.stat().st_mode)

    with pytest.raises(sqlite3.DatabaseError, match="not a recognized Rookery store"):
        SQLiteStore(path)

    assert path.read_bytes() == original
    assert stat.S_IMODE(path.stat().st_mode) == original_mode
    assert not Path(f"{path}-wal").exists()
    assert not Path(f"{path}-shm").exists()
    check = sqlite3.connect(path)
    try:
        assert check.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
        assert check.execute("SELECT value FROM foreign_state").fetchone()[0] == "preserve me"
    finally:
        check.close()


@pytest.mark.skipif(os.name != "nt", reason="Windows foreign-file DACL regression")
def test_foreign_sqlite_rejection_does_not_change_windows_dacl(tmp_path: Path) -> None:
    path = tmp_path / "foreign.sqlite3"
    foreign = sqlite3.connect(path)
    foreign.execute("CREATE TABLE foreign_state (value TEXT NOT NULL)")
    foreign.commit()
    foreign.close()
    icacls = Path(os.environ["SYSTEMROOT"]) / "System32" / "icacls.exe"
    subprocess.run(  # noqa: S603 - absolute Windows system executable
        (str(icacls), str(path), "/grant", "*S-1-1-0:F", "/Q"),
        check=True,
        capture_output=True,
    )
    before = tmp_path / "before-acl.txt"
    after = tmp_path / "after-acl.txt"
    subprocess.run(  # noqa: S603 - absolute Windows system executable
        (str(icacls), str(path), "/save", str(before), "/Q"),
        check=True,
        capture_output=True,
    )

    with pytest.raises(sqlite3.DatabaseError, match="not a recognized Rookery store"):
        SQLiteStore(path)

    subprocess.run(  # noqa: S603 - absolute Windows system executable
        (str(icacls), str(path), "/save", str(after), "/Q"),
        check=True,
        capture_output=True,
    )
    assert after.read_text(encoding="utf-16-le") == before.read_text(encoding="utf-16-le")


def test_existing_database_preflight_uses_immutable_read_only_uri(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "foreign.sqlite3"
    foreign = sqlite3.connect(path)
    foreign.execute("CREATE TABLE foreign_state (value TEXT NOT NULL)")
    foreign.commit()
    foreign.close()
    original_connect = sqlite_storage.sqlite3.connect
    opened: list[str] = []

    def track_connect(database: object, *args: object, **kwargs: object):
        opened.append(str(database))
        return original_connect(database, *args, **kwargs)

    monkeypatch.setattr(sqlite_storage.sqlite3, "connect", track_connect)

    with pytest.raises(sqlite3.DatabaseError, match="not a recognized Rookery store"):
        SQLiteStore(path)

    assert opened == [f"{path.as_uri()}?mode=ro&immutable=1"]


def test_unmarked_rookery_with_wal_sidecar_fails_without_sidecar_mutation(tmp_path: Path) -> None:
    path = tmp_path / "state" / "rookery.sqlite3"
    with SQLiteStore(path):
        pass
    markerless = sqlite3.connect(path)
    try:
        assert markerless.execute("PRAGMA journal_mode = DELETE").fetchone()[0] == "delete"
        markerless.execute("PRAGMA application_id = 0")
    finally:
        markerless.close()
    wal = Path(f"{path}-wal")
    wal.write_bytes(b"untrusted wal sentinel")
    database_bytes = path.read_bytes()
    wal_bytes = wal.read_bytes()

    with pytest.raises(sqlite3.DatabaseError, match="WAL sidecars cannot be adopted"):
        SQLiteStore(path)

    assert path.read_bytes() == database_bytes
    assert wal.read_bytes() == wal_bytes
    assert not Path(f"{path}-shm").exists()


def test_database_identity_change_before_write_open_fails_without_wal_transition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "state" / "rookery.sqlite3"
    with SQLiteStore(path):
        pass
    calls = 0
    original_verify = sqlite_storage.RegularFileGuard.verify
    original_connect = sqlite_storage.sqlite3.connect

    def changed_identity(guard: sqlite_storage.RegularFileGuard) -> None:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError("Rookery state file identity changed while guarded")
        original_verify(guard)

    def reject_write_open(database: object, *args: object, **kwargs: object):
        if "mode=rw" in str(database):
            pytest.fail("write-capable open ran after database identity changed")
        return original_connect(database, *args, **kwargs)

    monkeypatch.setattr(sqlite_storage.RegularFileGuard, "verify", changed_identity)
    monkeypatch.setattr(sqlite_storage.sqlite3, "connect", reject_write_open)
    monkeypatch.setattr(
        SQLiteStore,
        "_enable_wal_mode",
        lambda _store: pytest.fail("WAL transition ran after database identity changed"),
    )

    with pytest.raises(OSError, match="identity changed while guarded"):
        SQLiteStore(path)


def test_unrelated_view_only_schema_is_not_treated_as_empty(tmp_path) -> None:
    path = tmp_path / "unrelated-view.sqlite3"
    unrelated = sqlite3.connect(path)
    unrelated.execute("CREATE VIEW unrelated_view AS SELECT 1 AS value")
    unrelated.commit()
    unrelated.close()

    with pytest.raises(sqlite3.DatabaseError, match="not a recognized Rookery store"):
        SQLiteStore(path)

    check = sqlite3.connect(path)
    try:
        assert (
            check.execute(
                "SELECT type FROM sqlite_schema WHERE name = 'unrelated_view'"
            ).fetchone()[0]
            == "view"
        )
        assert (
            check.execute("SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table'").fetchone()[0]
            == 0
        )
    finally:
        check.close()


def test_foreign_application_id_is_rejected_before_schema_creation(tmp_path) -> None:
    path = tmp_path / "foreign.sqlite3"
    foreign = sqlite3.connect(path)
    foreign.execute("PRAGMA application_id = 12345")
    foreign.close()

    with pytest.raises(sqlite3.DatabaseError, match="not a recognized Rookery store"):
        SQLiteStore(path)

    check = sqlite3.connect(path)
    try:
        assert (
            check.execute("SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table'").fetchone()[0]
            == 0
        )
        assert check.execute("PRAGMA application_id").fetchone()[0] == 12345
    finally:
        check.close()


def test_markerless_incomplete_rookery_schema_is_rejected_before_migration(tmp_path) -> None:
    path = tmp_path / "incomplete-rookery.sqlite3"
    legacy = sqlite3.connect(path)
    legacy.executescript(
        "CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL);"
    )
    legacy.executescript(
        Path("src/async_api_view/storage/migrations/0001_initial.sql").read_text(encoding="utf-8")
    )
    legacy.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
        ("0001_initial", "2026-08-24T12:00:00Z"),
    )
    legacy.execute("ALTER TABLE relationships RENAME TO legacy_relationships")
    legacy.execute("CREATE TABLE relationships (unrelated TEXT)")
    legacy.commit()
    legacy.close()

    with pytest.raises(sqlite3.DatabaseError, match="relationships schema is incompatible"):
        SQLiteStore(path)

    check = sqlite3.connect(path)
    try:
        assert [row[0] for row in check.execute("SELECT version FROM schema_migrations")] == [
            "0001_initial"
        ]
        assert check.execute("PRAGMA application_id").fetchone()[0] == 0
    finally:
        check.close()


def test_unknown_future_migration_is_rejected_without_ledger_mutation(tmp_path) -> None:
    path = tmp_path / "future.sqlite3"
    with SQLiteStore(path) as store:
        store._connection.execute(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
            ("9999_future", "2026-08-29T12:00:00Z"),
        )

    with pytest.raises(sqlite3.DatabaseError, match="contains an unknown version"):
        SQLiteStore(path)

    check = sqlite3.connect(path)
    try:
        assert (
            check.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = ?",
                ("9999_future",),
            ).fetchone()[0]
            == 1
        )
        assert check.execute("PRAGMA application_id").fetchone()[0] == 0x524F4F4B
    finally:
        check.close()


def test_current_ledger_missing_later_table_fails_without_mutation(tmp_path) -> None:
    path = tmp_path / "missing-current-table.sqlite3"
    with SQLiteStore(path) as store:
        system = SystemBootstrapService(store).create_system(
            display_name="sentinel", system_kind="databricks.workspace", now=NOW
        )
        store._connection.execute("DROP TABLE relationship_coverage_watermarks")

    with pytest.raises(sqlite3.DatabaseError, match="current database schema is incomplete"):
        SQLiteStore(path)

    check = sqlite3.connect(path)
    try:
        assert check.execute("PRAGMA application_id").fetchone()[0] == 0x524F4F4B
        assert check.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] == 23
        assert (
            check.execute(
                "SELECT display_name FROM systems WHERE system_id = ?",
                (system.system_id,),
            ).fetchone()[0]
            == "sentinel"
        )
        assert (
            check.execute(
                """
                SELECT COUNT(*) FROM sqlite_schema
                WHERE type = 'table' AND name = 'relationship_coverage_watermarks'
                """
            ).fetchone()[0]
            == 0
        )
    finally:
        check.close()


def test_current_ledger_missing_unique_index_fails_without_mutation(tmp_path) -> None:
    path = tmp_path / "missing-current-index.sqlite3"
    with SQLiteStore(path) as store:
        system = SystemBootstrapService(store).create_system(
            display_name="sentinel", system_kind="databricks.workspace", now=NOW
        )
        store._connection.execute("DROP INDEX ux_refresh_overrides_identity")

    with pytest.raises(
        sqlite3.DatabaseError,
        match="schema object ux_refresh_overrides_identity is incompatible",
    ):
        SQLiteStore(path)

    check = sqlite3.connect(path)
    try:
        assert check.execute("PRAGMA application_id").fetchone()[0] == 0x524F4F4B
        assert check.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] == 23
        assert (
            check.execute(
                "SELECT display_name FROM systems WHERE system_id = ?",
                (system.system_id,),
            ).fetchone()[0]
            == "sentinel"
        )
        assert (
            check.execute(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name = ?",
                ("ux_refresh_overrides_identity",),
            ).fetchone()[0]
            == 0
        )
    finally:
        check.close()


def test_current_ledger_missing_projection_trigger_fails_without_mutation(tmp_path) -> None:
    path = tmp_path / "missing-projection-trigger.sqlite3"
    with SQLiteStore(path) as store:
        system = SystemBootstrapService(store).create_system(
            display_name="sentinel", system_kind="databricks.workspace", now=NOW
        )
        store._connection.execute("DROP TRIGGER trg_action_projection_update")

    with pytest.raises(
        sqlite3.DatabaseError,
        match="schema object trg_action_projection_update is incompatible",
    ):
        SQLiteStore(path)

    check = sqlite3.connect(path)
    try:
        assert check.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] == 23
        assert (
            check.execute(
                "SELECT display_name FROM systems WHERE system_id = ?",
                (system.system_id,),
            ).fetchone()[0]
            == "sentinel"
        )
        assert (
            check.execute(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'trg_action_projection_update'"
            ).fetchone()[0]
            == 0
        )
    finally:
        check.close()


def test_drifted_migration_prefix_rolls_back_all_later_mutations(tmp_path) -> None:
    path = tmp_path / "drifted-prefix.sqlite3"
    migrations = sorted(Path("src/async_api_view/storage/migrations").glob("*.sql"))
    legacy = sqlite3.connect(path)
    legacy.execute(
        "CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL)"
    )
    for migration in migrations[:10]:
        legacy.executescript(migration.read_text(encoding="utf-8"))
        legacy.execute(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
            (migration.stem, "2026-08-24T12:00:00Z"),
        )
    legacy.execute("DROP INDEX ix_configured_scopes_object")
    legacy.execute("ALTER TABLE configured_scopes RENAME TO configured_scopes_canonical")
    legacy.execute(
        """
        CREATE TABLE configured_scopes (
            scope_id TEXT PRIMARY KEY,
            system_id TEXT NOT NULL REFERENCES systems(system_id) ON DELETE RESTRICT,
            object_id TEXT REFERENCES remote_objects(object_id) ON DELETE CASCADE,
            object_type TEXT NOT NULL,
            enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
            display_name TEXT NOT NULL,
            record_created_at TEXT NOT NULL,
            record_updated_at TEXT NOT NULL
        )
        """
    )
    legacy.execute("DROP TABLE configured_scopes_canonical")
    legacy.executemany(
        """
        INSERT INTO refresh_overrides (
            level, scope_id, facet, interval_seconds, record_updated_at
        ) VALUES ('system', 'sentinel', NULL, ?, ?)
        """,
        (
            (28_800, "2026-08-24T12:00:00Z"),
            (14_400, "2026-08-24T13:00:00Z"),
        ),
    )
    legacy.commit()
    legacy.close()

    with pytest.raises(
        sqlite3.DatabaseError,
        match="schema object configured_scopes is incompatible",
    ):
        SQLiteStore(path)

    check = sqlite3.connect(path)
    try:
        assert [
            row[0]
            for row in check.execute("SELECT version FROM schema_migrations ORDER BY version")
        ] == [migration.stem for migration in migrations[:10]]
        assert [
            row[0]
            for row in check.execute(
                """
                SELECT interval_seconds FROM refresh_overrides
                WHERE level = 'system' AND scope_id = 'sentinel' AND facet IS NULL
                ORDER BY interval_seconds DESC
                """
            )
        ] == [28_800, 14_400]
        assert (
            check.execute(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'ux_refresh_overrides_identity'"
            ).fetchone()[0]
            == 0
        )
        assert (
            check.execute(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'ix_configured_scopes_object'"
            ).fetchone()[0]
            == 0
        )
        assert (
            check.execute(
                """
                SELECT COUNT(*) FROM sqlite_schema
                WHERE type = 'table' AND name = 'relationship_coverage_watermarks'
                """
            ).fetchone()[0]
            == 0
        )
        assert (
            "ON DELETE CASCADE"
            in check.execute(
                "SELECT sql FROM sqlite_schema WHERE name = 'configured_scopes'"
            ).fetchone()[0]
        )
        assert check.execute("PRAGMA application_id").fetchone()[0] == 0
    finally:
        check.close()


def test_online_backup_captures_live_wal_and_never_overwrites(tmp_path) -> None:
    source = tmp_path / "state.sqlite3"
    destination = tmp_path / "backups" / "state.sqlite3"
    with SQLiteStore(source) as store:
        store._connection.execute("PRAGMA wal_autocheckpoint = 0")
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local",
            profile="DEFAULT",
            workspace_root="/Shared",
            now=NOW,
        )
        assert Path(f"{source}-wal").is_file()

        published = backup_sqlite_database(source, destination)

        assert published == destination.resolve()
        assert store.get_system(seeded.system.system_id) is not None

    with sqlite3.connect(destination) as backup:
        assert backup.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert (
            backup.execute(
                "SELECT display_name FROM systems WHERE system_id = ?",
                (seeded.system.system_id,),
            ).fetchone()[0]
            == "local"
        )
    original = destination.read_bytes()
    with pytest.raises(FileExistsError, match="already exists"):
        backup_sqlite_database(source, destination)
    assert destination.read_bytes() == original
    assert list(destination.parent.glob(".rookery-backup-*.tmp")) == []


def test_backup_requires_snapshot_size_plus_recovery_reserve(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "state" / "rookery.sqlite3"
    destination = tmp_path / "backups" / "rookery.sqlite3"
    with SQLiteStore(source):
        pass
    with sqlite3.connect(source) as connection:
        snapshot_bytes = (
            connection.execute("PRAGMA page_count").fetchone()[0]
            * connection.execute("PRAGMA page_size").fetchone()[0]
        )
    required = snapshot_bytes + sqlite_storage.MIN_WRITE_RESERVE_BYTES
    available = [required - 1]
    monkeypatch.setattr(sqlite_storage, "available_bytes", lambda _path: available[0])

    with pytest.raises(
        sqlite_storage.StorageHeadroomUnavailable,
        match="snapshot size plus a 64 MiB safety reserve",
    ):
        backup_sqlite_database(source, destination)

    assert not destination.exists()
    assert list(destination.parent.glob(".rookery-backup-*.tmp")) == []
    available[0] = required
    assert backup_sqlite_database(source, destination) == destination.resolve()


def test_backup_rejects_foreign_sqlite_before_destination_creation(tmp_path: Path) -> None:
    source = tmp_path / "foreign.sqlite3"
    destination = tmp_path / "backups" / "state.sqlite3"
    foreign = sqlite3.connect(source)
    try:
        foreign.execute("CREATE TABLE foreign_state (value TEXT NOT NULL)")
        foreign.execute("INSERT INTO foreign_state VALUES ('preserve me')")
        foreign.commit()
        assert foreign.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    finally:
        foreign.close()
    original = source.read_bytes()

    with pytest.raises(RuntimeError, match="recognized Rookery store"):
        backup_sqlite_database(source, destination)

    assert source.read_bytes() == original
    assert not destination.parent.exists()
    assert not Path(f"{source}-wal").exists()
    assert not Path(f"{source}-shm").exists()


def test_backup_preserves_recognized_markerless_rookery_identity(tmp_path: Path) -> None:
    source = tmp_path / "state" / "rookery.sqlite3"
    destination = tmp_path / "backups" / "rookery.sqlite3"
    with SQLiteStore(source):
        pass
    markerless = sqlite3.connect(source)
    try:
        assert markerless.execute("PRAGMA journal_mode = DELETE").fetchone()[0] == "delete"
        markerless.execute("PRAGMA application_id = 0")
    finally:
        markerless.close()

    backup_sqlite_database(source, destination)

    check = sqlite3.connect(destination)
    try:
        assert check.execute("PRAGMA application_id").fetchone()[0] == 0
        assert check.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] == 23
    finally:
        check.close()


def test_backup_preserves_pre_migration_schema_for_upgrade_rollback(tmp_path: Path) -> None:
    source = tmp_path / "state" / "rookery.sqlite3"
    destination = tmp_path / "backups" / "rookery.sqlite3"
    source.parent.mkdir()
    legacy = sqlite3.connect(source)
    try:
        legacy.execute(
            "CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        legacy.executescript(
            Path("src/async_api_view/storage/migrations/0001_initial.sql").read_text(
                encoding="utf-8"
            )
        )
        legacy.execute(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
            ("0001_initial", "2026-08-24T12:00:00Z"),
        )
        legacy.commit()
    finally:
        legacy.close()

    backup_sqlite_database(source, destination)
    with SQLiteStore(source):
        pass

    checkpoint = sqlite3.connect(destination)
    try:
        assert [
            row[0]
            for row in checkpoint.execute("SELECT version FROM schema_migrations ORDER BY version")
        ] == ["0001_initial"]
        assert checkpoint.execute("PRAGMA application_id").fetchone()[0] == 0
    finally:
        checkpoint.close()


@pytest.mark.skipif(os.name == "nt", reason="POSIX ownership and mode regression")
def test_state_database_sidecars_and_backups_are_posix_private(tmp_path: Path) -> None:
    state_directory = tmp_path / "state"
    state_directory.mkdir(mode=0o777)
    state_directory.chmod(0o777)
    database = state_directory / "rookery.sqlite3"
    backup_directory = tmp_path / "backups"
    backup_directory.mkdir(mode=0o777)
    backup_directory.chmod(0o777)
    destination = backup_directory / "rookery.sqlite3"

    with SQLiteStore(database) as store:
        store._connection.execute("PRAGMA wal_autocheckpoint = 0")
        SystemBootstrapService(store).create_system(
            display_name="private",
            system_kind="databricks.workspace",
            now=NOW,
        )
        assert Path(f"{database}-wal").is_file()
        assert Path(f"{database}-shm").is_file()
        for directory in (state_directory,):
            assert directory.stat().st_mode & 0o777 == 0o700
        for path in (database, Path(f"{database}-wal"), Path(f"{database}-shm")):
            assert path.stat().st_mode & 0o777 == 0o600

        backup_sqlite_database(database, destination)

    assert backup_directory.stat().st_mode & 0o777 == 0o700
    assert destination.stat().st_mode & 0o777 == 0o600


@pytest.mark.skipif(os.name != "nt", reason="Windows ownership and DACL regression")
def test_state_database_sidecars_and_backups_replace_permissive_windows_security(
    tmp_path: Path,
) -> None:
    state_directory = tmp_path / "state"
    database = state_directory / "rookery.sqlite3"
    with SQLiteStore(database):
        pass
    backup_directory = tmp_path / "backups"
    backup_directory.mkdir()
    destination = backup_directory / "rookery.sqlite3"
    icacls = Path(os.environ["SYSTEMROOT"]) / "System32" / "icacls.exe"
    for path in (state_directory, database, backup_directory):
        grant = "*S-1-1-0:(OI)(CI)F" if path.is_dir() else "*S-1-1-0:F"
        subprocess.run(  # noqa: S603 - absolute Windows system executable
            (str(icacls), str(path), "/grant", grant, "/Q"),
            check=True,
            capture_output=True,
        )

    with SQLiteStore(database) as store:
        store._connection.execute("PRAGMA wal_autocheckpoint = 0")
        SystemBootstrapService(store).create_system(
            display_name="private",
            system_kind="databricks.workspace",
            now=NOW,
        )
        paths = (
            state_directory,
            database,
            Path(f"{database}-wal"),
            Path(f"{database}-shm"),
        )
        assert all(path.exists() for path in paths)
        backup_sqlite_database(database, destination)

        powershell = (
            Path(os.environ["SYSTEMROOT"])
            / "System32"
            / "WindowsPowerShell"
            / "v1.0"
            / "powershell.exe"
        )
        owner_script = (
            "if($env:ROOKERY_ACL_TEST_DIRECTORY -eq '1'){"
            "$acl=(New-Object System.IO.DirectoryInfo("
            "$env:ROOKERY_ACL_TEST_PATH)).GetAccessControl()}else{"
            "$acl=(New-Object System.IO.FileInfo("
            "$env:ROOKERY_ACL_TEST_PATH)).GetAccessControl()};"
            "$ownerSid=$acl.GetOwner("
            "[System.Security.Principal.SecurityIdentifier]).Value;"
            "$currentSid=[System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value;"
            "if($ownerSid -ne $currentSid){throw 'Rookery path owner mismatch'}"
        )
        for ordinal, path in enumerate((*paths, backup_directory, destination)):
            saved_acl = tmp_path / f"state-acl-{ordinal}.txt"
            subprocess.run(  # noqa: S603 - absolute Windows system executable
                (str(icacls), str(path), "/save", str(saved_acl), "/Q"),
                check=True,
                capture_output=True,
            )
            sddl = saved_acl.read_text(encoding="utf-16-le")
            assert "D:P" in sddl
            assert ";;;WD)" not in sddl
            owner_environment = dict(os.environ)
            owner_environment["ROOKERY_ACL_TEST_PATH"] = str(path)
            owner_environment["ROOKERY_ACL_TEST_DIRECTORY"] = str(int(path.is_dir()))
            owner_result = subprocess.run(  # noqa: S603 - absolute Windows system executable
                (
                    str(powershell),
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    owner_script,
                ),
                check=False,
                capture_output=True,
                env=owner_environment,
                text=True,
            )
            assert owner_result.returncode == 0, owner_result.stderr


def test_database_hard_links_are_rejected_before_open(tmp_path: Path) -> None:
    database = tmp_path / "state" / "rookery.sqlite3"
    with SQLiteStore(database):
        pass
    alias = tmp_path / "state" / "alias.sqlite3"
    os.link(database, alias)

    with pytest.raises(OSError, match="hard links"):
        SQLiteStore(database)


@pytest.mark.skipif(os.name != "nt", reason="Windows no-delete-share guard regression")
def test_windows_file_guard_blocks_path_replacement(tmp_path: Path) -> None:
    guarded = tmp_path / "guarded.sqlite3"
    replacement = tmp_path / "replacement.sqlite3"
    guarded.write_bytes(b"guarded")
    replacement.write_bytes(b"replacement")

    with sqlite_storage.RegularFileGuard(guarded) as guard:
        with pytest.raises(OSError):
            os.replace(replacement, guarded)
        guard.verify()

    assert guarded.read_bytes() == b"guarded"
    assert replacement.read_bytes() == b"replacement"


@pytest.mark.skipif(os.name == "nt", reason="POSIX file-identity replacement regression")
def test_posix_file_guard_detects_path_replacement(tmp_path: Path) -> None:
    guarded = tmp_path / "guarded.sqlite3"
    replacement = tmp_path / "replacement.sqlite3"
    guarded.write_bytes(b"guarded")
    replacement.write_bytes(b"replacement")

    with sqlite_storage.RegularFileGuard(guarded) as guard:
        os.replace(replacement, guarded)
        with pytest.raises(OSError, match="identity changed"):
            guard.verify()


@pytest.mark.skipif(os.name != "nt", reason="Windows no-delete-share guard regression")
def test_windows_directory_guard_blocks_path_replacement(tmp_path: Path) -> None:
    guarded = tmp_path / "guarded"
    replacement = tmp_path / "replacement"
    guarded.mkdir()
    replacement.mkdir()

    with sqlite_storage.PrivateDirectoryGuard(guarded) as guard:
        with pytest.raises(OSError):
            os.replace(replacement, guarded)
        guard.verify()

    assert guarded.is_dir()
    assert replacement.is_dir()


@pytest.mark.skipif(os.name == "nt", reason="POSIX directory-identity replacement regression")
def test_posix_directory_guard_detects_path_replacement(tmp_path: Path) -> None:
    guarded = tmp_path / "guarded"
    replacement = tmp_path / "replacement"
    guarded.mkdir()
    replacement.mkdir()

    with sqlite_storage.PrivateDirectoryGuard(guarded) as guard:
        os.replace(replacement, guarded)
        with pytest.raises(OSError, match="identity changed"):
            guard.verify()


def test_database_in_git_worktree_root_is_rejected_before_creation(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    database = tmp_path / "rookery.sqlite3"

    with pytest.raises(OSError, match="dedicated private directory"):
        SQLiteStore(database)

    assert not database.exists()


def test_backup_destination_directory_redirect_is_rejected(
    tmp_path: Path,
    create_directory_redirect,
) -> None:
    source = tmp_path / "state" / "rookery.sqlite3"
    with SQLiteStore(source):
        pass
    redirected = tmp_path / "redirected"
    redirected.mkdir()
    link = tmp_path / "backup-link"
    create_directory_redirect(link, redirected)

    with pytest.raises(OSError, match="filesystem redirect"):
        backup_sqlite_database(source, link / "backup.sqlite3")

    assert list(redirected.iterdir()) == []


def test_database_directory_redirect_is_rejected_before_creation(
    tmp_path: Path,
    create_directory_redirect,
) -> None:
    redirected = tmp_path / "redirected"
    redirected.mkdir()
    link = tmp_path / "state-link"
    create_directory_redirect(link, redirected)

    with pytest.raises(OSError, match="filesystem redirect"):
        SQLiteStore(link / "rookery.sqlite3")

    assert list(redirected.iterdir()) == []


@pytest.mark.parametrize("suffix", ["-wal", "-shm"])
def test_database_sidecar_redirect_is_rejected_before_wal_open(
    tmp_path: Path,
    suffix: str,
) -> None:
    database = tmp_path / "state" / "rookery.sqlite3"
    with SQLiteStore(database):
        pass
    unrelated = tmp_path / f"unrelated{suffix}"
    unrelated.write_bytes(b"preserve unrelated sidecar target")
    sidecar = Path(f"{database}{suffix}")
    try:
        sidecar.symlink_to(unrelated)
    except OSError as exc:
        pytest.skip(f"file symlinks unavailable: {exc}")

    with pytest.raises(OSError, match="filesystem redirect"):
        SQLiteStore(database)

    assert unrelated.read_bytes() == b"preserve unrelated sidecar target"
    assert sidecar.is_symlink()


@pytest.mark.parametrize("suffix", ["-wal", "-shm"])
def test_database_sidecar_hard_link_is_rejected_before_wal_open(
    tmp_path: Path,
    suffix: str,
) -> None:
    database = tmp_path / "state" / "rookery.sqlite3"
    with SQLiteStore(database):
        pass
    unrelated = tmp_path / f"unrelated{suffix}"
    unrelated.write_bytes(b"preserve unrelated hard-linked sidecar")
    sidecar = Path(f"{database}{suffix}")
    os.link(unrelated, sidecar)

    with pytest.raises(OSError, match="hard links"):
        SQLiteStore(database)

    assert unrelated.read_bytes() == b"preserve unrelated hard-linked sidecar"
    assert sidecar.read_bytes() == b"preserve unrelated hard-linked sidecar"


def test_backup_publication_race_preserves_competing_file(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "state.sqlite3"
    destination = tmp_path / "backups" / "state.sqlite3"
    with SQLiteStore(source):
        pass

    def lose_publication_race(_source: Path, contested: Path) -> None:
        contested.write_bytes(b"competing backup")
        raise FileExistsError("destination appeared during publication")

    monkeypatch.setattr(sqlite_storage.os, "link", lose_publication_race)

    with pytest.raises(FileExistsError, match="destination appeared"):
        backup_sqlite_database(source, destination)

    assert destination.read_bytes() == b"competing backup"
    assert list(destination.parent.glob(".rookery-backup-*.tmp")) == []


def test_backup_post_link_replacement_is_not_reported_or_deleted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "state" / "rookery.sqlite3"
    destination = tmp_path / "backups" / "rookery.sqlite3"
    with SQLiteStore(source):
        pass
    original_link = sqlite_storage.os.link

    def replace_after_link(temporary: Path, published: Path) -> None:
        original_link(temporary, published)
        published.unlink()
        published.write_bytes(b"competing backup")

    monkeypatch.setattr(sqlite_storage.os, "link", replace_after_link)

    with pytest.raises(OSError):
        backup_sqlite_database(source, destination)

    assert destination.read_bytes() == b"competing backup"
    assert list(destination.parent.glob(".rookery-backup-*.tmp")) == []


@pytest.mark.skipif(os.name == "nt", reason="POSIX guarded-hardening replacement regression")
def test_backup_guarded_hardening_rejects_and_preserves_post_link_competitor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "state" / "rookery.sqlite3"
    destination = tmp_path / "backups" / "rookery.sqlite3"
    with SQLiteStore(source):
        pass
    original_harden = sqlite_storage.RegularFileGuard.harden

    def replace_before_harden(guard: sqlite_storage.RegularFileGuard) -> None:
        if guard.path == destination:
            destination.unlink()
            destination.write_bytes(b"competing backup")
        original_harden(guard)

    monkeypatch.setattr(sqlite_storage.RegularFileGuard, "harden", replace_before_harden)

    with pytest.raises(OSError, match="identity changed"):
        backup_sqlite_database(source, destination)

    assert destination.read_bytes() == b"competing backup"
    assert list(destination.parent.glob(".rookery-backup-*.tmp")) == []


def test_backup_flushes_snapshot_and_publication_metadata_before_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "state" / "rookery.sqlite3"
    destination = tmp_path / "backups" / "rookery.sqlite3"
    with SQLiteStore(source):
        pass
    events: list[tuple[str, str]] = []
    original_file_sync = sqlite_storage.RegularFileGuard.sync
    original_directory_sync = sqlite_storage.PrivateDirectoryGuard.sync
    original_link = sqlite_storage.os.link

    def record_file_sync(guard: sqlite_storage.RegularFileGuard) -> None:
        events.append(("file-sync", guard.path.name))
        original_file_sync(guard)

    def record_directory_sync(guard: sqlite_storage.PrivateDirectoryGuard) -> None:
        events.append(("directory-sync", guard.path.name))
        original_directory_sync(guard)

    def record_link(temporary: Path, published: Path) -> None:
        events.append(("link", published.name))
        original_link(temporary, published)

    monkeypatch.setattr(sqlite_storage.RegularFileGuard, "sync", record_file_sync)
    monkeypatch.setattr(sqlite_storage.PrivateDirectoryGuard, "sync", record_directory_sync)
    monkeypatch.setattr(sqlite_storage.os, "link", record_link)

    backup_sqlite_database(source, destination)

    link_index = events.index(("link", destination.name))
    assert events[link_index - 1][0] == "file-sync"
    assert events[link_index + 1] == ("directory-sync", destination.parent.name)
    assert events.count(("directory-sync", destination.parent.name)) == 3
    assert events[-2:] == [
        ("file-sync", destination.name),
        ("directory-sync", destination.parent.name),
    ]


@pytest.mark.parametrize("failure", ["file", "directory"])
def test_backup_durability_failure_publishes_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    source = tmp_path / "state" / "rookery.sqlite3"
    destination = tmp_path / "backups" / "rookery.sqlite3"
    with SQLiteStore(source):
        pass

    if failure == "file":

        def fail_file_sync(guard: sqlite_storage.RegularFileGuard) -> None:
            if guard.path.name.startswith(".rookery-backup-"):
                raise OSError("injected file synchronization failure")

        monkeypatch.setattr(sqlite_storage.RegularFileGuard, "sync", fail_file_sync)
    else:

        def fail_directory_sync(_guard: sqlite_storage.PrivateDirectoryGuard) -> None:
            raise OSError("injected directory synchronization failure")

        monkeypatch.setattr(sqlite_storage.PrivateDirectoryGuard, "sync", fail_directory_sync)

    with pytest.raises(OSError, match="synchronization failure"):
        backup_sqlite_database(source, destination)

    assert not destination.exists()
    assert list(destination.parent.glob(".rookery-backup-*.tmp")) == []


@pytest.mark.skipif(os.name == "nt", reason="POSIX final-directory-sync race regression")
def test_backup_final_directory_sync_rejects_and_preserves_competitor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "state" / "rookery.sqlite3"
    destination = tmp_path / "backups" / "rookery.sqlite3"
    with SQLiteStore(source):
        pass
    original_sync = sqlite_storage.PrivateDirectoryGuard.sync
    sync_calls = 0

    def replace_during_final_sync(guard: sqlite_storage.PrivateDirectoryGuard) -> None:
        nonlocal sync_calls
        sync_calls += 1
        original_sync(guard)
        if sync_calls == 3:
            destination.unlink()
            destination.write_bytes(b"competing backup")

    monkeypatch.setattr(
        sqlite_storage.PrivateDirectoryGuard,
        "sync",
        replace_during_final_sync,
    )

    with pytest.raises(OSError, match="identity changed"):
        backup_sqlite_database(source, destination)

    assert destination.read_bytes() == b"competing backup"
    assert list(destination.parent.glob(".rookery-backup-*.tmp")) == []


def test_backup_wraps_malformed_source_and_cleans_temporary_file(tmp_path) -> None:
    source = tmp_path / "not-sqlite.sqlite3"
    destination = tmp_path / "backups" / "state.sqlite3"
    source.write_bytes(b"not a SQLite database")

    with pytest.raises(RuntimeError, match="consistent SQLite backup"):
        backup_sqlite_database(source, destination)

    assert not destination.exists()
    assert list(destination.parent.glob(".rookery-backup-*.tmp")) == []


@pytest.mark.parametrize(
    "index_name",
    [
        "ix_adapter_action_scopes_target_facet",
        "ix_configured_scopes_object",
        "ix_relationships_object_predicate",
        "ix_refresh_intent_scopes_claim_order",
        "ix_adapter_actions_claim_order",
        "ix_refresh_intent_scopes_deferred_due",
        "ix_refresh_intent_scopes_lease_due",
        "ix_adapter_actions_lease_due",
        "ix_adapter_actions_retry_due",
    ],
)
def test_reopen_repairs_missing_runtime_forced_indexes(tmp_path, index_name: str) -> None:
    path = tmp_path / "runtime-index.sqlite3"
    with SQLiteStore(path) as store:
        store._connection.execute(f"DROP INDEX {index_name}")

    with SQLiteStore(path) as reopened:
        indexes = {
            row["name"]
            for row in reopened._connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            ).fetchall()
        }
        assert index_name in indexes
        assert reopened.list_latest_facet_actions((str(uuid4()),)) == ()


@pytest.mark.parametrize(
    ("index_name", "wrong_statement", "expected_columns"),
    [
        (
            "ix_adapter_action_scopes_target_facet",
            "CREATE INDEX ix_adapter_action_scopes_target_facet "
            "ON adapter_action_scopes (action_id)",
            ("target_kind", "target_id", "facet", "action_id"),
        ),
        (
            "ix_configured_scopes_object",
            "CREATE INDEX ix_configured_scopes_object ON configured_scopes (scope_id)",
            ("object_id", "scope_id"),
        ),
        (
            "ix_relationships_object_predicate",
            "CREATE INDEX ix_relationships_object_predicate ON relationships (subject_id)",
            ("object_id", "predicate", "presence", "subject_id"),
        ),
        (
            "ix_refresh_intent_scopes_claim_order",
            "CREATE INDEX ix_refresh_intent_scopes_claim_order "
            "ON refresh_intent_scopes (intent_scope_id)",
            ("queue_priority", "queue_requested_at", "intent_scope_id"),
        ),
        (
            "ix_adapter_actions_claim_order",
            "CREATE INDEX ix_adapter_actions_claim_order ON adapter_actions (action_id)",
            ("adapter_key", "record_created_at", "action_id"),
        ),
        (
            "ix_refresh_intent_scopes_claim_order",
            "CREATE INDEX ix_refresh_intent_scopes_claim_order "
            "ON refresh_intent_scopes "
            "(queue_priority DESC, queue_requested_at, intent_scope_id) "
            "WHERE state = 'deferred'",
            ("queue_priority", "queue_requested_at", "intent_scope_id"),
        ),
        (
            "ix_adapter_actions_claim_order",
            "CREATE INDEX ix_adapter_actions_claim_order "
            "ON adapter_actions (adapter_key, record_created_at, action_id) "
            "WHERE state = 'retry_wait'",
            ("adapter_key", "record_created_at", "action_id"),
        ),
        (
            "ix_refresh_intent_scopes_deferred_due",
            "CREATE INDEX ix_refresh_intent_scopes_deferred_due "
            "ON refresh_intent_scopes (intent_scope_id)",
            ("eligible_at", "intent_scope_id"),
        ),
        (
            "ix_refresh_intent_scopes_lease_due",
            "CREATE INDEX ix_refresh_intent_scopes_lease_due "
            "ON refresh_intent_scopes (intent_scope_id)",
            ("leased_until", "intent_scope_id"),
        ),
        (
            "ix_adapter_actions_lease_due",
            "CREATE INDEX ix_adapter_actions_lease_due ON adapter_actions (action_id)",
            ("adapter_key", "leased_until", "action_id"),
        ),
        (
            "ix_adapter_actions_retry_due",
            "CREATE INDEX ix_adapter_actions_retry_due ON adapter_actions (action_id)",
            ("adapter_key", "retry_at", "action_id"),
        ),
    ],
)
def test_reopen_replaces_mismatched_runtime_forced_indexes(
    tmp_path,
    index_name: str,
    wrong_statement: str,
    expected_columns: tuple[str, ...],
) -> None:
    path = tmp_path / "runtime-index.sqlite3"
    with SQLiteStore(path) as store:
        store._connection.execute(f"DROP INDEX {index_name}")
        store._connection.execute(wrong_statement)

    with SQLiteStore(path) as reopened:
        columns = tuple(
            row["name"]
            for row in reopened._connection.execute(
                "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
                (index_name,),
            ).fetchall()
        )
        assert columns == expected_columns
        assert reopened.list_latest_facet_actions((str(uuid4()),)) == ()


def test_queue_claim_plans_use_ordered_partial_indexes_without_temp_sort(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "claim-plans.sqlite3")
    now_text = "2026-08-24T12:00:00.000000Z"
    intent_plan = store._connection.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT scope.intent_scope_id
        FROM refresh_intent_scopes AS scope
            INDEXED BY ix_refresh_intent_scopes_claim_order
        JOIN refresh_intents AS intent ON intent.intent_id = scope.intent_id
        WHERE scope.state = 'queued'
        ORDER BY scope.queue_priority DESC, scope.queue_requested_at, scope.intent_scope_id
        LIMIT 1
        """
    ).fetchall()
    action_plan = store._connection.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT * FROM adapter_actions INDEXED BY ix_adapter_actions_claim_order
        WHERE adapter_key = ? AND state = 'ready'
        ORDER BY record_created_at, action_id
        LIMIT 1
        """,
        ("databricks",),
    ).fetchall()
    intent_deferred_null_plan = store._connection.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT intent_scope_id FROM refresh_intent_scopes
            INDEXED BY ix_refresh_intent_scopes_deferred_due
        WHERE state = 'deferred' AND eligible_at IS NULL
        """
    ).fetchall()
    intent_deferred_due_plan = store._connection.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT intent_scope_id FROM refresh_intent_scopes
            INDEXED BY ix_refresh_intent_scopes_deferred_due
        WHERE state = 'deferred' AND eligible_at <= ?
        """,
        (now_text,),
    ).fetchall()
    intent_lease_plan = store._connection.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT intent_scope_id FROM refresh_intent_scopes
            INDEXED BY ix_refresh_intent_scopes_lease_due
        WHERE state = 'leased' AND leased_until <= ?
        """,
        (now_text,),
    ).fetchall()
    action_lease_plan = store._connection.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT action_id FROM adapter_actions INDEXED BY ix_adapter_actions_lease_due
        WHERE adapter_key = ? AND state IN ('leased', 'running') AND leased_until <= ?
        """,
        ("databricks", now_text),
    ).fetchall()
    action_retry_plan = store._connection.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT action_id FROM adapter_actions INDEXED BY ix_adapter_actions_retry_due
        WHERE adapter_key = ? AND state = 'retry_wait' AND retry_at <= ?
        """,
        ("databricks", now_text),
    ).fetchall()

    assert any("ix_refresh_intent_scopes_claim_order" in row[3] for row in intent_plan)
    assert any("ix_adapter_actions_claim_order" in row[3] for row in action_plan)
    assert any(
        "ix_refresh_intent_scopes_deferred_due" in row[3] and "eligible_at=?" in row[3]
        for row in intent_deferred_null_plan
    )
    assert any(
        "ix_refresh_intent_scopes_deferred_due" in row[3] and "eligible_at<?" in row[3]
        for row in intent_deferred_due_plan
    )
    assert any("ix_refresh_intent_scopes_lease_due" in row[3] for row in intent_lease_plan)
    assert any("ix_adapter_actions_lease_due" in row[3] for row in action_lease_plan)
    assert any("ix_adapter_actions_retry_due" in row[3] for row in action_retry_plan)
    assert all(
        "TEMP B-TREE" not in row[3]
        for row in (
            *intent_plan,
            *action_plan,
            *intent_deferred_null_plan,
            *intent_deferred_due_plan,
            *intent_lease_plan,
            *action_lease_plan,
            *action_retry_plan,
        )
    )


def test_runtime_index_repair_failure_rolls_back_drop(tmp_path, monkeypatch) -> None:
    path = tmp_path / "runtime-index.sqlite3"
    index_name = "ix_adapter_action_scopes_target_facet"
    wrong_statement = (
        "CREATE INDEX ix_adapter_action_scopes_target_facet ON adapter_action_scopes (action_id)"
    )
    with SQLiteStore(path) as store:
        store._connection.execute(f"DROP INDEX {index_name}")
        store._connection.execute(wrong_statement)

    def fail_recreation(_connection: sqlite3.Connection, _statement: str) -> None:
        raise RuntimeError("injected runtime index repair failure")

    monkeypatch.setattr(
        SQLiteStore,
        "_execute_required_runtime_index_statement",
        staticmethod(fail_recreation),
    )
    with pytest.raises(RuntimeError, match="injected runtime index repair failure"):
        SQLiteStore(path)

    check = sqlite3.connect(path)
    try:
        columns = tuple(
            row[2] for row in check.execute(f"PRAGMA index_info({index_name})").fetchall()
        )
    finally:
        check.close()
    assert columns == ("action_id",)


def test_store_close_is_idempotent(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")

    store.close()
    store.close()


def test_existing_v1_database_upgrades_scope_capability_columns(tmp_path) -> None:
    path = tmp_path / "legacy.sqlite3"
    legacy = sqlite3.connect(path)
    legacy.executescript(
        "CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL);"
    )
    migration = Path("src/async_api_view/storage/migrations/0001_initial.sql").read_text(
        encoding="utf-8"
    )
    legacy.executescript(migration)
    legacy.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
        ("0001_initial", "2026-08-24T12:00:00Z"),
    )
    legacy.commit()
    legacy.close()

    with SQLiteStore(path) as store:
        table_columns = (
            store._connection.execute("PRAGMA table_info(refresh_credit)").fetchall(),
            store._connection.execute("PRAGMA table_info(refresh_intent_scopes)").fetchall(),
            store._connection.execute("PRAGMA table_info(adapter_action_scopes)").fetchall(),
        )
        for columns in table_columns:
            assert "capability_key" in {column[1] for column in columns}


def test_override_identity_migration_keeps_latest_null_facet_row(tmp_path) -> None:
    path = tmp_path / "legacy-overrides.sqlite3"
    scope_id = str(uuid4())
    with SQLiteStore(path) as store:
        store._connection.execute("DROP INDEX ux_refresh_overrides_identity")
        store._connection.execute(
            "DELETE FROM schema_migrations WHERE version = '0011_refresh_override_identity'"
        )
        store._connection.executemany(
            """
            INSERT INTO refresh_overrides (
                level, scope_id, facet, interval_seconds, record_updated_at
            ) VALUES ('system', ?, NULL, ?, ?)
            """,
            (
                (scope_id, 28_800, "2026-08-24T12:00:00.000000Z"),
                (scope_id, 14_400, "2026-08-24T13:00:00.000000Z"),
            ),
        )

    with SQLiteStore(path) as migrated:
        rows = migrated._connection.execute(
            """
            SELECT interval_seconds
            FROM refresh_overrides
            WHERE level = 'system' AND scope_id = ? AND facet IS NULL
            """,
            (scope_id,),
        ).fetchall()
        indexes = {
            row["name"]
            for row in migrated._connection.execute(
                "SELECT name FROM sqlite_schema WHERE type = 'index'"
            ).fetchall()
        }
        assert [row["interval_seconds"] for row in rows] == [14_400]
        assert "ux_refresh_overrides_identity" in indexes


def test_concurrent_store_initialization_serializes_migrations(tmp_path) -> None:
    path = tmp_path / "concurrent.sqlite3"
    workers = 8
    barrier = threading.Barrier(workers)

    def open_store() -> tuple[str, ...]:
        barrier.wait(timeout=5)
        with SQLiteStore(path) as store:
            rows = store._connection.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall()
            return tuple(row[0] for row in rows)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        versions = tuple(executor.map(lambda _index: open_store(), range(workers)))

    expected = (
        "0001_initial",
        "0002_refresh_scope_capability_key",
        "0003_observation_batch_digest",
        "0004_configured_system_identities",
        "0005_operational_event_recency",
        "0006_relationship_navigation",
        "0007_operational_event_filters",
        "0008_action_activity",
        "0009_facet_action_status",
        "0010_dashboard_active_action",
        "0011_refresh_override_identity",
        "0012_deferred_scope_policy_indexes",
        "0013_capability_coverage_policy",
        "0014_coverage_policy_initialization",
        "0015_relationship_coverage_watermarks",
        "0016_projection_received_order",
        "0017_queue_claim_order",
        "0018_action_state_projections",
        "0019_web_cursor_indexes",
        "0020_retired_authorities",
        "0021_intent_aggregate_indexes",
        "0022_observation_receipt_order",
        "0023_time_authority",
    )
    assert versions == (expected,) * workers


def test_configuration_reconciliation_cannot_enable_another_system_kind(tmp_path) -> None:
    with SQLiteStore(tmp_path / "state.sqlite3") as store:
        other = SystemBootstrapService(store).create_system(
            display_name="other",
            system_kind="server.host",
            enabled=False,
            now=NOW,
        )

        store.reconcile_configured_resources(
            system_kind="databricks.workspace",
            system_ids=(other.system_id,),
            connection_binding_ids=(),
            capability_binding_ids=(),
            scope_ids=(),
            now=NOW,
        )

        unchanged = store.get_system(other.system_id)
        assert unchanged is not None and not unchanged.enabled


def test_null_facet_refresh_override_replaces_prior_value(tmp_path) -> None:
    with SQLiteStore(tmp_path / "state.sqlite3") as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
        )
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet="membership",
        )
        store.set_refresh_override(
            RefreshIntervalOverride("system", seeded.system.system_id, timedelta(hours=8)),
            now=NOW,
        )
        store.set_refresh_override(
            RefreshIntervalOverride("system", seeded.system.system_id, timedelta(hours=4)),
            now=NOW + timedelta(seconds=1),
        )

        rows = store._connection.execute(
            """
            SELECT interval_seconds
            FROM refresh_overrides
            WHERE level = 'system' AND scope_id = ? AND facet IS NULL
            """,
            (seeded.system.system_id,),
        ).fetchall()
        assert [row["interval_seconds"] for row in rows] == [14_400]
        assert store.effective_interval(scope) == timedelta(hours=4)


def test_migration_failure_rolls_back_schema_and_ledger(tmp_path, monkeypatch) -> None:
    path = tmp_path / "legacy.sqlite3"
    legacy = sqlite3.connect(path)
    legacy.executescript(
        "CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL);"
    )
    legacy.executescript(
        Path("src/async_api_view/storage/migrations/0001_initial.sql").read_text(encoding="utf-8")
    )
    legacy.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
        ("0001_initial", "2026-08-24T12:00:00Z"),
    )
    legacy.commit()
    legacy.close()

    original = SQLiteStore._execute_migration_statement

    def fail_mid_0002(store: SQLiteStore, statement: str) -> None:
        if "refresh_intent_scopes" in statement:
            raise RuntimeError("injected migration failure")
        original(store, statement)

    monkeypatch.setattr(SQLiteStore, "_execute_migration_statement", fail_mid_0002)
    with pytest.raises(RuntimeError, match="injected migration failure"):
        SQLiteStore(path)

    check = sqlite3.connect(path)
    assert [row[0] for row in check.execute("SELECT version FROM schema_migrations")] == [
        "0001_initial"
    ]
    assert "capability_key" not in {
        row[1] for row in check.execute("PRAGMA table_info(refresh_credit)")
    }
    assert check.execute("PRAGMA application_id").fetchone()[0] == 0
    check.close()

    monkeypatch.setattr(SQLiteStore, "_execute_migration_statement", original)
    with SQLiteStore(path) as recovered:
        assert recovered._connection.execute("PRAGMA application_id").fetchone()[0] == 0x524F4F4B
        assert "capability_key" in {
            row[1] for row in recovered._connection.execute("PRAGMA table_info(refresh_credit)")
        }


def test_reopen_repairs_legacy_partial_0002_before_recording_ledger(tmp_path) -> None:
    path = tmp_path / "legacy-partial.sqlite3"
    legacy = sqlite3.connect(path)
    legacy.executescript(
        "CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL);"
    )
    legacy.executescript(
        Path("src/async_api_view/storage/migrations/0001_initial.sql").read_text(encoding="utf-8")
    )
    legacy.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
        ("0001_initial", "2026-08-24T12:00:00Z"),
    )
    legacy.execute("ALTER TABLE refresh_credit ADD COLUMN capability_key TEXT")
    legacy.commit()
    legacy.close()

    with SQLiteStore(path) as recovered:
        versions = recovered._connection.execute("SELECT version FROM schema_migrations").fetchall()
        assert [row[0] for row in versions] == [
            "0001_initial",
            "0002_refresh_scope_capability_key",
            "0003_observation_batch_digest",
            "0004_configured_system_identities",
            "0005_operational_event_recency",
            "0006_relationship_navigation",
            "0007_operational_event_filters",
            "0008_action_activity",
            "0009_facet_action_status",
            "0010_dashboard_active_action",
            "0011_refresh_override_identity",
            "0012_deferred_scope_policy_indexes",
            "0013_capability_coverage_policy",
            "0014_coverage_policy_initialization",
            "0015_relationship_coverage_watermarks",
            "0016_projection_received_order",
            "0017_queue_claim_order",
            "0018_action_state_projections",
            "0019_web_cursor_indexes",
            "0020_retired_authorities",
            "0021_intent_aggregate_indexes",
            "0022_observation_receipt_order",
            "0023_time_authority",
        ]
        for table in ("refresh_credit", "refresh_intent_scopes", "adapter_action_scopes"):
            if table == "refresh_credit":
                columns = recovered._connection.execute(
                    "PRAGMA table_info(refresh_credit)"
                ).fetchall()
            elif table == "refresh_intent_scopes":
                columns = recovered._connection.execute(
                    "PRAGMA table_info(refresh_intent_scopes)"
                ).fetchall()
            else:
                columns = recovered._connection.execute(
                    "PRAGMA table_info(adapter_action_scopes)"
                ).fetchall()
            assert "capability_key" in {column[1] for column in columns}


def test_restart_read_helpers_expose_scopes_and_catalogs_root_without_secrets(tmp_path) -> None:
    path = tmp_path / "state.sqlite3"
    with SQLiteStore(path) as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local",
            profile="DEFAULT",
            workspace_root="/Shared",
            enabled_capability_keys=(
                "databricks.workspace.children.read",
                "databricks.uc.catalogs.read",
            ),
            now=NOW,
        )
        assert seeded.unity_catalog_root_object_id is not None
        assert seeded.unity_catalog_root_scope is not None
        repeated = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local",
            profile="DEFAULT",
            workspace_root="/Shared",
            enabled_capability_keys=(
                "databricks.workspace.children.read",
                "databricks.uc.catalogs.read",
            ),
            system_id=seeded.system.system_id,
            now=NOW,
        )
        assert repeated.unity_catalog_root_object_id == seeded.unity_catalog_root_object_id
        assert repeated.unity_catalog_root_scope == seeded.unity_catalog_root_scope
        assert len(store.list_configured_scopes(system_id=seeded.system.system_id)) == 2
        assert len(store.list_objects(system_id=seeded.system.system_id)) == 2
        assert {
            capability.capability_key
            for capability in store.list_capability_bindings(system_id=seeded.system.system_id)
        } == {"databricks.workspace.children.read", "databricks.uc.catalogs.read"}
        assert (
            store.list_connection_bindings(system_id=seeded.system.system_id)[0].secret_reference
            is None
        )

    with SQLiteStore(path) as reopened:
        scopes = reopened.list_configured_scopes()
        assert {scope.display_name for scope in scopes} == {"/Shared", "Unity Catalog catalogs"}
        assert reopened.get_object_sync(seeded.unity_catalog_root_object_id) is not None


def test_object_page_search_escapes_wildcards_and_bounds_results(tmp_path) -> None:
    with SQLiteStore(tmp_path / "state.sqlite3") as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/", now=NOW
        )
        for index, name in enumerate(
            ("100% real", "under_score", "back\\slash", "ordinary", "other")
        ):
            store.upsert_object(
                RemoteObject(
                    object_id=uuid4(),
                    system_id=seeded.system.system_id,
                    object_type="file",
                    object_type_version="1",
                    source_kind="databricks.workspace.file",
                    external_key=f"workspace-id:{index}",
                    display_name=name,
                    presence=PresenceState.PRESENT,
                    first_seen_at=NOW,
                    last_seen_at=NOW,
                )
            )

        assert store.count_objects(query="100%") == 1
        assert [
            item.display_name
            for item in store.list_objects_page(offset=0, limit=10, query="under_")
        ] == ["under_score"]
        assert store.count_objects(query="back\\") == 1
        assert len(store.list_objects_page(offset=1, limit=2)) == 2
        first = store.list_objects_after(
            after_name=None,
            after_id=None,
            limit=3,
        )
        second = store.list_objects_after(
            after_name=None,
            after_id=first[-1].object_id,
            limit=10,
        )
        assert len(first) == 3
        assert len({item.object_id for item in (*first, *second)}) == 6
        plan = store._connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT * FROM remote_objects
            WHERE object_id > ?
            ORDER BY object_id
            LIMIT ?
            """,
            (first[-1].object_id, 10),
        ).fetchall()
        assert any("sqlite_autoindex_remote_objects_1" in row[3] for row in plan)
        assert not any("TEMP B-TREE" in row[3] for row in plan)
        filtered_first = store.list_objects_after(
            after_name=None,
            after_id=None,
            limit=1,
            query="o",
        )
        filtered_second = store.list_objects_after(
            after_name=filtered_first[-1].display_name,
            after_id=filtered_first[-1].object_id,
            limit=2,
            query="o",
        )
        assert [item.display_name for item in (*filtered_first, *filtered_second)] == [
            "ordinary",
            "other",
        ]
        filtered_plan = store._connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT * FROM remote_objects
            WHERE display_name LIKE ? ESCAPE '\' COLLATE NOCASE
              AND (display_name COLLATE NOCASE, object_id) > (? COLLATE NOCASE, ?)
            ORDER BY display_name COLLATE NOCASE, object_id
            LIMIT ?
            """,
            (
                "o%",
                filtered_first[-1].display_name,
                filtered_first[-1].object_id,
                2,
            ),
        ).fetchall()
        assert any("ix_remote_objects_display_cursor" in row[3] for row in filtered_plan)
        assert not any("TEMP B-TREE" in row[3] for row in filtered_plan)


def test_invalid_bootstrap_capability_does_not_leave_partial_state(tmp_path) -> None:
    with SQLiteStore(tmp_path / "state.sqlite3") as store:
        service = SystemBootstrapService(store)

        with pytest.raises(ValueError, match="unsupported Databricks capability"):
            service.configure_databricks_workspace(
                display_name="partial",
                profile="DEFAULT",
                workspace_root="/",
                enabled_capability_keys=("bad.read",),
            )

        assert store.list_systems() == ()


def test_bootstrap_settings_cannot_override_profile_or_workspace_root(tmp_path) -> None:
    with (
        SQLiteStore(tmp_path / "state.sqlite3") as store,
        pytest.raises(ValueError, match="reserved settings"),
    ):
        SystemBootstrapService(store).configure_databricks_workspace(
            display_name="reserved",
            profile="DEFAULT",
            workspace_root="/",
            non_secret_settings={"profile": "OTHER"},
        )


def test_capability_version_contract_and_coverage_policy_are_immutable(tmp_path) -> None:
    with SQLiteStore(tmp_path / "state.sqlite3") as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
        )
        capability = store.list_capability_bindings(
            connection_binding_id=seeded.connection_binding_id
        )[0]

        with pytest.raises(ValueError, match="coverage policy requires a version change"):
            store.upsert_capability_binding(
                replace(capability, coverage_policies=()),
                now=NOW + timedelta(seconds=1),
            )
        with pytest.raises(ValueError, match="version contract is immutable"):
            store.upsert_capability_binding(
                replace(capability, capability_key="different.read"),
                now=NOW + timedelta(seconds=1),
            )
        store._connection.execute(
            """
            UPDATE capability_bindings
            SET coverage_policies_json = '[]', coverage_policy_initialized = 0
            WHERE capability_binding_id = ?
            """,
            (capability.capability_binding_id,),
        )
        store.upsert_capability_binding(capability, now=NOW + timedelta(seconds=2))
        hydrated = store._connection.execute(
            """
            SELECT coverage_policies_json, coverage_policy_initialized
            FROM capability_bindings WHERE capability_binding_id = ?
            """,
            (capability.capability_binding_id,),
        ).fetchone()
        assert hydrated["coverage_policy_initialized"] == 1
        assert hydrated["coverage_policies_json"] != "[]"

        content_seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="content",
            profile="CONTENT",
            workspace_root="/Content",
            enabled_capability_keys=("databricks.workspace.content.read",),
            now=NOW,
        )
        content_capability = store.list_capability_bindings(
            connection_binding_id=content_seeded.connection_binding_id
        )[0]
        assert content_capability.coverage_policies == ()
        with pytest.raises(ValueError, match="coverage policy requires a version change"):
            store.upsert_capability_binding(
                replace(
                    content_capability,
                    coverage_policies=(
                        CapabilityCoveragePolicy(
                            TargetKind.OBJECT,
                            "content",
                            RefreshCoverage.FACET,
                            CollectionCoverage.COMPLETE,
                        ),
                    ),
                ),
                now=NOW + timedelta(seconds=1),
            )


def test_failed_logical_action_anchors_cooldown_and_creates_one_event(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3", clock=lambda: NOW)
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
        coverage=RefreshCoverage.FACET,
    )
    action = AdapterAction(
        action_id=uuid4(),
        correlation_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        capability_key="databricks.workspace.children.read",
        capability_version="1",
        target=scope.target,
        requested_scopes=(scope,),
    )
    run(store.enqueue(action))
    lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=NOW))
    assert lease is not None
    started_at = NOW
    run(
        store.mark_running(
            action_id=action.action_id, lease_id=lease.lease_id, started_at=started_at
        )
    )
    completion = ActionCompletion(
        action_id=action.action_id,
        outcome=ActionOutcome.FAILED,
        completed_at=NOW + timedelta(seconds=30),
        error_class=ErrorClass.CONNECTION_TIMEOUT,
        redacted_diagnostic="token=do-not-persist",
    )
    run(store.complete_action(completion, lease_id=lease.lease_id))
    with pytest.raises(ValueError, match="lease"):
        run(store.complete_action(completion, lease_id=lease.lease_id))

    cooldown_statements: list[str] = []
    store._connection.set_trace_callback(cooldown_statements.append)
    try:
        policy = store.scope_policy_state(scope)
    finally:
        store._connection.set_trace_callback(None)
    assert policy.latest_targeted_action_started_at == started_at
    assert (
        store._connection.execute("SELECT COUNT(*) FROM action_scope_cooldown").fetchone()[0] == 1
    )
    assert not any("adapter_action" in statement for statement in cooldown_statements)
    events = store.list_operational_events(alertable_only=True)
    assert len(events) == 1
    assert events[0].event_type == "refresh.action.failed"
    assert "do-not-persist" not in events[0].redacted_summary


def test_legacy_mark_running_rejects_exact_lease_expiry(tmp_path) -> None:
    current_time = [NOW]
    with SQLiteStore(tmp_path / "state.sqlite3", clock=lambda: current_time[0]) as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
        )
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet="membership",
        )
        action = AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            capability_key="databricks.workspace.children.read",
            capability_version="1",
            target=scope.target,
            requested_scopes=(scope,),
        )
        run(store.enqueue(action))
        lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=NOW))
        assert lease is not None
        current_time[0] = lease.leased_until

        with pytest.raises(ValueError, match="lease is not valid"):
            run(
                store.mark_running(
                    action_id=action.action_id,
                    lease_id=lease.lease_id,
                    started_at=lease.leased_until,
                )
            )

        with pytest.raises(ActionLeaseLost, match="lease is no longer current"):
            run(
                store.heartbeat(
                    action_id=action.action_id,
                    lease_id=lease.lease_id,
                    worker_id="worker",
                    at=lease.leased_until,
                )
            )

        assert store.get_stored_action(action.action_id).state.value == "leased"


def test_action_leases_reject_stale_lifecycle_times_and_reclaim_with_store_clock(tmp_path) -> None:
    def leased_action(name: str):
        current_time = [NOW]
        store = SQLiteStore(tmp_path / f"{name}.sqlite3", clock=lambda: current_time[0])
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
        )
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet="membership",
        )
        action = AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            capability_key="databricks.workspace.children.read",
            capability_version="1",
            target=scope.target,
            requested_scopes=(scope,),
        )
        run(store.enqueue(action))
        lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=NOW))
        assert lease is not None
        return store, action, lease, current_time

    authority_store, authority_action, authority_lease, _authority_time = leased_action(
        "caller-start-authority"
    )
    run(
        authority_store.mark_running(
            action_id=authority_action.action_id,
            lease_id=authority_lease.lease_id,
            started_at=NOW + timedelta(days=30),
        )
    )
    assert authority_store.get_stored_action(authority_action.action_id).started_at == NOW
    assert (
        authority_store.scope_policy_state(
            authority_action.requested_scopes[0]
        ).latest_targeted_action_started_at
        == NOW
    )
    authority_store.close()

    marked_store, marked_action, marked_lease, marked_time = leased_action("stale-mark")
    marked_time[0] = marked_lease.leased_until
    with pytest.raises(ValueError, match="lease is not valid"):
        run(
            marked_store.mark_running(
                action_id=marked_action.action_id,
                lease_id=marked_lease.lease_id,
                started_at=NOW,
            )
        )
    replacement = run(
        marked_store.lease_next(adapter_key="databricks", worker_id="replacement", now=NOW)
    )
    assert replacement is not None and replacement.lease_id != marked_lease.lease_id
    marked_store.close()

    heartbeat_store, heartbeat_action, heartbeat_lease, heartbeat_time = leased_action(
        "stale-heartbeat"
    )
    heartbeat_time[0] = NOW + timedelta(seconds=10)
    run(
        heartbeat_store.heartbeat(
            action_id=heartbeat_action.action_id,
            lease_id=heartbeat_lease.lease_id,
            worker_id="worker",
            at=NOW,
        )
    )
    extended_until = heartbeat_time[0] + timedelta(seconds=60)
    assert (
        heartbeat_store.get_stored_action(heartbeat_action.action_id).leased_until == extended_until
    )
    heartbeat_time[0] = extended_until
    with pytest.raises(ActionLeaseLost, match="lease is no longer current"):
        run(
            heartbeat_store.heartbeat(
                action_id=heartbeat_action.action_id,
                lease_id=heartbeat_lease.lease_id,
                worker_id="worker",
                at=NOW,
            )
        )
    assert heartbeat_store.get_stored_action(heartbeat_action.action_id).leased_until == (
        extended_until
    )
    heartbeat_store.close()

    lifecycle_store, lifecycle_action, lifecycle_lease, lifecycle_time = leased_action(
        "stale-lifecycle"
    )
    run(
        lifecycle_store.mark_running(
            action_id=lifecycle_action.action_id,
            lease_id=lifecycle_lease.lease_id,
            started_at=NOW,
        )
    )
    lifecycle_time[0] = lifecycle_lease.leased_until
    with pytest.raises(ActionLeaseLost, match="lease has expired"):
        run(
            lifecycle_store.record_attempt(
                ActionAttempt(
                    uuid4(),
                    lifecycle_action.action_id,
                    1,
                    NOW,
                    NOW + timedelta(seconds=1),
                ),
                lease_id=lifecycle_lease.lease_id,
            )
        )
    with pytest.raises(ActionLeaseLost, match="lease has expired"):
        run(
            lifecycle_store.complete_action(
                ActionCompletion(
                    action_id=lifecycle_action.action_id,
                    outcome=ActionOutcome.SUCCEEDED,
                    completed_at=NOW,
                ),
                lease_id=lifecycle_lease.lease_id,
            )
        )
    root_object_id = lifecycle_store.list_objects(system_id=lifecycle_action.system_id)[0].object_id
    rejected = run(
        lifecycle_store.ingest(
            ObservationBatch(
                batch_id=uuid4(),
                system_id=lifecycle_action.system_id,
                connection_binding_id=lifecycle_action.connection_binding_id,
                adapter_key=lifecycle_action.adapter_key,
                adapter_version=lifecycle_action.adapter_version,
                action_id=lifecycle_action.action_id,
                observed_at=NOW,
                received_at=NOW,
                facet_observations=(
                    FacetObservation(
                        observation_id=uuid4(),
                        target=ObjectLocator(object_type="folder", object_id=root_object_id),
                        facet="membership",
                        facet_version="1",
                        update_mode=UpdateMode.PATCH,
                        field_coverage=FieldCoverage.PARTIAL,
                        payload={"member_count": 1},
                        field_mask=("member_count",),
                    ),
                ),
            ),
            lease_id=lifecycle_lease.lease_id,
        )
    )
    assert rejected.status.value == "rejected"
    assert lifecycle_store.list_action_attempts(lifecycle_action.action_id) == ()
    assert lifecycle_store.get_stored_action(lifecycle_action.action_id).state.value == "running"
    lifecycle_store.close()

    guard_store, guard_action, guard_lease, guard_time = leased_action("stale-guard")
    guard_time[0] = guard_lease.leased_until
    evaluate = run(
        guard_store.evaluate(
            action_id=guard_action.action_id,
            lease_id=guard_lease.lease_id,
            now=NOW,
        )
    )
    authorize = run(
        guard_store.authorize_start(
            action_id=guard_action.action_id,
            lease_id=guard_lease.lease_id,
            binding_revision="stale-revision",
            now=NOW,
        )
    )
    assert evaluate.reason == authorize.reason == "expired_action_lease"
    assert guard_store.get_stored_action(guard_action.action_id).state.value == "leased"
    guard_store.close()


def test_action_guard_uses_store_clock_for_deadlines_and_origin_expiry(tmp_path) -> None:
    deadline_time = [NOW]
    deadline_store = SQLiteStore(
        tmp_path / "deadline-authority.sqlite3", clock=lambda: deadline_time[0]
    )
    seeded = SystemBootstrapService(deadline_store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
    )
    deadline = NOW + timedelta(seconds=1)
    deadline_action = AdapterAction(
        action_id=uuid4(),
        correlation_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        capability_key="databricks.workspace.children.read",
        capability_version="1",
        target=scope.target,
        requested_scopes=(scope,),
        deadline=deadline,
    )
    run(deadline_store.enqueue(deadline_action))
    deadline_lease = run(
        deadline_store.lease_next(adapter_key="databricks", worker_id="worker", now=NOW)
    )
    assert deadline_lease is not None
    deadline_time[0] = deadline
    deadline_decision = run(
        deadline_store.authorize_start(
            action_id=deadline_action.action_id,
            lease_id=deadline_lease.lease_id,
            binding_revision="stale-caller-revision",
            now=NOW,
        )
    )
    assert deadline_decision.reason == "action_deadline_expired"
    assert deadline_decision.disposition.value == "cancel"
    deadline_stored = deadline_store.get_stored_action(deadline_action.action_id)
    assert deadline_stored is not None
    assert deadline_stored.state.value == "cancelled"
    assert deadline_stored.started_at is None
    deadline_store.close()

    origin_time = [NOW]
    origin_store = SQLiteStore(tmp_path / "origin-authority.sqlite3", clock=lambda: origin_time[0])
    origin_seeded = SystemBootstrapService(origin_store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    origin_scope = RefreshScope(
        system_id=origin_seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, origin_seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
    )
    live = run(
        origin_store.submit_refresh(
            RefreshIntent(
                intent_id=uuid4(),
                idempotency_key=str(uuid4()),
                origin=RefreshOrigin.MANUAL,
                actor_id="local-user",
                scopes=(origin_scope,),
                requested_at=NOW,
            )
        )
    )
    admitted = run(DurableCoordinator(origin_store).run_once(now=NOW))
    assert admitted is not None and admitted.action_id is not None
    expiring = run(
        origin_store.submit_refresh(
            RefreshIntent(
                intent_id=uuid4(),
                idempotency_key=str(uuid4()),
                origin=RefreshOrigin.MANUAL,
                actor_id="local-user",
                scopes=(origin_scope,),
                requested_at=NOW,
                expires_at=NOW + timedelta(seconds=1),
            )
        )
    )
    coalesced = run(DurableCoordinator(origin_store).run_once(now=NOW))
    assert coalesced is not None and coalesced.state.value == "coalesced"
    origin_lease = run(
        origin_store.lease_next(adapter_key="databricks", worker_id="worker", now=NOW)
    )
    assert origin_lease is not None and origin_lease.action.action_id == admitted.action_id
    origin_time[0] = NOW + timedelta(seconds=1)
    origin_decision = run(
        origin_store.evaluate(
            action_id=admitted.action_id,
            lease_id=origin_lease.lease_id,
            now=NOW,
        )
    )
    assert origin_decision.disposition.value == "dispatch"
    assert origin_store.get_stored_action(admitted.action_id).state.value == "leased"
    assert origin_store.list_intent_scopes(expiring.intent_id)[0].state.value == "expired"
    assert origin_store.list_intent_scopes(live.intent_id)[0].state.value == "admitted"
    origin_store.close()


def test_write_headroom_fences_new_intents_admission_and_final_dispatch(tmp_path) -> None:
    current_time = [NOW]
    available = [sqlite_storage.MIN_WRITE_RESERVE_BYTES]
    store = SQLiteStore(
        tmp_path / "headroom.sqlite3",
        clock=lambda: current_time[0],
        available_bytes_probe=lambda: available[0],
    )
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
        capability_key="databricks.workspace.children.read",
    )
    intent = RefreshIntent(
        intent_id=uuid4(),
        idempotency_key=str(uuid4()),
        origin=RefreshOrigin.MANUAL,
        actor_id="local-user",
        scopes=(scope,),
        requested_at=NOW,
    )
    receipt = run(store.submit_refresh(intent))
    available[0] -= 1

    duplicate = run(store.submit_refresh(intent))
    assert duplicate == receipt
    with pytest.raises(sqlite_storage.StorageHeadroomUnavailable, match="write headroom"):
        run(store.submit_refresh(replace(intent, intent_id=uuid4(), idempotency_key=str(uuid4()))))

    deferred = run(DurableCoordinator(store, worker_id="headroom").run_once(now=NOW))
    assert deferred is not None and deferred.state is IntentScopeState.DEFERRED
    assert deferred.reason == "storage_headroom_low"
    assert deferred.eligible_at is not None
    assert store.list_actions() == ()

    store.record_runtime_failure(
        event_type="queue.coordinator.failed",
        summary="coordinator stopped unexpectedly (StorageHeadroomUnavailable)",
        occurred_at=NOW,
    )
    assert len(store.list_operational_events(alertable_only=True)) == 1

    available[0] += 1
    current_time[0] = deferred.eligible_at
    admitted = run(DurableCoordinator(store, worker_id="headroom").run_once(now=current_time[0]))
    assert admitted is not None and admitted.action_id is not None
    lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=current_time[0]))
    binding = store.list_connection_bindings(system_id=seeded.system.system_id)[0]
    assert lease is not None and binding.revision is not None

    available[0] -= 1
    decision = run(
        store.authorize_start(
            action_id=lease.action.action_id,
            lease_id=lease.lease_id,
            binding_revision=binding.revision,
            now=current_time[0],
        )
    )
    assert decision.disposition is GuardDisposition.FAIL
    assert decision.reason == "storage_headroom_low"
    stored = store.get_stored_action(lease.action.action_id)
    assert stored is not None and stored.state is ActionState.RETRY_WAIT
    assert stored.retry_at == current_time[0] + timedelta(seconds=60)


def test_low_headroom_does_not_block_final_authority_cancellation(tmp_path) -> None:
    available = [1 << 40]
    store = SQLiteStore(
        tmp_path / "headroom-cancellation.sqlite3",
        clock=lambda: NOW,
        available_bytes_probe=lambda: available[0],
    )
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
        capability_key="databricks.workspace.children.read",
    )
    receipt = run(
        store.submit_refresh(
            RefreshIntent(
                intent_id=uuid4(),
                idempotency_key=str(uuid4()),
                origin=RefreshOrigin.MANUAL,
                actor_id="local-user",
                scopes=(scope,),
                requested_at=NOW,
            )
        )
    )
    admitted = run(DurableCoordinator(store, worker_id="headroom").run_once(now=NOW))
    assert admitted is not None and admitted.action_id is not None
    lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=NOW))
    binding = store.list_connection_bindings(system_id=seeded.system.system_id)[0]
    assert lease is not None and binding.revision is not None
    store._connection.execute(
        "UPDATE connection_bindings SET enabled = 0 WHERE binding_id = ?",
        (binding.binding_id,),
    )
    available[0] = 0

    decision = run(
        store.authorize_start(
            action_id=lease.action.action_id,
            lease_id=lease.lease_id,
            binding_revision=binding.revision,
            now=NOW,
        )
    )

    assert decision.disposition is GuardDisposition.CANCEL
    assert decision.reason == "binding_disabled"
    stored = store.get_stored_action(lease.action.action_id)
    assert stored is not None and stored.state is ActionState.CANCELLED
    assert store.list_intent_scopes(receipt.intent_id)[0].state is IntentScopeState.ADMITTED


def test_default_store_authority_uses_elapsed_time_across_utc_jumps(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wall_time = [NOW]
    elapsed_time = [100.0]
    monkeypatch.setattr(sqlite_storage, "_now", lambda: wall_time[0])
    monkeypatch.setattr(sqlite_storage.time, "monotonic", lambda: elapsed_time[0])
    store = SQLiteStore(tmp_path / "elapsed-authority.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
        capability_key="databricks.workspace.children.read",
    )
    receipt = run(
        store.submit_refresh(
            RefreshIntent(
                intent_id=uuid4(),
                idempotency_key=str(uuid4()),
                origin=RefreshOrigin.MANUAL,
                actor_id="local-user",
                scopes=(scope,),
                requested_at=NOW,
            )
        )
    )
    first = run(store.lease_next_intent_scope(worker_id="first", now=NOW))
    assert first is not None

    wall_time[0] = NOW + timedelta(days=1)
    elapsed_time[0] += 1
    assert store.authority_time() == NOW + timedelta(seconds=1)
    wall_time[0] = NOW - timedelta(hours=1)
    elapsed_time[0] += 60

    recovered = run(store.lease_next_intent_scope(worker_id="recovered", now=wall_time[0]))
    assert recovered is not None and recovered.intent_scope_id == first.intent_scope_id
    assert recovered.lease_id != first.lease_id
    assert store.list_intent_scopes(receipt.intent_id)[0].state is IntentScopeState.LEASED


def test_restart_restores_actual_mixed_duration_lease_authority(tmp_path) -> None:
    path = tmp_path / "mixed-duration-leases.sqlite3"
    current_time = [NOW]
    store = SQLiteStore(path, clock=lambda: current_time[0])
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
        capability_key="databricks.workspace.children.read",
    )

    def submit(requested_at: datetime) -> str:
        receipt = run(
            store.submit_refresh(
                RefreshIntent(
                    intent_id=uuid4(),
                    idempotency_key=str(uuid4()),
                    origin=RefreshOrigin.MANUAL,
                    actor_id="local-user",
                    scopes=(scope,),
                    requested_at=requested_at,
                )
            )
        )
        return receipt.scope_ids[0]

    long_scope_id = submit(NOW)
    short_scope_id = submit(NOW + timedelta(microseconds=1))
    long_lease = run(
        store.lease_next_intent_scope(
            worker_id="long",
            now=NOW,
            lease_duration=timedelta(minutes=10),
        )
    )
    short_lease = run(
        store.lease_next_intent_scope(
            worker_id="short",
            now=NOW,
            lease_duration=timedelta(seconds=60),
        )
    )
    assert long_lease is not None and long_lease.intent_scope_id == long_scope_id
    assert short_lease is not None and short_lease.intent_scope_id == short_scope_id
    store.close()

    reopened = SQLiteStore(path, clock=lambda: current_time[0])
    assert reopened.authority_time() == NOW
    assert run(reopened.lease_next_intent_scope(worker_id="early", now=NOW)) is None
    current_time[0] = NOW + timedelta(seconds=60)
    recovered = run(reopened.lease_next_intent_scope(worker_id="replacement", now=current_time[0]))
    assert recovered is not None and recovered.intent_scope_id == short_scope_id
    reopened.set_intent_scope_disposition(
        intent_scope_id=long_scope_id,
        lease_id=long_lease.lease_id,
        state=IntentScopeState.SATISFIED,
        reason="still_current",
    )
    assert reopened.list_intent_scopes(long_lease.intent.intent_id)[0].state is (
        IntentScopeState.SATISFIED
    )


def test_intent_scope_leases_use_the_store_clock_for_claims_and_dispositions(tmp_path) -> None:
    current_time = [NOW]
    store = SQLiteStore(tmp_path / "intent-authority.sqlite3", clock=lambda: current_time[0])
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
    )

    def submit() -> str:
        receipt = run(
            store.submit_refresh(
                RefreshIntent(
                    intent_id=uuid4(),
                    idempotency_key=str(uuid4()),
                    origin=RefreshOrigin.MANUAL,
                    actor_id="local-user",
                    scopes=(scope,),
                    requested_at=NOW,
                )
            )
        )
        return receipt.scope_ids[0]

    first_scope_id = submit()
    future_caller_now = NOW + timedelta(hours=1)
    first = run(store.lease_next_intent_scope(worker_id="coordinator", now=future_caller_now))
    assert first is not None and first.intent_scope_id == first_scope_id
    assert first.leased_until == NOW + timedelta(seconds=60)

    current_time[0] = first.leased_until
    with pytest.raises(ValueError, match="lease is no longer current"):
        store.set_intent_scope_disposition(
            intent_scope_id=first.intent_scope_id,
            lease_id=first.lease_id,
            state=IntentScopeState.SATISFIED,
            reason="evidence_satisfied",
        )
    reclaimed = run(store.lease_next_intent_scope(worker_id="recovered", now=NOW))
    assert reclaimed is not None and reclaimed.intent_scope_id == first_scope_id
    store.set_intent_scope_disposition(
        intent_scope_id=reclaimed.intent_scope_id,
        lease_id=reclaimed.lease_id,
        state=IntentScopeState.SATISFIED,
        reason="evidence_satisfied",
    )
    assert (
        store.list_intent_scopes(reclaimed.intent.intent_id)[0].state is IntentScopeState.SATISFIED
    )

    deferred_scope_id = submit()
    deferred = run(store.lease_next_intent_scope(worker_id="coordinator", now=NOW))
    assert deferred is not None and deferred.intent_scope_id == deferred_scope_id
    eligible_at = current_time[0] + timedelta(seconds=1)
    store.set_intent_scope_disposition(
        intent_scope_id=deferred.intent_scope_id,
        lease_id=deferred.lease_id,
        state=IntentScopeState.DEFERRED,
        reason="minimum_interval_not_elapsed",
        eligible_at=eligible_at,
    )
    assert (
        run(
            store.lease_next_intent_scope(
                worker_id="future-caller",
                now=eligible_at + timedelta(hours=1),
            )
        )
        is None
    )
    current_time[0] = eligible_at
    due = run(store.lease_next_intent_scope(worker_id="coordinator", now=NOW))
    assert due is not None and due.intent_scope_id == deferred_scope_id
    store.close()


def test_expired_running_lease_reopens_with_new_authority_and_preserves_start(tmp_path) -> None:
    path = tmp_path / "state.sqlite3"
    current_time = [NOW]
    with SQLiteStore(path, clock=lambda: current_time[0]) as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
        )
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet="membership",
        )
        action = AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            capability_key="databricks.workspace.children.read",
            capability_version="1",
            target=scope.target,
            requested_scopes=(scope,),
        )
        run(store.enqueue(action))
        first_lease = run(store.lease_next(adapter_key="databricks", worker_id="first", now=NOW))
        assert first_lease is not None
        run(
            store.mark_running(
                action_id=action.action_id, lease_id=first_lease.lease_id, started_at=NOW
            )
        )

    with SQLiteStore(path, clock=lambda: current_time[0]) as reopened:
        recovery_at = NOW + timedelta(seconds=61)
        current_time[0] = recovery_at
        second_lease = run(
            reopened.lease_next(adapter_key="databricks", worker_id="second", now=recovery_at)
        )
        assert second_lease is not None
        assert second_lease.lease_id != first_lease.lease_id
        with pytest.raises(ValueError, match="lease"):
            run(
                reopened.record_attempt(
                    ActionAttempt(uuid4(), action.action_id, 1, recovery_at),
                    lease_id=first_lease.lease_id,
                )
            )
        with pytest.raises(ValueError, match="lease"):
            run(
                reopened.complete_action(
                    ActionCompletion(
                        action_id=action.action_id,
                        outcome=ActionOutcome.FAILED,
                        completed_at=recovery_at,
                        error_class=ErrorClass.CONNECTION_TIMEOUT,
                    ),
                    lease_id=first_lease.lease_id,
                )
            )
        assert reopened.get_stored_action(action.action_id).state.value == "leased"
        run(
            reopened.mark_running(
                action_id=action.action_id,
                lease_id=second_lease.lease_id,
                started_at=recovery_at,
            )
        )
        assert reopened.get_stored_action(action.action_id).started_at == NOW


def test_final_start_strictly_fences_expiry_and_renews_running_lease(tmp_path) -> None:
    def leased_action(path: Path, current_time: list[datetime]):
        store = SQLiteStore(path, clock=lambda: current_time[0])
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local",
            profile="DEFAULT",
            workspace_root="/Shared",
            now=NOW,
        )
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet="membership",
            capability_key="databricks.workspace.children.read",
        )
        action_now = current_time[0]
        run(
            store.submit_refresh(
                RefreshIntent(
                    intent_id=uuid4(),
                    idempotency_key=str(uuid4()),
                    origin=RefreshOrigin.MANUAL,
                    actor_id="test",
                    scopes=(scope,),
                    requested_at=action_now,
                )
            )
        )
        admitted = run(DurableCoordinator(store).run_once(now=action_now))
        assert admitted is not None and admitted.action_id is not None
        stored = store.get_stored_action(admitted.action_id)
        assert stored is not None
        action = stored.action
        lease = run(store.lease_next(adapter_key="databricks", worker_id="first", now=action_now))
        binding = store.list_connection_bindings(system_id=seeded.system.system_id)[0]
        assert lease is not None and binding.revision is not None
        return store, action, lease, binding.revision

    exact_time = [datetime.now(UTC)]
    exact_store, exact_action, exact_lease, exact_revision = leased_action(
        tmp_path / "exact-expiry.sqlite3", exact_time
    )
    exact_time[0] = exact_lease.leased_until
    exact_decision = run(
        exact_store.authorize_start(
            action_id=exact_action.action_id,
            lease_id=exact_lease.lease_id,
            binding_revision=exact_revision,
            now=exact_lease.leased_until,
        )
    )
    assert exact_decision.disposition.value == "fail"
    assert exact_decision.reason == "expired_action_lease"
    replacement = run(
        exact_store.lease_next(
            adapter_key="databricks",
            worker_id="replacement",
            now=exact_lease.leased_until,
        )
    )
    assert replacement is not None and replacement.lease_id != exact_lease.lease_id
    exact_store.close()

    renewed_time = [datetime.now(UTC)]
    renewed_store, renewed_action, renewed_lease, renewed_revision = leased_action(
        tmp_path / "renewed-running.sqlite3", renewed_time
    )
    just_before_expiry = renewed_lease.leased_until - timedelta(microseconds=1)
    renewed_time[0] = just_before_expiry
    future_caller_time = just_before_expiry + timedelta(days=30)
    dispatch = run(
        renewed_store.authorize_start(
            action_id=renewed_action.action_id,
            lease_id=renewed_lease.lease_id,
            binding_revision=renewed_revision,
            now=future_caller_time,
        )
    )
    assert dispatch.disposition.value == "dispatch", dispatch
    running = renewed_store.get_stored_action(renewed_action.action_id)
    assert running is not None and running.state.value == "running"
    assert running.started_at == just_before_expiry
    assert running.leased_until == just_before_expiry + timedelta(seconds=60)
    assert (
        run(
            renewed_store.lease_next(
                adapter_key="databricks",
                worker_id="too-early",
                now=renewed_lease.leased_until,
            )
        )
        is None
    )
    renewed_time[0] = running.leased_until
    reclaimed = run(
        renewed_store.lease_next(
            adapter_key="databricks",
            worker_id="after-renewal",
            now=running.leased_until,
        )
    )
    assert reclaimed is not None and reclaimed.lease_id != renewed_lease.lease_id
    renewed_store.close()


def test_action_projection_migration_backfills_history_and_triggers(tmp_path) -> None:
    path = tmp_path / "action-projection-migration.sqlite3"
    with SQLiteStore(path) as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
        )
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet="membership",
            capability_key="databricks.workspace.children.read",
        )
        action_ids: list[str] = []
        for index in range(2):
            action = AdapterAction(
                action_id=uuid4(),
                correlation_id=uuid4(),
                system_id=seeded.system.system_id,
                connection_binding_id=seeded.connection_binding_id,
                adapter_key="databricks",
                adapter_version="1",
                capability_key="databricks.workspace.children.read",
                capability_version="1",
                target=scope.target,
                requested_scopes=(scope,),
            )
            run(store.enqueue(action))
            action_ids.append(action.action_id)
            occurred_at = NOW + timedelta(minutes=index)
            occurred_text = occurred_at.isoformat().replace("+00:00", "Z")
            store._connection.execute(
                """
                UPDATE adapter_actions
                SET state = 'failed', record_created_at = ?, started_at = ?, completed_at = ?,
                    redacted_diagnostic = ?
                WHERE action_id = ?
                """,
                (
                    occurred_text,
                    occurred_text,
                    occurred_text,
                    f"failure {index}",
                    action.action_id,
                ),
            )
        store._connection.execute("DROP TRIGGER trg_action_scope_projection_insert")
        store._connection.execute("DROP TRIGGER trg_action_projection_update")
        store._connection.execute("DROP TRIGGER trg_action_projection_timestamp_correction")
        store._connection.execute("DROP TABLE facet_action_status")
        store._connection.execute("DROP TABLE action_scope_cooldown")
        store._connection.execute(
            "DELETE FROM schema_migrations WHERE version = '0018_action_state_projections'"
        )

    with SQLiteStore(path) as migrated:
        policy = migrated.scope_policy_state(scope)
        assert policy.latest_targeted_action_started_at == NOW + timedelta(minutes=1)
        status = migrated.list_latest_facet_actions((seeded.workspace_root_object_id,))
        assert len(status) == 1 and status[0].action_id == action_ids[1]
        assert status[0].redacted_diagnostic == "failure 1"
        triggers = {
            row[0]
            for row in migrated._connection.execute(
                "SELECT name FROM sqlite_schema WHERE type = 'trigger'"
            )
        }
        assert triggers >= {
            "trg_action_scope_projection_insert",
            "trg_action_projection_update",
            "trg_action_projection_timestamp_correction",
        }
        corrected_at = NOW - timedelta(minutes=1)
        corrected_text = corrected_at.isoformat().replace("+00:00", "Z")
        migrated._connection.execute(
            """
            UPDATE adapter_actions
            SET record_created_at = ?, started_at = ?, completed_at = ?
            WHERE action_id = ?
            """,
            (corrected_text, corrected_text, corrected_text, action_ids[1]),
        )
        corrected_policy = migrated.scope_policy_state(scope)
        assert corrected_policy.latest_targeted_action_started_at == NOW
        corrected_status = migrated.list_latest_facet_actions((seeded.workspace_root_object_id,))
        assert len(corrected_status) == 1 and corrected_status[0].action_id == action_ids[0]
        migrated._connection.execute(
            """
            UPDATE adapter_actions SET started_at = NULL, completed_at = NULL
            WHERE action_id = ?
            """,
            (action_ids[1],),
        )
        null_policy = migrated.scope_policy_state(scope)
        assert null_policy.latest_targeted_action_started_at == NOW


def test_retry_wait_is_durable_and_next_lease_advances_attempt_ordinal(tmp_path) -> None:
    path = tmp_path / "state.sqlite3"
    current_time = [NOW]
    with SQLiteStore(path, clock=lambda: current_time[0]) as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
        )
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet="membership",
        )
        action = AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            capability_key="databricks.workspace.children.read",
            capability_version="1",
            target=scope.target,
            requested_scopes=(scope,),
        )
        run(store.enqueue(action))
        first = run(store.lease_next(adapter_key="databricks", worker_id="first", now=NOW))
        assert first is not None and first.attempt_ordinal == 1
        run(store.mark_running(action_id=action.action_id, lease_id=first.lease_id, started_at=NOW))
        retry_at = NOW + timedelta(seconds=5)
        first_attempt = ActionAttempt(
            uuid4(),
            action.action_id,
            1,
            NOW,
            NOW + timedelta(seconds=1),
            ActionOutcome.FAILED,
            ErrorClass.CONNECTION_TIMEOUT,
            retry_at=retry_at,
            redacted_diagnostic="token=do-not-persist",
        )
        run(
            store.record_attempt(
                first_attempt,
                lease_id=first.lease_id,
            )
        )
        assert store.get_stored_action(action.action_id).state.value == "retry_wait"
        attempt_events = store.list_operational_events()
        assert len(attempt_events) == 1
        assert attempt_events[0].event_type == "refresh.action.attempt_failed"
        assert not attempt_events[0].alertable
        assert attempt_events[0].action_id == action.action_id
        assert attempt_events[0].attempt_id == first_attempt.attempt_id
        assert attempt_events[0].error_class == ErrorClass.CONNECTION_TIMEOUT.value
        assert "do-not-persist" not in attempt_events[0].redacted_summary
        assert store.list_operational_events(alertable_only=True) == ()

    with SQLiteStore(path, clock=lambda: current_time[0]) as reopened:
        activity = reopened.get_action_activity(action.action_id)
        attempts = reopened.list_action_attempts(action.action_id)
        assert activity is not None and activity.action_id == action.action_id
        assert len(attempts) == 1
        assert attempts[0].ordinal == 1
        assert attempts[0].error_class == "connection_timeout"
        assert attempts[0].retry_at == retry_at
        assert "do-not-persist" not in (attempts[0].redacted_diagnostic or "")
        assert reopened.list_operational_events()[0].attempt_id == first_attempt.attempt_id
        with pytest.raises(ValueError, match="attempt limit"):
            reopened.list_action_attempts(action.action_id, limit=102)
        current_time[0] = retry_at - timedelta(microseconds=1)
        assert (
            run(
                reopened.lease_next(
                    adapter_key="databricks",
                    worker_id="early",
                    now=retry_at - timedelta(microseconds=1),
                )
            )
            is None
        )
        current_time[0] = retry_at
        second = run(
            reopened.lease_next(adapter_key="databricks", worker_id="second", now=retry_at)
        )
        assert second is not None
        assert second.action.action_id == action.action_id
        assert second.attempt_ordinal == 2
        run(
            reopened.mark_running(
                action_id=action.action_id,
                lease_id=second.lease_id,
                started_at=retry_at,
            )
        )
        assert reopened.get_stored_action(action.action_id).retry_at is None


def test_due_action_backlog_promotes_only_one_bounded_batch(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "action-backlog.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
        capability_key="databricks.workspace.children.read",
    )
    seed = AdapterAction(
        action_id=uuid4(),
        correlation_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        capability_key="databricks.workspace.children.read",
        capability_version="1",
        target=scope.target,
        requested_scopes=(scope,),
    )
    run(store.enqueue(seed))
    now_text = NOW.isoformat(timespec="microseconds").replace("+00:00", "Z")
    store._connection.execute(
        """
        WITH RECURSIVE item(number) AS (
            VALUES(1)
            UNION ALL
            SELECT number + 1 FROM item WHERE number < 1500
        )
        INSERT INTO adapter_actions (
            action_id, correlation_id, system_id, connection_binding_id,
            adapter_key, adapter_version, capability_key, capability_version,
            target_kind, target_id, deadline, contract_version, dedupe_key,
            state, retry_at, record_created_at
        )
        SELECT printf('00000000-0000-4000-8000-%012d', number),
               printf('10000000-0000-4000-8000-%012d', number),
               seed.system_id, seed.connection_binding_id, seed.adapter_key,
               seed.adapter_version, seed.capability_key, seed.capability_version,
               seed.target_kind, seed.target_id, seed.deadline, seed.contract_version,
               printf('due-action-%06d', number), 'retry_wait', ?,
               printf('2000-01-01T00:00:00.%06dZ', number)
        FROM item
        CROSS JOIN adapter_actions AS seed
        WHERE seed.action_id = ?
        """,
        (now_text, seed.action_id),
    )
    store._connection.execute(
        """
        WITH RECURSIVE item(number) AS (
            VALUES(1)
            UNION ALL
            SELECT number + 1 FROM item WHERE number < 1500
        )
        INSERT INTO adapter_action_scopes (
            action_id, system_id, target_kind, target_id, object_type, facet,
            capability_key, coverage, field_mask_json
        )
        SELECT printf('00000000-0000-4000-8000-%012d', number),
               scope.system_id, scope.target_kind, scope.target_id,
               scope.object_type, scope.facet, scope.capability_key,
               scope.coverage, scope.field_mask_json
        FROM item
        CROSS JOIN adapter_action_scopes AS scope
        WHERE scope.action_id = ?
        """,
        (seed.action_id,),
    )
    progress_callbacks = 0

    def count_vm_steps() -> int:
        nonlocal progress_callbacks
        progress_callbacks += 1
        return 0

    store._connection.set_progress_handler(count_vm_steps, 100)
    try:
        lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=NOW))
    finally:
        store._connection.set_progress_handler(None, 0)

    assert lease is not None and lease.action.action_id.startswith("00000000-")
    assert dict(
        store._connection.execute(
            "SELECT state, COUNT(*) FROM adapter_actions GROUP BY state"
        ).fetchall()
    ) == {"leased": 1, "ready": 1_000, "retry_wait": 500}
    assert progress_callbacks < 120_000


@pytest.mark.parametrize(
    ("state", "field", "value"),
    [
        ("leased", "leased_until", None),
        ("running", "leased_until", "not-a-timestamp"),
        ("retry_wait", "retry_at", "not-a-timestamp"),
    ],
)
def test_malformed_action_timing_terminalizes_once_and_releases_dedupe(
    tmp_path: Path,
    state: str,
    field: str,
    value: str | None,
) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
        capability_key="databricks.workspace.children.read",
    )
    action = AdapterAction(
        action_id=uuid4(),
        correlation_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        capability_key="databricks.workspace.children.read",
        capability_version="1",
        target=scope.target,
        requested_scopes=(scope,),
    )
    run(store.enqueue(action))
    store._connection.execute(
        f"UPDATE adapter_actions SET state = ?, {field} = ? WHERE action_id = ?",
        (state, value, action.action_id),
    )

    store.close()

    with SQLiteStore(tmp_path / "state.sqlite3") as reopened:
        poisoned = reopened._connection.execute(
            "SELECT state, error_class FROM adapter_actions WHERE action_id = ?",
            (action.action_id,),
        ).fetchone()
        assert tuple(poisoned) == ("failed", "adapter_contract_mismatch")
        assert len(reopened.list_operational_events(alertable_only=True)) == 1
    with SQLiteStore(tmp_path / "state.sqlite3") as reopened:
        assert len(reopened.list_operational_events(alertable_only=True)) == 1
        replacement = replace(action, action_id=uuid4(), correlation_id=uuid4())
        run(reopened.enqueue(replacement))
        lease = run(reopened.lease_next(adapter_key="databricks", worker_id="worker", now=NOW))
        assert lease is not None and lease.action.action_id == replacement.action_id


@pytest.mark.parametrize(
    ("state", "field", "value"),
    [
        ("leased", "leased_until", None),
        ("leased", "leased_until", "not-a-timestamp"),
        ("deferred", "eligible_at", "not-a-timestamp"),
    ],
)
def test_malformed_intent_timing_rejects_once_without_blocking_valid_work(
    tmp_path: Path,
    state: str,
    field: str,
    value: str | None,
) -> None:
    store = SQLiteStore(tmp_path / "intent-timing.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
    )
    poisoned = run(
        store.submit_refresh(
            RefreshIntent(
                intent_id=uuid4(),
                idempotency_key=f"poison-{state}-{field}",
                origin=RefreshOrigin.MANUAL,
                actor_id="local-user",
                ui_session_id=uuid4(),
                requested_at=NOW,
                scopes=(scope,),
            )
        )
    )
    valid = run(
        store.submit_refresh(
            RefreshIntent(
                intent_id=uuid4(),
                idempotency_key=f"valid-{state}-{field}",
                origin=RefreshOrigin.MANUAL,
                actor_id="local-user",
                ui_session_id=uuid4(),
                requested_at=NOW + timedelta(seconds=1),
                scopes=(scope,),
            )
        )
    )
    store._connection.execute(
        f"""
        UPDATE refresh_intent_scopes
        SET state = ?, {field} = ?
        WHERE intent_scope_id = ?
        """,
        (state, value, poisoned.scope_ids[0]),
    )
    store.close()

    with SQLiteStore(tmp_path / "intent-timing.sqlite3") as reopened:
        leased = run(
            reopened.lease_next_intent_scope(
                worker_id="coordinator", now=NOW + timedelta(seconds=1)
            )
        )
        assert leased is not None and leased.intent.intent_id == valid.intent_id
        poison_scope = reopened.list_intent_scopes(poisoned.intent_id)[0]
        assert poison_scope.state.value == "rejected"
        assert poison_scope.disposition_reason == "persisted_intent_contract_mismatch"
        assert len(reopened.list_operational_events(alertable_only=True)) == 1


def test_sqlite_write_contention_is_bounded_for_event_loop_callers(tmp_path: Path) -> None:
    path = tmp_path / "contention.sqlite3"
    store = SQLiteStore(path)
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
    )
    action = AdapterAction(
        action_id=uuid4(),
        correlation_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        capability_key="databricks.workspace.children.read",
        capability_version="1",
        target=scope.target,
        requested_scopes=(scope,),
    )
    blocker = sqlite3.connect(path, isolation_level=None)
    blocker.execute("PRAGMA busy_timeout = 1000")
    blocker.execute("BEGIN IMMEDIATE")
    assert store._connection.execute("PRAGMA busy_timeout").fetchone()[0] == 250
    started = time.monotonic()
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            run(store.enqueue(action))
    finally:
        blocker.rollback()
        blocker.close()
        store.close()
    elapsed = time.monotonic() - started
    assert elapsed < 0.75


def test_startup_does_not_materialize_large_canonical_deferred_backlog(tmp_path: Path) -> None:
    path = tmp_path / "startup-backlog.sqlite3"
    store = SQLiteStore(path)
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
    )
    receipt = run(
        store.submit_refresh(
            RefreshIntent(
                intent_id=uuid4(),
                idempotency_key="startup-backlog",
                origin=RefreshOrigin.MANUAL,
                actor_id="local-user",
                requested_at=NOW,
                scopes=(scope,),
            )
        )
    )
    store._connection.execute(
        """
        WITH RECURSIVE item(number) AS (
            VALUES(1)
            UNION ALL
            SELECT number + 1 FROM item WHERE number < 50000
        )
        INSERT INTO refresh_intent_scopes (
            intent_scope_id, intent_id, system_id, target_kind, target_id,
            object_type, facet, capability_key, coverage, field_mask_json,
            state, eligible_at, queue_priority, queue_requested_at
        )
        SELECT printf('future-%06d', item.number), scope.intent_id, scope.system_id,
               scope.target_kind, scope.target_id, scope.object_type, scope.facet,
               scope.capability_key, scope.coverage, scope.field_mask_json,
               'deferred', '2099-01-01T00:00:00.000000Z', 100,
               '2000-01-01T00:00:00.000000Z'
        FROM item
        CROSS JOIN refresh_intent_scopes AS scope
        WHERE scope.intent_scope_id = ?
        """,
        (receipt.scope_ids[0],),
    )
    store.close()

    tracemalloc.start()
    started = time.perf_counter()
    with SQLiteStore(path):
        pass
    elapsed = time.perf_counter() - started
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert elapsed < 3.0
    assert peak < 16 * 1024 * 1024


def test_authority_cancellation_scales_linearly_across_many_intents(tmp_path: Path) -> None:
    with SQLiteStore(tmp_path / "authority-cancellation.sqlite3") as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
        )
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet="membership",
        )
        now_text = NOW.isoformat(timespec="microseconds").replace("+00:00", "Z")
        store._connection.execute(
            """
            WITH RECURSIVE item(number) AS (
                VALUES(1)
                UNION ALL
                SELECT number + 1 FROM item WHERE number < 10000
            )
            INSERT INTO refresh_intents (
                intent_id, idempotency_key, origin, actor_id, requested_at,
                priority, aggregate_state, contract_version, accepted_at
            )
            SELECT printf('bulk-intent-%06d', number),
                   printf('bulk-idempotency-%06d', number),
                   'manual', 'local-user', ?, 100, 'open', '1', ?
            FROM item
            """,
            (now_text, now_text),
        )
        store._connection.execute(
            """
            WITH RECURSIVE item(number) AS (
                VALUES(1)
                UNION ALL
                SELECT number + 1 FROM item WHERE number < 10000
            )
            INSERT INTO refresh_intent_scopes (
                intent_scope_id, intent_id, system_id, target_kind, target_id,
                object_type, facet, capability_key, coverage, field_mask_json,
                state, queue_priority, queue_requested_at
            )
            SELECT printf('bulk-scope-%06d', number),
                   printf('bulk-intent-%06d', number), ?, ?, ?, ?, ?, ?, ?, ?,
                   'queued', 100, ?
            FROM item
            """,
            (
                scope.system_id,
                scope.target.kind.value,
                scope.target.target_id,
                scope.object_type,
                scope.facet,
                scope.capability_key,
                scope.coverage.value,
                "[]",
                now_text,
            ),
        )
        progress_callbacks = 0

        def count_vm_steps() -> int:
            nonlocal progress_callbacks
            progress_callbacks += 1
            return 0

        store._connection.set_progress_handler(count_vm_steps, 100)
        started = time.perf_counter()
        try:
            store.set_authority_retired(scope.system_id, retired=True, now=NOW)
        finally:
            store._connection.set_progress_handler(None, 0)
        elapsed = time.perf_counter() - started

        assert elapsed < 3.0
        assert progress_callbacks < 200_000
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM refresh_intent_scopes WHERE state = 'cancelled'"
            ).fetchone()[0]
            == 10_000
        )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM refresh_intents WHERE aggregate_state = 'complete'"
            ).fetchone()[0]
            == 10_000
        )


def test_failed_attempt_event_is_idempotent_with_attempt_replay(tmp_path) -> None:
    with SQLiteStore(tmp_path / "state.sqlite3") as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
        )
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet="membership",
        )
        action = AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            capability_key="databricks.workspace.children.read",
            capability_version="1",
            target=scope.target,
            requested_scopes=(scope,),
        )
        run(store.enqueue(action))
        lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=NOW))
        assert lease is not None
        run(store.mark_running(action_id=action.action_id, lease_id=lease.lease_id, started_at=NOW))
        attempt = ActionAttempt(
            attempt_id=uuid4(),
            action_id=action.action_id,
            ordinal=1,
            started_at=NOW,
            ended_at=NOW + timedelta(seconds=1),
            outcome=ActionOutcome.FAILED,
            error_class=ErrorClass.CONNECTION_TIMEOUT,
            redacted_diagnostic="token=do-not-persist",
        )

        run(store.record_attempt(attempt, lease_id=lease.lease_id))
        run(store.record_attempt(attempt, lease_id=lease.lease_id))

        assert len(store.list_action_attempts(action.action_id)) == 1
        events = store.list_operational_events()
        assert len(events) == 1
        assert events[0].attempt_id == attempt.attempt_id
        assert not events[0].alertable
        run(
            store.complete_action(
                ActionCompletion(
                    action_id=action.action_id,
                    outcome=ActionOutcome.FAILED,
                    completed_at=NOW + timedelta(seconds=2),
                    error_class=ErrorClass.CONNECTION_TIMEOUT,
                ),
                lease_id=lease.lease_id,
            )
        )
        terminal_events = store.list_operational_events(alertable_only=True)
        assert len(terminal_events) == 1
        assert terminal_events[0].event_type == "refresh.action.failed"


def test_malformed_action_contract_terminalizes_once_and_queue_progresses(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )

    def action_for(*, facet: str, capability_key: str) -> AdapterAction:
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet=facet,
            capability_key=capability_key,
        )
        return AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            capability_key=capability_key,
            capability_version="1",
            target=scope.target,
            requested_scopes=(scope,),
        )

    poisoned = action_for(facet="membership", capability_key="databricks.workspace.children.read")
    healthy = action_for(facet="metadata", capability_key="databricks.workspace.metadata.read")
    run(store.enqueue(poisoned))
    run(store.enqueue(healthy))
    store._connection.execute(
        "UPDATE adapter_actions SET deadline = ?, record_created_at = ? WHERE action_id = ?",
        ("not-a-timestamp", "2026-08-24T12:00:00.000000Z", poisoned.action_id),
    )
    store._connection.execute(
        "UPDATE adapter_actions SET record_created_at = ? WHERE action_id = ?",
        ("2026-08-24T12:00:01.000000Z", healthy.action_id),
    )

    next_lease = run(store.lease_next(adapter_key="databricks", worker_id="first", now=NOW))
    assert next_lease is not None and next_lease.action.action_id == healthy.action_id
    poisoned_row = store._connection.execute(
        "SELECT state, error_class FROM adapter_actions WHERE action_id = ?",
        (poisoned.action_id,),
    ).fetchone()
    assert tuple(poisoned_row) == ("failed", "adapter_contract_mismatch")
    events = store.list_operational_events(alertable_only=True)
    assert len(events) == 1
    assert events[0].action_id == poisoned.action_id
    assert events[0].redacted_summary == "malformed_action_contract"

    assert len(store.list_operational_events(alertable_only=True)) == 1
    store._connection.execute(
        "UPDATE adapter_actions SET record_created_at = ? WHERE action_id = ?",
        ("2026-08-24T12:00:02.000000Z", poisoned.action_id),
    )
    latest_activity = store.list_latest_system_activity()
    assert len(latest_activity) == 1
    assert latest_activity[0].action_id == healthy.action_id
    assert latest_activity[0].state == "leased"

    run(
        store.complete_action(
            ActionCompletion(
                action_id=healthy.action_id,
                outcome=ActionOutcome.SUCCEEDED,
                completed_at=NOW + timedelta(seconds=3),
            ),
            lease_id=next_lease.lease_id,
        )
    )
    latest_terminal = store.list_latest_system_activity()
    assert latest_terminal[0].action_id == poisoned.action_id
    plan = store._connection.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT active.action_id
        FROM adapter_actions AS active
        WHERE active.system_id = ?
          AND active.state IN ('ready', 'leased', 'running', 'retry_wait')
        ORDER BY active.record_created_at DESC, active.action_id
        LIMIT 1
        """,
        (seeded.system.system_id,),
    ).fetchall()
    assert any("ix_adapter_actions_system_active_recency" in row[3] for row in plan)
    assert all("TEMP B-TREE" not in row[3] for row in plan)


@pytest.mark.parametrize("ordinal", [float("inf"), "1e309", 0, -1])
def test_malformed_attempt_ordinal_terminalizes_once_and_queue_progresses(
    tmp_path, ordinal: object
) -> None:
    store = SQLiteStore(tmp_path / "malformed-attempt.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )

    def action_for(target_id: str) -> AdapterAction:
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, target_id),
            object_type="folder",
            facet="membership",
            capability_key="databricks.workspace.children.read",
        )
        return AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            capability_key="databricks.workspace.children.read",
            capability_version="1",
            target=scope.target,
            requested_scopes=(scope,),
        )

    poisoned = action_for(str(uuid4()))
    healthy = action_for(str(uuid4()))
    run(store.enqueue(poisoned))
    run(store.enqueue(healthy))
    store._connection.execute(
        "UPDATE adapter_actions SET record_created_at = ? WHERE action_id = ?",
        ("2026-08-24T12:00:00.000000Z", poisoned.action_id),
    )
    store._connection.execute(
        "UPDATE adapter_actions SET record_created_at = ? WHERE action_id = ?",
        ("2026-08-24T12:00:01.000000Z", healthy.action_id),
    )
    store._connection.execute(
        """
        INSERT INTO action_attempts (attempt_id, action_id, ordinal, started_at)
        VALUES (?, ?, ?, ?)
        """,
        (str(uuid4()), poisoned.action_id, ordinal, "2026-08-24T12:00:00.000000Z"),
    )

    lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=NOW))

    assert lease is not None and lease.action.action_id == healthy.action_id
    poisoned_row = store._connection.execute(
        "SELECT state, error_class FROM adapter_actions WHERE action_id = ?",
        (poisoned.action_id,),
    ).fetchone()
    assert tuple(poisoned_row) == ("failed", "adapter_contract_mismatch")
    events = store.list_operational_events(alertable_only=True)
    assert len(events) == 1
    assert events[0].action_id == poisoned.action_id
    assert events[0].redacted_summary == "malformed_attempt_contract"


def test_malformed_action_quarantine_is_bounded_per_queue_claim(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "bounded-action-quarantine.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    total = sqlite_storage._MAX_ACTION_CONTRACT_QUARANTINES_PER_CLAIM + 5
    for _index in range(total):
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, uuid4()),
            object_type="folder",
            facet="membership",
            capability_key="databricks.workspace.children.read",
        )
        run(
            store.enqueue(
                AdapterAction(
                    action_id=uuid4(),
                    correlation_id=uuid4(),
                    system_id=seeded.system.system_id,
                    connection_binding_id=seeded.connection_binding_id,
                    adapter_key="databricks",
                    adapter_version="1",
                    capability_key="databricks.workspace.children.read",
                    capability_version="1",
                    target=scope.target,
                    requested_scopes=(scope,),
                )
            )
        )
    store._connection.execute("UPDATE adapter_actions SET contract_version = 'unsupported'")

    assert run(store.lease_next(adapter_key="databricks", worker_id="bounded", now=NOW)) is None
    first_counts = dict(
        store._connection.execute(
            "SELECT state, COUNT(*) FROM adapter_actions GROUP BY state"
        ).fetchall()
    )
    assert first_counts == {
        "failed": sqlite_storage._MAX_ACTION_CONTRACT_QUARANTINES_PER_CLAIM,
        "ready": 5,
    }

    assert run(store.lease_next(adapter_key="databricks", worker_id="bounded", now=NOW)) is None
    assert (
        store._connection.execute(
            "SELECT COUNT(*) FROM adapter_actions WHERE state = 'ready'"
        ).fetchone()[0]
        == 0
    )
    events = store.list_operational_events(alertable_only=True)
    assert len(events) == total
    assert len({event.event_id for event in events}) == total


def test_overlong_corrupt_action_id_quarantines_without_blocking_queue(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "overlong-action-poison.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local",
        profile="DEFAULT",
        workspace_root="/Shared",
        now=NOW,
    )

    def action_for(*, facet: str, capability_key: str) -> AdapterAction:
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet=facet,
            capability_key=capability_key,
        )
        return AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            capability_key=capability_key,
            capability_version="1",
            target=scope.target,
            requested_scopes=(scope,),
        )

    poisoned = action_for(
        facet="membership",
        capability_key="databricks.workspace.children.read",
    )
    healthy = action_for(
        facet="metadata",
        capability_key="databricks.workspace.metadata.read",
    )
    run(store.enqueue(poisoned))
    run(store.enqueue(healthy))
    overlong_action_id = "a" * 600
    dangling_system_id = str(uuid4())
    store._connection.execute("PRAGMA foreign_keys = OFF")
    store._connection.execute(
        """
        UPDATE adapter_action_scopes SET action_id = ?, system_id = ?
        WHERE action_id = ?
        """,
        (overlong_action_id, dangling_system_id, poisoned.action_id),
    )
    store._connection.execute(
        """
        UPDATE adapter_actions SET action_id = ?, system_id = ?, record_created_at = ?
        WHERE action_id = ?
        """,
        (
            overlong_action_id,
            dangling_system_id,
            "2026-08-24T12:00:00.000000Z",
            poisoned.action_id,
        ),
    )
    store._connection.execute("PRAGMA foreign_keys = ON")
    store._connection.execute(
        "UPDATE adapter_actions SET record_created_at = ? WHERE action_id = ?",
        ("2026-08-24T12:00:01.000000Z", healthy.action_id),
    )
    assert store._connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert store._connection.execute("PRAGMA foreign_key_check").fetchall()

    lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=NOW))

    assert lease is not None and lease.action.action_id == healthy.action_id
    poisoned_row = store._connection.execute(
        "SELECT state, error_class FROM adapter_actions WHERE action_id = ?",
        (overlong_action_id,),
    ).fetchone()
    assert tuple(poisoned_row) == ("failed", "adapter_contract_mismatch")
    events = store.list_operational_events(alertable_only=True)
    assert len(events) == 1
    assert events[0].action_id == overlong_action_id
    assert events[0].system_id is None
    event_key = store._connection.execute(
        "SELECT idempotency_key FROM operational_events WHERE event_id = ?",
        (events[0].event_id,),
    ).fetchone()[0]
    assert len(event_key) < 512


def test_action_activity_pages_filters_and_uses_recency_indexes(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    scope = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
        capability_key="databricks.workspace.children.read",
    )
    action_ids: list[str] = []
    for index, state in enumerate(("failed", "succeeded", "failed")):
        action = AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            capability_key="databricks.workspace.children.read",
            capability_version="1",
            target=scope.target,
            requested_scopes=(scope,),
        )
        run(store.enqueue(action))
        action_ids.append(action.action_id)
        occurred_at = NOW + timedelta(minutes=index)
        store._connection.execute(
            """
            UPDATE adapter_actions
            SET state = ?, record_created_at = ?, completed_at = ?,
                error_class = ?, redacted_diagnostic = ?
            WHERE action_id = ?
            """,
            (
                state,
                occurred_at.isoformat().replace("+00:00", "Z"),
                occurred_at.isoformat().replace("+00:00", "Z"),
                "connection_timeout" if state == "failed" else None,
                "redacted failure" if state == "failed" else None,
                action.action_id,
            ),
        )

    assert store.count_action_activity() == 3
    assert store.count_action_activity(state="failed") == 2
    assert store.count_action_activity(system_id=seeded.system.system_id) == 3
    assert store.count_action_activity(system_id=seeded.system.system_id, state="failed") == 2
    assert store.count_action_activity(action_id=action_ids[1]) == 1
    assert [item.action_id for item in store.list_action_activity_page(offset=0, limit=2)] == [
        action_ids[2],
        action_ids[1],
    ]
    first_cursor_page = store.list_action_activity_after(
        after_created_at=None,
        after_action_id=None,
        limit=2,
    )
    assert [item.action_id for item in first_cursor_page] == [action_ids[2], action_ids[1]]
    second_cursor_page = store.list_action_activity_after(
        after_created_at=first_cursor_page[-1].created_at,
        after_action_id=first_cursor_page[-1].action_id,
        limit=2,
    )
    assert [item.action_id for item in second_cursor_page] == [action_ids[0]]
    assert all(
        item.state == "failed"
        for item in store.list_action_activity_page(offset=0, limit=10, state="failed")
    )
    assert (
        store.list_action_activity_page(offset=0, limit=10, action_id=action_ids[1])[0].state
        == "succeeded"
    )
    facet_statements: list[str] = []
    store._connection.set_trace_callback(facet_statements.append)
    try:
        facet_actions = store.list_latest_facet_actions((seeded.workspace_root_object_id,))
    finally:
        store._connection.set_trace_callback(None)
    assert len(facet_actions) == 1
    assert facet_actions[0].object_id == seeded.workspace_root_object_id
    assert facet_actions[0].facet == "membership"
    assert facet_actions[0].action_id == action_ids[2]
    assert facet_actions[0].state == "failed"
    assert facet_actions[0].redacted_diagnostic == "redacted failure"
    assert store._connection.execute("SELECT COUNT(*) FROM facet_action_status").fetchone()[0] == 1
    assert not any("adapter_action" in statement for statement in facet_statements)
    store._connection.execute(
        "UPDATE adapter_actions SET state = 'running', completed_at = NULL WHERE action_id = ?",
        (action_ids[0],),
    )
    refreshing_actions = store.list_latest_facet_actions((seeded.workspace_root_object_id,))
    assert len(refreshing_actions) == 1
    assert refreshing_actions[0].action_id == action_ids[0]
    assert refreshing_actions[0].state == "running"
    assert store._connection.execute("SELECT COUNT(*) FROM facet_action_status").fetchone()[0] == 2
    with pytest.raises(ValueError, match="100 object IDs"):
        store.list_latest_facet_actions(tuple(str(uuid4()) for _ in range(101)))

    with pytest.raises(ValueError, match="offset"):
        store.list_action_activity_page(offset=-1, limit=10)
    with pytest.raises(ValueError, match="limit"):
        store.list_action_activity_page(offset=0, limit=101)
    with pytest.raises(ValueError, match="state"):
        store.count_action_activity(state="unknown")
    with pytest.raises(ValueError, match="combined"):
        store.count_action_activity(action_id=action_ids[0], state="failed")

    plans = {
        "ix_adapter_actions_recency": (
            "SELECT * FROM adapter_actions "
            "ORDER BY record_created_at DESC, action_id LIMIT ? OFFSET ?",
            (10, 0),
        ),
        "ix_adapter_actions_state_recency": (
            "SELECT * FROM adapter_actions WHERE state = ? "
            "ORDER BY record_created_at DESC, action_id LIMIT ? OFFSET ?",
            ("failed", 10, 0),
        ),
        "ix_adapter_actions_system_recency": (
            "SELECT * FROM adapter_actions WHERE system_id = ? "
            "ORDER BY record_created_at DESC, action_id LIMIT ? OFFSET ?",
            (seeded.system.system_id, 10, 0),
        ),
        "ix_adapter_actions_system_state_recency": (
            "SELECT * FROM adapter_actions WHERE system_id = ? AND state = ? "
            "ORDER BY record_created_at DESC, action_id LIMIT ? OFFSET ?",
            (seeded.system.system_id, "failed", 10, 0),
        ),
    }
    for expected_index, (statement, parameters) in plans.items():
        plan = store._connection.execute(
            f"EXPLAIN QUERY PLAN {statement}",
            parameters,
        ).fetchall()
        assert any(expected_index in row[3] for row in plan)
        assert not any("TEMP B-TREE" in row[3] for row in plan)

    store._connection.executemany(
        """
        INSERT INTO action_attempts (attempt_id, action_id, ordinal, started_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            (
                str(uuid4()),
                action_ids[0],
                ordinal,
                (NOW + timedelta(seconds=ordinal)).isoformat().replace("+00:00", "Z"),
            )
            for ordinal in range(1, 102)
        ),
    )
    latest_attempts = store.list_action_attempts(action_ids[0], limit=100)
    assert store.count_action_attempts(action_ids[0]) == 101
    assert [attempt.ordinal for attempt in latest_attempts] == list(range(2, 102))


def test_runtime_failure_is_bounded_and_diagnostics_redact_json_and_home_paths(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    event = store.record_runtime_failure(
        event_type="queue.coordinator.failed",
        summary="Coordinator stopped unexpectedly",
        occurred_at=NOW,
    )
    assert event.alertable
    assert event == store.record_runtime_failure(
        event_type="queue.coordinator.failed",
        summary="Coordinator stopped unexpectedly",
        occurred_at=NOW,
    )
    later = store.record_runtime_failure(
        event_type="queue.adapter_worker.failed",
        summary="Worker stopped unexpectedly",
        occurred_at=NOW + timedelta(seconds=1),
    )
    store._connection.execute(
        """
        INSERT INTO operational_events (
            event_id, idempotency_key, event_type, severity, alertable,
            redacted_summary, occurred_at
        ) VALUES (?, ?, ?, ?, 0, ?, ?)
        """,
        (
            str(uuid4()),
            str(uuid4()),
            "refresh.expected",
            "info",
            "Expected local disposition",
            NOW.isoformat(),
        ),
    )
    assert len(store.list_operational_events(alertable_only=True)) == 2
    assert store.list_operational_events(alertable_only=True, limit=1) == (later,)
    assert store.count_alertable_events() == 2
    assert store.count_alertable_events(event_type="queue.coordinator.failed") == 1
    assert store.count_alertable_events(severity="error") == 2
    assert len(store.list_alertable_events_page(offset=0, limit=10)) == 2
    first_cursor_page = store.list_alertable_events_after(
        after_occurred_at=None,
        after_event_id=None,
        limit=1,
    )
    assert first_cursor_page == (later,)
    assert store.list_alertable_events_after(
        after_occurred_at=later.occurred_at,
        after_event_id=later.event_id,
        limit=2,
    ) == (event,)
    with pytest.raises(ValueError, match="offset"):
        store.list_alertable_events_page(offset=-1, limit=10)
    with pytest.raises(ValueError, match="limit"):
        store.list_alertable_events_page(offset=0, limit=101)
    with pytest.raises(ValueError, match="event_type"):
        store.list_alertable_events_page(offset=0, limit=10, event_type="BAD TYPE")
    with pytest.raises(ValueError, match="severity"):
        store.list_alertable_events_page(offset=0, limit=10, severity="debug")
    query_plan = store._connection.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT * FROM operational_events
        WHERE alertable = 1
        ORDER BY occurred_at DESC, event_id
        LIMIT 10
        """
    ).fetchall()
    assert any("ix_operational_events_alertable_recency" in row[3] for row in query_plan)
    combined_plan = store._connection.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT * FROM operational_events
        WHERE alertable = 1 AND event_type = ? AND severity = ?
        ORDER BY occurred_at DESC, event_id
        LIMIT 50
        """,
        ("queue.coordinator.failed", "error"),
    ).fetchall()
    type_plan = store._connection.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT * FROM operational_events
        WHERE alertable = 1 AND event_type = ?
        ORDER BY occurred_at DESC, event_id
        LIMIT 50
        """,
        ("queue.coordinator.failed",),
    ).fetchall()
    severity_plan = store._connection.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT * FROM operational_events
        WHERE alertable = 1 AND severity = ?
        ORDER BY occurred_at DESC, event_id
        LIMIT 50
        """,
        ("error",),
    ).fetchall()
    assert any(
        "ix_operational_events_alertable_type_severity_recency" in row[3] for row in combined_plan
    )
    assert any("ix_operational_events_alertable_type_recency" in row[3] for row in type_plan)
    assert any(
        "ix_operational_events_alertable_severity_recency" in row[3] for row in severity_plan
    )
    assert not any("TEMP B-TREE" in row[3] for row in (*combined_plan, *type_plan, *severity_plan))
    with pytest.raises(ValueError, match="registered"):
        store.record_runtime_failure(
            event_type="refresh.action.failed",
            summary="Coordinator stopped unexpectedly",
            occurred_at=NOW,
        )
    with pytest.raises(ValueError, match="safe message"):
        store.record_runtime_failure(
            event_type="queue.coordinator.failed",
            summary='{"token":"raw"}',
            occurred_at=NOW,
        )
    redacted = _redact(
        'payload {"token":"secret-value","DATABRICKS_TOKEN":"json-secret"} '
        "DATABRICKS_TOKEN=environment-secret safe\x1b[2J\u202eevil "
        "C:\\Users\\alice\\.databrickscfg /home/alice/.config"
    )
    assert "secret-value" not in redacted
    assert "json-secret" not in redacted
    assert "environment-secret" not in redacted
    assert "alice" not in redacted
    assert "\x1b" not in redacted
    assert "\u202e" not in redacted


def test_workspace_root_uses_normalized_external_identity(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    authority = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
        capability_key="databricks.workspace.children.read",
        coverage=RefreshCoverage.COLLECTION_MEMBERS,
    )
    batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW,
        received_at=NOW,
        facet_observations=(
            FacetObservation(
                observation_id=uuid4(),
                target=ObjectLocator(
                    object_type="folder",
                    source_kind="databricks.workspace.folder",
                    external_key="workspace:/Shared",
                    display_name="Shared",
                ),
                facet="metadata",
                facet_version="1",
                update_mode=UpdateMode.SNAPSHOT,
                field_coverage=FieldCoverage.COMPLETE,
                payload={"path": "/Shared"},
                authorized_by=(authority,),
            ),
        ),
    )
    assert run(store.ingest(batch)).status.value == "accepted"
    assert len(store.list_objects(system_id=seeded.system.system_id)) == 1
    assert store.get_facet_sync(seeded.workspace_root_object_id, "metadata") is not None


def test_regressed_databricks_local_time_uses_strict_store_receipt_order(tmp_path) -> None:
    path = tmp_path / "receipt-order.sqlite3"
    current_time = [NOW]
    store = SQLiteStore(path, clock=lambda: current_time[0])
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
    )
    authority = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
        capability_key="databricks.workspace.children.read",
        coverage=RefreshCoverage.COLLECTION_MEMBERS,
    )

    def batch(value: str, observed_at: datetime) -> ObservationBatch:
        return ObservationBatch(
            batch_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            observed_at=observed_at,
            received_at=observed_at,
            observed_at_is_local=True,
            facet_observations=(
                FacetObservation(
                    observation_id=uuid4(),
                    target=ObjectLocator(
                        object_type="folder",
                        source_kind="databricks.workspace.folder",
                        external_key="workspace:/Shared",
                        display_name="Shared",
                    ),
                    facet="metadata",
                    facet_version="1",
                    update_mode=UpdateMode.SNAPSHOT,
                    field_coverage=FieldCoverage.COMPLETE,
                    payload={"value": value},
                    authorized_by=(authority,),
                ),
            ),
        )

    first = batch("first", NOW)
    assert run(store.ingest(first)).status is IngestionStatus.ACCEPTED
    current_time[0] = NOW - timedelta(hours=1)
    second = batch("second", current_time[0])
    assert run(store.ingest(second)).status is IngestionStatus.ACCEPTED
    projected = store.get_facet_sync(seeded.workspace_root_object_id, "metadata")
    assert projected is not None and projected.payload == {"value": "second"}
    second_row = store._connection.execute(
        "SELECT observed_at, received_at FROM observation_batches WHERE batch_id = ?",
        (second.batch_id,),
    ).fetchone()
    assert second_row["observed_at"] == second_row["received_at"]
    assert second_row["received_at"] == "2026-08-24T12:00:00.000001Z"
    store.close()

    reopened = SQLiteStore(path, clock=lambda: current_time[0])
    third = batch("third", current_time[0])
    assert run(reopened.ingest(third)).status is IngestionStatus.ACCEPTED
    projected = reopened.get_facet_sync(seeded.workspace_root_object_id, "metadata")
    assert projected is not None and projected.payload == {"value": "third"}
    assert run(reopened.ingest(third)).status is IngestionStatus.DUPLICATE


@pytest.mark.parametrize("locator_kind", ["canonical", "external"])
def test_first_accepted_observation_can_predate_local_object_creation(
    tmp_path, locator_kind: str
) -> None:
    store = SQLiteStore(tmp_path / f"first-seen-{locator_kind}.sqlite3")
    seeded = SystemBootstrapService(store).configure_databricks_workspace(
        display_name="local",
        profile="DEFAULT",
        workspace_root="/Shared",
        now=NOW + timedelta(seconds=1),
    )
    authority = RefreshScope(
        system_id=seeded.system.system_id,
        target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
        object_type="folder",
        facet="membership",
        capability_key="databricks.workspace.children.read",
        coverage=RefreshCoverage.COLLECTION_MEMBERS,
    )
    locator = (
        ObjectLocator(object_type="folder", object_id=seeded.workspace_root_object_id)
        if locator_kind == "canonical"
        else ObjectLocator(
            object_type="folder",
            source_kind="databricks.workspace.folder",
            external_key="workspace:/Shared",
            display_name="Shared",
        )
    )
    batch = ObservationBatch(
        batch_id=uuid4(),
        system_id=seeded.system.system_id,
        connection_binding_id=seeded.connection_binding_id,
        adapter_key="databricks",
        adapter_version="1",
        observed_at=NOW,
        received_at=NOW + timedelta(seconds=2),
        facet_observations=(
            FacetObservation(
                observation_id=uuid4(),
                target=locator,
                facet="metadata",
                facet_version="1",
                update_mode=UpdateMode.SNAPSHOT,
                field_coverage=FieldCoverage.COMPLETE,
                payload={"path": "/Shared"},
                authorized_by=(authority,),
            ),
        ),
    )

    assert run(store.ingest(batch)).status.value == "accepted"
    root = store.get_object_sync(seeded.workspace_root_object_id)
    assert root is not None
    assert root.first_seen_at == NOW
    assert root.last_seen_at == NOW


def test_legacy_pre_digest_batch_redelivery_backfills_only_exact_match(tmp_path) -> None:
    path = tmp_path / "state.sqlite3"
    current_time = [datetime.now(UTC)]
    with SQLiteStore(path, clock=lambda: current_time[0]) as store:
        seeded = SystemBootstrapService(store).configure_databricks_workspace(
            display_name="local", profile="DEFAULT", workspace_root="/Shared", now=NOW
        )
        scope = RefreshScope(
            system_id=seeded.system.system_id,
            target=TargetRef(TargetKind.CONFIGURED_SCOPE, seeded.workspace_root_scope.scope_id),
            object_type="folder",
            facet="membership",
            capability_key="databricks.workspace.children.read",
        )
        action = AdapterAction(
            action_id=uuid4(),
            correlation_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            capability_key="databricks.workspace.children.read",
            capability_version="1",
            target=scope.target,
            requested_scopes=(scope,),
        )
        run(store.enqueue(action))
        lease_now = current_time[0]
        lease = run(store.lease_next(adapter_key="databricks", worker_id="worker", now=lease_now))
        assert lease is not None
        run(
            store.mark_running(
                action_id=action.action_id,
                lease_id=lease.lease_id,
                started_at=lease_now,
            )
        )
        observation = FacetObservation(
            observation_id=uuid4(),
            target=ObjectLocator(object_type="folder", object_id=seeded.workspace_root_object_id),
            facet="membership",
            facet_version="1",
            update_mode=UpdateMode.PATCH,
            field_coverage=FieldCoverage.PARTIAL,
            payload={"member_count": 1},
            field_mask=("member_count",),
        )
        batch = ObservationBatch(
            batch_id=uuid4(),
            system_id=seeded.system.system_id,
            connection_binding_id=seeded.connection_binding_id,
            adapter_key="databricks",
            adapter_version="1",
            action_id=action.action_id,
            observed_at=NOW,
            received_at=NOW,
            facet_observations=(observation,),
        )
        assert run(store.ingest(batch, lease_id=lease.lease_id)).status.value == "accepted"
        store._connection.execute(
            "UPDATE observation_batches SET batch_digest = '' WHERE batch_id = ?",
            (batch.batch_id,),
        )

    current_time[0] = lease.leased_until + timedelta(microseconds=1)
    with SQLiteStore(path, clock=lambda: current_time[0]) as reopened:
        exact = run(reopened.ingest(batch))
        assert exact.status.value == "duplicate"
        digest = reopened._connection.execute(
            "SELECT batch_digest FROM observation_batches WHERE batch_id = ?", (batch.batch_id,)
        ).fetchone()[0]
        assert digest
        recovered = run(
            reopened.lease_next(
                adapter_key="databricks",
                worker_id="recovered",
                now=lease.leased_until + timedelta(microseconds=1),
            )
        )
        assert recovered is not None and recovered.action.action_id == action.action_id
        conflicting = ObservationBatch(
            batch_id=batch.batch_id,
            system_id=batch.system_id,
            connection_binding_id=batch.connection_binding_id,
            adapter_key=batch.adapter_key,
            adapter_version=batch.adapter_version,
            action_id=batch.action_id,
            observed_at=batch.observed_at,
            received_at=batch.received_at,
            facet_observations=(
                FacetObservation(
                    observation_id=observation.observation_id,
                    target=observation.target,
                    facet="membership",
                    facet_version="1",
                    update_mode=UpdateMode.PATCH,
                    field_coverage=FieldCoverage.PARTIAL,
                    payload={"member_count": 2},
                    field_mask=("member_count",),
                ),
            ),
        )
        assert run(reopened.ingest(conflicting)).status.value == "rejected"
