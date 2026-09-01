"""SQLite-backed local durable state.

``SQLiteStore`` is the composition-facing concrete implementation of every
shared local port.  It also provides synchronous configuration/query helpers
for bootstrap and future presentation code; adapter workers should use only the
shared action-lifecycle and observation-ingestion ports.
"""

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
from .sqlite import (
    MIN_WRITE_RESERVE_BYTES,
    SQLiteStore,
    StorageHeadroomUnavailable,
    backup_sqlite_database,
)

__all__ = [
    "MIN_WRITE_RESERVE_BYTES",
    "ActionActivityRecord",
    "ActionAttemptRecord",
    "AuthorityRecord",
    "ConfiguredScopeRecord",
    "FacetActionStatusRecord",
    "FacetEvidenceRecord",
    "IntentScopeRecord",
    "IntentScopeWork",
    "OperationalEventRecord",
    "RelatedObjectRecord",
    "SQLiteStore",
    "StorageHeadroomUnavailable",
    "StoredAction",
    "SystemRecord",
    "backup_sqlite_database",
]
