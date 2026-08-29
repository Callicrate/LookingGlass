"""SQLite-backed local durable state.

``SQLiteStore`` is the composition-facing concrete implementation of every
shared local port.  It also provides synchronous configuration/query helpers
for bootstrap and future presentation code; adapter workers should use only the
shared action-lifecycle and observation-ingestion ports.
"""

from .models import (
    ActionActivityRecord,
    ActionAttemptRecord,
    ConfiguredScopeRecord,
    FacetActionStatusRecord,
    IntentScopeRecord,
    IntentScopeWork,
    OperationalEventRecord,
    RelatedObjectRecord,
    StoredAction,
    SystemRecord,
)
from .sqlite import SQLiteStore, backup_sqlite_database

__all__ = [
    "ActionActivityRecord",
    "ActionAttemptRecord",
    "ConfiguredScopeRecord",
    "FacetActionStatusRecord",
    "IntentScopeRecord",
    "IntentScopeWork",
    "OperationalEventRecord",
    "RelatedObjectRecord",
    "SQLiteStore",
    "StoredAction",
    "SystemRecord",
    "backup_sqlite_database",
]
