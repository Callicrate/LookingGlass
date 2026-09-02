"""The narrow canonical-write facade available to adapter workers."""

from __future__ import annotations

from lookingglass.contracts import IngestionResult, ObservationBatch
from lookingglass.storage import SQLiteStore


class SQLiteObservationIngestor:
    """Expose the observation-ingestion port without any adapter dependency."""

    def __init__(self, store: SQLiteStore) -> None:
        self._store = store

    async def ingest(
        self, batch: ObservationBatch, *, lease_id: str | None = None
    ) -> IngestionResult:
        return await self._store.ingest(batch, lease_id=lease_id)
