DELETE FROM refresh_overrides
WHERE rowid IN (
    SELECT rowid
    FROM (
        SELECT
            rowid,
            ROW_NUMBER() OVER (
                PARTITION BY level, scope_id, COALESCE(facet, '')
                ORDER BY record_updated_at DESC, rowid DESC
            ) AS duplicate_rank
        FROM refresh_overrides
    )
    WHERE duplicate_rank > 1
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_refresh_overrides_identity
ON refresh_overrides (level, scope_id, COALESCE(facet, ''));
