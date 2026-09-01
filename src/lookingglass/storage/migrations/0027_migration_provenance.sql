CREATE TABLE migration_provenance (
    version TEXT PRIMARY KEY REFERENCES schema_migrations(version) ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL UNIQUE CHECK (ordinal > 0),
    content_sha256 TEXT NOT NULL CHECK (
        length(content_sha256) = 64
        AND content_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    content_bytes INTEGER NOT NULL CHECK (content_bytes >= 0),
    chain_sha256 TEXT NOT NULL CHECK (
        length(chain_sha256) = 64
        AND chain_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    basis TEXT NOT NULL CHECK (basis IN ('executed', 'ledger_adopted')),
    recorded_at TEXT NOT NULL
);

CREATE TRIGGER migration_provenance_reject_update
BEFORE UPDATE ON migration_provenance
BEGIN
    SELECT RAISE(ABORT, 'migration provenance is immutable');
END;

CREATE TRIGGER migration_provenance_reject_delete
BEFORE DELETE ON migration_provenance
BEGIN
    SELECT RAISE(ABORT, 'migration provenance is immutable');
END;
