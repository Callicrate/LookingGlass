CREATE TABLE IF NOT EXISTS configured_system_identities (
    system_kind TEXT NOT NULL,
    config_id TEXT NOT NULL,
    authority_key TEXT NOT NULL,
    system_id TEXT NOT NULL REFERENCES systems(system_id) ON DELETE RESTRICT,
    record_created_at TEXT NOT NULL,
    record_updated_at TEXT NOT NULL,
    PRIMARY KEY (system_kind, config_id, authority_key),
    UNIQUE (system_kind, system_id)
);
