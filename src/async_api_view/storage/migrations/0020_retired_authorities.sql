CREATE TABLE IF NOT EXISTS retired_system_authorities (
    system_id TEXT PRIMARY KEY REFERENCES systems(system_id) ON DELETE RESTRICT,
    retired_at TEXT NOT NULL
);
