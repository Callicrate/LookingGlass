CREATE TABLE IF NOT EXISTS systems (
    system_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    system_kind TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    record_created_at TEXT NOT NULL,
    record_updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS connection_bindings (
    binding_id TEXT PRIMARY KEY,
    system_id TEXT NOT NULL REFERENCES systems(system_id) ON DELETE RESTRICT,
    adapter_key TEXT NOT NULL,
    adapter_version TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    non_secret_settings_json TEXT NOT NULL,
    secret_reference TEXT,
    record_created_at TEXT NOT NULL,
    record_updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS capability_bindings (
    capability_binding_id TEXT PRIMARY KEY,
    connection_binding_id TEXT NOT NULL REFERENCES connection_bindings(binding_id) ON DELETE RESTRICT,
    capability_key TEXT NOT NULL,
    capability_version TEXT NOT NULL,
    operation_class TEXT NOT NULL CHECK (operation_class = 'observe'),
    target_kinds_json TEXT NOT NULL,
    produced_facets_json TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    selection_priority INTEGER NOT NULL,
    collateral_effects_json TEXT NOT NULL,
    mitigations_json TEXT NOT NULL,
    record_created_at TEXT NOT NULL,
    record_updated_at TEXT NOT NULL,
    UNIQUE (connection_binding_id, capability_key, capability_version)
);

CREATE TABLE IF NOT EXISTS remote_objects (
    object_id TEXT PRIMARY KEY,
    system_id TEXT NOT NULL REFERENCES systems(system_id) ON DELETE RESTRICT,
    object_type TEXT NOT NULL,
    object_type_version TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    external_key TEXT NOT NULL,
    display_name TEXT NOT NULL,
    presence TEXT NOT NULL CHECK (presence IN ('unknown', 'present', 'absent')),
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT,
    UNIQUE (system_id, source_kind, external_key)
);

CREATE TABLE IF NOT EXISTS configured_scopes (
    scope_id TEXT PRIMARY KEY,
    system_id TEXT NOT NULL REFERENCES systems(system_id) ON DELETE RESTRICT,
    object_id TEXT REFERENCES remote_objects(object_id) ON DELETE RESTRICT,
    object_type TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    display_name TEXT NOT NULL,
    record_created_at TEXT NOT NULL,
    record_updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS facets (
    object_id TEXT NOT NULL REFERENCES remote_objects(object_id) ON DELETE RESTRICT,
    facet TEXT NOT NULL,
    facet_version TEXT NOT NULL,
    knowledge TEXT NOT NULL CHECK (knowledge IN ('unknown', 'known', 'unsupported')),
    payload_json TEXT NOT NULL,
    observed_at TEXT,
    state_changed_at TEXT NOT NULL,
    supporting_observation_id TEXT,
    source_revision TEXT,
    PRIMARY KEY (object_id, facet)
);

CREATE TABLE IF NOT EXISTS relationships (
    relationship_id TEXT PRIMARY KEY,
    system_id TEXT NOT NULL REFERENCES systems(system_id) ON DELETE RESTRICT,
    subject_id TEXT NOT NULL REFERENCES remote_objects(object_id) ON DELETE RESTRICT,
    predicate TEXT NOT NULL,
    object_id TEXT NOT NULL REFERENCES remote_objects(object_id) ON DELETE RESTRICT,
    presence TEXT NOT NULL CHECK (presence IN ('unknown', 'present', 'absent')),
    observed_at TEXT NOT NULL,
    supporting_observation_id TEXT NOT NULL,
    UNIQUE (system_id, subject_id, predicate, object_id)
);

CREATE TABLE IF NOT EXISTS observation_batches (
    batch_id TEXT PRIMARY KEY,
    system_id TEXT NOT NULL REFERENCES systems(system_id) ON DELETE RESTRICT,
    connection_binding_id TEXT NOT NULL REFERENCES connection_bindings(binding_id) ON DELETE RESTRICT,
    adapter_key TEXT NOT NULL,
    adapter_version TEXT NOT NULL,
    action_id TEXT,
    observed_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('accepted', 'partial', 'duplicate', 'rejected')),
    accepted_ids_json TEXT NOT NULL,
    issue_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS observation_journal (
    observation_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES observation_batches(batch_id) ON DELETE RESTRICT,
    item_kind TEXT NOT NULL CHECK (item_kind IN ('facet', 'relationship', 'coverage')),
    item_json TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    received_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS refresh_credit (
    credit_id TEXT PRIMARY KEY,
    observation_id TEXT NOT NULL REFERENCES observation_journal(observation_id) ON DELETE RESTRICT,
    system_id TEXT NOT NULL REFERENCES systems(system_id) ON DELETE RESTRICT,
    target_kind TEXT NOT NULL,
    target_id TEXT NOT NULL,
    object_type TEXT NOT NULL,
    facet TEXT NOT NULL,
    coverage TEXT NOT NULL,
    field_mask_json TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    UNIQUE (observation_id, system_id, target_kind, target_id, object_type, facet, coverage, field_mask_json)
);

CREATE INDEX IF NOT EXISTS ix_refresh_credit_scope ON refresh_credit (
    system_id, target_kind, target_id, object_type, facet, coverage, observed_at DESC
);

CREATE TABLE IF NOT EXISTS refresh_overrides (
    level TEXT NOT NULL CHECK (level IN ('object', 'system')),
    scope_id TEXT NOT NULL,
    facet TEXT,
    interval_seconds INTEGER NOT NULL CHECK (interval_seconds > 0),
    record_updated_at TEXT NOT NULL,
    PRIMARY KEY (level, scope_id, facet)
);

CREATE TABLE IF NOT EXISTS refresh_intents (
    intent_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    origin TEXT NOT NULL CHECK (origin IN ('manual', 'automatic')),
    actor_id TEXT NOT NULL,
    ui_session_id TEXT,
    requested_at TEXT NOT NULL,
    expires_at TEXT,
    priority INTEGER NOT NULL,
    aggregate_state TEXT NOT NULL CHECK (aggregate_state IN ('open', 'complete', 'cancelled')),
    contract_version TEXT NOT NULL,
    accepted_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS refresh_intent_scopes (
    intent_scope_id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL REFERENCES refresh_intents(intent_id) ON DELETE RESTRICT,
    system_id TEXT NOT NULL REFERENCES systems(system_id) ON DELETE RESTRICT,
    target_kind TEXT NOT NULL,
    target_id TEXT NOT NULL,
    object_type TEXT NOT NULL,
    facet TEXT NOT NULL,
    coverage TEXT NOT NULL,
    field_mask_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('queued', 'leased', 'deferred', 'coalesced', 'admitted', 'satisfied', 'rejected', 'expired', 'cancelled')),
    disposition_reason TEXT,
    eligible_at TEXT,
    linked_action_id TEXT,
    satisfying_observation_id TEXT,
    lease_id TEXT,
    lease_worker_id TEXT,
    leased_until TEXT
);

CREATE INDEX IF NOT EXISTS ix_intent_scope_queue ON refresh_intent_scopes (
    state, eligible_at, leased_until
);

CREATE TABLE IF NOT EXISTS adapter_actions (
    action_id TEXT PRIMARY KEY,
    correlation_id TEXT NOT NULL,
    system_id TEXT NOT NULL REFERENCES systems(system_id) ON DELETE RESTRICT,
    connection_binding_id TEXT NOT NULL REFERENCES connection_bindings(binding_id) ON DELETE RESTRICT,
    adapter_key TEXT NOT NULL,
    adapter_version TEXT NOT NULL,
    capability_key TEXT NOT NULL,
    capability_version TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    target_id TEXT NOT NULL,
    deadline TEXT,
    contract_version TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ready', 'leased', 'running', 'retry_wait', 'satisfied', 'succeeded', 'partial', 'failed', 'cancelled')),
    started_at TEXT,
    completed_at TEXT,
    lease_id TEXT,
    lease_worker_id TEXT,
    leased_until TEXT,
    error_class TEXT,
    redacted_diagnostic TEXT,
    retry_at TEXT,
    record_created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_active_action_dedupe ON adapter_actions (dedupe_key)
WHERE state IN ('ready', 'leased', 'running', 'retry_wait');

CREATE INDEX IF NOT EXISTS ix_action_queue ON adapter_actions (adapter_key, state, leased_until);

CREATE TABLE IF NOT EXISTS adapter_action_scopes (
    action_id TEXT NOT NULL REFERENCES adapter_actions(action_id) ON DELETE RESTRICT,
    system_id TEXT NOT NULL REFERENCES systems(system_id) ON DELETE RESTRICT,
    target_kind TEXT NOT NULL,
    target_id TEXT NOT NULL,
    object_type TEXT NOT NULL,
    facet TEXT NOT NULL,
    coverage TEXT NOT NULL,
    field_mask_json TEXT NOT NULL,
    PRIMARY KEY (action_id, system_id, target_kind, target_id, object_type, facet, coverage, field_mask_json)
);

CREATE TABLE IF NOT EXISTS action_intent_scopes (
    action_id TEXT NOT NULL REFERENCES adapter_actions(action_id) ON DELETE RESTRICT,
    intent_scope_id TEXT NOT NULL UNIQUE REFERENCES refresh_intent_scopes(intent_scope_id) ON DELETE RESTRICT,
    PRIMARY KEY (action_id, intent_scope_id)
);

CREATE TABLE IF NOT EXISTS action_attempts (
    attempt_id TEXT PRIMARY KEY,
    action_id TEXT NOT NULL REFERENCES adapter_actions(action_id) ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    outcome TEXT,
    error_class TEXT,
    retry_at TEXT,
    redacted_diagnostic TEXT,
    UNIQUE (action_id, ordinal)
);

CREATE TABLE IF NOT EXISTS operational_events (
    event_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    alertable INTEGER NOT NULL CHECK (alertable IN (0, 1)),
    system_id TEXT REFERENCES systems(system_id) ON DELETE RESTRICT,
    intent_scope_id TEXT REFERENCES refresh_intent_scopes(intent_scope_id) ON DELETE RESTRICT,
    action_id TEXT REFERENCES adapter_actions(action_id) ON DELETE RESTRICT,
    attempt_id TEXT REFERENCES action_attempts(attempt_id) ON DELETE RESTRICT,
    error_class TEXT,
    redacted_summary TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingestion_issues (
    issue_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES observation_batches(batch_id) ON DELETE RESTRICT,
    action_id TEXT,
    item_kind TEXT NOT NULL,
    error_class TEXT NOT NULL,
    redacted_detail TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
