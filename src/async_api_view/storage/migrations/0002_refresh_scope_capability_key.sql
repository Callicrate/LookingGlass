ALTER TABLE refresh_credit ADD COLUMN capability_key TEXT;
ALTER TABLE refresh_intent_scopes ADD COLUMN capability_key TEXT;
ALTER TABLE adapter_action_scopes ADD COLUMN capability_key TEXT;

CREATE INDEX IF NOT EXISTS ix_refresh_credit_scope_capability ON refresh_credit (
    system_id,
    target_kind,
    target_id,
    object_type,
    facet,
    capability_key,
    coverage,
    observed_at DESC
);
