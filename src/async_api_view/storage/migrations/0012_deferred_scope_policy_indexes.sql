CREATE INDEX IF NOT EXISTS ix_intent_scopes_deferred_system_facet
ON refresh_intent_scopes (system_id, facet, intent_scope_id)
WHERE state = 'deferred';

CREATE INDEX IF NOT EXISTS ix_intent_scopes_deferred_target_facet
ON refresh_intent_scopes (target_kind, target_id, facet, intent_scope_id)
WHERE state = 'deferred';
