CREATE INDEX IF NOT EXISTS ix_adapter_action_scopes_target_facet
ON adapter_action_scopes (target_kind, target_id, facet, action_id);

CREATE INDEX IF NOT EXISTS ix_configured_scopes_object
ON configured_scopes (object_id, scope_id);
