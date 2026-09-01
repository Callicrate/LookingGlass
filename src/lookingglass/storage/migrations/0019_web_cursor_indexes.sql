CREATE INDEX IF NOT EXISTS ix_remote_objects_display_cursor
ON remote_objects (display_name COLLATE NOCASE, object_id);

CREATE INDEX IF NOT EXISTS ix_refresh_intent_scopes_deferred_priority_due
ON refresh_intent_scopes (
    queue_priority DESC, eligible_at, queue_requested_at, intent_scope_id
)
WHERE state = 'deferred';
