CREATE INDEX IF NOT EXISTS ix_adapter_actions_recency
ON adapter_actions (record_created_at DESC, action_id);

CREATE INDEX IF NOT EXISTS ix_adapter_actions_state_recency
ON adapter_actions (state, record_created_at DESC, action_id);

CREATE INDEX IF NOT EXISTS ix_adapter_actions_system_recency
ON adapter_actions (system_id, record_created_at DESC, action_id);

CREATE INDEX IF NOT EXISTS ix_adapter_actions_system_state_recency
ON adapter_actions (system_id, state, record_created_at DESC, action_id);
