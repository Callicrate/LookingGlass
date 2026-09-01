CREATE INDEX IF NOT EXISTS ix_adapter_actions_system_active_recency
ON adapter_actions (system_id, record_created_at DESC, action_id)
WHERE state IN ('ready', 'leased', 'running', 'retry_wait');
