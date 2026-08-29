CREATE INDEX IF NOT EXISTS ix_operational_events_alertable_recency
ON operational_events (alertable, occurred_at DESC, event_id);
