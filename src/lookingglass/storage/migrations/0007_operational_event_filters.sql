CREATE INDEX IF NOT EXISTS ix_operational_events_alertable_type_severity_recency
ON operational_events (alertable, event_type, severity, occurred_at DESC, event_id);

CREATE INDEX IF NOT EXISTS ix_operational_events_alertable_type_recency
ON operational_events (alertable, event_type, occurred_at DESC, event_id);

CREATE INDEX IF NOT EXISTS ix_operational_events_alertable_severity_recency
ON operational_events (alertable, severity, occurred_at DESC, event_id);
