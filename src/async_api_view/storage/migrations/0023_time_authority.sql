ALTER TABLE observation_batches
ADD COLUMN observed_at_is_local INTEGER NOT NULL DEFAULT 0
CHECK (observed_at_is_local IN (0, 1));

UPDATE observation_batches
SET observed_at_is_local = 1
WHERE adapter_key = 'databricks' AND action_id IS NOT NULL;

ALTER TABLE refresh_intent_scopes ADD COLUMN lease_authority_at TEXT;

UPDATE refresh_intent_scopes
SET state = 'queued', lease_id = NULL, lease_worker_id = NULL,
    leased_until = NULL, lease_authority_at = NULL
WHERE state = 'leased';
