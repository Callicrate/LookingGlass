CREATE INDEX ix_observation_batches_received_at
ON observation_batches (received_at DESC, batch_id DESC);
