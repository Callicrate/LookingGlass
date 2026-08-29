ALTER TABLE refresh_intent_scopes
ADD COLUMN queue_priority INTEGER NOT NULL DEFAULT 0;

ALTER TABLE refresh_intent_scopes
ADD COLUMN queue_requested_at TEXT NOT NULL DEFAULT '';

UPDATE refresh_intent_scopes
SET queue_priority = COALESCE(
        (SELECT intent.priority FROM refresh_intents AS intent
         WHERE intent.intent_id = refresh_intent_scopes.intent_id),
        0
    ),
    queue_requested_at = COALESCE(
        (SELECT intent.requested_at FROM refresh_intents AS intent
         WHERE intent.intent_id = refresh_intent_scopes.intent_id),
        ''
    );

CREATE INDEX ix_refresh_intent_scopes_claim_order
ON refresh_intent_scopes (
    queue_priority DESC, queue_requested_at, intent_scope_id
)
WHERE state = 'queued';

CREATE INDEX ix_refresh_intent_scopes_deferred_due
ON refresh_intent_scopes (eligible_at, intent_scope_id)
WHERE state = 'deferred';

CREATE INDEX ix_refresh_intent_scopes_lease_due
ON refresh_intent_scopes (leased_until, intent_scope_id)
WHERE state = 'leased';

CREATE INDEX ix_adapter_actions_claim_order
ON adapter_actions (adapter_key, record_created_at, action_id)
WHERE state = 'ready';

CREATE INDEX ix_adapter_actions_lease_due
ON adapter_actions (adapter_key, leased_until, action_id)
WHERE state IN ('leased', 'running');

CREATE INDEX ix_adapter_actions_retry_due
ON adapter_actions (adapter_key, retry_at, action_id)
WHERE state = 'retry_wait';
