CREATE TRIGGER reject_null_intent_scope_id_insert
BEFORE INSERT ON refresh_intent_scopes
WHEN NEW.intent_scope_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'refresh intent scope ID must not be NULL');
END;

CREATE TRIGGER reject_null_intent_scope_id_update
BEFORE UPDATE OF intent_scope_id ON refresh_intent_scopes
WHEN NEW.intent_scope_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'refresh intent scope ID must not be NULL');
END;

CREATE TRIGGER reject_null_action_id_insert
BEFORE INSERT ON adapter_actions
WHEN NEW.action_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'adapter action ID must not be NULL');
END;

CREATE TRIGGER reject_null_action_id_update
BEFORE UPDATE OF action_id ON adapter_actions
WHEN NEW.action_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'adapter action ID must not be NULL');
END;

CREATE VIEW readable_systems AS
SELECT * FROM systems
WHERE rookery_read_is_uuid(system_id) = 1
  AND rookery_read_is_text(display_name, 1024) = 1
  AND rookery_read_is_contract_key(system_kind) = 1
  AND enabled IN (0, 1)
  AND rookery_read_is_timestamp(record_created_at) = 1
  AND rookery_read_is_timestamp(record_updated_at) = 1
  AND rookery_read_timestamp_order_is_valid(record_created_at, record_updated_at) = 1;

CREATE VIEW readable_remote_objects AS
SELECT * FROM remote_objects
WHERE rookery_read_is_uuid(object_id) = 1
  AND rookery_read_is_uuid(system_id) = 1
  AND rookery_read_is_contract_key(object_type) = 1
  AND rookery_read_is_text(object_type_version, 32) = 1
  AND rookery_read_is_contract_key(source_kind) = 1
  AND rookery_read_is_text(external_key, 4096) = 1
  AND rookery_read_is_text(display_name, 1024) = 1
  AND presence IN ('unknown', 'present', 'absent')
  AND rookery_read_is_timestamp(first_seen_at) = 1
  AND (last_seen_at IS NULL OR rookery_read_is_timestamp(last_seen_at) = 1)
  AND (
      last_seen_at IS NULL
      OR rookery_read_timestamp_order_is_valid(first_seen_at, last_seen_at) = 1
  );

CREATE VIEW readable_facets AS
SELECT * FROM facets
WHERE rookery_read_is_uuid(object_id) = 1
  AND rookery_read_is_contract_key(facet) = 1
  AND rookery_read_is_text(facet_version, 32) = 1
  AND knowledge IN ('unknown', 'known', 'unsupported')
  AND rookery_read_is_json_object(payload_json) = 1
  AND (observed_at IS NULL OR rookery_read_is_timestamp(observed_at) = 1)
  AND rookery_read_is_timestamp(state_changed_at) = 1
  AND (
      supporting_observation_id IS NULL
      OR rookery_read_is_uuid(supporting_observation_id) = 1
  )
  AND (source_revision IS NULL OR rookery_read_is_text(source_revision, 512) = 1);

CREATE VIEW readable_relationships AS
SELECT * FROM relationships
WHERE rookery_read_is_uuid(relationship_id) = 1
  AND rookery_read_is_uuid(system_id) = 1
  AND rookery_read_is_uuid(subject_id) = 1
  AND rookery_read_is_contract_key(predicate) = 1
  AND rookery_read_is_uuid(object_id) = 1
  AND presence IN ('unknown', 'present', 'absent')
  AND rookery_read_is_timestamp(observed_at) = 1
  AND rookery_read_is_uuid(supporting_observation_id) = 1
  AND (
      (
          EXISTS (
              SELECT 1 FROM readable_remote_objects AS subject
              WHERE subject.object_id = relationships.subject_id
                AND subject.system_id = relationships.system_id
          )
          AND EXISTS (
              SELECT 1 FROM readable_remote_objects AS child
              WHERE child.object_id = relationships.object_id
                AND child.system_id = relationships.system_id
          )
      )
      OR rookery_read_corruption()
  );

CREATE VIEW readable_action_activity AS
SELECT * FROM adapter_actions
WHERE rookery_read_is_uuid(action_id) = 1
  AND rookery_read_is_uuid(system_id) = 1
  AND rookery_read_is_contract_key(capability_key) = 1
  AND target_kind IN ('object', 'configured_scope', 'system')
  AND rookery_read_is_uuid(target_id) = 1
  AND state IN (
      'ready', 'leased', 'running', 'retry_wait', 'satisfied',
      'succeeded', 'partial', 'failed', 'cancelled'
  )
  AND rookery_read_is_timestamp(record_created_at) = 1
  AND (started_at IS NULL OR rookery_read_is_timestamp(started_at) = 1)
  AND (completed_at IS NULL OR rookery_read_is_timestamp(completed_at) = 1)
  AND (retry_at IS NULL OR rookery_read_is_timestamp(retry_at) = 1);

CREATE VIEW readable_action_attempts AS
SELECT * FROM action_attempts
WHERE rookery_read_is_uuid(attempt_id) = 1
  AND rookery_read_is_uuid(action_id) = 1
  AND typeof(ordinal) = 'integer'
  AND ordinal > 0
  AND rookery_read_is_timestamp(started_at) = 1
  AND (ended_at IS NULL OR rookery_read_is_timestamp(ended_at) = 1)
  AND (retry_at IS NULL OR rookery_read_is_timestamp(retry_at) = 1);

CREATE VIEW readable_facet_action_status AS
SELECT * FROM facet_action_status
WHERE rookery_read_is_uuid(system_id) = 1
  AND target_kind IN ('object', 'configured_scope')
  AND rookery_read_is_uuid(target_id) = 1
  AND rookery_read_is_contract_key(facet) = 1
  AND candidate_class IN ('active', 'terminal')
  AND rookery_read_is_uuid(action_id) = 1
  AND state IN (
      'ready', 'leased', 'running', 'retry_wait', 'satisfied',
      'succeeded', 'partial', 'failed', 'cancelled'
  )
  AND rookery_read_is_timestamp(occurred_at) = 1;

CREATE VIEW readable_operational_events AS
SELECT
    event_id,
    idempotency_key,
    event_type,
    severity,
    alertable,
    CASE WHEN system_id IS NULL OR rookery_read_is_uuid(system_id) = 1
         THEN system_id ELSE NULL END AS system_id,
    intent_scope_id,
    CASE
        WHEN action_id IS NULL THEN NULL
        WHEN rookery_read_is_uuid(action_id) = 1
         AND EXISTS (
             SELECT 1 FROM readable_action_activity AS action
             WHERE action.action_id = operational_events.action_id
         ) THEN action_id
        ELSE rookery_read_corruption()
    END AS action_id,
    CASE WHEN attempt_id IS NULL OR rookery_read_is_uuid(attempt_id) = 1
         THEN attempt_id ELSE NULL END AS attempt_id,
    error_class,
    redacted_summary,
    occurred_at
FROM operational_events
WHERE rookery_read_is_uuid(event_id) = 1
  AND rookery_read_is_contract_key(event_type) = 1
  AND rookery_read_is_contract_key(severity) = 1
  AND alertable IN (0, 1)
  AND typeof(redacted_summary) = 'text'
  AND rookery_read_is_timestamp(occurred_at) = 1;

UPDATE adapter_actions
SET record_created_at = rookery_canonicalize_timestamp(record_created_at)
WHERE rookery_canonicalize_timestamp(record_created_at) IS NOT NULL;

UPDATE operational_events
SET occurred_at = rookery_canonicalize_timestamp(occurred_at)
WHERE rookery_canonicalize_timestamp(occurred_at) IS NOT NULL;
