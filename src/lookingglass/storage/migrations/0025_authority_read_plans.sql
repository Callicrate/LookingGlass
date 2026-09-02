DROP VIEW readable_relationships;

CREATE TABLE relationship_read_index (
    relationship_id TEXT PRIMARY KEY REFERENCES relationships(relationship_id) ON DELETE CASCADE,
    system_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    predicate TEXT NOT NULL,
    presence TEXT NOT NULL,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL
);

INSERT INTO relationship_read_index (
    relationship_id, system_id, subject_id, predicate, presence, object_type, object_id
)
SELECT
    relationship.relationship_id,
    relationship.system_id,
    relationship.subject_id,
    relationship.predicate,
    relationship.presence,
    object.object_type,
    relationship.object_id
FROM relationships AS relationship
JOIN remote_objects AS object ON object.object_id = relationship.object_id;

CREATE TRIGGER populate_relationship_read_index
AFTER INSERT ON relationships
BEGIN
    INSERT INTO relationship_read_index (
        relationship_id, system_id, subject_id, predicate, presence, object_type, object_id
    )
    SELECT
        NEW.relationship_id, NEW.system_id, NEW.subject_id, NEW.predicate,
        NEW.presence, object.object_type, NEW.object_id
    FROM remote_objects AS object WHERE object.object_id = NEW.object_id;
END;

CREATE TRIGGER update_relationship_read_index
AFTER UPDATE OF system_id, subject_id, predicate, presence, object_id ON relationships
BEGIN
    INSERT INTO relationship_read_index (
        relationship_id, system_id, subject_id, predicate, presence, object_type, object_id
    )
    SELECT
        NEW.relationship_id, NEW.system_id, NEW.subject_id, NEW.predicate,
        NEW.presence, object.object_type, NEW.object_id
    FROM remote_objects AS object WHERE object.object_id = NEW.object_id
    ON CONFLICT(relationship_id) DO UPDATE SET
        system_id = excluded.system_id,
        subject_id = excluded.subject_id,
        predicate = excluded.predicate,
        presence = excluded.presence,
        object_type = excluded.object_type,
        object_id = excluded.object_id;
END;

CREATE TRIGGER delete_relationship_read_index
AFTER DELETE ON relationships
BEGIN
    DELETE FROM relationship_read_index WHERE relationship_id = OLD.relationship_id;
END;

CREATE INDEX ix_relationships_subject_type_cursor
ON relationship_read_index (
    subject_id,
    predicate,
    presence,
    object_type,
    object_id
);

CREATE VIEW readable_relationships AS
SELECT
    relationship.relationship_id,
    relationship.system_id,
    projection.subject_id,
    projection.predicate,
    projection.object_id,
    projection.presence,
    relationship.observed_at,
    relationship.supporting_observation_id,
    relationship.received_at,
    projection.object_type
FROM relationship_read_index AS projection
JOIN relationships AS relationship
  ON relationship.relationship_id = projection.relationship_id
WHERE lookingglass_read_is_uuid(relationship.relationship_id) = 1
  AND lookingglass_read_is_uuid(relationship.system_id) = 1
  AND lookingglass_read_is_uuid(projection.subject_id) = 1
  AND lookingglass_read_is_contract_key(projection.predicate) = 1
  AND lookingglass_read_is_uuid(projection.object_id) = 1
  AND lookingglass_read_is_contract_key(projection.object_type) = 1
  AND projection.presence IN ('unknown', 'present', 'absent')
  AND lookingglass_read_is_timestamp(relationship.observed_at) = 1
  AND lookingglass_read_is_uuid(relationship.supporting_observation_id) = 1
  AND CASE
      WHEN EXISTS (
          SELECT 1 FROM readable_remote_objects AS subject
          WHERE subject.object_id = projection.subject_id
            AND subject.system_id = relationship.system_id
      ) AND EXISTS (
          SELECT 1 FROM readable_remote_objects AS child
          WHERE child.object_id = projection.object_id
            AND child.system_id = relationship.system_id
            AND child.object_type = projection.object_type
      ) THEN 1
      ELSE lookingglass_read_corruption()
  END = 1;

CREATE VIEW readable_connection_bindings AS
SELECT binding.*
FROM connection_bindings AS binding
JOIN readable_systems AS system ON system.system_id = binding.system_id
WHERE lookingglass_read_is_uuid(binding.binding_id) = 1
  AND lookingglass_read_is_contract_key(binding.adapter_key) = 1
  AND lookingglass_read_is_text(binding.adapter_version, 64) = 1
  AND binding.enabled IN (0, 1)
  AND lookingglass_read_is_binding_settings(binding.non_secret_settings_json) = 1
  AND (
      binding.secret_reference IS NULL
      OR lookingglass_read_is_text(binding.secret_reference, 1024) = 1
  )
  AND lookingglass_read_is_timestamp(binding.record_created_at) = 1
  AND lookingglass_read_is_timestamp(binding.record_updated_at) = 1
  AND lookingglass_read_timestamp_order_is_valid(
      binding.record_created_at,
      binding.record_updated_at
  ) = 1;

CREATE VIEW readable_capability_bindings AS
SELECT capability.*
FROM capability_bindings AS capability
JOIN readable_connection_bindings AS binding
  ON binding.binding_id = capability.connection_binding_id
WHERE lookingglass_read_is_uuid(capability.capability_binding_id) = 1
  AND lookingglass_read_is_contract_key(capability.capability_key) = 1
  AND lookingglass_read_is_text(capability.capability_version, 64) = 1
  AND capability.operation_class = 'observe'
  AND lookingglass_read_is_json_array(capability.target_kinds_json) = 1
  AND lookingglass_read_is_json_array(capability.produced_facets_json) = 1
  AND capability.enabled IN (0, 1)
  AND typeof(capability.selection_priority) = 'integer'
  AND lookingglass_read_is_json_array(capability.collateral_effects_json) = 1
  AND lookingglass_read_is_json_array(capability.mitigations_json) = 1
  AND lookingglass_read_is_json_array(capability.coverage_policies_json) = 1
  AND capability.coverage_policy_initialized IN (0, 1)
  AND lookingglass_read_is_capability(
      capability.capability_binding_id,
      capability.connection_binding_id,
      capability.capability_key,
      capability.capability_version,
      capability.operation_class,
      capability.target_kinds_json,
      capability.produced_facets_json,
      capability.enabled,
      capability.selection_priority,
      capability.collateral_effects_json,
      capability.mitigations_json,
      capability.coverage_policies_json,
      capability.coverage_policy_initialized
  ) = 1
  AND lookingglass_read_is_timestamp(capability.record_created_at) = 1
  AND lookingglass_read_is_timestamp(capability.record_updated_at) = 1
  AND lookingglass_read_timestamp_order_is_valid(
      capability.record_created_at,
      capability.record_updated_at
  ) = 1;

CREATE VIEW readable_configured_scopes AS
SELECT scope.*
FROM configured_scopes AS scope
JOIN readable_systems AS system ON system.system_id = scope.system_id
WHERE lookingglass_read_is_uuid(scope.scope_id) = 1
  AND (
      scope.object_id IS NULL
      OR (
          lookingglass_read_is_uuid(scope.object_id) = 1
          AND EXISTS (
              SELECT 1 FROM readable_remote_objects AS object
              WHERE object.object_id = scope.object_id
                AND object.system_id = scope.system_id
                AND object.object_type = scope.object_type
          )
      )
      OR lookingglass_read_corruption()
  )
  AND lookingglass_read_is_contract_key(scope.object_type) = 1
  AND scope.enabled IN (0, 1)
  AND lookingglass_read_is_text(scope.display_name, 1024) = 1
  AND lookingglass_read_is_timestamp(scope.record_created_at) = 1
  AND lookingglass_read_is_timestamp(scope.record_updated_at) = 1
  AND lookingglass_read_timestamp_order_is_valid(
      scope.record_created_at,
      scope.record_updated_at
  ) = 1;

CREATE VIEW readable_configured_system_identities AS
SELECT identity.*
FROM configured_system_identities AS identity
JOIN readable_systems AS system
  ON system.system_id = identity.system_id
 AND system.system_kind = identity.system_kind
WHERE lookingglass_read_is_contract_key(identity.system_kind) = 1
  AND lookingglass_read_is_config_id(identity.config_id) = 1
  AND lookingglass_read_is_authority_key(identity.authority_key) = 1
  AND lookingglass_read_is_timestamp(identity.record_created_at) = 1
  AND lookingglass_read_is_timestamp(identity.record_updated_at) = 1
  AND lookingglass_read_timestamp_order_is_valid(
      identity.record_created_at,
      identity.record_updated_at
  ) = 1
  AND EXISTS (
      SELECT 1 FROM readable_connection_bindings AS binding
      WHERE binding.system_id = identity.system_id
        AND lookingglass_read_identity_matches_binding(
            identity.authority_key,
            binding.non_secret_settings_json
        ) = 1
  );

CREATE INDEX ix_adapter_actions_readable_recency
ON adapter_actions (record_created_at DESC, action_id)
WHERE state IN (
    'ready', 'leased', 'running', 'retry_wait', 'satisfied',
    'succeeded', 'partial', 'failed', 'cancelled'
);

CREATE VIEW readable_action_activity_recency AS
SELECT *
FROM adapter_actions INDEXED BY ix_adapter_actions_readable_recency
WHERE lookingglass_read_is_uuid(action_id) = 1
  AND lookingglass_read_is_uuid(system_id) = 1
  AND lookingglass_read_is_contract_key(capability_key) = 1
  AND target_kind IN ('object', 'configured_scope', 'system')
  AND lookingglass_read_is_uuid(target_id) = 1
  AND state IN (
      'ready', 'leased', 'running', 'retry_wait', 'satisfied',
      'succeeded', 'partial', 'failed', 'cancelled'
  )
  AND lookingglass_read_is_timestamp(record_created_at) = 1
  AND (started_at IS NULL OR lookingglass_read_is_timestamp(started_at) = 1)
  AND (completed_at IS NULL OR lookingglass_read_is_timestamp(completed_at) = 1)
  AND (retry_at IS NULL OR lookingglass_read_is_timestamp(retry_at) = 1);
