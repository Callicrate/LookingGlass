DROP VIEW readable_configured_scopes;

CREATE VIEW readable_configured_scopes AS
SELECT scope.*
FROM configured_scopes AS scope
JOIN readable_systems AS system ON system.system_id = scope.system_id
WHERE lookingglass_read_is_uuid(scope.scope_id) = 1
  AND CASE
      WHEN scope.object_id IS NULL THEN 1
      WHEN lookingglass_read_is_uuid(scope.object_id) = 1
       AND EXISTS (
          SELECT 1 FROM readable_remote_objects AS object
          WHERE object.object_id = scope.object_id
            AND object.system_id = scope.system_id
            AND object.object_type = scope.object_type
      ) THEN 1
      ELSE lookingglass_read_corruption()
  END = 1
  AND lookingglass_read_is_contract_key(scope.object_type) = 1
  AND scope.enabled IN (0, 1)
  AND lookingglass_read_is_text(scope.display_name, 1024) = 1
  AND lookingglass_read_is_timestamp(scope.record_created_at) = 1
  AND lookingglass_read_is_timestamp(scope.record_updated_at) = 1
  AND lookingglass_read_timestamp_order_is_valid(
      scope.record_created_at,
      scope.record_updated_at
  ) = 1;
