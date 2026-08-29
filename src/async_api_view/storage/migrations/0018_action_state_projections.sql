CREATE TABLE action_scope_cooldown (
    system_id TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    target_id TEXT NOT NULL,
    object_type TEXT NOT NULL,
    facet TEXT NOT NULL,
    capability_key_is_null INTEGER NOT NULL CHECK (capability_key_is_null IN (0, 1)),
    capability_key TEXT NOT NULL,
    coverage TEXT NOT NULL,
    field_mask_json TEXT NOT NULL,
    latest_started_at TEXT NOT NULL,
    PRIMARY KEY (
        system_id, target_kind, target_id, object_type, facet,
        capability_key_is_null, capability_key, coverage, field_mask_json
    )
);

CREATE TABLE facet_action_status (
    system_id TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    target_id TEXT NOT NULL,
    facet TEXT NOT NULL,
    candidate_class TEXT NOT NULL CHECK (candidate_class IN ('active', 'terminal')),
    action_id TEXT NOT NULL REFERENCES adapter_actions(action_id) ON DELETE CASCADE,
    state TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    redacted_diagnostic TEXT,
    PRIMARY KEY (system_id, target_kind, target_id, facet, candidate_class, action_id)
);

CREATE UNIQUE INDEX ux_facet_action_status_terminal
ON facet_action_status (system_id, target_kind, target_id, facet)
WHERE candidate_class = 'terminal';

CREATE INDEX ix_facet_action_status_target
ON facet_action_status (
    target_kind, target_id, facet, candidate_class, occurred_at DESC, action_id DESC
);

INSERT INTO action_scope_cooldown (
    system_id, target_kind, target_id, object_type, facet,
    capability_key_is_null, capability_key, coverage, field_mask_json,
    latest_started_at
)
SELECT
    scope.system_id, scope.target_kind, scope.target_id, scope.object_type, scope.facet,
    scope.capability_key IS NULL, COALESCE(scope.capability_key, ''),
    scope.coverage, scope.field_mask_json, MAX(action.started_at)
FROM adapter_action_scopes AS scope
JOIN adapter_actions AS action ON action.action_id = scope.action_id
WHERE action.started_at IS NOT NULL
GROUP BY
    scope.system_id, scope.target_kind, scope.target_id, scope.object_type, scope.facet,
    scope.capability_key IS NULL, COALESCE(scope.capability_key, ''),
    scope.coverage, scope.field_mask_json;

INSERT INTO facet_action_status (
    system_id, target_kind, target_id, facet, candidate_class,
    action_id, state, occurred_at, redacted_diagnostic
)
SELECT DISTINCT
    scope.system_id, scope.target_kind, scope.target_id, scope.facet, 'active',
    action.action_id, action.state,
    COALESCE(action.completed_at, action.started_at, action.record_created_at),
    action.redacted_diagnostic
FROM adapter_action_scopes AS scope
JOIN adapter_actions AS action ON action.action_id = scope.action_id
WHERE action.state IN ('ready', 'leased', 'running', 'retry_wait');

INSERT INTO facet_action_status (
    system_id, target_kind, target_id, facet, candidate_class,
    action_id, state, occurred_at, redacted_diagnostic
)
SELECT
    system_id, target_kind, target_id, facet, 'terminal',
    action_id, state, occurred_at, redacted_diagnostic
FROM (
    SELECT
        scope.system_id, scope.target_kind, scope.target_id, scope.facet,
        action.action_id, action.state,
        COALESCE(action.completed_at, action.started_at, action.record_created_at) AS occurred_at,
        action.redacted_diagnostic,
        ROW_NUMBER() OVER (
            PARTITION BY scope.system_id, scope.target_kind, scope.target_id, scope.facet
            ORDER BY
                COALESCE(action.completed_at, action.started_at, action.record_created_at) DESC,
                action.action_id DESC
        ) AS candidate_rank
    FROM adapter_action_scopes AS scope
    JOIN adapter_actions AS action ON action.action_id = scope.action_id
    WHERE action.state IN ('satisfied', 'succeeded', 'partial', 'failed', 'cancelled')
)
WHERE candidate_rank = 1;

CREATE TRIGGER trg_action_scope_projection_insert
AFTER INSERT ON adapter_action_scopes
BEGIN
    INSERT INTO action_scope_cooldown (
        system_id, target_kind, target_id, object_type, facet,
        capability_key_is_null, capability_key, coverage, field_mask_json,
        latest_started_at
    )
    SELECT
        NEW.system_id, NEW.target_kind, NEW.target_id, NEW.object_type, NEW.facet,
        NEW.capability_key IS NULL, COALESCE(NEW.capability_key, ''),
        NEW.coverage, NEW.field_mask_json, action.started_at
    FROM adapter_actions AS action
    WHERE action.action_id = NEW.action_id AND action.started_at IS NOT NULL
    ON CONFLICT (
        system_id, target_kind, target_id, object_type, facet,
        capability_key_is_null, capability_key, coverage, field_mask_json
    ) DO UPDATE SET latest_started_at = CASE
        WHEN excluded.latest_started_at > action_scope_cooldown.latest_started_at
        THEN excluded.latest_started_at
        ELSE action_scope_cooldown.latest_started_at
    END;

    INSERT INTO facet_action_status (
        system_id, target_kind, target_id, facet, candidate_class,
        action_id, state, occurred_at, redacted_diagnostic
    )
    SELECT
        NEW.system_id, NEW.target_kind, NEW.target_id, NEW.facet, 'active',
        action.action_id, action.state,
        COALESCE(action.completed_at, action.started_at, action.record_created_at),
        action.redacted_diagnostic
    FROM adapter_actions AS action
    WHERE action.action_id = NEW.action_id
      AND action.state IN ('ready', 'leased', 'running', 'retry_wait')
    ON CONFLICT (
        system_id, target_kind, target_id, facet, candidate_class, action_id
    ) DO UPDATE SET
        state = excluded.state,
        occurred_at = excluded.occurred_at,
        redacted_diagnostic = excluded.redacted_diagnostic;

    INSERT INTO facet_action_status (
        system_id, target_kind, target_id, facet, candidate_class,
        action_id, state, occurred_at, redacted_diagnostic
    )
    SELECT
        NEW.system_id, NEW.target_kind, NEW.target_id, NEW.facet, 'terminal',
        action.action_id, action.state,
        COALESCE(action.completed_at, action.started_at, action.record_created_at),
        action.redacted_diagnostic
    FROM adapter_actions AS action
    WHERE action.action_id = NEW.action_id
      AND action.state IN ('satisfied', 'succeeded', 'partial', 'failed', 'cancelled')
    ON CONFLICT (system_id, target_kind, target_id, facet)
        WHERE candidate_class = 'terminal'
    DO UPDATE SET
        action_id = excluded.action_id,
        state = excluded.state,
        occurred_at = excluded.occurred_at,
        redacted_diagnostic = excluded.redacted_diagnostic
    WHERE excluded.occurred_at > facet_action_status.occurred_at
       OR (excluded.occurred_at = facet_action_status.occurred_at
           AND excluded.action_id >= facet_action_status.action_id);
END;

CREATE TRIGGER trg_action_projection_update
AFTER UPDATE OF state, started_at, completed_at, record_created_at, redacted_diagnostic
ON adapter_actions
BEGIN
    INSERT INTO action_scope_cooldown (
        system_id, target_kind, target_id, object_type, facet,
        capability_key_is_null, capability_key, coverage, field_mask_json,
        latest_started_at
    )
    SELECT
        scope.system_id, scope.target_kind, scope.target_id, scope.object_type, scope.facet,
        scope.capability_key IS NULL, COALESCE(scope.capability_key, ''),
        scope.coverage, scope.field_mask_json, NEW.started_at
    FROM adapter_action_scopes AS scope
    WHERE scope.action_id = NEW.action_id AND NEW.started_at IS NOT NULL
    ON CONFLICT (
        system_id, target_kind, target_id, object_type, facet,
        capability_key_is_null, capability_key, coverage, field_mask_json
    ) DO UPDATE SET latest_started_at = CASE
        WHEN excluded.latest_started_at > action_scope_cooldown.latest_started_at
        THEN excluded.latest_started_at
        ELSE action_scope_cooldown.latest_started_at
    END;

    DELETE FROM facet_action_status
    WHERE candidate_class = 'active' AND action_id = NEW.action_id;

    INSERT INTO facet_action_status (
        system_id, target_kind, target_id, facet, candidate_class,
        action_id, state, occurred_at, redacted_diagnostic
    )
    SELECT DISTINCT
        scope.system_id, scope.target_kind, scope.target_id, scope.facet, 'active',
        NEW.action_id, NEW.state,
        COALESCE(NEW.completed_at, NEW.started_at, NEW.record_created_at),
        NEW.redacted_diagnostic
    FROM adapter_action_scopes AS scope
    WHERE scope.action_id = NEW.action_id
      AND NEW.state IN ('ready', 'leased', 'running', 'retry_wait')
    ON CONFLICT (
        system_id, target_kind, target_id, facet, candidate_class, action_id
    ) DO UPDATE SET
        state = excluded.state,
        occurred_at = excluded.occurred_at,
        redacted_diagnostic = excluded.redacted_diagnostic;

    INSERT INTO facet_action_status (
        system_id, target_kind, target_id, facet, candidate_class,
        action_id, state, occurred_at, redacted_diagnostic
    )
    SELECT DISTINCT
        scope.system_id, scope.target_kind, scope.target_id, scope.facet, 'terminal',
        NEW.action_id, NEW.state,
        COALESCE(NEW.completed_at, NEW.started_at, NEW.record_created_at),
        NEW.redacted_diagnostic
    FROM adapter_action_scopes AS scope
    WHERE scope.action_id = NEW.action_id
      AND NEW.state IN ('satisfied', 'succeeded', 'partial', 'failed', 'cancelled')
    ON CONFLICT (system_id, target_kind, target_id, facet)
        WHERE candidate_class = 'terminal'
    DO UPDATE SET
        action_id = excluded.action_id,
        state = excluded.state,
        occurred_at = excluded.occurred_at,
        redacted_diagnostic = excluded.redacted_diagnostic
    WHERE excluded.occurred_at > facet_action_status.occurred_at
       OR (excluded.occurred_at = facet_action_status.occurred_at
           AND excluded.action_id >= facet_action_status.action_id);
END;

CREATE TRIGGER trg_action_projection_timestamp_correction
AFTER UPDATE OF started_at, completed_at, record_created_at
ON adapter_actions
WHEN (OLD.started_at IS NOT NULL AND OLD.started_at IS NOT NEW.started_at)
  OR (OLD.completed_at IS NOT NULL AND OLD.completed_at IS NOT NEW.completed_at)
  OR OLD.record_created_at IS NOT NEW.record_created_at
BEGIN
    DELETE FROM action_scope_cooldown
    WHERE EXISTS (
        SELECT 1 FROM adapter_action_scopes AS affected
        WHERE affected.action_id = NEW.action_id
          AND affected.system_id = action_scope_cooldown.system_id
          AND affected.target_kind = action_scope_cooldown.target_kind
          AND affected.target_id = action_scope_cooldown.target_id
          AND affected.object_type = action_scope_cooldown.object_type
          AND affected.facet = action_scope_cooldown.facet
          AND (affected.capability_key IS NULL) = action_scope_cooldown.capability_key_is_null
          AND COALESCE(affected.capability_key, '') = action_scope_cooldown.capability_key
          AND affected.coverage = action_scope_cooldown.coverage
          AND affected.field_mask_json = action_scope_cooldown.field_mask_json
    );

    INSERT INTO action_scope_cooldown (
        system_id, target_kind, target_id, object_type, facet,
        capability_key_is_null, capability_key, coverage, field_mask_json,
        latest_started_at
    )
    SELECT
        scope.system_id, scope.target_kind, scope.target_id, scope.object_type, scope.facet,
        scope.capability_key IS NULL, COALESCE(scope.capability_key, ''),
        scope.coverage, scope.field_mask_json, MAX(action.started_at)
    FROM adapter_action_scopes AS scope
    JOIN adapter_actions AS action ON action.action_id = scope.action_id
    JOIN adapter_action_scopes AS affected
      ON affected.action_id = NEW.action_id
     AND affected.system_id = scope.system_id
     AND affected.target_kind = scope.target_kind
     AND affected.target_id = scope.target_id
     AND affected.object_type = scope.object_type
     AND affected.facet = scope.facet
     AND affected.capability_key IS scope.capability_key
     AND affected.coverage = scope.coverage
     AND affected.field_mask_json = scope.field_mask_json
    WHERE action.started_at IS NOT NULL
    GROUP BY
        scope.system_id, scope.target_kind, scope.target_id, scope.object_type, scope.facet,
        scope.capability_key IS NULL, COALESCE(scope.capability_key, ''),
        scope.coverage, scope.field_mask_json;

    DELETE FROM facet_action_status
    WHERE candidate_class = 'terminal'
      AND EXISTS (
          SELECT 1 FROM adapter_action_scopes AS affected
          WHERE affected.action_id = NEW.action_id
            AND affected.system_id = facet_action_status.system_id
            AND affected.target_kind = facet_action_status.target_kind
            AND affected.target_id = facet_action_status.target_id
            AND affected.facet = facet_action_status.facet
      );

    INSERT INTO facet_action_status (
        system_id, target_kind, target_id, facet, candidate_class,
        action_id, state, occurred_at, redacted_diagnostic
    )
    SELECT
        system_id, target_kind, target_id, facet, 'terminal',
        action_id, state, occurred_at, redacted_diagnostic
    FROM (
        SELECT
            scope.system_id, scope.target_kind, scope.target_id, scope.facet,
            action.action_id, action.state,
            COALESCE(action.completed_at, action.started_at, action.record_created_at)
                AS occurred_at,
            action.redacted_diagnostic,
            ROW_NUMBER() OVER (
                PARTITION BY scope.system_id, scope.target_kind, scope.target_id, scope.facet
                ORDER BY
                    COALESCE(action.completed_at, action.started_at, action.record_created_at)
                        DESC,
                    action.action_id DESC
            ) AS candidate_rank
        FROM adapter_action_scopes AS scope
        JOIN adapter_actions AS action ON action.action_id = scope.action_id
        WHERE action.state IN ('satisfied', 'succeeded', 'partial', 'failed', 'cancelled')
          AND EXISTS (
              SELECT 1 FROM adapter_action_scopes AS affected
              WHERE affected.action_id = NEW.action_id
                AND affected.system_id = scope.system_id
                AND affected.target_kind = scope.target_kind
                AND affected.target_id = scope.target_id
                AND affected.facet = scope.facet
          )
    )
    WHERE candidate_rank = 1;
END;
