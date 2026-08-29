CREATE TABLE relationship_coverage_watermarks (
    system_id TEXT NOT NULL REFERENCES systems(system_id) ON DELETE RESTRICT,
    subject_id TEXT NOT NULL REFERENCES remote_objects(object_id) ON DELETE RESTRICT,
    predicate TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    supporting_observation_id TEXT NOT NULL
        REFERENCES observation_journal(observation_id) ON DELETE RESTRICT,
    PRIMARY KEY (system_id, subject_id, predicate)
);

WITH ranked_boundaries AS (
    SELECT
        credit.system_id,
        subject.object_id AS subject_id,
        credit.observed_at,
        credit.observation_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                credit.system_id,
                subject.object_id
            ORDER BY credit.observed_at DESC, credit.credit_id DESC
        ) AS recency
    FROM refresh_credit AS credit
    JOIN observation_journal AS journal
      ON journal.observation_id = credit.observation_id
     AND journal.item_kind = 'coverage'
    LEFT JOIN configured_scopes AS configured
      ON credit.target_kind = 'configured_scope'
     AND configured.scope_id = credit.target_id
     AND configured.system_id = credit.system_id
     AND configured.object_type = credit.object_type
    JOIN remote_objects AS subject
      ON subject.system_id = credit.system_id
     AND subject.object_type = credit.object_type
     AND (
          (credit.target_kind = 'object' AND subject.object_id = credit.target_id)
          OR
          (credit.target_kind = 'configured_scope' AND subject.object_id = configured.object_id)
     )
    WHERE credit.facet = 'membership'
      AND json_extract(journal.item_json, '$.completeness') = 'complete'
      AND EXISTS (
          SELECT 1
          FROM json_each(journal.item_json, '$.absence_authority') AS authority
          WHERE authority.value = 'relationship'
      )
)
INSERT INTO relationship_coverage_watermarks (
    system_id, subject_id, predicate, observed_at, supporting_observation_id
)
SELECT system_id, subject_id, 'contains', observed_at, observation_id
FROM ranked_boundaries
WHERE recency = 1 AND subject_id IS NOT NULL;

UPDATE relationships
SET presence = 'absent',
    observed_at = (
        SELECT watermark.observed_at
        FROM relationship_coverage_watermarks AS watermark
        WHERE watermark.system_id = relationships.system_id
          AND watermark.subject_id = relationships.subject_id
          AND watermark.predicate = relationships.predicate
    ),
    supporting_observation_id = (
        SELECT watermark.supporting_observation_id
        FROM relationship_coverage_watermarks AS watermark
        WHERE watermark.system_id = relationships.system_id
          AND watermark.subject_id = relationships.subject_id
          AND watermark.predicate = relationships.predicate
    )
WHERE presence = 'present'
  AND EXISTS (
      SELECT 1
      FROM relationship_coverage_watermarks AS watermark
      WHERE watermark.system_id = relationships.system_id
        AND watermark.subject_id = relationships.subject_id
        AND watermark.predicate = relationships.predicate
        AND relationships.observed_at < watermark.observed_at
  );
