ALTER TABLE remote_objects ADD COLUMN last_seen_received_at TEXT;

ALTER TABLE facets ADD COLUMN received_at TEXT;
UPDATE facets
SET received_at = COALESCE(
    (
        SELECT journal.received_at
        FROM observation_journal AS journal
        WHERE journal.observation_id = facets.supporting_observation_id
    ),
    observed_at,
    state_changed_at
)
WHERE received_at IS NULL;

ALTER TABLE relationships ADD COLUMN received_at TEXT;
UPDATE relationships
SET received_at = COALESCE(
    (
        SELECT journal.received_at
        FROM observation_journal AS journal
        WHERE journal.observation_id = relationships.supporting_observation_id
    ),
    observed_at
)
WHERE received_at IS NULL;

ALTER TABLE relationship_coverage_watermarks ADD COLUMN received_at TEXT;
UPDATE relationship_coverage_watermarks
SET received_at = COALESCE(
    (
        SELECT journal.received_at
        FROM observation_journal AS journal
        WHERE journal.observation_id = relationship_coverage_watermarks.supporting_observation_id
    ),
    observed_at
)
WHERE received_at IS NULL;

ALTER TABLE refresh_credit ADD COLUMN received_at TEXT;
UPDATE refresh_credit
SET received_at = COALESCE(
    (
        SELECT journal.received_at
        FROM observation_journal AS journal
        WHERE journal.observation_id = refresh_credit.observation_id
    ),
    observed_at
)
WHERE received_at IS NULL;

CREATE TEMP TABLE invalid_legacy_facets (
    object_id TEXT NOT NULL,
    facet TEXT NOT NULL,
    PRIMARY KEY (object_id, facet)
);

INSERT INTO invalid_legacy_facets (object_id, facet)
SELECT facet.object_id, facet.facet
FROM facets AS facet
JOIN remote_objects AS object ON object.object_id = facet.object_id
JOIN observation_journal AS journal
  ON journal.item_kind = 'facet'
 AND json_extract(journal.item_json, '$.facet') = facet.facet
 AND (
      json_extract(journal.item_json, '$.target.object_id') = facet.object_id
      OR (
          json_extract(journal.item_json, '$.target.object_id') IS NULL
          AND json_extract(journal.item_json, '$.target.source_kind') = object.source_kind
          AND json_extract(journal.item_json, '$.target.external_key') = object.external_key
      )
 )
JOIN observation_batches AS batch
  ON batch.batch_id = journal.batch_id
 AND batch.system_id = object.system_id
GROUP BY facet.object_id, facet.facet
HAVING SUM(CASE WHEN journal.observed_at = facet.observed_at THEN 1 ELSE 0 END) > 1
    OR COUNT(DISTINCT CASE
        WHEN json_extract(journal.item_json, '$.source_revision') GLOB '[0-9]*'
         AND json_extract(journal.item_json, '$.source_revision') NOT GLOB '*[^0-9]*'
        THEN COALESCE(
            NULLIF(LTRIM(json_extract(journal.item_json, '$.source_revision'), '0'), ''),
            '0'
        )
    END) > 1;

DELETE FROM refresh_credit
WHERE EXISTS (
    SELECT 1
    FROM invalid_legacy_facets AS invalid
    JOIN remote_objects AS object ON object.object_id = invalid.object_id
    LEFT JOIN configured_scopes AS configured
      ON configured.object_id = invalid.object_id
     AND configured.system_id = object.system_id
    WHERE refresh_credit.system_id = object.system_id
      AND refresh_credit.facet = invalid.facet
      AND (
          (refresh_credit.target_kind = 'object'
           AND refresh_credit.target_id = invalid.object_id)
          OR
          (refresh_credit.target_kind = 'configured_scope'
           AND refresh_credit.target_id = configured.scope_id)
      )
);

UPDATE facets
SET knowledge = 'unknown', payload_json = '{}', observed_at = NULL,
    supporting_observation_id = NULL, source_revision = NULL, received_at = NULL
WHERE EXISTS (
    SELECT 1 FROM invalid_legacy_facets AS invalid
    WHERE invalid.object_id = facets.object_id AND invalid.facet = facets.facet
);

DROP TABLE invalid_legacy_facets;

CREATE TEMP TABLE invalid_legacy_relationships (
    system_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    predicate TEXT NOT NULL,
    object_id TEXT NOT NULL,
    PRIMARY KEY (system_id, subject_id, predicate, object_id)
);

INSERT INTO invalid_legacy_relationships (system_id, subject_id, predicate, object_id)
SELECT relationship.system_id, relationship.subject_id,
       relationship.predicate, relationship.object_id
FROM relationships AS relationship
JOIN remote_objects AS subject ON subject.object_id = relationship.subject_id
JOIN remote_objects AS object ON object.object_id = relationship.object_id
JOIN observation_journal AS journal
  ON journal.item_kind = 'relationship'
 AND json_extract(journal.item_json, '$.predicate') = relationship.predicate
 AND (
      json_extract(journal.item_json, '$.subject.object_id') = relationship.subject_id
      OR (
          json_extract(journal.item_json, '$.subject.object_id') IS NULL
          AND json_extract(journal.item_json, '$.subject.source_kind') = subject.source_kind
          AND json_extract(journal.item_json, '$.subject.external_key') = subject.external_key
      )
 )
JOIN observation_batches AS batch
  ON batch.batch_id = journal.batch_id
 AND batch.system_id = relationship.system_id
 AND (
      json_extract(journal.item_json, '$.object.object_id') = relationship.object_id
      OR (
          json_extract(journal.item_json, '$.object.object_id') IS NULL
          AND json_extract(journal.item_json, '$.object.source_kind') = object.source_kind
          AND json_extract(journal.item_json, '$.object.external_key') = object.external_key
      )
 )
GROUP BY relationship.system_id, relationship.subject_id,
         relationship.predicate, relationship.object_id
HAVING SUM(CASE WHEN journal.observed_at = relationship.observed_at THEN 1 ELSE 0 END) > 1;

DELETE FROM refresh_credit
WHERE facet = 'membership'
  AND EXISTS (
      SELECT 1
      FROM invalid_legacy_relationships AS invalid
      LEFT JOIN configured_scopes AS configured
        ON configured.object_id = invalid.subject_id
       AND configured.system_id = invalid.system_id
      WHERE refresh_credit.system_id = invalid.system_id
        AND (
            (refresh_credit.target_kind = 'object'
             AND refresh_credit.target_id = invalid.subject_id)
            OR
            (refresh_credit.target_kind = 'configured_scope'
             AND refresh_credit.target_id = configured.scope_id)
        )
  );

UPDATE relationships
SET presence = 'unknown', received_at = NULL
WHERE EXISTS (
    SELECT 1 FROM invalid_legacy_relationships AS invalid
    WHERE invalid.system_id = relationships.system_id
      AND invalid.subject_id = relationships.subject_id
      AND invalid.predicate = relationships.predicate
      AND invalid.object_id = relationships.object_id
);

DELETE FROM relationship_coverage_watermarks
WHERE EXISTS (
    SELECT 1 FROM invalid_legacy_relationships AS invalid
    WHERE invalid.system_id = relationship_coverage_watermarks.system_id
      AND invalid.subject_id = relationship_coverage_watermarks.subject_id
      AND invalid.predicate = relationship_coverage_watermarks.predicate
);

DROP TABLE invalid_legacy_relationships;

DELETE FROM relationship_coverage_watermarks;

WITH ranked_boundaries AS (
    SELECT
        credit.system_id,
        subject.object_id AS subject_id,
        credit.observed_at,
        credit.received_at,
        credit.observation_id,
        ROW_NUMBER() OVER (
            PARTITION BY credit.system_id, subject.object_id
            ORDER BY credit.observed_at DESC, credit.received_at DESC, credit.rowid ASC
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
    system_id, subject_id, predicate, observed_at,
    supporting_observation_id, received_at
)
SELECT system_id, subject_id, 'contains', observed_at, observation_id, received_at
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
    ),
    received_at = (
        SELECT watermark.received_at
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
        AND (
            relationships.observed_at < watermark.observed_at
            OR (
                relationships.observed_at = watermark.observed_at
                AND COALESCE(relationships.received_at, relationships.observed_at)
                    < watermark.received_at
            )
        )
  );
