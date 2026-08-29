CREATE INDEX IF NOT EXISTS ix_relationships_subject_predicate
ON relationships (subject_id, predicate, presence, object_id);
