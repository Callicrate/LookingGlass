ALTER TABLE capability_bindings
ADD COLUMN coverage_policies_json TEXT NOT NULL DEFAULT '[]';
