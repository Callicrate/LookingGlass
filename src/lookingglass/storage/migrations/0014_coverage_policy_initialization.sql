ALTER TABLE capability_bindings
ADD COLUMN coverage_policy_initialized INTEGER NOT NULL DEFAULT 0
CHECK (coverage_policy_initialized IN (0, 1));
