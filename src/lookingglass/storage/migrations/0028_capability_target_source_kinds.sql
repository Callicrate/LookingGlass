ALTER TABLE capability_bindings
    ADD COLUMN target_source_kinds_json TEXT NOT NULL DEFAULT '[]' CHECK (
        json_valid(target_source_kinds_json)
        AND json_type(target_source_kinds_json) = 'array'
    );
