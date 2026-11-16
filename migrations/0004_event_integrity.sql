CREATE EXTENSION IF NOT EXISTS btree_gist;

-- PostgreSQL normalizes catalog expressions. Build the expected definition on an
-- empty temporary relation so whitespace and parser rewrites cannot hide a mismatch.
DO $$
DECLARE spec record; actual text; expected text; defer_validation boolean;
BEGIN
    FOR spec IN SELECT * FROM (VALUES
        ('event_snapshots', 'uq_event_snapshots_company_snapshot', $definition$UNIQUE (company_id, snapshot_id)$definition$),
        ('event_snapshot_items', 'fk_snapshot_items_same_company_snapshot', $definition$FOREIGN KEY (company_id, snapshot_id)
  REFERENCES event_snapshots(company_id, snapshot_id) ON DELETE CASCADE$definition$),
        ('event_versions', 'ex_event_versions_no_overlap', $definition$EXCLUDE USING gist (
    company_id WITH =,
    event_id WITH =,
    int8range(valid_from_sequence, valid_to_sequence, '[)') WITH &&
  )$definition$),
        ('event_versions', 'uq_event_versions_start_sequence', $definition$UNIQUE (company_id, event_id, valid_from_sequence)$definition$),
        ('event_versions', 'chk_acknowledgement_fields_consistent', $definition$CHECK (
    (status = 'ACKNOWLEDGED' AND acknowledged_at IS NOT NULL AND acknowledged_by IS NOT NULL)
    OR
    (status IN ('NEW', 'LEGACY_UNKNOWN') AND acknowledged_at IS NULL AND acknowledged_by IS NULL)
  )$definition$),
        ('event_versions', 'chk_erroneous_fields_consistent', $definition$CHECK (
    (is_erroneous = TRUE AND erroneous_marked_at IS NOT NULL AND erroneous_marked_by IS NOT NULL)
    OR
    (is_erroneous = FALSE AND erroneous_marked_at IS NULL AND erroneous_marked_by IS NULL)
  )$definition$),
        ('event_versions', 'chk_event_versions_safe_integer', $definition$CHECK (event_version <= 9007199254740991 AND classification_version <= 9007199254740991 AND acknowledgement_version <= 9007199254740991 AND valid_from_sequence <= 9007199254740991 AND (valid_to_sequence IS NULL OR valid_to_sequence <= 9007199254740991))$definition$),
        ('events', 'chk_events_safe_integer', $definition$CHECK (created_sequence <= 9007199254740991)$definition$),
        ('event_snapshots', 'chk_event_snapshots_safe_integer', $definition$CHECK (as_of_sequence <= 9007199254740991)$definition$),
        ('export_jobs', 'chk_export_snapshot_safe_integer', $definition$CHECK (snapshot_sequence <= 9007199254740991)$definition$)
    ) AS definitions(relation, name, expression)
    LOOP
        IF spec.expression LIKE 'FOREIGN KEY%' THEN
            -- A temporary table cannot reference the permanent parent relation.
            expected := regexp_replace(spec.expression, '[[:space:]]+', ' ', 'g');
        ELSE
            EXECUTE format('CREATE TEMP TABLE expected_event_constraint (LIKE %I) ON COMMIT DROP', spec.relation);
            EXECUTE format('ALTER TABLE expected_event_constraint ADD CONSTRAINT %I %s', spec.name, spec.expression);
            SELECT pg_get_constraintdef(oid) INTO STRICT expected FROM pg_constraint
              WHERE conrelid='expected_event_constraint'::regclass AND conname=spec.name;
            DROP TABLE expected_event_constraint;
        END IF;
        SELECT pg_get_constraintdef(oid) INTO actual FROM pg_constraint
          WHERE conrelid=to_regclass(spec.relation) AND conname=spec.name;
        defer_validation := spec.expression LIKE 'CHECK%' OR spec.expression LIKE 'FOREIGN KEY%';
        IF NOT FOUND THEN
            EXECUTE format('ALTER TABLE %I ADD CONSTRAINT %I %s%s',spec.relation,spec.name,spec.expression,
                CASE WHEN defer_validation THEN ' NOT VALID' ELSE '' END);
        ELSIF regexp_replace(actual, ' NOT VALID$', '') IS DISTINCT FROM expected THEN
            RAISE EXCEPTION 'Incompatible constraint % on %', spec.name, spec.relation;
        END IF;
        IF defer_validation THEN
            EXECUTE format('ALTER TABLE %I VALIDATE CONSTRAINT %I',spec.relation,spec.name);
        END IF;
    END LOOP;
END $$;
