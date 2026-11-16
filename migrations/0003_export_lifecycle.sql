
ALTER TABLE export_jobs
  ADD COLUMN IF NOT EXISTS queue_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS processing_started_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS absolute_deadline_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 3,
  ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  ADD COLUMN IF NOT EXISTS file_sha256 TEXT,
  ADD COLUMN IF NOT EXISTS file_size_bytes BIGINT;

UPDATE export_jobs
SET queue_expires_at = COALESCE(queue_expires_at, created_at + INTERVAL '24 hours'),
    processing_started_at = CASE
      WHEN status IN ('PROCESSING','COMPLETED','EXPIRED') THEN COALESCE(processing_started_at, created_at)
      ELSE processing_started_at END,
    absolute_deadline_at = CASE
      WHEN status IN ('PROCESSING','COMPLETED','EXPIRED') THEN COALESCE(absolute_deadline_at, COALESCE(processing_started_at, created_at) + INTERVAL '30 minutes')
      ELSE absolute_deadline_at END,
    finished_at = CASE
      WHEN status IN ('COMPLETED', 'FAILED', 'EXPIRED') THEN COALESCE(finished_at, created_at)
      ELSE NULL END;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema=current_schema() AND table_name='export_jobs' AND column_name='error_code')
     AND NOT EXISTS (SELECT 1 FROM information_schema.columns
                     WHERE table_schema=current_schema() AND table_name='export_jobs' AND column_name='failure_code') THEN
    ALTER TABLE export_jobs RENAME COLUMN error_code TO failure_code;
  ELSIF EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema=current_schema() AND table_name='export_jobs' AND column_name='error_code')
    AND EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema=current_schema() AND table_name='export_jobs' AND column_name='failure_code') THEN
    IF EXISTS (
      SELECT 1 FROM export_jobs
      WHERE failure_code IS NOT NULL AND error_code IS NOT NULL
        AND failure_code IS DISTINCT FROM error_code
    ) THEN
      RAISE EXCEPTION 'export_jobs.error_code conflicts with failure_code; resolve rows before migration';
    END IF;
    UPDATE export_jobs SET failure_code = COALESCE(failure_code, error_code);
    ALTER TABLE export_jobs DROP COLUMN error_code;
  END IF;
END $$;

ALTER TABLE export_jobs
  ADD COLUMN IF NOT EXISTS failure_code TEXT NULL;

UPDATE export_jobs
SET attempts = COALESCE(attempts, 0),
    max_attempts = COALESCE(max_attempts, 3);
ALTER TABLE export_jobs
  ALTER COLUMN attempts SET DEFAULT 0,
  ALTER COLUMN attempts SET NOT NULL,
  ALTER COLUMN max_attempts SET DEFAULT 3,
  ALTER COLUMN max_attempts SET NOT NULL;



UPDATE export_jobs
SET failure_code = 'EXPORT_WORKER_FAILED'
WHERE status = 'FAILED' AND failure_code IS NULL;

ALTER TABLE export_jobs
  ALTER COLUMN queue_expires_at SET NOT NULL,
  ALTER COLUMN queue_expires_at DROP DEFAULT;





DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM export_jobs WHERE format_preset NOT IN ('EXCEL_SEMICOLON_BOM','STANDARD_COMMA_UTF8')) THEN
        RAISE EXCEPTION 'Legacy export format requires an explicitly approved mapping before migration';
    END IF;
END $$;
UPDATE export_jobs SET failure_code = 'EXPORT_EXPIRED' WHERE status = 'EXPIRED' AND failure_code IS NULL;
UPDATE export_jobs SET lease_token = NULL, lease_until = NULL WHERE status <> 'PROCESSING';
ALTER TABLE export_jobs ADD COLUMN IF NOT EXISTS session_id uuid;
CREATE TEMP TABLE expected_export_constraints (LIKE export_jobs) ON COMMIT DROP;
ALTER TABLE expected_export_constraints ADD CONSTRAINT chk_export_attempt_counts_v292 CHECK (attempts >= 0 AND max_attempts BETWEEN 1 AND 10);
ALTER TABLE expected_export_constraints ADD CONSTRAINT chk_export_deadline_fields CHECK ((status = 'QUEUED' AND processing_started_at IS NULL AND absolute_deadline_at IS NULL)
 OR (status IN ('PROCESSING','COMPLETED','EXPIRED') AND processing_started_at IS NOT NULL AND absolute_deadline_at IS NOT NULL)
 OR (status = 'FAILED' AND (processing_started_at IS NULL) = (absolute_deadline_at IS NULL)));
ALTER TABLE expected_export_constraints ADD CONSTRAINT chk_export_terminal_timestamps CHECK ((status IN ('QUEUED','PROCESSING') AND finished_at IS NULL)
 OR (status IN ('COMPLETED','FAILED','EXPIRED') AND finished_at IS NOT NULL));
ALTER TABLE expected_export_constraints ADD CONSTRAINT chk_export_format_preset_v291 CHECK (format_preset IN ('EXCEL_SEMICOLON_BOM','STANDARD_COMMA_UTF8'));
ALTER TABLE expected_export_constraints ADD CONSTRAINT chk_export_queue_deadline_v292 CHECK (queue_expires_at = created_at + INTERVAL '24 hours');
ALTER TABLE expected_export_constraints ADD CONSTRAINT chk_export_processing_deadline_v292 CHECK (absolute_deadline_at = processing_started_at + INTERVAL '30 minutes');
ALTER TABLE expected_export_constraints ADD CONSTRAINT chk_export_terminal_ownership_v292 CHECK (status = 'PROCESSING' OR (lease_token IS NULL AND lease_until IS NULL));
ALTER TABLE expected_export_constraints ADD CONSTRAINT chk_export_failure_v292 CHECK (CASE status WHEN 'FAILED' THEN failure_code IS NOT NULL AND failure_code IN
('EXPORT_QUEUE_TTL_EXCEEDED','EXPORT_ABANDONED_TIMEOUT','EXPORT_MAX_ATTEMPTS_EXCEEDED','EXPORT_LIMIT_EXCEEDED',
 'EXPORT_STORAGE_FAILED','EXPORT_WORKER_FAILED','EXPORT_ACCESS_REVOKED')
 WHEN 'EXPIRED' THEN failure_code IS NOT NULL AND failure_code = 'EXPORT_EXPIRED' ELSE failure_code IS NULL END);
ALTER TABLE expected_export_constraints ADD CONSTRAINT chk_export_file_metadata CHECK (status <> 'COMPLETED' OR (file_sha256 IS NOT NULL AND file_sha256 ~ '^[0-9a-f]{64}$'
 AND file_size_bytes IS NOT NULL AND file_size_bytes >= 0 AND finished_at IS NOT NULL));

DO $$
DECLARE expected record; actual text;
BEGIN
    FOR expected IN SELECT conname, pg_get_expr(conbin, conrelid) AS expression
        FROM pg_constraint WHERE conrelid = 'expected_export_constraints'::regclass AND contype = 'c'
    LOOP
        SELECT pg_get_expr(conbin, conrelid) INTO actual FROM pg_constraint
          WHERE conrelid = 'export_jobs'::regclass AND conname = expected.conname;
        IF NOT FOUND THEN
            EXECUTE format('ALTER TABLE export_jobs ADD CONSTRAINT %I CHECK (%s) NOT VALID', expected.conname, expected.expression);
        ELSIF actual IS DISTINCT FROM expected.expression THEN
            RAISE EXCEPTION 'Incompatible export constraint: %', expected.conname;
        END IF;
        EXECUTE format('ALTER TABLE export_jobs VALIDATE CONSTRAINT %I', expected.conname);
    END LOOP;
END $$;
CREATE INDEX IF NOT EXISTS idx_export_claim ON export_jobs (created_at, export_id)
    WHERE status IN ('QUEUED','PROCESSING');

CREATE TABLE IF NOT EXISTS export_quota (
    company_id uuid PRIMARY KEY REFERENCES companies ON DELETE CASCADE,
    revision bigint NOT NULL DEFAULT 0
);
