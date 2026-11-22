#!/usr/bin/env bash
set -euo pipefail

database="control_migration_test_${RANDOM}_$$"
postgres() { docker compose exec -T postgres "$@"; }
apply() { postgres psql -q -1 -v ON_ERROR_STOP=1 -U control -d "$database"; }
postgres createdb -U control "$database"
trap 'postgres dropdb -U control "$database"' EXIT

# The v2.8 baseline has an error_code column and no persisted queue deadline.
cat migrations/0001_access.sql migrations/0002_events.sql | apply
apply <<'SQL'
INSERT INTO companies(id,name) VALUES ('00000000-0000-0000-0000-000000000001','Migration fixture');
INSERT INTO export_jobs(export_id,company_id,user_id,status,snapshot_sequence,filters_json,format_preset,include_media,created_at)
VALUES ('legacy','00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000002',
'FAILED',0,'{}','STANDARD_COMMA_UTF8',false,'2025-01-01T12:00:00Z');
UPDATE export_jobs SET error_code='EXPORT_STORAGE_FAILED' WHERE export_id='legacy';
SQL
apply < migrations/0003_export_lifecycle.sql
apply < migrations/0004_event_integrity.sql
# Re-running the guarded transformations must accept matching constraints.
apply < migrations/0003_export_lifecycle.sql
apply < migrations/0004_event_integrity.sql
for migration in migrations/*.sql; do
    [[ "${migration##*/}" < 0005 ]] && continue
    if [[ "$(head -n 1 "$migration")" == '-- no-transaction'* ]]; then
        postgres psql -q -v ON_ERROR_STOP=1 -U control -d "$database" < "$migration"
    else
        apply < "$migration"
    fi
done
apply < migrations/0007_snapshot_item_event_index.sql

apply <<'SQL'
INSERT INTO export_jobs(export_id,company_id,user_id,status,snapshot_sequence,filters_json,format_preset,include_media,created_at,queue_expires_at)
SELECT 'queued','00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000002',
'QUEUED',0,'{}','STANDARD_COMMA_UTF8',false,t,t+interval '24 hours'
FROM (SELECT '2025-02-01T12:00:00Z'::timestamptz t) historical;
DO $$
DECLARE valid boolean;
BEGIN
  SELECT queue_expires_at='2025-01-02T12:00:00Z' AND failure_code='EXPORT_STORAGE_FAILED'
  INTO STRICT valid FROM export_jobs WHERE export_id='legacy';
  IF NOT valid THEN RAISE EXCEPTION 'legacy export incorrectly migrated'; END IF;

  SELECT queue_expires_at='2025-02-02T12:00:00Z' AND absolute_deadline_at IS NULL AND processing_started_at IS NULL
  INTO STRICT valid FROM export_jobs WHERE export_id='queued';
  IF NOT valid THEN RAISE EXCEPTION 'incorrect deadline for historical created_at'; END IF;

  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='export_jobs'
      AND (column_name='error_code' OR (column_name='queue_expires_at' AND column_default IS NOT NULL))) THEN
    RAISE EXCEPTION 'invalid export lifecycle schema';
  END IF;
END $$;
SQL
echo 'PostgreSQL 16 legacy export migration, exact constraint checks and historical deadline passed'

apply < tests/constraints.sql
# A same-named constraint with a different definition must stop an upgrade.
apply <<'SQL'
ALTER TABLE export_jobs DROP CONSTRAINT chk_export_attempt_counts_v292;
ALTER TABLE export_jobs ADD CONSTRAINT chk_export_attempt_counts_v292 CHECK(attempts>=0);
SQL
if apply < migrations/0003_export_lifecycle.sql >/dev/null 2>&1; then
    echo 'Incompatible export constraint was silently accepted' >&2
    exit 1
fi
apply <<'SQL'
ALTER TABLE export_jobs DROP CONSTRAINT chk_export_attempt_counts_v292;
SQL
apply < migrations/0003_export_lifecycle.sql

apply <<'SQL'
ALTER TABLE export_jobs ADD COLUMN error_code text;
UPDATE export_jobs SET error_code='EXPORT_WORKER_FAILED' WHERE export_id='legacy';
SQL
if apply < migrations/0003_export_lifecycle.sql >/dev/null 2>&1; then
    echo 'Conflicting legacy failure codes were silently merged' >&2
    exit 1
fi
apply <<'SQL'
UPDATE export_jobs SET error_code=failure_code;
SQL
apply < migrations/0003_export_lifecycle.sql

echo 'Constraint isolation, interval exclusion, terminal metadata and fail-closed upgrade checks passed'
