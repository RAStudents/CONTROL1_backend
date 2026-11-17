pub(crate) const GC_WITH_EVENT_SNAPSHOTS: &str = r#"
WITH batch AS (
    SELECT snapshot_id
    FROM event_snapshots
    WHERE company_id = $1 AND expires_at <= clock_timestamp()
    ORDER BY expires_at LIMIT 1000 FOR UPDATE SKIP LOCKED
)

DELETE FROM event_snapshots s USING batch b
WHERE s.company_id = $1 AND s.snapshot_id = b.snapshot_id
"#;
pub(crate) const GC_WITH_EXPORT_JOBS: &str = include_str!("sql/export_reaper.sql");
pub(crate) const GC_WITH_EVENT_SNAPSHOTS_2: &str = include_str!("sql/gc_watermark.sql");
pub(crate) const GC_WITH_EVENT_VERSIONS: &str = r#"
WITH batch AS (
    SELECT
        event_id,
        event_version
    FROM event_versions
    WHERE company_id = $1 AND valid_to_sequence <= $2
    ORDER BY valid_to_sequence LIMIT 1000 FOR UPDATE SKIP LOCKED
)

DELETE FROM event_versions v USING batch b
WHERE v.company_id = $1 AND (v.event_id, v.event_version) = (b.event_id, b.event_version)
"#;
pub(crate) const OUTBOX_WITH_TENANT_OUTBOX: &str = r#"
WITH batch AS (
    SELECT
        company_id,
        tenant_sequence
    FROM tenant_outbox
    WHERE
        broker_dispatched_at IS NOT NULL
        AND created_at <= clock_timestamp() - interval '72 hours'
    ORDER BY created_at
    LIMIT 1000
    FOR UPDATE SKIP LOCKED
)

DELETE FROM tenant_outbox
    o
USING batch b
WHERE
    (o.company_id, o.tenant_sequence) = (b.company_id, b.tenant_sequence)
    AND o.broker_dispatched_at IS NOT NULL
    AND o.created_at <= clock_timestamp() - interval '72 hours'
"#;
pub(crate) const OUTBOX_SELECT_TENANT_OUTBOX: &str = r#"
SELECT EXISTS(
    SELECT 1
    FROM tenant_outbox
    WHERE
        broker_dispatched_at IS NULL
        AND created_at <= clock_timestamp() - interval '72 hours'
)
"#;
pub(crate) const RECORDS_WITH_IDEMPOTENCY_KEYS: &str = r#"
WITH batch AS (
    SELECT
        user_id,
        company_id,
        endpoint,
        idempotency_key
    FROM idempotency_keys
    WHERE expires_at <= clock_timestamp()
    ORDER BY expires_at LIMIT 1000 FOR UPDATE SKIP LOCKED
)

DELETE FROM idempotency_keys
    k
USING batch b
WHERE
    (k.user_id, k.company_id, k.endpoint, k.idempotency_key)
    = (b.user_id, b.company_id, b.endpoint, b.idempotency_key)
"#;
pub(crate) const RECORDS_DELETE_LOGIN_LIMITS: &str = r#"
DELETE FROM login_limits
WHERE bucket IN (
    SELECT bucket FROM login_limits
    WHERE until_at < clock_timestamp()
    ORDER BY until_at LIMIT 1000 FOR UPDATE SKIP LOCKED
)
"#;
pub(crate) const RECORDS_DELETE_EXPORT_JOBS: &str = r#"
DELETE FROM export_jobs
WHERE export_id IN (
    SELECT export_id
    FROM export_jobs
    WHERE
        status IN ('FAILED', 'EXPIRED')
        AND finished_at <= clock_timestamp() - interval '24 hours' AND storage_path IS NULL
    ORDER BY finished_at
    LIMIT 500
    FOR UPDATE SKIP LOCKED
)
"#;
pub(crate) const EXPIRED_FILES_SELECT_EXPORT_JOBS: &str = r#"
SELECT
    company_id,
    export_id,
    storage_path
FROM export_jobs
WHERE status = 'EXPIRED' AND storage_path IS NOT NULL
ORDER BY finished_at LIMIT 500
"#;
pub(crate) const EXPIRED_FILES_UPDATE_EXPORT_JOBS: &str = r#"
UPDATE export_jobs SET storage_path = NULL
WHERE company_id = $1 AND export_id = $2 AND status = 'EXPIRED' AND storage_path = $3
"#;
pub(crate) const EXPORTED_OBJECTS_SELECT_EXPORT_JOBS: &str = r#"
SELECT EXISTS(
    SELECT 1
    FROM export_jobs
    WHERE
        company_id = $1 AND export_id = $2
        AND (
            (status = 'PROCESSING' AND lease_token = $3)
            OR (status = 'COMPLETED' AND storage_path = $4)
        )
)
"#;
pub(crate) const EXPORTED_OBJECTS_UPDATE_EXPORT_JOBS: &str = r#"
UPDATE export_jobs SET storage_path = NULL
WHERE company_id = $1 AND export_id = $2 AND status = 'EXPIRED' AND storage_path = $3
"#;
pub(crate) const RUN_SELECT_COMPANY_STREAM_COUNTERS: &str = r#"
SELECT company_id FROM company_stream_counters
WHERE company_id > $1
ORDER BY company_id LIMIT 100
"#;
pub(crate) const GC_EXPORT_METRICS: &str = r#"
SELECT
    coalesce(
        max(extract(EPOCH FROM clock_timestamp() - created_at)) FILTER (WHERE status = 'QUEUED'), 0
    )::float8,
    coalesce(
        max(extract(EPOCH FROM clock_timestamp() - processing_started_at)) FILTER (
            WHERE status = 'PROCESSING'
        ),
        0
    )::float8,
    count(*) FILTER (
        WHERE failure_code IN (
            'EXPORT_QUEUE_TTL_EXCEEDED', 'EXPORT_ABANDONED_TIMEOUT', 'EXPORT_MAX_ATTEMPTS_EXCEEDED'
        )
    ),
    coalesce(max(attempts) FILTER (WHERE status IN ('QUEUED', 'PROCESSING')), 0)::bigint
FROM export_jobs
WHERE company_id = $1
"#;
