WITH t AS MATERIALIZED (SELECT clock_timestamp() now)

INSERT INTO export_jobs (
    export_id,
    company_id,
    user_id,
    session_id,
    status,
    snapshot_sequence,
    filters_json,
    format_preset,
    include_media,
    created_at,
    queue_expires_at,
    updated_at,
    processed_records
)
SELECT
    $1,
    $2,
    $3,
    $4,
    'QUEUED',
    $5,
    $6,
    $7,
    $8,
    now,
    now + interval '24 hours',
    now,
    0
FROM t
RETURNING created_at
