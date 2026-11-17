WITH chosen AS MATERIALIZED (
    SELECT export_id FROM export_jobs
    WHERE
        attempts < max_attempts
        AND (
            (status = 'QUEUED' AND queue_expires_at > clock_timestamp())
            OR (
                status = 'PROCESSING'
                AND lease_until <= clock_timestamp()
                AND absolute_deadline_at > clock_timestamp()
            )
        )
    ORDER BY created_at, export_id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
),

t AS MATERIALIZED (SELECT clock_timestamp() now FROM chosen)

UPDATE export_jobs j
SET
    status = 'PROCESSING',
    lease_token = $1,
    attempts = j.attempts + 1,
    updated_at = t.now,
    processed_records = 0,
    processing_started_at = coalesce(j.processing_started_at, t.now),
    absolute_deadline_at = coalesce(j.absolute_deadline_at, t.now + interval '30 minutes'),
    lease_until = least(
        t.now + interval '5 minutes',
        coalesce(j.absolute_deadline_at, t.now + interval '30 minutes')
    )
FROM chosen, t
WHERE
    j.export_id = chosen.export_id
    AND (
        (j.status = 'QUEUED' AND queue_expires_at > t.now)
        OR (j.status = 'PROCESSING' AND lease_until <= t.now AND absolute_deadline_at > t.now)
    )
RETURNING
    j.company_id,
    j.export_id,
    j.user_id,
    j.session_id,
    j.snapshot_sequence,
    j.format_preset,
    j.filters_json,
    j.include_media,
    j.lease_token,
    j.created_at,
    j.processing_started_at,
    j.attempts
