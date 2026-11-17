WITH candidates AS (
    SELECT
        export_id,
        CASE
            WHEN status = 'QUEUED' THEN 'EXPORT_QUEUE_TTL_EXCEEDED'
            WHEN status = 'COMPLETED' THEN 'EXPORT_EXPIRED'
            WHEN absolute_deadline_at <= clock_timestamp() THEN 'EXPORT_ABANDONED_TIMEOUT'
            ELSE 'EXPORT_MAX_ATTEMPTS_EXCEEDED'
        END reason
    FROM export_jobs
    WHERE
        company_id = $1 AND (
            (status = 'QUEUED' AND queue_expires_at <= clock_timestamp())
            OR (status = 'COMPLETED' AND expires_at <= clock_timestamp())
            OR (
                status = 'PROCESSING'
                AND (
                    absolute_deadline_at <= clock_timestamp()
                    OR (lease_until <= clock_timestamp() AND attempts >= max_attempts)
                )
            )
        )
    ORDER BY created_at
    LIMIT 500
    FOR UPDATE SKIP LOCKED
)

UPDATE export_jobs e
SET
    status = CASE WHEN c.reason = 'EXPORT_EXPIRED' THEN 'EXPIRED' ELSE 'FAILED' END,
    failure_code = c.reason,
    finished_at = clock_timestamp(),
    updated_at = clock_timestamp(),
    lease_token = NULL,
    lease_until = NULL
FROM candidates c
WHERE e.company_id = $1 AND e.export_id = c.export_id
