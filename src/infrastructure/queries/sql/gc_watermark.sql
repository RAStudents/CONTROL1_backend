WITH anchor AS (
    SELECT as_of_sequence seq
    FROM event_snapshots
    WHERE company_id = $1 AND expires_at > clock_timestamp()
    UNION ALL
    SELECT snapshot_sequence
    FROM export_jobs
    WHERE company_id = $1 AND status IN ('QUEUED', 'PROCESSING')
),

retention AS (
    SELECT
        coalesce(
            min(tenant_sequence) - 1,
            (
                SELECT last_sequence FROM company_stream_counters
                WHERE company_id = $1
            ),
            0
        ) floor
    FROM tenant_outbox
    WHERE company_id = $1 AND created_at > clock_timestamp() - interval '48 hours'
)

SELECT
    least(coalesce((SELECT min(seq) FROM anchor), floor), floor),
    (SELECT count(*) FROM anchor),
    (
        SELECT min(valid_from_sequence) FROM event_versions
        WHERE company_id = $1
    )
FROM retention
