WITH context AS (
    SELECT
        business_timezone tz,
        (transaction_timestamp() AT TIME ZONE business_timezone)::date AS local_day
    FROM companies
    WHERE id = $1
)

SELECT jsonb_build_object(
    'summary_as_of_sequence', $2::bigint, 'business_timezone', tz, 'business_date', local_day,
    'today_events_count',
    (
        SELECT count(*)
        FROM events e
        JOIN event_versions v ON (v.company_id, v.event_id) = (e.company_id, e.id)
        WHERE
            e.company_id = $1 AND e.occurred_at >= (local_day::timestamp AT TIME ZONE tz)
            AND e.occurred_at < ((local_day + 1)::timestamp AT TIME ZONE tz)
            AND v.valid_from_sequence <= $2
            AND (v.valid_to_sequence IS NULL OR v.valid_to_sequence > $2) AND NOT v.is_erroneous
    ),
    'categories', (
        SELECT
            coalesce(
                jsonb_agg(
                    jsonb_build_object(
                        'category_id', id, 'name', name, 'score', score, 'trend', trend
                    )
                    ORDER BY id
                ),
                '[]'
            )
        FROM event_categories
        WHERE company_id = $1
    ),
    'devices', (SELECT
        jsonb_build_object(
            'total_count', count(*), 'online_ok_count', count(*) FILTER (WHERE health = 'OK'),
            'unknown_count',
            count(*) FILTER (WHERE health = 'UNKNOWN'),
            'error_count',
            count(*) FILTER (WHERE health IN ('WARNING', 'CRITICAL'))
        )
    FROM devices
    WHERE company_id = $1)
) FROM context
