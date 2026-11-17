pub(crate) const DELIVER_WITH_DEVICE_OPERATIONS: &str = r#"
WITH batch AS (
    SELECT id
    FROM device_operations
    WHERE
        delivery_state = 'IN_PROGRESS'
        AND delivery_started_at <= clock_timestamp() - interval '30 seconds'
    ORDER BY delivery_started_at
    LIMIT 100
    FOR UPDATE SKIP LOCKED
)

UPDATE device_operations d SET status = 'TIMED_OUT', delivery_state = 'UNKNOWN'
FROM batch
WHERE d.id = batch.id
"#;
pub(crate) const DELIVER_WITH_DEVICE_OPERATIONS_2: &str = r#"
WITH chosen AS (
    SELECT id FROM device_operations
    WHERE delivery_state = 'READY'
    ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED
)

UPDATE device_operations d SET
    delivery_state = 'IN_PROGRESS', delivery_started_at = clock_timestamp()
FROM chosen
WHERE d.id = chosen.id
RETURNING d.id, d.company_id, d.device_id, d.command
"#;
pub(crate) const DELIVER_UPDATE_DEVICE_OPERATIONS: &str = r#"
UPDATE device_operations SET status = $2, delivery_state = $3
WHERE id = $1 AND delivery_state = 'IN_PROGRESS'
"#;
