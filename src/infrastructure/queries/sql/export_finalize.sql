UPDATE export_jobs
SET
    status = 'COMPLETED',
    storage_path = $4,
    file_sha256 = $5,
    file_size_bytes = $6,
    total_records = $7,
    processed_records = $7,
    expires_at = clock_timestamp() + interval '24 hours',
    finished_at = clock_timestamp(),
    updated_at = clock_timestamp(),
    lease_token = NULL,
    lease_until = NULL
WHERE
    company_id = $1 AND export_id = $2
    AND status = 'PROCESSING'
    AND lease_token = $3
    AND lease_until > clock_timestamp()
    AND absolute_deadline_at > clock_timestamp()
