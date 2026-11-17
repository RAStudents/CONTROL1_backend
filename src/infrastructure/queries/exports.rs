pub(crate) const CREATE_INSERT_EXPORT_USER_QUOTA: &str = r#"
INSERT INTO export_user_quota (user_id) VALUES ($1) ON CONFLICT (user_id) DO UPDATE
    SET revision = export_user_quota.revision + 1
"#;
pub(crate) const CREATE_INSERT_EXPORT_QUOTA: &str = r#"
INSERT INTO export_quota (company_id) VALUES ($1) ON CONFLICT (company_id) DO UPDATE
    SET revision = export_quota.revision + 1
"#;
pub(crate) const CREATE_SELECT_EXPORT_JOBS: &str = r#"
SELECT
    (
        SELECT count(*) FROM export_jobs
        WHERE company_id = $1 AND status IN ('QUEUED', 'PROCESSING')
    ),
    (
        SELECT count(*) FROM export_jobs
        WHERE user_id = $2 AND status IN ('QUEUED', 'PROCESSING')
    )
"#;
pub(crate) const CREATE_WITH_EXPORT_JOBS: &str = include_str!("sql/export_create.sql");
pub(crate) const EXPIRE_FOR_OWNER_SELECT_EXPORT_JOBS: &str = r#"
SELECT status = 'COMPLETED' AND expires_at <= clock_timestamp() FROM export_jobs
WHERE company_id = $1 AND user_id = $2 AND export_id = $3
"#;
pub(crate) const EXPIRE_FOR_OWNER_UPDATE_EXPORT_JOBS: &str = r#"
UPDATE export_jobs SET
    status = 'EXPIRED', failure_code = 'EXPORT_EXPIRED', updated_at = clock_timestamp(),
    finished_at = clock_timestamp(), lease_token = NULL, lease_until = NULL
WHERE
    company_id = $1
    AND user_id = $2
    AND export_id = $3
    AND status = 'COMPLETED'
    AND expires_at <= clock_timestamp()
"#;
pub(crate) const STATUS_SELECT_EXPORT_JOBS: &str = r#"
SELECT
    jsonb_build_object(
        'export_id', export_id, 'status', status,
        'created_at',
        created_at,
        'updated_at',
        updated_at,
        'finished_at',
        finished_at,
        'processed_records',
        processed_records,
        'total_records',
        total_records,
        'expires_at',
        expires_at,
        'failure_code',
        failure_code,
        'include_media_links',
        include_media,
        'format_preset',
        format_preset
    ),
    status,
    coalesce(expires_at <= clock_timestamp(), false)
FROM export_jobs
WHERE company_id = $1 AND user_id = $2 AND export_id = $3
"#;
pub(crate) const DOWNLOAD_SELECT_EXPORT_JOBS: &str = r#"
SELECT
    status,
    coalesce(expires_at <= clock_timestamp(), false),
    storage_path,
    file_size_bytes
FROM export_jobs
WHERE company_id = $1 AND user_id = $2 AND export_id = $3
"#;
