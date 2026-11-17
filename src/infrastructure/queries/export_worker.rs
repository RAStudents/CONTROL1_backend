pub(crate) const CLAIM_WITH_EXPORT_JOBS: &str = include_str!("sql/export_claim.sql");
pub(crate) const RENEW_UPDATE_EXPORT_JOBS: &str = r#"
UPDATE
    export_jobs SET
    lease_until = least(clock_timestamp() + interval '5 minutes', absolute_deadline_at),
    processed_records = $4, updated_at = clock_timestamp()
WHERE
    company_id = $1 AND export_id = $2 AND status = 'PROCESSING' AND lease_token = $3
    AND lease_until > clock_timestamp() AND absolute_deadline_at > clock_timestamp()
"#;
pub(crate) const FAIL_UPDATE_EXPORT_JOBS: &str = r#"
UPDATE
    export_jobs SET
    status = 'FAILED', failure_code = $4, finished_at = clock_timestamp(),
    updated_at = clock_timestamp(), lease_token = NULL, lease_until = NULL
WHERE
    company_id = $1 AND export_id = $2 AND status = 'PROCESSING'
    AND lease_token = $3
    AND lease_until > clock_timestamp()
    AND absolute_deadline_at > clock_timestamp()
"#;
pub(crate) const FINALIZE_UPDATE_EXPORT_JOBS: &str = include_str!("sql/export_finalize.sql");
pub(crate) const FINALIZE_SELECT_EXPORT_JOBS: &str = r#"
SELECT EXISTS(
    SELECT 1
    FROM export_jobs
    WHERE
        company_id = $1 AND export_id = $2
        AND status = 'COMPLETED' AND storage_path = $3
)
"#;
pub(crate) const GENERATE_SELECT_COMPANIES: &str = r#"
SELECT business_timezone FROM companies
WHERE id = $1
"#;
