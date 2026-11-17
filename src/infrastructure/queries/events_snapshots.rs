pub(crate) const PAGE_SELECT_EVENT_SNAPSHOTS: &str = r#"
SELECT
    filter_hash,
    as_of_sequence,
    total_count,
    expires_at <= clock_timestamp() expired
FROM event_snapshots
WHERE snapshot_id = $1 AND company_id = $2 AND user_id = $3
"#;
pub(crate) const NEW_SNAPSHOT_INSERT_EVENT_SNAPSHOTS: &str = r#"
INSERT INTO event_snapshots (
    snapshot_id, company_id, user_id, filter_hash, as_of_sequence, total_count, expires_at
)
VALUES ($1, $2, $3, $4, $5, $6, clock_timestamp() + interval '30 minutes')
"#;
pub(crate) const GET_EVENTS_SELECT_EVENT_SNAPSHOTS: &str = r#"
SELECT
    filter_hash,
    as_of_sequence
FROM event_snapshots
WHERE snapshot_id = $1 AND company_id = $2 AND user_id = $3
"#;
pub(crate) const SUMMARY_SET_READ: &str =
    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY";
