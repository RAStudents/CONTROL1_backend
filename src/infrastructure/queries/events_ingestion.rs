pub(crate) const INGEST_INSERT_EVENTS: &str = r#"
INSERT INTO events (
    company_id,
    id,
    created_sequence,
    event_type_id,
    category_code,
    location_id,
    device_id,
    occurred_at,
    received_at,
    preview_url,
    video_url
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, clock_timestamp(), $9, $10)
"#;
pub(crate) const INGEST_INSERT_EVENT_VERSIONS: &str = r#"
INSERT INTO event_versions (
    company_id,
    event_id,
    event_version,
    classification_version,
    acknowledgement_version,
    valid_from_sequence,
    status,
    is_erroneous
)
VALUES ($1, $2, 1, 1, 1, $3, 'NEW', false)
"#;
