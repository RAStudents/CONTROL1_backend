pub(crate) const MUTATE_SELECT_EVENTS: &str = r#"
SELECT id FROM events
WHERE company_id = $1 AND id = $2 FOR UPDATE
"#;
pub(crate) const MUTATE_SELECT_EVENT_VERSIONS: &str = r#"
SELECT event_version
FROM event_versions
WHERE company_id = $1 AND event_id = $2 AND valid_to_sequence IS NULL FOR UPDATE
"#;
pub(crate) const MUTATE_WITH_EVENT_VERSIONS: &str = include_str!("sql/review_transition.sql");
