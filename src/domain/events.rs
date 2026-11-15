use crate::domain::error::{Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Filters {
    pub(crate) search_id: Option<String>,
    pub(crate) from: Option<DateTime<Utc>>,
    pub(crate) to: Option<DateTime<Utc>>,
    pub(crate) event_type_ids: Vec<String>,
    pub(crate) device_ids: Vec<String>,
    pub(crate) validity: Validity,
}

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum Validity {
    #[default]
    All,
    Valid,
    Erroneous,
}

impl Filters {
    pub(crate) fn normalize(mut self) -> Result<Self> {
        self.search_id = self
            .search_id
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty());
        let mut invalid = Vec::new();
        if self
            .search_id
            .as_ref()
            .is_some_and(|s| s.chars().count() > 64 || s.contains('\0'))
        {
            invalid.push("search_id");
        }
        if self.from.zip(self.to).is_some_and(|(f, t)| t <= f) {
            invalid.extend(["from", "to"]);
        }
        for (field, ids) in [
            ("event_type_ids", &mut self.event_type_ids),
            ("device_ids", &mut self.device_ids),
        ] {
            ids.sort();
            ids.dedup();
            if ids.iter().any(|id| id.is_empty() || id.contains('\0')) {
                invalid.push(field);
            }
        }
        if invalid.is_empty() {
            Ok(self)
        } else {
            Err(filter_error(&invalid))
        }
    }
    pub(crate) fn escaped(&self) -> Option<String> {
        self.search_id.as_ref().map(|v| {
            v.replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_")
        })
    }
}

pub(crate) fn filter_error(fields: &[&str]) -> Error {
    Error::new(422, "FILTER_INVALID").details(json!({"invalid_fields": fields}))
}

pub(crate) const ERRONEOUS: &str =
    "POST /api/v1/companies/{company_id}/events/{event_id}/erroneous";
pub(crate) const EXPORTS: &str = "POST /api/v1/companies/{company_id}/events/exports";
pub(crate) const INGEST: &str = "POST /api/v1/companies/{company_id}/events/ingest";

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn normalization_deduplicates_sets_preserves_case_and_literal_search() {
        let f = Filters {
            search_id: Some("  cam_01%test\\2  ".into()),
            device_ids: vec!["b".into(), "a".into(), "b".into()],
            ..Default::default()
        }
        .normalize()
        .unwrap();
        assert_eq!(f.search_id.as_deref(), Some("cam_01%test\\2"));
        assert_eq!(f.escaped().as_deref(), Some("cam\\_01\\%test\\\\2"));
        assert_eq!(f.device_ids, vec!["a", "b"]);
    }
    #[test]
    fn invalid_interval_reports_both_fields() {
        let time = "2026-01-01T00:00:00Z".parse().unwrap();
        let error = Filters {
            from: Some(time),
            to: Some(time),
            ..Default::default()
        }
        .normalize()
        .unwrap_err();
        assert_eq!(error.code, "FILTER_INVALID");
        assert_eq!(error.details["invalid_fields"], json!(["from", "to"]));
    }
}
