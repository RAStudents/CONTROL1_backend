use crate::domain::exports::Preset;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use serde_json::Value;

const COLUMNS: [&str; 16] = [
    "event_id",
    "occurred_at_utc",
    "occurred_at_local",
    "event_type_id",
    "event_type_name",
    "category_code",
    "location_id",
    "location_name",
    "device_id",
    "device_name",
    "acknowledgement_status",
    "acknowledged_by",
    "acknowledged_at_utc",
    "is_erroneous",
    "preview_url",
    "video_url",
];

pub(crate) struct CsvLayout {
    pub(crate) preset: Preset,
    pub(crate) timezone: Tz,
    pub(crate) include_media: bool,
}

impl CsvLayout {
    pub(crate) fn columns(&self) -> &'static [&'static str] {
        &COLUMNS[..if self.include_media { 16 } else { 14 }]
    }

    // Media values must already have passed the application's CDN allowlist.
    pub(crate) fn record(&self, event: &Value) -> Result<Vec<String>, chrono::ParseError> {
        let text = |key: &str| event[key].as_str().unwrap_or_default();
        let occurred: DateTime<Utc> = text("occurred_at").parse()?;
        let acknowledged = text("acknowledged_at")
            .parse::<DateTime<Utc>>()
            .ok()
            .map(|instant| instant.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
            .unwrap_or_default();

        let mut cells = vec![
            neutralize(text("id")),
            occurred.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
            occurred.with_timezone(&self.timezone).to_rfc3339(),
            neutralize(text("event_type_id")),
            neutralize(text("event_type_name")),
            neutralize(text("category_code")),
            neutralize(text("location_id")),
            neutralize(text("location_name")),
            neutralize(text("device_id")),
            neutralize(text("device_name")),
            neutralize(text("status")),
            neutralize(text("acknowledged_by")),
            acknowledged,
            event["is_erroneous"]
                .as_bool()
                .unwrap_or_default()
                .to_string(),
        ];

        if self.include_media {
            cells.push(neutralize(text("preview_url")));
            cells.push(neutralize(text("video_url")));
        }

        Ok(cells)
    }
}

pub(crate) fn neutralize(s: &str) -> String {
    let first = s
        .chars()
        .find(|&c| !(c.is_whitespace() || c <= '\u{1f}' || ('\u{7f}'..='\u{9f}').contains(&c)));
    if matches!(first, Some('=' | '+' | '-' | '@')) {
        format!("'{s}")
    } else {
        s.to_owned()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn layout() -> CsvLayout {
        CsvLayout {
            preset: Preset::StandardCommaUtf8,
            timezone: chrono_tz::America::New_York,
            include_media: false,
        }
    }

    #[test]
    fn export_record_preserves_columns_dates_and_formula_protection() {
        let event = json!({
            "id": " =SUM(1,2)", "occurred_at": "2025-11-02T05:59:59.123Z",
            "event_type_id": "motion", "event_type_name": "Door \"A\", floor; 1\r\nback",
            "category_code": "SECURITY", "location_id": "entrance", "location_name": "Lobby",
            "device_id": "camera", "device_name": "@external", "status": "ACKNOWLEDGED",
            "acknowledged_by": "operator", "acknowledged_at": "2025-11-02T09:00:00+03:00",
            "is_erroneous": true
        });
        let cells = layout().record(&event).unwrap();
        assert_eq!(
            cells,
            [
                "' =SUM(1,2)",
                "2025-11-02T05:59:59.123Z",
                "2025-11-02T01:59:59.123-04:00",
                "motion",
                "Door \"A\", floor; 1\r\nback",
                "SECURITY",
                "entrance",
                "Lobby",
                "camera",
                "'@external",
                "ACKNOWLEDGED",
                "operator",
                "2025-11-02T06:00:00Z",
                "true",
            ]
        );
        assert_eq!(layout().columns().len(), cells.len());
    }

    #[test]
    fn export_record_handles_dst_fold_nulls_and_invalid_acknowledgement() {
        let cells = layout()
            .record(&json!({
                "occurred_at": "2025-11-02T06:00:00Z", "acknowledged_at": "invalid"
            }))
            .unwrap();
        assert_eq!(cells[2], "2025-11-02T01:00:00-05:00");
        assert_eq!(cells[0], "");
        assert_eq!(cells[12], "");
        assert_eq!(cells[13], "false");
    }

    #[test]
    fn export_record_requires_valid_occurrence_time() {
        assert!(layout().record(&json!({"occurred_at": "invalid"})).is_err());
        assert!(layout().record(&json!({})).is_err());
    }

    #[test]
    fn export_record_includes_only_requested_media_columns() {
        let event = json!({"occurred_at": "2025-01-01T00:00:00Z",
                           "preview_url": "https://cdn.example.test/image.jpg", "video_url": null});
        let mut layout = layout();
        assert_eq!(layout.record(&event).unwrap().len(), 14);
        layout.include_media = true;
        let cells = layout.record(&event).unwrap();
        assert_eq!(cells.len(), 16);
        assert_eq!(&layout.columns()[14..], ["preview_url", "video_url"]);
        assert_eq!(cells[14], "https://cdn.example.test/image.jpg");
        assert_eq!(cells[15], "");
    }

    #[test]
    fn spreadsheet_formula_vectors() {
        for raw in [
            "=1",
            " +2",
            "\t-3",
            "\r\n@x",
            "\u{a0}\u{2003}=x",
            "\u{1f}+2",
            "\u{85}\u{9f}-2",
            "\t=\"quotes, delimiters; and\r\nnewlines\"",
            "=data",
        ] {
            assert_eq!(neutralize(raw), format!("'{raw}"));
        }
        for raw in [
            "",
            " ",
            "plain",
            "'@already text",
            "⟶=still text",
            "\u{feff}=not whitespace",
        ] {
            assert_eq!(neutralize(raw), raw);
        }
    }
}
