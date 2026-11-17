use crate::state::App;
use std::path::PathBuf;
use uuid::Uuid;
pub(crate) fn storage_path(app: &App, company: Uuid, id: &str, key: &str) -> Option<PathBuf> {
    if id.is_empty()
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return None;
    }
    let prefix = format!("exports/{company}/{id}/");
    let token = key.strip_prefix(&prefix)?.strip_suffix(".csv")?;
    Uuid::parse_str(token).ok()?;
    Some(app.export_dir.join(key))
}
