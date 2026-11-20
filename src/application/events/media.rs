use crate::state::App;
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MediaKind {
    Preview,
    Video,
}

pub(crate) fn valid_media(app: &App, value: &str, kind: MediaKind) -> bool {
    if value.chars().count() > 2048
        || !value.starts_with("https://")
        || value
            .chars()
            .any(|c| c == '\\' || c.is_whitespace() || c.is_control())
    {
        return false;
    }
    if value.split('/').nth(2).is_none_or(|a| a.contains('@')) {
        return false;
    }
    let Ok(url) = url::Url::parse(value) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };
    let suffix = url.path().to_ascii_lowercase();
    let allowed_path = if kind == MediaKind::Video {
        suffix.ends_with(".mp4")
    } else {
        suffix.ends_with(".jpg") || suffix.ends_with(".jpeg")
    };
    url.scheme() == "https"
        && url.fragment().is_none()
        && allowed_path
        && app.cdn_hosts.contains(&format!(
            "{}:{}",
            host,
            url.port_or_known_default().unwrap_or(443)
        ))
}

pub(crate) fn sanitize_media(app: &App, dto: &mut Value) {
    if !dto["preview_url"]
        .as_str()
        .is_some_and(|v| valid_media(app, v, MediaKind::Preview))
    {
        dto["preview_url"] = json!("");
    }
    if !dto["video_url"]
        .as_str()
        .is_some_and(|v| valid_media(app, v, MediaKind::Video))
    {
        dto["video_url"] = Value::Null;
    }
}
