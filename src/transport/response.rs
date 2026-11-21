use crate::{
    application::{CachePolicy, Outcome, ReplayStatus},
    domain::error::Error,
};
use axum::{
    Json,
    body::Body,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use uuid::Uuid;
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let retryable = self.status.is_server_error()
            || matches!(
                self.code,
                "RATE_LIMITED"
                    | "RESOURCE_LOCKED"
                    | "IDEMPOTENCY_REQUEST_IN_PROGRESS"
                    | "EXPORT_NOT_READY"
            );
        let status =
            StatusCode::from_u16(self.status.as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let body = json!({"error": {
            "code": self.code, "message": self.code, "request_id": Uuid::new_v4(),
            "retryable": retryable, "details": self.details,
        }});
        let mut response = (status, Json(body)).into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        if self.status.as_u16() == 401 {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }
}

pub(crate) fn etag(headers: &HeaderMap, tag: &str, value: Value, policy: CachePolicy) -> Response {
    let Ok(etag) = HeaderValue::from_str(tag) else {
        return Error::internal().into_response();
    };
    let unchanged = policy == CachePolicy::Conditional
        && headers
            .get_all(header::IF_NONE_MATCH)
            .iter()
            .filter_map(|h| h.to_str().ok())
            .flat_map(|h| h.split(','))
            .any(|t| t.trim() == "*" || t.trim().trim_start_matches("W/") == tag);
    let mut response = if unchanged {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        Json(value).into_response()
    };
    response.headers_mut().insert(header::ETAG, etag);
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-cache"),
    );
    response
}
impl Outcome {
    pub(crate) fn render(self, headers: &HeaderMap) -> Response {
        match self {
            Self::Text(text) => (
                [(
                    header::CONTENT_TYPE,
                    "text/plain; version=0.0.4; charset=utf-8",
                )],
                text,
            )
                .into_response(),
            Self::Json(v) => Json(v).into_response(),
            Self::Cached { tag, value, policy } => etag(headers, &tag, value, policy),
            Self::Reply {
                code,
                value,
                replayed,
            } => {
                let Ok(status) = StatusCode::from_u16(code) else {
                    return Error::internal().into_response();
                };
                let mut r = (status, Json(value)).into_response();
                if replayed == ReplayStatus::Replayed {
                    r.headers_mut()
                        .insert("X-Idempotency-Replayed", HeaderValue::from_static("true"));
                }
                r
            }
            Self::Download { file, size } => (
                [
                    (header::CONTENT_TYPE, "text/csv; charset=utf-8".to_owned()),
                    (
                        header::CONTENT_DISPOSITION,
                        "attachment; filename=\"events.csv\"".to_owned(),
                    ),
                    (header::CONTENT_LENGTH, size.to_string()),
                    (header::CACHE_CONTROL, "private, no-store".to_owned()),
                ],
                Body::from_stream(tokio_util::io::ReaderStream::new(file)),
            )
                .into_response(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CachePolicy, HeaderMap, HeaderValue, Outcome, ReplayStatus, StatusCode, etag, header,
    };
    use serde_json::json;

    #[test]
    fn conditional_etag_accepts_weak_and_multiple_validators() {
        let mut headers = HeaderMap::new();
        headers.append(header::IF_NONE_MATCH, HeaderValue::from_static("\"old\""));
        headers.append(
            header::IF_NONE_MATCH,
            HeaderValue::from_static("W/\"revision\""),
        );
        let response = etag(
            &headers,
            "\"revision\"",
            json!({}),
            CachePolicy::Conditional,
        );
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers()[header::ETAG], "\"revision\"");
    }

    #[test]
    fn expiring_grant_always_returns_a_fresh_body() {
        let mut headers = HeaderMap::new();
        headers.insert(header::IF_NONE_MATCH, HeaderValue::from_static("*"));
        let response = etag(
            &headers,
            "\"revision\"",
            json!({"remaining_ttl_sec": 42}),
            CachePolicy::AlwaysFresh,
        );
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn invalid_internal_metadata_returns_an_error_instead_of_panicking() {
        let response = etag(
            &HeaderMap::new(),
            "\"bad\r\ntag\"",
            json!({}),
            CachePolicy::Conditional,
        );
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let response = Outcome::Reply {
            code: 0,
            value: json!({}),
            replayed: ReplayStatus::Original,
        }
        .render(&HeaderMap::new());
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
