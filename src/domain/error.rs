use serde_json::{Value, json};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Status(pub(crate) u16);
impl Status {
    pub(crate) fn as_u16(self) -> u16 {
        self.0
    }
    pub(crate) fn is_server_error(self) -> bool {
        (500..600).contains(&self.0)
    }
}
pub(crate) use crate::domain::limits::MAX_SEQ;
pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub(crate) struct Error {
    pub(crate) status: Status,
    pub(crate) code: &'static str,
    pub(crate) details: Value,
    pub(crate) serialization_failure: bool,
}

impl Error {
    pub(crate) fn new(status: u16, code: &'static str) -> Self {
        Self {
            status: Status(status),
            code,
            details: json!({}),
            serialization_failure: false,
        }
    }
    pub(crate) fn details(mut self, details: Value) -> Self {
        self.details = details;
        self
    }
    pub(crate) fn invalid() -> Self {
        Self::new(422, "REQUEST_INVALID")
    }
    pub(crate) fn missing() -> Self {
        Self::new(404, "RESOURCE_NOT_FOUND")
    }
    pub(crate) fn unavailable() -> Self {
        Self::new(503, "ACCESS_CHECK_UNAVAILABLE")
    }
    pub(crate) fn internal() -> Self {
        Self::new(500, "INTERNAL_ERROR")
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.code, self.status.as_u16())
    }
}
impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::Error;
    use serde_json::json;

    #[test]
    fn diagnostic_format_omits_request_details() {
        let error = Error::invalid().details(json!({"private": "request payload"}));
        let erased: &(dyn std::error::Error + Send + Sync) = &error;
        assert_eq!(erased.to_string(), "REQUEST_INVALID (422)");
        assert!(erased.source().is_none());
    }
}
