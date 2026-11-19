use crate::domain::{
    access::Identity,
    error::{Error, Result},
};
use serde_json::Value;
use uuid::Uuid;
/// Authenticated transport metadata. No framework extractors cross this boundary.
pub(crate) struct RequestContext {
    pub(crate) identity: Identity,
    pub(crate) idempotency_key: Option<String>,
    pub(crate) if_match: Option<String>,
}
impl RequestContext {
    pub(crate) fn key(&self) -> Result<String> {
        self.idempotency_key
            .as_deref()
            .and_then(|s| Uuid::parse_str(s).ok())
            .map(|id| id.to_string())
            .ok_or_else(Error::invalid)
    }
}
/// Application result; HTTP caching, headers and body streaming are rendered by transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CachePolicy {
    Conditional,
    AlwaysFresh,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReplayStatus {
    Original,
    Replayed,
}

pub(crate) enum Outcome {
    Text(String),
    Json(Value),
    Reply {
        code: u16,
        value: Value,
        replayed: ReplayStatus,
    },
    Cached {
        tag: String,
        value: Value,
        policy: CachePolicy,
    },
    Download {
        file: tokio::fs::File,
        size: i64,
    },
}
impl Outcome {
    pub(crate) fn cached(tag: &str, value: Value, policy: CachePolicy) -> Self {
        Self::Cached {
            tag: tag.to_owned(),
            value,
            policy,
        }
    }
}
