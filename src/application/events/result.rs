use crate::{
    application::{Outcome, ReplayStatus},
    domain::error::{Error, Result},
};
use serde_json::Value;
use uuid::Uuid;

pub(crate) fn company(value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|_| Error::missing())
}

pub(crate) fn response(code: i32, value: Value, replayed: ReplayStatus) -> Outcome {
    Outcome::Reply {
        code: u16::try_from(code).unwrap_or(500),
        value,
        replayed,
    }
}
