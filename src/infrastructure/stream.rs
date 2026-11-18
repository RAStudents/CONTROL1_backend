mod buffer;
mod dispatcher;
mod outbox;

pub(crate) use buffer::CommitBuffer;
pub(crate) use outbox::relay;

use crate::{
    infrastructure::support::{self as api, Error, MAX_SEQ, Result},
    state::App,
};
use async_nats::jetstream::{
    self,
    stream::{Config, RetentionPolicy, StorageType},
};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{Mutex, watch};
use uuid::Uuid;

pub(crate) const NAME: &str = "EVENTS_TENANT";
const DIGEST: &str = "Control-Payload-Sha256";
const MAX_GAP: i64 = 2048;
const MAX_GAP_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, sqlx::FromRow)]
pub(crate) struct Commit {
    pub(crate) tenant_sequence: i64,
    pub(crate) payload: Value,
    pub(crate) payload_sha256: String,
}

impl Commit {
    pub(crate) fn encoded(&self) -> Result<Vec<u8>> {
        let value = &self.payload;
        if value.as_object().is_none_or(|o| o.len() != 7)
            || value["schema_version"] != 1
            || value["type"] != "TENANT_COMMIT"
            || value["tenant_sequence"] != self.tenant_sequence
            || !(1..=MAX_SEQ).contains(&self.tenant_sequence)
            || ![
                "event_created",
                "event_updated",
                "summary_updated",
                "filter_options_updated",
            ]
            .iter()
            .all(|k| value.get(k).is_some_and(|v| v.is_null() || v.is_object()))
            || ![
                "event_created",
                "event_updated",
                "summary_updated",
                "filter_options_updated",
            ]
            .iter()
            .any(|k| value[k].is_object())
            || (!value["event_created"].is_null() && !value["event_updated"].is_null())
        {
            return Err(Error::new(500, "STREAM_INTEGRITY_VIOLATION"));
        }
        let bytes = api::canonical(value)?;
        if api::sha(&bytes) != self.payload_sha256 {
            return Err(Error::new(500, "STREAM_INTEGRITY_VIOLATION"));
        }
        Ok(bytes)
    }
}

pub(crate) async fn setup(nats: &async_nats::Client) -> anyhow::Result<jetstream::stream::Stream> {
    let ctx = jetstream::new(nats.clone());
    let config = Config {
        name: NAME.into(),
        subjects: vec!["events.company.*".into()],
        storage: StorageType::File,
        retention: RetentionPolicy::Limits,
        max_age: Duration::from_secs(86400),
        duplicate_window: Duration::from_secs(86400),
        ..Default::default()
    };
    let stream = ctx.get_or_create_stream(config.clone()).await?;
    let existing = &stream.cached_info().config;
    anyhow::ensure!(
        existing.subjects == config.subjects
            && existing.storage == config.storage
            && existing.retention == config.retention
            && existing.max_age == config.max_age
            && existing.duplicate_window == config.duplicate_window,
        "incompatible JetStream stream configuration"
    );
    Ok(stream)
}

async fn chunk(app: &App, company: Uuid, start: i64, end: i64, limit: i64) -> Result<Vec<Commit>> {
    let mut conn = app.pool.acquire().await?;
    chunk_on(&mut conn, company, start, end, limit).await
}

pub(crate) async fn chunk_on(
    conn: &mut sqlx::PgConnection,
    company: Uuid,
    start: i64,
    end: i64,
    limit: i64,
) -> Result<Vec<Commit>> {
    Ok(sqlx::query_as(
        r#"
SELECT
    tenant_sequence,
    payload,
    payload_sha256
FROM tenant_outbox
WHERE
    company_id = $1
    AND tenant_sequence BETWEEN $2 AND $3
ORDER BY tenant_sequence
LIMIT $4
"#,
    )
    .bind(company)
    .bind(start)
    .bind(end)
    .bind(limit)
    .fetch_all(conn)
    .await?)
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Signal {
    Starting,
    Head(i64),
    Unavailable,
    Resync(&'static str, i64),
}

#[derive(Clone)]
pub(crate) struct Subscription {
    pub(crate) state: watch::Receiver<Signal>,
    journal: Arc<Mutex<CommitBuffer>>,
}

impl Subscription {
    pub(crate) async fn cached(&self, start: i64, end: i64) -> Option<Vec<Commit>> {
        self.journal.lock().await.range(start, end)
    }
}

#[derive(Clone)]
pub(crate) struct Dispatcher {
    signal: watch::Sender<Signal>,
    journal: Arc<Mutex<CommitBuffer>>,
}

pub(crate) type Dispatchers = Arc<Mutex<HashMap<Uuid, Dispatcher>>>;

pub(crate) async fn attach(app: &App, company: Uuid) -> Subscription {
    let mut active = app.dispatchers.lock().await;
    if let Some(d) = active.get(&company) {
        return Subscription {
            state: d.signal.subscribe(),
            journal: Arc::clone(&d.journal),
        };
    }
    let (tx, rx) = watch::channel(Signal::Starting);
    let journal = Arc::new(Mutex::new(CommitBuffer::default()));
    active.insert(
        company,
        Dispatcher {
            signal: tx.clone(),
            journal: Arc::clone(&journal),
        },
    );
    tokio::spawn(dispatcher::run(
        app.clone(),
        company,
        tx,
        Arc::clone(&journal),
    ));
    Subscription { state: rx, journal }
}

fn integrity() -> Error {
    Error::new(500, "STREAM_INTEGRITY_VIOLATION")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    pub(super) fn commit(seq: i64, text: &str) -> Commit {
        let payload = json!({
            "schema_version": 1,
            "type": "TENANT_COMMIT",
            "tenant_sequence": seq,
            "event_created": null,
            "event_updated": null,
            "summary_updated": {"text": text},
            "filter_options_updated": null,
        });
        let payload_sha256 = api::sha(api::canonical(&payload).unwrap());
        Commit {
            tenant_sequence: seq,
            payload,
            payload_sha256,
        }
    }

    #[test]
    fn transport_fields_and_corruption_never_enter_journal() {
        let mut row = commit(1, "");
        row.payload["replay_id"] = json!(1);
        assert!(row.encoded().is_err());
        let mut row = commit(1, "");
        row.payload_sha256 = "0".repeat(64);
        assert!(row.encoded().is_err());
        let mut row = commit(1, "");
        row.payload["event_created"] = json!("invalid");
        row.payload_sha256 = api::sha(api::canonical(&row.payload).unwrap());
        assert!(row.encoded().is_err());
    }
}
