use super::buffer::{BufferMode, CommitBuffer};
use super::{Commit, DIGEST, MAX_GAP, MAX_GAP_BYTES, Signal, chunk, integrity};
use crate::{
    infrastructure::support::{self as api, Error, Result},
    state::App,
};
use async_nats::jetstream::{
    self,
    consumer::{DeliverPolicy, pull},
};
use futures_util::StreamExt;
use serde_json::Value;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::{Mutex, broadcast, watch};
use uuid::Uuid;

pub(super) async fn run(
    app: App,
    company: Uuid,
    tx: watch::Sender<Signal>,
    retained: Arc<Mutex<CommitBuffer>>,
) {
    if let Err(e) = dispatch(&app, company, &tx, &retained).await {
        tracing::error!(%company,reason=e.code,"tenant stream dispatcher stopped");
        let sequence = match *tx.borrow() {
            Signal::Head(h) | Signal::Resync(_, h) => h,
            _ => 0,
        };
        let signal = if e.code == "STREAM_INTEGRITY_VIOLATION" || !e.status.is_server_error() {
            Signal::Resync(e.code, sequence)
        } else {
            Signal::Unavailable
        };
        if e.code != "STREAM_INTEGRITY_VIOLATION" {
            // A new replay can attach to a fresh dispatcher without waiting for other sockets.
            app.dispatchers.lock().await.remove(&company);
        }
        tx.send_replace(signal);
        tx.closed().await;
    }
    let mut active = app.dispatchers.lock().await;
    if active
        .get(&company)
        .is_some_and(|d| Arc::ptr_eq(&d.journal, &retained))
    {
        active.remove(&company);
    }
}

fn message_commit(payload: &[u8], headers: Option<&async_nats::HeaderMap>) -> Result<Commit> {
    let digest = headers
        .and_then(|h| h.get(DIGEST))
        .map(|v| v.as_str().to_owned())
        .ok_or_else(integrity)?;
    let payload: Value = serde_json::from_slice(payload).map_err(|_| integrity())?;
    let seq = payload["tenant_sequence"].as_i64().ok_or_else(integrity)?;
    let row = Commit {
        tenant_sequence: seq,
        payload,
        payload_sha256: digest,
    };
    row.encoded()?;
    Ok(row)
}

async fn consumer(app: &App, company: Uuid) -> Result<pull::Ordered> {
    let c: jetstream::consumer::Consumer<pull::OrderedConfig> = app
        .stream
        .create_consumer(pull::OrderedConfig {
            filter_subject: format!("events.company.{company}"),
            deliver_policy: DeliverPolicy::New,
            ..Default::default()
        })
        .await
        .map_err(|_| Error::unavailable())?;
    let messages = c.messages().await.map_err(|_| Error::unavailable())?;
    app.nats.flush().await.map_err(|_| Error::unavailable())?;
    Ok(messages)
}

async fn head(app: &App, company: Uuid) -> Result<i64> {
    let mut conn = app.pool.acquire().await?;
    api::sequence(&mut conn, company).await
}

async fn initialize(
    app: &App,
    company: Uuid,
    signal: &watch::Sender<Signal>,
    messages: &mut pull::Ordered,
) -> Result<Option<(i64, VecDeque<Commit>)>> {
    // Consumer activation precedes H. No buffered commit fans out before H is known.
    let mut initialization = CommitBuffer::default();
    let initial_head = head(app, company);
    tokio::pin!(initial_head);
    let sequence = loop {
        tokio::select! {
            result = &mut initial_head => break result?,
            _ = signal.closed() => return Ok(None),
            incoming = messages.next() => match incoming {
                Some(Ok(message)) => initialization.insert(
                    message_commit(&message.payload, message.headers.as_ref())?,
                    BufferMode::Initialization,
                )?,
                _ => return Err(Error::unavailable()),
            }
        }
    };
    Ok(Some((sequence, initialization.into_rows())))
}

enum LiveInput {
    Closed,
    Reconcile,
    Commit(Commit),
}

async fn next_live(
    app: &App,
    company: Uuid,
    signal: &watch::Sender<Signal>,
    reconnect: &mut broadcast::Receiver<()>,
    messages: &mut pull::Ordered,
) -> Result<LiveInput> {
    tokio::select! {
        _ = signal.closed() => Ok(LiveInput::Closed),
        _ = reconnect.recv() => {
            *messages = consumer(app, company).await?;
            Ok(LiveInput::Reconcile)
        }
        incoming = messages.next() => match incoming {
            Some(Ok(message)) => Ok(LiveInput::Commit(
                message_commit(&message.payload, message.headers.as_ref())?,
            )),
            _ => {
                // Recreate the consumer without discarding the contiguous cursor.
                *messages = consumer(app, company).await?;
                Ok(LiveInput::Reconcile)
            }
        }
    }
}

// A gap remains private until every row has passed sequence, size and digest checks.
struct GapBatch {
    after: i64,
    rows: Vec<Commit>,
    bytes: usize,
}

impl GapBatch {
    fn new(after: i64) -> Self {
        Self {
            after,
            rows: Vec::new(),
            bytes: 0,
        }
    }

    fn next_sequence(&self) -> Result<i64> {
        self.after
            .checked_add(self.rows.len() as i64 + 1)
            .ok_or_else(integrity)
    }

    fn push(&mut self, commit: Commit, announced: Option<&Commit>) -> Result<()> {
        if commit.tenant_sequence != self.next_sequence()? {
            return Err(gap_error());
        }
        let size = commit.encoded()?.len() + 32;
        if self.bytes + size > MAX_GAP_BYTES {
            return Err(gap_error());
        }
        if let Some(announced) = announced
            && commit.tenant_sequence == announced.tenant_sequence
            && commit.payload_sha256 != announced.payload_sha256
        {
            return Err(integrity());
        }
        self.bytes += size;
        self.rows.push(commit);
        Ok(())
    }
}

async fn load_gap(
    app: &App,
    company: Uuid,
    after: i64,
    target: i64,
    announced: Option<&Commit>,
) -> Result<GapBatch> {
    let mut batch = GapBatch::new(after);
    while batch
        .rows
        .last()
        .map_or(after, |commit| commit.tenant_sequence)
        < target
    {
        let rows = chunk(app, company, batch.next_sequence()?, target, 64).await?;
        if rows.is_empty() {
            return Err(gap_error());
        }
        for commit in rows {
            batch.push(commit, announced)?;
        }
    }
    Ok(batch)
}

struct DispatchCursor {
    last: i64,
    recent: VecDeque<(i64, String)>,
}

impl DispatchCursor {
    fn new(last: i64) -> Self {
        Self {
            last,
            recent: VecDeque::with_capacity(MAX_GAP as usize),
        }
    }

    fn cached_duplicate(&self, commit: &Commit) -> Result<bool> {
        let previous = self
            .recent
            .iter()
            .find(|(sequence, _)| *sequence == commit.tenant_sequence);
        match previous {
            Some((_, hash)) if *hash != commit.payload_sha256 => Err(integrity()),
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    async fn verify_duplicate(&self, app: &App, company: Uuid, commit: &Commit) -> Result<()> {
        if self.cached_duplicate(commit)? {
            return Ok(());
        }
        let rows = chunk(
            app,
            company,
            commit.tenant_sequence,
            commit.tenant_sequence,
            1,
        )
        .await?;
        let Some(previous) = rows.first() else {
            tracing::error!(%company, sequence=commit.tenant_sequence,
                "INTEGRITY_UNVERIFIABLE_OLD_DUPLICATE");
            return Err(Error::new(410, "OUTBOX_RETENTION_EXCEEDED"));
        };
        previous.encoded()?;
        if previous.payload_sha256 != commit.payload_sha256 {
            return Err(integrity());
        }
        Ok(())
    }

    async fn publish(&mut self, batch: GapBatch, journal: &Mutex<CommitBuffer>) -> Result<()> {
        for commit in batch.rows {
            let pair = (commit.tenant_sequence, commit.payload_sha256.clone());
            journal.lock().await.insert(commit, BufferMode::Journal)?;
            self.last = pair.0;
            if self.recent.len() == MAX_GAP as usize {
                self.recent.pop_front();
            }
            self.recent.push_back(pair);
        }
        Ok(())
    }

    async fn advance(
        &mut self,
        app: &App,
        company: Uuid,
        announced: Option<&Commit>,
        journal: &Mutex<CommitBuffer>,
    ) -> Result<Option<i64>> {
        let target = match announced {
            Some(commit) => commit.tenant_sequence,
            None => head(app, company).await?,
        };
        if let Some(commit) = announced {
            commit.encoded()?;
        }
        if target <= self.last {
            if let Some(commit) = announced {
                self.verify_duplicate(app, company, commit).await?;
            }
            return Ok(None);
        }
        if target - self.last > MAX_GAP {
            crate::infrastructure::metrics::increment("dispatcher_replay_gaps_total");
            return Err(gap_error());
        }
        let batch = load_gap(app, company, self.last, target, announced).await?;
        self.publish(batch, journal).await?;
        Ok(Some(self.last))
    }
}

async fn dispatch(
    app: &App,
    company: Uuid,
    signal: &watch::Sender<Signal>,
    journal: &Arc<Mutex<CommitBuffer>>,
) -> Result<()> {
    let mut reconnect = app.nats_reconnected.subscribe();
    let mut messages = consumer(app, company).await?;
    let Some((sequence, mut startup)) = initialize(app, company, signal, &mut messages).await?
    else {
        return Ok(());
    };
    let mut cursor = DispatchCursor::new(sequence);
    signal.send_replace(Signal::Head(sequence));

    loop {
        let announced = if let Some(commit) = startup.pop_front() {
            Some(commit)
        } else {
            match next_live(app, company, signal, &mut reconnect, &mut messages).await? {
                LiveInput::Closed => return Ok(()),
                LiveInput::Reconcile => None,
                LiveInput::Commit(commit) => Some(commit),
            }
        };
        if let Some(sequence) = cursor
            .advance(app, company, announced.as_ref(), journal)
            .await?
        {
            // Publish a head only after the complete gap is verified and journaled.
            signal.send_replace(Signal::Head(sequence));
        }
    }
}

fn gap_error() -> Error {
    Error::new(409, "OUTBOX_GAP_OR_EXPIRED")
}

#[cfg(test)]
mod tests {
    use super::super::tests::commit;
    use super::*;

    #[test]
    fn messages_require_valid_digest_headers_and_a_valid_envelope() {
        let row = commit(1, "verified");
        let bytes = row.encoded().unwrap();
        assert_eq!(
            message_commit(&bytes, None).err().unwrap().code,
            "STREAM_INTEGRITY_VIOLATION"
        );
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(DIGEST, row.payload_sha256.clone());
        let parsed = message_commit(&bytes, Some(&headers)).unwrap();
        assert_eq!(parsed.tenant_sequence, 1);
        assert_eq!(parsed.payload_sha256, row.payload_sha256);
        assert_eq!(
            message_commit(b"invalid", Some(&headers))
                .err()
                .unwrap()
                .code,
            "STREAM_INTEGRITY_VIOLATION"
        );
        headers.insert(DIGEST, "0".repeat(64));
        assert_eq!(
            message_commit(&bytes, Some(&headers)).err().unwrap().code,
            "STREAM_INTEGRITY_VIOLATION"
        );
    }

    #[test]
    fn gap_rows_must_be_contiguous_and_use_checked_arithmetic() {
        let mut batch = GapBatch::new(0);
        batch.push(commit(1, ""), None).unwrap();
        assert_eq!(batch.next_sequence().unwrap(), 2);
        assert_eq!(
            batch.push(commit(3, ""), None).unwrap_err().code,
            "OUTBOX_GAP_OR_EXPIRED"
        );
        assert_eq!(batch.next_sequence().unwrap(), 2);
        assert_eq!(
            GapBatch::new(i64::MAX).next_sequence().unwrap_err().code,
            "STREAM_INTEGRITY_VIOLATION"
        );
    }

    #[test]
    fn gap_byte_limit_counts_utf8_payload_and_metadata() {
        let mut batch = GapBatch::new(0);
        let oversized = commit(1, &"é".repeat(MAX_GAP_BYTES / 2));
        assert_eq!(
            batch.push(oversized, None).unwrap_err().code,
            "OUTBOX_GAP_OR_EXPIRED"
        );
    }

    #[test]
    fn gap_target_must_match_the_announced_digest() {
        let announced = commit(2, "announced");
        let mut batch = GapBatch::new(0);
        batch.push(commit(1, "prior"), Some(&announced)).unwrap();
        assert_eq!(
            batch
                .push(commit(2, "different"), Some(&announced))
                .unwrap_err()
                .code,
            "STREAM_INTEGRITY_VIOLATION"
        );
        batch.push(announced.clone(), Some(&announced)).unwrap();
        assert_eq!(batch.next_sequence().unwrap(), 3);
    }

    #[tokio::test]
    async fn recent_duplicate_window_evicts_oldest_hashes_and_rejects_conflicts() {
        let journal = Mutex::new(CommitBuffer::default());
        let mut cursor = DispatchCursor::new(0);
        let mut batch = GapBatch::new(0);
        for sequence in 1..=MAX_GAP {
            batch.push(commit(sequence, ""), None).unwrap();
        }
        cursor.publish(batch, &journal).await.unwrap();
        assert!(cursor.cached_duplicate(&commit(1, "")).unwrap());
        assert_eq!(
            cursor
                .cached_duplicate(&commit(1, "changed"))
                .unwrap_err()
                .code,
            "STREAM_INTEGRITY_VIOLATION"
        );
        let mut next = GapBatch::new(MAX_GAP);
        next.push(commit(MAX_GAP + 1, ""), None).unwrap();
        cursor.publish(next, &journal).await.unwrap();
        assert_eq!(cursor.last, MAX_GAP + 1);
        assert!(!cursor.cached_duplicate(&commit(1, "")).unwrap());
        assert!(cursor.cached_duplicate(&commit(2, "")).unwrap());
        assert!(journal.lock().await.range(1, 1).is_none());
    }

    #[tokio::test]
    async fn journal_conflict_does_not_advance_the_cursor() {
        let mut journal = CommitBuffer::default();
        journal
            .insert(commit(1, "existing"), BufferMode::Journal)
            .unwrap();
        let journal = Mutex::new(journal);
        let mut cursor = DispatchCursor::new(0);
        let mut batch = GapBatch::new(0);
        batch.push(commit(1, "conflicting"), None).unwrap();
        assert_eq!(
            cursor.publish(batch, &journal).await.unwrap_err().code,
            "STREAM_INTEGRITY_VIOLATION"
        );
        assert_eq!(cursor.last, 0);
        assert!(!cursor.cached_duplicate(&commit(1, "conflicting")).unwrap());
    }
}
