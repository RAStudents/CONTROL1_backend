use crate::infrastructure::stream;
use sqlx::PgPool;
use std::{collections::HashSet, path::PathBuf, sync::Arc};
use tokio::sync::{Semaphore, broadcast};
#[derive(Clone)]
pub(crate) struct App {
    pub(crate) pool: PgPool,
    pub(crate) jwt_key: Arc<Vec<u8>>,
    pub(crate) cursor_key: Arc<Vec<u8>>,
    pub(crate) cursor_key_id: String,
    pub(crate) dummy_hash: String,
    pub(crate) password_checks: Arc<Semaphore>,
    pub(crate) nats: async_nats::Client,
    pub(crate) stream: async_nats::jetstream::stream::Stream,
    pub(crate) dispatchers: stream::Dispatchers,
    pub(crate) cdn_hosts: Arc<HashSet<String>>,
    pub(crate) max_batch: i64,
    pub(crate) max_snapshot_items: i64,
    pub(crate) export_dir: PathBuf,
    pub(crate) access_changes: broadcast::Sender<()>,
    pub(crate) nats_reconnected: broadcast::Sender<()>,
}
