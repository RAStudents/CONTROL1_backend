use crate::domain::error::{Error, Result};
use sha2::{Digest, Sha256};
use sqlx::{Acquire, PgConnection, PgPool, Postgres, Transaction};
use uuid::Uuid;
impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        let class = e
            .as_database_error()
            .and_then(|e| e.code())
            .map(|v| v.into_owned());
        tracing::error!(code = ?class, kind = ?std::mem::discriminant(&e), "database operation failed");
        match class.as_deref() {
            Some("40001") => {
                let mut err = Self::unavailable();
                err.serialization_failure = true;
                err
            }
            Some("23505" | "23P01") => Self::new(409, "STATE_CONFLICT"),
            Some("55P03") => Self::new(423, "RESOURCE_LOCKED"),
            Some("57014") => Self::unavailable(),
            _ if matches!(
                e,
                sqlx::Error::PoolTimedOut
                    | sqlx::Error::PoolClosed
                    | sqlx::Error::Io(_)
                    | sqlx::Error::Tls(_)
            ) =>
            {
                Self::unavailable()
            }
            _ => Self::internal(),
        }
    }
}

pub(crate) async fn sequence(conn: &mut PgConnection, company: Uuid) -> Result<i64> {
    sqlx::query_scalar(
        r#"
SELECT last_sequence FROM company_stream_counters
WHERE company_id = $1
"#,
    )
    .bind(company)
    .fetch_optional(conn)
    .await?
    .ok_or_else(Error::missing)
}

// Dedicated namespace. The first four bytes of SHA-256(UUID bytes) give a stable signed key.
const ANCHOR_NAMESPACE: i32 = 0x4354524c;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BarrierMode {
    Shared,
    Exclusive,
}

pub(crate) struct Barrier {
    conn: sqlx::pool::PoolConnection<Postgres>,
    pid: i32,
    key: i32,
    mode: BarrierMode,
}

impl Barrier {
    pub(crate) async fn acquire(pool: &PgPool, company: Uuid, mode: BarrierMode) -> Result<Self> {
        let started = std::time::Instant::now();
        let mut conn = pool.acquire().await?;
        // Closing the session on drop also releases the lock on cancellation and errors.
        conn.close_on_drop();
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *conn)
            .await?;
        let digest = Sha256::digest(company.as_bytes());
        let key = i32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]);
        let query = if mode == BarrierMode::Shared {
            "SELECT pg_advisory_lock_shared($1, $2)"
        } else {
            "SELECT pg_advisory_lock($1, $2)"
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            sqlx::query(query)
                .bind(ANCHOR_NAMESPACE)
                .bind(key)
                .execute(&mut *conn),
        )
        .await
        .map_err(|_| Error::new(423, "RESOURCE_LOCKED"))??;
        tracing::debug!(%company, ?mode, wait_ms = started.elapsed().as_millis(), "anchor barrier acquired");
        Ok(Self {
            conn,
            pid,
            key,
            mode,
        })
    }

    pub(crate) async fn begin_snapshot(
        &mut self,
        company: Uuid,
    ) -> Result<(Transaction<'_, Postgres>, i64)> {
        let mut tx = self.conn.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *tx)
            .await?;
        // This must be the first snapshot-reading statement after BEGIN.
        let (seq, pid): (i64, i32) = sqlx::query_as(
            r#"
SELECT
    last_sequence,
    pg_backend_pid()
FROM company_stream_counters
WHERE company_id = $1
"#,
        )
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(Error::missing)?;
        if pid != self.pid {
            return Err(Error::unavailable());
        }
        Ok((tx, seq))
    }

    pub(crate) async fn begin(&mut self) -> Result<Transaction<'_, Postgres>> {
        let mut tx = self.conn.begin().await?;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *tx)
            .await?;
        if pid != self.pid {
            return Err(Error::unavailable());
        }
        Ok(tx)
    }

    pub(crate) async fn unlock(mut self) -> Result<()> {
        let query = if self.mode == BarrierMode::Shared {
            r#"
SELECT
    pg_backend_pid(),
    pg_advisory_unlock_shared($1, $2)
"#
        } else {
            r#"
SELECT
    pg_backend_pid(),
    pg_advisory_unlock($1, $2)
"#
        };
        let (pid, unlocked): (i32, bool) = sqlx::query_as(query)
            .bind(ANCHOR_NAMESPACE)
            .bind(self.key)
            .fetch_one(&mut *self.conn)
            .await?;
        if pid != self.pid || !unlocked {
            return Err(Error::unavailable());
        }
        Ok(())
    }
}

pub(crate) async fn retry<'a, T>(
    mut run: impl FnMut() -> futures_util::future::BoxFuture<'a, Result<T>>,
) -> Result<T> {
    for delay in [10, 30, 100] {
        match run().await {
            Err(e) if e.serialization_failure => {
                let jitter = u64::from(Uuid::new_v4().as_bytes()[0]) % delay;
                tokio::time::sleep(std::time::Duration::from_millis(delay + jitter)).await;
            }
            result => return result,
        }
    }
    run().await
}

/// Each cancellable query keeps its connection pinned until the backend has been
/// signalled. An obsolete result is discarded by the socket actor immediately.
pub(crate) fn cancellable_query<T: Send + 'static>(
    pool: PgPool,
    run: impl for<'a> FnOnce(&'a mut PgConnection) -> futures_util::future::BoxFuture<'a, Result<T>>
    + Send
    + 'static,
) -> futures_util::future::BoxFuture<'static, Result<T>> {
    use sqlx::Connection;
    use std::time::Duration;
    use tokio::time::{sleep, timeout};
    let cancel = tokio_util::sync::CancellationToken::new();
    let on_drop = cancel.clone().drop_guard();
    let worker = tokio::spawn(async move {
        let mut conn = tokio::select! {
            _=cancel.cancelled()=>return Err(Error::unavailable()),
            result=pool.acquire()=>result?,
        };
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *conn)
            .await?;
        let result = tokio::select! {
            biased;
            _=cancel.cancelled()=>None,
            _=sleep(Duration::from_secs(5))=>None,
            result=run(&mut conn)=>Some(result),
        };
        if let Some(result) = result {
            return result;
        }
        conn.close_on_drop();
        // A second connection sends the PostgreSQL cancellation signal. The
        // original connection stays checked out even if the signal times out.
        let signal = sqlx::query_scalar::<_, bool>("SELECT pg_cancel_backend($1)")
            .bind(pid)
            .fetch_one(&pool);
        match timeout(Duration::from_secs(1), signal).await {
            Ok(Ok(true)) => {
                let _ = timeout(Duration::from_secs(1), conn.ping()).await;
            }
            _ => tracing::debug!("query cancellation fell back to PostgreSQL statement timeout"),
        }
        Err(Error::unavailable())
    });
    Box::pin(async move {
        let _on_drop = on_drop;
        worker.await.map_err(|_| Error::internal())?
    })
}

pub(crate) async fn ping(pool: &PgPool) -> Result<()> {
    sqlx::query("SELECT 1").execute(pool).await?;
    Ok(())
}
