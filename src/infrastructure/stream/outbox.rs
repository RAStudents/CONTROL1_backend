use super::{Commit, DIGEST};
use crate::state::App;
use async_nats::jetstream;
use futures_util::StreamExt;
use sqlx::PgConnection;
use std::time::Duration;
use uuid::Uuid;

// Errors leave this lease to expire; only successful sweeps explicitly release it.
struct PublisherLease {
    company: Uuid,
    token: Uuid,
    epoch: i64,
}

impl PublisherLease {
    async fn acquire(app: &App, company: Uuid) -> anyhow::Result<Option<Self>> {
        let token = Uuid::new_v4();
        let epoch: Option<i64> = sqlx::query_scalar(
            r#"
INSERT INTO outbox_company_leases (company_id, lease_token, lease_until)
VALUES ($1, $2, clock_timestamp() + interval '5 seconds') ON CONFLICT (company_id) DO UPDATE SET
    lease_token = excluded.lease_token,
    lease_epoch = outbox_company_leases.lease_epoch + 1,
    lease_until = excluded.lease_until
WHERE outbox_company_leases.lease_until <= clock_timestamp() RETURNING lease_epoch
"#,
        )
        .bind(company)
        .bind(token)
        .fetch_optional(&app.pool)
        .await?;
        Ok(epoch.map(|epoch| Self {
            company,
            token,
            epoch,
        }))
    }

    async fn renew(&self, conn: &mut PgConnection) -> anyhow::Result<bool> {
        let epoch: Option<i64> = sqlx::query_scalar(
            r#"
UPDATE outbox_company_leases SET lease_until = clock_timestamp() + interval '5 seconds'
WHERE
    company_id = $1 AND lease_token = $2 AND lease_epoch = $3 AND lease_until > clock_timestamp()
RETURNING lease_epoch
"#,
        )
        .bind(self.company)
        .bind(self.token)
        .bind(self.epoch)
        .fetch_optional(conn)
        .await?;
        Ok(epoch.is_some())
    }

    async fn mark_dispatched(&self, conn: &mut PgConnection, sequence: i64) -> anyhow::Result<()> {
        sqlx::query(
            r#"
UPDATE
    tenant_outbox o
SET broker_dispatched_at = clock_timestamp()
WHERE
    company_id = $1 AND tenant_sequence = $4
    AND EXISTS (
        SELECT 1
        FROM outbox_company_leases l
        WHERE
            l.company_id = o.company_id AND l.lease_token = $2
            AND l.lease_epoch = $3 AND l.lease_until > clock_timestamp()
    )
"#,
        )
        .bind(self.company)
        .bind(self.token)
        .bind(self.epoch)
        .bind(sequence)
        .execute(conn)
        .await?;
        Ok(())
    }

    async fn release(self, app: &App) -> anyhow::Result<()> {
        sqlx::query(
            r#"
UPDATE outbox_company_leases SET lease_until = clock_timestamp()
WHERE company_id = $1 AND lease_token = $2 AND lease_epoch = $3
"#,
        )
        .bind(self.company)
        .bind(self.token)
        .bind(self.epoch)
        .execute(&app.pool)
        .await?;
        Ok(())
    }
}

async fn publish_commit(
    context: &jetstream::Context,
    subject: &str,
    company: Uuid,
    commit: Commit,
) -> anyhow::Result<()> {
    let bytes = commit
        .encoded()
        .map_err(|error| anyhow::anyhow!(error.code))?;
    let mut headers = async_nats::HeaderMap::new();
    headers.insert(
        "Nats-Msg-Id",
        format!("{company}:{}", commit.tenant_sequence),
    );
    headers.insert(DIGEST, commit.payload_sha256);
    tokio::time::timeout(Duration::from_secs(3), async {
        context
            .publish_with_headers(subject.to_owned(), headers, bytes.into())
            .await?
            .await?;
        anyhow::Ok(())
    })
    .await??;
    Ok(())
}

async fn deliver_next(
    app: &App,
    lease: &PublisherLease,
    context: &jetstream::Context,
    subject: &str,
) -> anyhow::Result<bool> {
    let mut tx = app.pool.begin().await?;
    // Hold the lease row through ACK so a successor cannot publish ahead of this attempt.
    if !lease.renew(&mut tx).await? {
        return Ok(false);
    }
    let commit: Option<Commit> = sqlx::query_as(
        r#"
SELECT
    tenant_sequence,
    payload,
    payload_sha256
FROM tenant_outbox
WHERE company_id = $1 AND broker_dispatched_at IS NULL
ORDER BY tenant_sequence LIMIT 1
"#,
    )
    .bind(lease.company)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(commit) = commit else {
        tx.commit().await?;
        return Ok(false);
    };

    let sequence = commit.tenant_sequence;
    publish_commit(context, subject, lease.company, commit).await?;
    lease.mark_dispatched(&mut tx, sequence).await?;
    tx.commit().await?;
    Ok(true)
}

async fn deliver(app: &App, company: Uuid) -> anyhow::Result<()> {
    let Some(lease) = PublisherLease::acquire(app, company).await? else {
        return Ok(());
    };
    let context = jetstream::new(app.nats.clone());
    let subject = format!("events.company.{company}");
    for _ in 0..64 {
        if !deliver_next(app, &lease, &context, &subject).await? {
            break;
        }
    }
    lease.release(app).await
}

pub(crate) async fn relay(app: App) {
    loop {
        match sqlx::query_scalar::<_, Uuid>(
            r#"
SELECT company_id FROM tenant_outbox
WHERE broker_dispatched_at IS NULL
GROUP BY company_id
ORDER BY min(created_at) LIMIT 100
"#,
        )
        .fetch_all(&app.pool)
        .await
        {
            Ok(companies) => {
                futures_util::stream::iter(companies)
                    .for_each_concurrent(8, |company| {
                        let app = &app;
                        async move {
                            if let Err(e) = deliver(app, company).await {
                                tracing::warn!(%company,kind=%e,"tenant outbox delivery paused");
                            }
                        }
                    })
                    .await;
            }
            Err(_) => tracing::warn!("tenant outbox database unavailable"),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
