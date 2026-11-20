use crate::application::access::AccessLock;
use crate::infrastructure::event_repository::Projection;
use crate::infrastructure::postgres::BarrierMode;
use crate::infrastructure::queries::export_worker as queries;
use crate::{
    application::{
        access::Identity,
        events::{self, Filters},
        exports::check,
    },
    domain::{
        csv::CsvLayout,
        exports::{GenerationError, Preset},
    },
    infrastructure::export_file::{ExportFile, FileData},
    infrastructure::storage::storage_path,
    infrastructure::support::{Barrier, Error, Result},
    state::App,
};
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use serde_json::{Value, json};
use sqlx::PgConnection;
use std::{path::Path as FsPath, time::Duration};
use uuid::Uuid;
#[derive(sqlx::FromRow)]
struct Job {
    company_id: Uuid,
    export_id: String,
    user_id: Uuid,
    session_id: Option<Uuid>,
    snapshot_sequence: i64,
    format_preset: String,
    filters_json: sqlx::types::Json<Filters>,
    include_media: bool,
    lease_token: Uuid,
    created_at: DateTime<Utc>,
    processing_started_at: DateTime<Utc>,
    attempts: i32,
}

async fn claim(app: &App) -> Result<Option<Job>> {
    Ok(sqlx::query_as(queries::CLAIM_WITH_EXPORT_JOBS)
        .bind(Uuid::new_v4())
        .fetch_optional(&app.pool)
        .await?)
}

async fn job_check(conn: &mut PgConnection, job: &Job) -> Result<()> {
    let who = Identity {
        sub: job.user_id,
        sid: job
            .session_id
            .ok_or_else(|| Error::new(401, "SESSION_REVOKED"))?,
        exp: usize::MAX,
    };
    check(conn, &who, job.company_id, AccessLock::Shared).await
}

fn is_denial(e: &Error) -> bool {
    matches!(e.status.as_u16(), 401 | 403)
}

async fn renew(app: &App, job: &Job, progress: i64) -> Result<bool> {
    let mut tx = app.pool.begin().await?;
    job_check(&mut tx, job).await?;
    let rows = sqlx::query(queries::RENEW_UPDATE_EXPORT_JOBS)
        .bind(job.company_id)
        .bind(&job.export_id)
        .bind(job.lease_token)
        .bind(progress)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(rows == 1)
}

async fn fail(conn: &mut PgConnection, job: &Job, reason: &str) -> Result<()> {
    let changed = sqlx::query(queries::FAIL_UPDATE_EXPORT_JOBS)
        .bind(job.company_id)
        .bind(&job.export_id)
        .bind(job.lease_token)
        .bind(reason)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    if changed == 1 && reason == "EXPORT_ACCESS_REVOKED" {
        events::audit(
            conn,
            job.user_id,
            job.company_id,
            "EXPORT_ACCESS_REVOKED",
            json!({"export_id":job.export_id}),
        )
        .await?;
    }
    Ok(())
}

async fn finalize(
    app: &App,
    job: &Job,
    key: &str,
    outcome: std::result::Result<&FileData, &str>,
) -> Result<bool> {
    let mut guard = Barrier::acquire(&app.pool, job.company_id, BarrierMode::Exclusive).await?;
    let mut tx = guard.begin().await?;
    let published = match outcome {
        Err(reason) => {
            fail(&mut tx, job, reason).await?;
            false
        }
        Ok(data) => match job_check(&mut tx, job).await {
            Err(e) if is_denial(&e) => {
                fail(&mut tx, job, "EXPORT_ACCESS_REVOKED").await?;
                false
            }
            Err(e) => return Err(e),
            Ok(()) => {
                sqlx::query(queries::FINALIZE_UPDATE_EXPORT_JOBS)
                    .bind(job.company_id)
                    .bind(&job.export_id)
                    .bind(job.lease_token)
                    .bind(key)
                    .bind(&data.digest)
                    .bind(data.size)
                    .bind(data.records)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1
            }
        },
    };
    // This also recognizes a previously committed CAS if its ACK was lost.
    let already_published: bool = if published {
        true
    } else {
        sqlx::query_scalar(queries::FINALIZE_SELECT_EXPORT_JOBS)
            .bind(job.company_id)
            .bind(&job.export_id)
            .bind(key)
            .fetch_one(&mut *tx)
            .await?
    };
    tx.commit().await?;
    guard.unlock().await?;
    Ok(already_published)
}

async fn finalize_retry(
    app: &App,
    job: &Job,
    key: &str,
    outcome: std::result::Result<&FileData, &str>,
) -> Result<bool> {
    for delay in [0, 1, 2, 4, 8] {
        if delay != 0 {
            let jitter = u64::from(Uuid::new_v4().as_bytes()[0]);
            tokio::time::sleep(Duration::from_millis(delay * 1000 + jitter)).await;
        }
        match finalize(app, job, key, outcome).await {
            Ok(value) => return Ok(value),
            Err(e) if e.status.is_server_error() || e.status.as_u16() == 423 => {
                tracing::warn!(company=%job.company_id,code=e.code,"export finalization retry");
            }
            Err(e) => return Err(e),
        }
    }
    Err(Error::unavailable())
}

async fn load_csv_layout(
    conn: &mut PgConnection,
    job: &Job,
) -> std::result::Result<CsvLayout, GenerationError> {
    let zone: String = sqlx::query_scalar(queries::GENERATE_SELECT_COMPANIES)
        .bind(job.company_id)
        .fetch_one(&mut *conn)
        .await
        .map_err(|_| GenerationError::Worker)?;
    Ok(CsvLayout {
        preset: if job.format_preset == "EXCEL_SEMICOLON_BOM" {
            Preset::ExcelSemicolonBom
        } else {
            Preset::StandardCommaUtf8
        },
        timezone: zone.parse().map_err(|_| GenerationError::Worker)?,
        include_media: job.include_media,
    })
}

async fn generate(
    app: &App,
    job: &Job,
    path: &FsPath,
    progress: &std::sync::atomic::AtomicI64,
) -> std::result::Result<FileData, GenerationError> {
    let mut conn = app
        .pool
        .acquire()
        .await
        .map_err(|_| GenerationError::Worker)?;
    let layout = load_csv_layout(&mut conn, job).await?;
    let mut output = ExportFile::create(path, &layout).await?;
    let sql = format!(
        "{} LIMIT 250001",
        events::matched_sql(events::EVENT_FIELDS, Projection::DisplayNames)
    );
    let query = events::filter_binds(
        sqlx::query_as::<_, (Value,)>(&sql),
        job.company_id,
        job.snapshot_sequence,
        &job.filters_json,
    );
    let mut rows = query.fetch(&mut *conn);

    while let Some((mut event,)) = rows.try_next().await.map_err(|_| GenerationError::Worker)? {
        if output.records() >= crate::domain::limits::MAX_EXPORT_ROWS {
            return Err(GenerationError::Limit);
        }
        if job.include_media {
            events::sanitize_media(app, &mut event);
        }
        let cells = layout.record(&event).map_err(|_| GenerationError::Worker)?;
        output.write_record(&cells)?;
        progress.store(output.records(), std::sync::atomic::Ordering::Relaxed);
        if output.records() % 256 == 0 {
            output.flush().await?;
        }
    }

    output.finish().await
}

async fn renew_retry(app: &App, job: &Job, progress: i64) -> Result<bool> {
    for delay in [0, 1, 2, 4, 8, 16] {
        if delay != 0 {
            let jitter = u64::from(Uuid::new_v4().as_bytes()[0]);
            tokio::time::sleep(Duration::from_millis(delay * 1000 + jitter)).await;
        }
        match tokio::time::timeout(Duration::from_secs(5), renew(app, job, progress)).await {
            Ok(Ok(v)) => return Ok(v),
            Ok(Err(e)) if is_denial(&e) => return Err(e),
            _ => tracing::warn!(company=%job.company_id,"export authorization check unavailable"),
        }
    }
    Err(Error::unavailable())
}

fn record_claim(job: &Job) {
    let now = Utc::now();
    crate::infrastructure::metrics::set(
        "export_queue_age_seconds",
        Some(job.company_id),
        (job.processing_started_at - job.created_at).num_milliseconds() as f64 / 1000.0,
    );
    crate::infrastructure::metrics::set(
        "export_processing_age_seconds",
        Some(job.company_id),
        (now - job.processing_started_at).num_milliseconds() as f64 / 1000.0,
    );
    crate::infrastructure::metrics::observe("export_attempts_total", 1.0);
    tracing::debug!(company=%job.company_id,attempt=job.attempts,
        queue_age_ms=(job.processing_started_at-job.created_at).num_milliseconds(),
        processing_age_ms=(now-job.processing_started_at).num_milliseconds(),"export claim");
}

async fn generate_with_renewal(
    app: &App,
    job: &Job,
    path: &FsPath,
    invalidation: &mut tokio::sync::broadcast::Receiver<()>,
) -> Option<std::result::Result<FileData, &'static str>> {
    let progress = std::sync::atomic::AtomicI64::new(0);
    let generate = generate(app, job, path, &progress);
    tokio::pin!(generate);
    let renew = tokio::time::sleep(Duration::from_secs(60));
    tokio::pin!(renew);
    loop {
        tokio::select! {
            biased;
            _ = &mut renew => {}
            _ = invalidation.recv() => {}
            result = &mut generate => break Some(result.map_err(GenerationError::code)),
        }
        match renew_retry(
            app,
            job,
            progress.load(std::sync::atomic::Ordering::Relaxed),
        )
        .await
        {
            Ok(true) => renew
                .as_mut()
                .reset(tokio::time::Instant::now() + Duration::from_secs(60)),
            Ok(false) => break None,
            Err(e) if is_denial(&e) => break Some(Err("EXPORT_ACCESS_REVOKED")),
            Err(_) => break None,
        }
    }
}

async fn process(app: &App, job: Job) -> Result<()> {
    record_claim(&job);

    let key = format!(
        "exports/{}/{}/{}.csv",
        job.company_id, job.export_id, job.lease_token
    );
    let path =
        storage_path(app, job.company_id, &job.export_id, &key).ok_or_else(Error::internal)?;
    let mut invalidation = app.access_changes.subscribe();
    match renew_retry(app, &job, 0).await {
        Ok(true) => {}
        Ok(false) => return Ok(()),
        Err(e) if is_denial(&e) => {
            finalize_retry(app, &job, &key, Err("EXPORT_ACCESS_REVOKED")).await?;
            return Ok(());
        }
        Err(e) => return Err(e),
    }
    let outcome = generate_with_renewal(app, &job, &path, &mut invalidation).await;
    if let Some(outcome) = outcome {
        // Leave ambiguous commit results for the orphan cleaner to check against PostgreSQL.
        if finalize_retry(app, &job, &key, outcome.as_ref().map_err(|reason| *reason)).await? {
            return Ok(());
        }
    }
    if let Err(e) = tokio::fs::remove_file(&path).await
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!("export staging cleanup failed");
    }
    Ok(())
}

pub(crate) async fn worker(app: App) {
    loop {
        match claim(&app).await {
            Ok(Some(j)) => {
                if let Err(e) = process(&app, j).await {
                    tracing::warn!(code = e.code, "export worker paused");
                }
            }
            Ok(None) => tokio::time::sleep(Duration::from_millis(500)).await,
            Err(e) => {
                tracing::warn!(code = e.code, "export claim failed");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
