use crate::infrastructure::postgres::BarrierMode;
use crate::infrastructure::queries::retention as queries;
use crate::{
    infrastructure::support::{Barrier, Result},
    state::App,
};
use std::{path::Path, time::Duration};
use tokio::fs::DirEntry;
use uuid::Uuid;

async fn gc(app: &App, company: Uuid) -> Result<()> {
    let mut guard = Barrier::acquire(&app.pool, company, BarrierMode::Exclusive).await?;
    let mut tx = guard.begin().await?;
    let expired = sqlx::query(queries::GC_WITH_EVENT_SNAPSHOTS)
        .bind(company)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    let terminal = sqlx::query(queries::GC_WITH_EXPORT_JOBS)
        .bind(company)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    // The first commit at or after the time boundary also bounds out-of-order timestamps.
    let (watermark, anchors, oldest): (i64, i64, Option<i64>) =
        sqlx::query_as(queries::GC_WITH_EVENT_SNAPSHOTS_2)
            .bind(company)
            .fetch_one(&mut *tx)
            .await?;
    let versions = sqlx::query(queries::GC_WITH_EVENT_VERSIONS)
        .bind(company)
        .bind(watermark)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    let (queue_age, processing_age, deadline_failures, attempts): (f64, f64, i64, i64) =
        sqlx::query_as(queries::GC_EXPORT_METRICS)
            .bind(company)
            .fetch_one(&mut *tx)
            .await?;
    tx.commit().await?;
    guard.unlock().await?;
    crate::infrastructure::metrics::set("export_queue_age_seconds", Some(company), queue_age);
    crate::infrastructure::metrics::set(
        "export_processing_age_seconds",
        Some(company),
        processing_age,
    );
    crate::infrastructure::metrics::set(
        "export_deadline_failures_retained",
        Some(company),
        deadline_failures as f64,
    );
    crate::infrastructure::metrics::set(
        "export_active_attempts_max",
        Some(company),
        attempts as f64,
    );
    crate::infrastructure::metrics::set("active_anchors", Some(company), anchors as f64);
    crate::infrastructure::metrics::set("gc_watermark", Some(company), watermark as f64);
    if let Some(oldest) = oldest {
        crate::infrastructure::metrics::set(
            "oldest_retained_sequence",
            Some(company),
            oldest as f64,
        );
    }
    crate::infrastructure::metrics::observe("export_terminalized_total", terminal as f64);
    tracing::info!(
        %company, watermark, anchors, ?oldest, expired, terminal, versions,
        "tenant retention sweep"
    );
    Ok(())
}

async fn outbox(app: &App) -> Result<()> {
    let rows = sqlx::query(queries::OUTBOX_WITH_TENANT_OUTBOX)
        .execute(&app.pool)
        .await?
        .rows_affected();
    let overdue: bool = sqlx::query_scalar(queries::OUTBOX_SELECT_TENANT_OUTBOX)
        .fetch_one(&app.pool)
        .await?;
    if overdue {
        tracing::error!("undispatched tenant commits exceed the outbox retention age");
    }
    tracing::debug!(rows, "outbox retention sweep");
    Ok(())
}

async fn records(app: &App) -> Result<()> {
    sqlx::query(queries::RECORDS_WITH_IDEMPOTENCY_KEYS)
        .execute(&app.pool)
        .await?;
    sqlx::query(queries::RECORDS_DELETE_LOGIN_LIMITS)
        .execute(&app.pool)
        .await?;
    sqlx::query(queries::RECORDS_DELETE_EXPORT_JOBS)
        .execute(&app.pool)
        .await?;
    Ok(())
}

async fn expired_files(app: &App) -> anyhow::Result<()> {
    let retired: Vec<(Uuid, String, String)> =
        sqlx::query_as(queries::EXPIRED_FILES_SELECT_EXPORT_JOBS)
            .fetch_all(&app.pool)
            .await?;
    for (company, id, key) in retired {
        let Some(path) = crate::infrastructure::storage::storage_path(app, company, &id, &key)
        else {
            tracing::error!(%company,"expired export has an invalid storage key");
            continue;
        };
        match tokio::fs::remove_file(path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => {
                tracing::warn!(%company,"expired file cleanup failed");
                continue;
            }
        }
        sqlx::query(queries::EXPIRED_FILES_UPDATE_EXPORT_JOBS)
            .bind(company)
            .bind(id)
            .bind(key)
            .execute(&app.pool)
            .await?;
    }
    Ok(())
}

async fn remove_orphan_file(
    app: &App,
    company: Uuid,
    export_id: &str,
    file: &DirEntry,
) -> anyhow::Result<bool> {
    if !file.file_type().await?.is_file() {
        return Ok(false);
    }
    let filename = file.file_name().to_string_lossy().into_owned();
    let Some(token) = filename
        .strip_suffix(".csv")
        .and_then(|name| Uuid::parse_str(name).ok())
    else {
        return Ok(false);
    };
    let key = format!("exports/{company}/{export_id}/{filename}");
    let retained: bool = sqlx::query_scalar(queries::EXPORTED_OBJECTS_SELECT_EXPORT_JOBS)
        .bind(company)
        .bind(export_id)
        .bind(token)
        .bind(&key)
        .fetch_one(&app.pool)
        .await?;
    if retained {
        return Ok(false);
    }

    match tokio::fs::remove_file(file.path()).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    sqlx::query(queries::EXPORTED_OBJECTS_UPDATE_EXPORT_JOBS)
        .bind(company)
        .bind(export_id)
        .bind(&key)
        .execute(&app.pool)
        .await?;
    Ok(true)
}

async fn sweep_company_files(
    app: &App,
    company: Uuid,
    directory: &Path,
    remaining: usize,
) -> anyhow::Result<usize> {
    let mut jobs = tokio::fs::read_dir(directory).await?;
    let mut removed = 0;
    while let Some(job) = jobs.next_entry().await? {
        if !job.file_type().await?.is_dir() {
            continue;
        }
        let export_id = job.file_name().to_string_lossy().into_owned();
        let mut files = tokio::fs::read_dir(job.path()).await?;
        while let Some(file) = files.next_entry().await? {
            if remove_orphan_file(app, company, &export_id, &file).await? {
                removed += 1;
            }
            if removed >= remaining {
                return Ok(removed);
            }
        }
    }
    Ok(removed)
}

async fn exported_objects(app: &App) -> anyhow::Result<()> {
    let root = app.export_dir.join("exports");
    let mut companies = match tokio::fs::read_dir(&root).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let mut removed = 0;
    while let Some(directory) = companies.next_entry().await? {
        if !directory.file_type().await?.is_dir() {
            continue;
        }
        let name = directory.file_name().to_string_lossy().into_owned();
        let Ok(company) = Uuid::parse_str(&name) else {
            continue;
        };
        removed += sweep_company_files(app, company, &directory.path(), 500 - removed).await?;
        if removed >= 500 {
            break;
        }
    }
    tracing::debug!(removed, "export object retention sweep");
    Ok(())
}

async fn sweep_companies(app: &App) {
    let mut after = Uuid::nil();
    loop {
        let Ok(companies) =
            sqlx::query_scalar::<_, Uuid>(queries::RUN_SELECT_COMPANY_STREAM_COUNTERS)
                .bind(after)
                .fetch_all(&app.pool)
                .await
        else {
            return;
        };
        if companies.is_empty() {
            return;
        }
        for company in companies {
            after = company;
            if let Err(error) = gc(app, company).await {
                tracing::warn!(%company,code=error.code,"tenant retention failed");
            }
        }
    }
}

async fn cleanup_once(app: &App) {
    sweep_companies(app).await;
    if let Err(error) = outbox(app).await {
        tracing::warn!(code = error.code, "outbox cleanup failed");
    }
    if let Err(error) = records(app).await {
        tracing::warn!(code = error.code, "metadata cleanup failed");
    }
    if expired_files(app).await.is_err() {
        tracing::warn!("expired file cleanup failed");
    }
    if exported_objects(app).await.is_err() {
        tracing::warn!("export object cleanup failed");
    }
}

pub(crate) async fn run(app: App) {
    loop {
        cleanup_once(&app).await;
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}
