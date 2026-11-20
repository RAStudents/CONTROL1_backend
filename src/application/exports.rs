use crate::application::ReplayStatus;
use crate::application::access::AccessLock;
use crate::application::{
    access::{
        self as auth, Identity,
        TenantPermission::{EventsRead, ReportsExport},
    },
    events::{self, EXPORTS, Filters},
};
use crate::domain::exports::Preset;
use crate::infrastructure::postgres::BarrierMode;
use crate::infrastructure::queries::exports as queries;
use crate::infrastructure::{storage::storage_path, support::Barrier};
use crate::{
    application::{Outcome, RequestContext},
    domain::error::{Error, Result},
    infrastructure::support as api,
    state::App,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgConnection;
use uuid::Uuid;
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExportRequest {
    filters: Filters,
    #[serde(default)]
    include_media_links: bool,
    format_preset: Preset,
}

pub(crate) async fn check(
    conn: &mut PgConnection,
    who: &Identity,
    company: Uuid,
    lock: AccessLock,
) -> Result<()> {
    let access = auth::access(conn, who, company, lock).await?;
    access.require(&[EventsRead, ReportsExport])?;
    access.active_company()
}

pub(crate) async fn create(
    app: App,
    id: String,
    context: RequestContext,
    request: Result<ExportRequest>,
) -> Result<Outcome> {
    let company = events::company(&id)?;
    let who = context.identity;
    let mut conn = app.pool.acquire().await?;
    check(&mut conn, &who, company, AccessLock::Unlocked).await?;
    drop(conn);
    let mut body = request?;
    body.filters = body.filters.normalize()?;
    let key = context.key()?;
    let hash = api::sha(
        [
            format!("POST:/api/v1/companies/{company}/events/exports:").into_bytes(),
            api::canonical(&body)?,
        ]
        .concat(),
    );
    api::retry(|| Box::pin(create_attempt(&app, company, &who, &body, (&key, &hash)))).await
}

async fn reserve_quota(conn: &mut PgConnection, company: Uuid, user: Uuid) -> Result<()> {
    // Every creator locks user quota before company quota, including across tenants.
    // A write conflict retries the entire RR transaction with a fresh snapshot.
    sqlx::query(queries::CREATE_INSERT_EXPORT_USER_QUOTA)
        .bind(user)
        .execute(&mut *conn)
        .await?;
    sqlx::query(queries::CREATE_INSERT_EXPORT_QUOTA)
        .bind(company)
        .execute(&mut *conn)
        .await?;
    let (company_jobs, user_jobs): (i64, i64) = sqlx::query_as(queries::CREATE_SELECT_EXPORT_JOBS)
        .bind(company)
        .bind(user)
        .fetch_one(conn)
        .await?;
    if company_jobs >= 10 || user_jobs >= 3 {
        return Err(Error::new(429, "RATE_LIMITED"));
    }
    Ok(())
}

async fn register_export(
    conn: &mut PgConnection,
    company: Uuid,
    who: &Identity,
    sequence: i64,
    body: &ExportRequest,
) -> Result<Value> {
    let sql = events::export_count_sql();
    let (count,): (i64,) =
        events::filter_binds(sqlx::query_as(&sql), company, sequence, &body.filters)
            .fetch_one(&mut *conn)
            .await?;
    if count > crate::domain::limits::MAX_EXPORT_ROWS {
        return Err(Error::new(422, "EXPORT_LIMIT_EXCEEDED"));
    }

    let id = Uuid::new_v4().to_string();
    let created: DateTime<Utc> = sqlx::query_scalar(queries::CREATE_WITH_EXPORT_JOBS)
        .bind(&id)
        .bind(company)
        .bind(who.sub)
        .bind(who.sid)
        .bind(sequence)
        .bind(json!(&body.filters))
        .bind(body.format_preset.as_str())
        .bind(body.include_media_links)
        .fetch_one(&mut *conn)
        .await?;
    events::audit(
        conn,
        who.sub,
        company,
        "EVENTS_EXPORT_REQUESTED",
        json!({"export_id": id, "include_media_links": body.include_media_links}),
    )
    .await?;

    let mut dto = json!({
        "export_id": id, "status": "QUEUED", "created_at": created, "snapshot_sequence": sequence,
        "status_url": format!("/api/v1/companies/{company}/events/exports/{id}"),
    });
    if body.include_media_links {
        dto["warnings"] = json!([{"code": "PUBLIC_MEDIA_LINKS", "message":
            "Public media URLs can be shared and remain accessible after CONTROL access is revoked."}]);
    }
    Ok(dto)
}

async fn create_attempt(
    app: &App,
    company: Uuid,
    who: &Identity,
    body: &ExportRequest,
    idempotency: (&str, &str),
) -> Result<Outcome> {
    let (key, hash) = idempotency;
    let mut barrier = Barrier::acquire(&app.pool, company, BarrierMode::Shared).await?;
    let (mut tx, sequence) = barrier.begin_snapshot(company).await?;
    check(&mut tx, who, company, AccessLock::Shared).await?;
    events::lock_key(&mut tx, who.sub, company, EXPORTS, key).await?;
    if let Some((code, dto)) =
        events::saved(&mut tx, (who.sub, company, EXPORTS, key), hash).await?
    {
        tx.commit().await?;
        barrier.unlock().await?;
        return Ok(events::response(code, dto, ReplayStatus::Replayed));
    }

    reserve_quota(&mut tx, company, who.sub).await?;
    let dto = register_export(&mut tx, company, who, sequence, body).await?;
    events::save(&mut tx, (who.sub, company, EXPORTS, key), hash, 202, &dto).await?;
    tx.commit().await?;
    barrier.unlock().await?;
    Ok(events::response(202, dto, ReplayStatus::Original))
}

async fn expire_for_owner(app: &App, company: Uuid, who: &Identity, id: &str) -> Result<()> {
    let mut conn = app.pool.acquire().await?;
    check(&mut conn, who, company, AccessLock::Unlocked).await?;
    let expired: bool = sqlx::query_scalar(queries::EXPIRE_FOR_OWNER_SELECT_EXPORT_JOBS)
        .bind(company)
        .bind(who.sub)
        .bind(id)
        .fetch_optional(&mut *conn)
        .await?
        .ok_or_else(Error::missing)?;
    drop(conn);
    if expired {
        let mut guard = Barrier::acquire(&app.pool, company, BarrierMode::Exclusive).await?;
        let mut tx = guard.begin().await?;
        check(&mut tx, who, company, AccessLock::Shared).await?;
        sqlx::query(queries::EXPIRE_FOR_OWNER_UPDATE_EXPORT_JOBS)
            .bind(company)
            .bind(who.sub)
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        guard.unlock().await?;
    }
    Ok(())
}

pub(crate) async fn status(
    app: App,
    (company, id): (String, String),
    context: RequestContext,
) -> Result<Outcome> {
    let company = events::company(&company)?;
    let who = context.identity;
    loop {
        expire_for_owner(&app, company, &who, &id).await?;
        let mut tx = app.pool.begin().await?;
        check(&mut tx, &who, company, AccessLock::Shared).await?;
        let (mut dto, status, expired): (Value, String, bool) =
            sqlx::query_as(queries::STATUS_SELECT_EXPORT_JOBS)
                .bind(company)
                .bind(who.sub)
                .bind(&id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(Error::missing)?;
        tx.commit().await?;
        if status == "COMPLETED" && expired {
            continue;
        }
        dto["download_url"] = if status == "COMPLETED" {
            json!(format!(
                "/api/v1/companies/{company}/events/exports/{id}/download"
            ))
        } else {
            Value::Null
        };
        return Ok(Outcome::Json(dto));
    }
}

pub(crate) async fn download(
    app: App,
    (company, id): (String, String),
    context: RequestContext,
) -> Result<Outcome> {
    let company = events::company(&company)?;
    let who = context.identity;
    expire_for_owner(&app, company, &who, &id).await?;
    let mut guard = Barrier::acquire(&app.pool, company, BarrierMode::Shared).await?;
    let mut tx = guard.begin().await?;
    check(&mut tx, &who, company, AccessLock::Shared).await?;
    let (status, expired, path, size): (String, bool, Option<String>, Option<i64>) =
        sqlx::query_as(queries::DOWNLOAD_SELECT_EXPORT_JOBS)
            .bind(company)
            .bind(who.sub)
            .bind(&id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(Error::missing)?;
    if expired || status == "EXPIRED" {
        return Err(Error::new(410, "EXPORT_EXPIRED"));
    }
    let path = path
        .filter(|_| status == "COMPLETED")
        .ok_or_else(|| Error::new(409, "EXPORT_NOT_READY"))?;
    // Open the immutable file before releasing the barrier. An open handle survives unlink.
    let path = storage_path(&app, company, &id, &path).ok_or_else(Error::internal)?;
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|_| Error::new(503, "EXPORT_STORAGE_FAILED"))?;
    tx.commit().await?;
    guard.unlock().await?;
    Ok(Outcome::Download {
        file,
        size: size.ok_or_else(Error::internal)?,
    })
}
