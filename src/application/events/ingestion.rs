use super::media::MediaKind;
use super::{company, response, valid_media};
use crate::application::access::AccessLock;
use crate::application::{
    ReplayStatus,
    access::{self as auth, TenantPermission::DevicesCreate},
};
use crate::domain::events::INGEST;
use crate::infrastructure::event_repository::{
    allocate, current, lock_key, outbox, save, saved, summary_value,
};
use crate::{
    application::{Outcome, RequestContext},
    domain::error::{Error, Result},
    infrastructure::support as api,
    state::App,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::infrastructure::queries::events_ingestion as queries;
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct NewEvent {
    id: String,
    event_type_id: String,
    category_code: String,
    location_id: String,
    device_id: String,
    occurred_at: DateTime<Utc>,
    preview_url: String,
    video_url: Option<String>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Ingest {
    events: Vec<NewEvent>,
}

pub(crate) async fn ingest(
    app: App,
    id: String,
    context: RequestContext,
    data: Result<Ingest>,
) -> Result<Outcome> {
    let company = company(&id)?;
    let who = context.identity;
    let mut tx = app.pool.begin().await?;
    auth::access(&mut tx, &who, company, AccessLock::Shared)
        .await?
        .require(&[DevicesCreate])?;
    let batch = data?;
    let size = batch.events.len() as i64;
    if !(1..=app.max_batch).contains(&size)
        || batch.events.iter().any(|e| {
            !(1..=64).contains(&e.id.chars().count())
                || e.id.contains('\0')
                || !valid_media(&app, &e.preview_url, MediaKind::Preview)
                || e.video_url
                    .as_ref()
                    .is_some_and(|url| !valid_media(&app, url, MediaKind::Video))
                || [
                    &e.event_type_id,
                    &e.category_code,
                    &e.location_id,
                    &e.device_id,
                ]
                .iter()
                .any(|s| s.is_empty() || s.contains('\0'))
        })
    {
        return Err(Error::invalid());
    }
    let key = context.key()?;
    let hash = api::sha(
        [
            format!("POST:/api/v1/companies/{company}/events/ingest:").into_bytes(),
            api::canonical(&batch)?,
        ]
        .concat(),
    );
    lock_key(&mut tx, who.sub, company, INGEST, &key).await?;
    if let Some((code, dto)) = saved(&mut tx, (who.sub, company, INGEST, &key), &hash).await? {
        tx.commit().await?;
        return Ok(response(code, dto, ReplayStatus::Replayed));
    }
    let last = allocate(&mut tx, company, size, app.max_batch).await?;
    let first = last
        .checked_sub(size - 1)
        .filter(|&s| s > 0)
        .ok_or_else(Error::internal)?;
    for (seq, e) in (first..=last).zip(batch.events) {
        sqlx::query(queries::INGEST_INSERT_EVENTS)
            .bind(company)
            .bind(&e.id)
            .bind(seq)
            .bind(e.event_type_id)
            .bind(e.category_code)
            .bind(e.location_id)
            .bind(e.device_id)
            .bind(e.occurred_at)
            .bind(e.preview_url)
            .bind(e.video_url)
            .execute(&mut *tx)
            .await?;
        sqlx::query(queries::INGEST_INSERT_EVENT_VERSIONS)
            .bind(company)
            .bind(&e.id)
            .bind(seq)
            .execute(&mut *tx)
            .await?;
        let dto = current(&mut tx, company, &e.id).await?;
        let summary = summary_value(&mut tx, company, seq).await?;
        let previous = summary_value(&mut tx, company, seq - 1).await?;
        let summary =
            (summary["today_events_count"] != previous["today_events_count"]).then_some(summary);
        outbox(
            &mut tx,
            company,
            seq,
            crate::infrastructure::event_repository::CommitChanges {
                created: Some(&dto),
                summary,
                ..Default::default()
            },
        )
        .await?;
    }
    let dto = json!({"first_sequence":first,"last_sequence":last,"count":size});
    save(&mut tx, (who.sub, company, INGEST, &key), &hash, 201, &dto).await?;
    tx.commit().await?;
    Ok(response(201, dto, ReplayStatus::Original))
}
