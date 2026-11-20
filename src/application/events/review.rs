use super::{company, response, sanitize_media};
use crate::application::access::AccessLock;
use crate::application::{
    ReplayStatus,
    access::{
        self as auth,
        TenantPermission::{EventsRead, EventsReview},
    },
};
use crate::domain::{events::ERRONEOUS, limits::MAX_SEQ};
use crate::infrastructure::event_repository::{
    allocate, audit, current, lock_key, outbox, save, saved, summary_value,
};
use crate::{
    application::{Outcome, RequestContext},
    domain::error::{Error, Result},
    infrastructure::support as api,
    state::App,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::infrastructure::queries::events_review as queries;
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Mark {
    expected_classification_version: i64,
}

pub(crate) async fn acknowledge(
    app: App,
    (id, event): (String, String),
    context: RequestContext,
) -> Result<Outcome> {
    mutate(app, id, event, context, None).await
}

pub(crate) async fn erroneous(
    app: App,
    (id, event): (String, String),
    context: RequestContext,
    data: Result<Mark>,
) -> Result<Outcome> {
    mutate(app, id, event, context, Some(data)).await
}

async fn mutate(
    app: App,
    company_id: String,
    event: String,
    context: RequestContext,
    mark: Option<Result<Mark>>,
) -> Result<Outcome> {
    let who = context.identity;
    let company = company(&company_id)?;
    let mut tx = app.pool.begin().await?;
    let access = auth::access(&mut tx, &who, company, AccessLock::Shared)
        .await
        .and_then(|a| a.require(&[EventsRead, EventsReview]));
    if let Err(error) = access {
        tx.rollback().await?;
        if error.status == crate::domain::error::Status(403) && mark.is_some() {
            let mut conn = app.pool.acquire().await?;
            audit(
                &mut conn,
                who.sub,
                company,
                "EVENT_REVIEW_DENIED",
                json!({"reason":error.code}),
            )
            .await?;
        }
        return Err(error);
    }
    let mark = mark.transpose()?;
    let request = if let Some(mark) = &mark {
        if !(1..=MAX_SEQ).contains(&mark.expected_classification_version) {
            return Err(Error::invalid());
        }
        let key = context.key()?;
        let hash = api::sha(
            [
                format!("POST:/api/v1/companies/{company}/events/{event}/erroneous:").into_bytes(),
                api::canonical(mark)?,
            ]
            .concat(),
        );
        lock_key(&mut tx, who.sub, company, ERRONEOUS, &key).await?;
        if let Some((code, mut dto)) =
            saved(&mut tx, (who.sub, company, ERRONEOUS, &key), &hash).await?
        {
            sanitize_media(&app, &mut dto);
            tx.commit().await?;
            return Ok(response(code, dto, ReplayStatus::Replayed));
        }
        Some((key, hash))
    } else {
        None
    };
    sqlx::query_scalar::<_, String>(queries::MUTATE_SELECT_EVENTS)
        .bind(company)
        .bind(&event)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(Error::missing)?;
    sqlx::query_scalar::<_, i64>(queries::MUTATE_SELECT_EVENT_VERSIONS)
        .bind(company)
        .bind(&event)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(Error::internal)?;
    let mut dto = current(&mut tx, company, &event).await?;
    sanitize_media(&app, &mut dto);
    let is_mark = mark.is_some();
    use crate::domain::review::{Acknowledgement, ReviewAction, ReviewDecision, ReviewState};
    let state = ReviewState {
        acknowledgement: dto["status"]
            .as_str()
            .and_then(Acknowledgement::parse)
            .ok_or_else(Error::internal)?,
        erroneous: dto["is_erroneous"].as_bool().ok_or_else(Error::internal)?,
        classification_version: dto["classification_version"]
            .as_i64()
            .ok_or_else(Error::internal)?,
    };
    let action = if let Some(mark) = mark {
        ReviewAction::MarkErroneous {
            expected_classification_version: mark.expected_classification_version,
        }
    } else {
        ReviewAction::Acknowledge
    };
    let no_op = match state.plan(action) {
        ReviewDecision::NoOp => true,
        ReviewDecision::VersionConflict => {
            return Err(Error::new(409, "STATE_CONFLICT").details(json!({"current_event":dto})));
        }
        ReviewDecision::Acknowledge | ReviewDecision::MarkErroneous => false,
    };
    if !no_op {
        let seq = allocate(&mut tx, company, 1, 1).await?;
        sqlx::query(queries::MUTATE_WITH_EVENT_VERSIONS)
            .bind(company)
            .bind(&event)
            .bind(seq)
            .bind(is_mark)
            .bind(who.sub)
            .execute(&mut *tx)
            .await?;
        dto = current(&mut tx, company, &event).await?;
        sanitize_media(&app, &mut dto);
        let summary = if is_mark {
            audit(
                &mut tx,
                who.sub,
                company,
                "EVENT_MARKED_ERRONEOUS",
                json!({"event_id":event,"sequence":seq}),
            )
            .await?;
            let summary = summary_value(&mut tx, company, seq).await?;
            let previous = summary_value(&mut tx, company, seq - 1).await?;
            (summary["today_events_count"] != previous["today_events_count"]).then_some(summary)
        } else {
            None
        };
        outbox(
            &mut tx,
            company,
            seq,
            crate::infrastructure::event_repository::CommitChanges {
                updated: Some(&dto),
                summary,
                ..Default::default()
            },
        )
        .await?;
    }
    if let Some((key, hash)) = request {
        save(
            &mut tx,
            (who.sub, company, ERRONEOUS, &key),
            &hash,
            200,
            &dto,
        )
        .await?;
    }
    tx.commit().await?;
    Ok(response(200, dto, ReplayStatus::Original))
}
