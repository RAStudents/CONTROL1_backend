use crate::application::access::AccessLock;
use crate::{
    application::{
        Outcome,
        access::{self as auth, TenantPermission::EventsRead},
    },
    domain::access::Identity,
    infrastructure::{
        stream,
        support::{self as api, Error, Result},
    },
    state::App,
};
use futures_util::future::BoxFuture;
use uuid::Uuid;
const CHUNK: i64 = 64;
pub(crate) type Check = BoxFuture<'static, Result<Option<chrono::DateTime<chrono::Utc>>>>;
pub(crate) type Query = BoxFuture<'static, DbAnswer>;

pub(crate) fn audit_denial(app: &App, company: Uuid, user: Uuid, reason: &'static str) {
    let app = app.clone();
    tokio::spawn(async move {
        if let Ok(mut conn) = app.pool.acquire().await {
            let _ = crate::infrastructure::event_repository::audit(
                &mut conn,
                user,
                company,
                "STREAM_ACCESS_DENIED",
                serde_json::json!({"reason": reason}),
            )
            .await;
        }
    });
}
pub(crate) enum DbAnswer {
    Bounds(i64, Result<(i64, Option<i64>)>),
    Rows(i64, i64, i64, Result<Vec<stream::Commit>>),
}
pub(crate) fn bounds(app: &App, company: Uuid, id: i64) -> Query {
    let db = api::cancellable_query(app.pool.clone(), move |conn| {
        Box::pin(async move {
            crate::infrastructure::event_repository::stream_bounds(conn, company).await
        })
    });
    Box::pin(async move { DbAnswer::Bounds(id, db.await) })
}
pub(crate) fn rows(
    app: &App,
    company: Uuid,
    id: i64,
    range: std::ops::RangeInclusive<i64>,
    live: Option<stream::Subscription>,
) -> Query {
    let (start, end) = range.into_inner();
    let pool = app.pool.clone();
    Box::pin(async move {
        let cached = if let Some(ref live) = live {
            live.cached(start, end).await
        } else {
            None
        };
        let result = if let Some(rows) = cached {
            Ok(rows)
        } else {
            api::cancellable_query(pool, move |conn| {
                Box::pin(stream::chunk_on(conn, company, start, end, CHUNK))
            })
            .await
        };
        DbAnswer::Rows(id, start, end, result)
    })
}
pub(crate) fn check(app: &App, company: Uuid, who: &Identity) -> Check {
    let who = *who;
    api::cancellable_query(app.pool.clone(), move |conn| {
        Box::pin(async move {
            let mut tx = sqlx::Acquire::begin(conn).await?;
            let a = auth::access(&mut tx, &who, company, AccessLock::Shared).await?;
            a.require(&[EventsRead])?;
            let grant = a.grant_deadline();
            tx.commit().await?;
            Ok(grant)
        })
    })
}
pub(crate) async fn health(app: &App) -> Result<Outcome> {
    crate::infrastructure::postgres::ping(&app.pool).await?;
    if app.nats.connection_state() != async_nats::connection::State::Connected {
        return Err(Error::unavailable());
    }
    Ok(Outcome::Json(serde_json::json!({"status":"OK"})))
}
