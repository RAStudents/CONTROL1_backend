use super::{company, sanitize_media};
use crate::application::access::AccessLock;
use crate::application::access::{self as auth, Identity, TenantPermission::EventsRead};
use crate::domain::{
    events::{Filters, filter_error},
    limits::MAX_SEQ,
};
use crate::infrastructure::postgres::BarrierMode;
use crate::infrastructure::{
    event_repository::{
        filter_binds, options_value, page_sql, snapshot_count_sql, snapshot_materialization_sql,
        summary_value,
    },
    support::Barrier,
};
use crate::{
    application::{Outcome, RequestContext},
    domain::error::{Error, Result},
    infrastructure::support as api,
    state::App,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::infrastructure::queries::events_snapshots as queries;
impl Filters {
    fn hash(&self, company: Uuid) -> Result<String> {
        Ok(api::sha(api::canonical(
            &json!({"company_id":company,"sort":"OCCURRED_AT_DESC","filters":self}),
        )?))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PageKind {
    List,
    Bootstrap,
}

struct EventQuery {
    filters: Filters,
    cursor: Option<String>,
    limit: i64,
    has_filters: bool,
}
fn parse_query(q: Option<&str>) -> Result<EventQuery> {
    let mut filters = Filters::default();
    let mut cursor = None;
    let mut limit = 50;
    let mut seen = std::collections::HashSet::new();
    let mut has_filters = false;
    for (key, value) in url::form_urlencoded::parse(q.unwrap_or_default().as_bytes()) {
        let key = key.as_ref();
        let value = value.into_owned();
        if !matches!(key, "event_type_ids" | "device_ids") && !seen.insert(key.to_owned()) {
            return Err(filter_error(&[key]));
        }
        match key {
            "cursor" => cursor = Some(value),
            "limit" => {
                limit = value
                    .parse()
                    .ok()
                    .filter(|n| (1..=100).contains(n))
                    .ok_or_else(|| filter_error(&[key]))?
            }
            "sort" if value == "OCCURRED_AT_DESC" => has_filters = true,
            "event_type_ids" => {
                has_filters = true;
                filters.event_type_ids.push(value);
            }
            "device_ids" => {
                has_filters = true;
                filters.device_ids.push(value);
            }
            "search_id" => {
                has_filters = true;
                filters.search_id = Some(value);
            }
            "from" | "to" => {
                has_filters = true;
                let time = Some(value.parse().map_err(|_| filter_error(&[key]))?);
                if key == "from" {
                    filters.from = time;
                } else {
                    filters.to = time;
                }
            }
            "validity" => {
                has_filters = true;
                filters.validity =
                    serde_json::from_value(json!(value)).map_err(|_| filter_error(&[key]))?;
            }
            _ => return Err(filter_error(&[key])),
        }
    }
    Ok(EventQuery {
        filters: filters.normalize()?,
        cursor,
        limit,
        has_filters,
    })
}

#[derive(Serialize, Deserialize)]
struct Cursor {
    format_version: u8,
    key_id: String,
    company_id: Uuid,
    user_id: Uuid,
    snapshot_id: String,
    filter_hash: String,
    as_of_sequence: i64,
    last_position: i32,
    issued_at: i64,
}

#[derive(sqlx::FromRow)]
struct Snapshot {
    filter_hash: String,
    as_of_sequence: i64,
    total_count: i32,
    expired: bool,
}
#[derive(Clone, Copy)]
struct SnapshotOwner {
    company: Uuid,
    user: Uuid,
}

struct PageRequest<'a> {
    owner: SnapshotOwner,
    snapshot: &'a str,
    limit: i64,
    after: i32,
}

async fn page(conn: &mut PgConnection, app: &App, request: PageRequest<'_>) -> Result<Value> {
    let PageRequest {
        owner: SnapshotOwner { company, user },
        snapshot,
        limit,
        after,
    } = request;
    let meta: Snapshot = sqlx::query_as(queries::PAGE_SELECT_EVENT_SNAPSHOTS)
        .bind(snapshot)
        .bind(company)
        .bind(user)
        .fetch_optional(&mut *conn)
        .await?
        .ok_or_else(|| Error::new(410, "SNAPSHOT_EXPIRED"))?;
    if meta.expired {
        return Err(Error::new(410, "SNAPSHOT_EXPIRED"));
    }
    let sql = page_sql();
    let rows: Vec<(i32, Value)> = sqlx::query_as(&sql)
        .bind(company)
        .bind(meta.as_of_sequence)
        .bind(snapshot)
        .bind(after)
        .bind(limit)
        .fetch_all(conn)
        .await?;
    let next_cursor = rows
        .last()
        .filter(|(pos, _)| pos + 1 < meta.total_count)
        .map(|(pos, _)| {
            api::sign(
                &app.cursor_key,
                &Cursor {
                    format_version: 1,
                    key_id: app.cursor_key_id.clone(),
                    company_id: company,
                    user_id: user,
                    snapshot_id: snapshot.into(),
                    filter_hash: meta.filter_hash.clone(),
                    as_of_sequence: meta.as_of_sequence,
                    last_position: *pos,
                    issued_at: Utc::now().timestamp(),
                },
            )
        })
        .transpose()?;
    let items = rows
        .into_iter()
        .map(|(_, mut value)| {
            sanitize_media(app, &mut value);
            value
        })
        .collect::<Vec<_>>();
    Ok(
        json!({"items":items,"pagination":{"snapshot_id":snapshot,"as_of_sequence":meta.as_of_sequence,
        "has_more":next_cursor.is_some(),"next_cursor":next_cursor,"total_count":meta.total_count}}),
    )
}

struct MaterializedSnapshot {
    id: String,
    count: i32,
}

async fn materialize_snapshot(
    conn: &mut PgConnection,
    app: &App,
    owner: SnapshotOwner,
    sequence: i64,
    filters: &Filters,
) -> Result<MaterializedSnapshot> {
    let SnapshotOwner { company, user } = owner;
    let count_sql = snapshot_count_sql(app.max_snapshot_items);
    let (count,): (i32,) = filter_binds(sqlx::query_as(&count_sql), company, sequence, filters)
        .fetch_one(&mut *conn)
        .await?;
    if i64::from(count) > app.max_snapshot_items {
        return Err(Error::new(422, "SNAPSHOT_TOO_LARGE")
            .details(json!({"max_items": app.max_snapshot_items})));
    }

    let id = format!("snap_{}", Uuid::new_v4().simple());
    let hash = filters.hash(company)?;
    sqlx::query(queries::NEW_SNAPSHOT_INSERT_EVENT_SNAPSHOTS)
        .bind(&id)
        .bind(company)
        .bind(user)
        .bind(hash)
        .bind(sequence)
        .bind(count)
        .execute(&mut *conn)
        .await?;

    let materialization_sql = snapshot_materialization_sql();
    let (written,): (i32,) = filter_binds(
        sqlx::query_as(&materialization_sql),
        company,
        sequence,
        filters,
    )
    .bind(&id)
    .fetch_one(conn)
    .await?;
    if written != count {
        return Err(Error::internal());
    }
    Ok(MaterializedSnapshot { id, count })
}

async fn snapshot_attempt(
    app: &App,
    company: Uuid,
    who: &Identity,
    query: &EventQuery,
    kind: PageKind,
) -> Result<(Value, i32)> {
    let mut barrier = Barrier::acquire(&app.pool, company, BarrierMode::Shared).await?;
    let (mut tx, sequence) = barrier.begin_snapshot(company).await?;
    auth::access(&mut tx, who, company, AccessLock::Shared)
        .await?
        .require(&[EventsRead])?;

    let snapshot = materialize_snapshot(
        &mut tx,
        app,
        SnapshotOwner {
            company,
            user: who.sub,
        },
        sequence,
        &query.filters,
    )
    .await?;
    let events = page(
        &mut tx,
        app,
        PageRequest {
            owner: SnapshotOwner {
                company,
                user: who.sub,
            },
            snapshot: &snapshot.id,
            limit: query.limit,
            after: -1,
        },
    )
    .await?;
    let value = match kind {
        PageKind::List => events,
        PageKind::Bootstrap => json!({
            "stream_cursor": sequence,
            "events": events,
            "summary": summary_value(&mut tx, company, sequence).await?,
        }),
    };
    tx.commit().await?;
    barrier.unlock().await?;
    Ok((value, snapshot.count))
}

async fn new_snapshot(
    app: &App,
    company: Uuid,
    who: &Identity,
    query: &EventQuery,
    kind: PageKind,
) -> Result<Value> {
    let started = std::time::Instant::now();
    let (value, count) =
        api::retry(|| Box::pin(snapshot_attempt(app, company, who, query, kind))).await?;
    crate::infrastructure::metrics::increment("snapshot_materializations_total");
    crate::infrastructure::metrics::observe(
        "snapshot_materialization_seconds_sum",
        started.elapsed().as_secs_f64(),
    );
    crate::infrastructure::metrics::set("snapshot_items_last", Some(company), f64::from(count));
    tracing::info!(%company, count, duration_ms = started.elapsed().as_millis(), "snapshot materialized");
    Ok(value)
}

async fn get_events(
    app: App,
    id: String,
    context: RequestContext,
    q: Option<String>,
    kind: PageKind,
) -> Result<Outcome> {
    let who = context.identity;
    let company = company(&id)?;
    let query = parse_query(q.as_deref())?;
    let mut tx = app.pool.begin().await?;
    auth::access(&mut tx, &who, company, AccessLock::Shared)
        .await?
        .require(&[EventsRead])?;
    tx.commit().await?;
    let Some(raw) = &query.cursor else {
        return Ok(Outcome::Json(
            new_snapshot(&app, company, &who, &query, kind).await?,
        ));
    };
    let invalid = || Error::new(400, "CURSOR_INVALID");
    let cur: Cursor = api::verify(&app.cursor_key, raw).ok_or_else(invalid)?;
    let now = Utc::now().timestamp();
    if kind == PageKind::Bootstrap
        || cur.format_version != 1
        || cur.key_id != app.cursor_key_id
        || cur.company_id != company
        || cur.user_id != who.sub
        || cur.last_position < 0
        || !(0..=MAX_SEQ).contains(&cur.as_of_sequence)
        || cur.issued_at > now
        || query.has_filters && query.filters.hash(company)? != cur.filter_hash
    {
        return Err(invalid());
    }
    let mut guard = Barrier::acquire(&app.pool, company, BarrierMode::Shared).await?;
    let (mut tx, _) = guard.begin_snapshot(company).await?;
    auth::access(&mut tx, &who, company, AccessLock::Unlocked)
        .await?
        .require(&[EventsRead])?;
    let meta: Option<(String, i64)> = sqlx::query_as(queries::GET_EVENTS_SELECT_EVENT_SNAPSHOTS)
        .bind(&cur.snapshot_id)
        .bind(company)
        .bind(who.sub)
        .fetch_optional(&mut *tx)
        .await?;
    if let Some((hash, seq)) = meta
        && (hash != cur.filter_hash || seq != cur.as_of_sequence)
    {
        return Err(invalid());
    }
    let value = page(
        &mut tx,
        &app,
        PageRequest {
            owner: SnapshotOwner {
                company,
                user: who.sub,
            },
            snapshot: &cur.snapshot_id,
            limit: query.limit,
            after: cur.last_position,
        },
    )
    .await?;
    tx.commit().await?;
    guard.unlock().await?;
    Ok(Outcome::Json(value))
}

pub(crate) async fn bootstrap(
    app: App,
    id: String,
    context: RequestContext,
    q: Option<String>,
) -> Result<Outcome> {
    get_events(app, id, context, q, PageKind::Bootstrap).await
}
pub(crate) async fn list(
    app: App,
    id: String,
    context: RequestContext,
    q: Option<String>,
) -> Result<Outcome> {
    get_events(app, id, context, q, PageKind::List).await
}

pub(crate) async fn summary(app: App, id: String, context: RequestContext) -> Result<Outcome> {
    let who = context.identity;
    let company = company(&id)?;
    let mut tx = app.pool.begin().await?;
    sqlx::query(queries::SUMMARY_SET_READ)
        .execute(&mut *tx)
        .await?;
    let seq = api::sequence(&mut tx, company).await?;
    auth::access(&mut tx, &who, company, AccessLock::Unlocked)
        .await?
        .require(&[EventsRead])?;
    let mut summary = summary_value(&mut tx, company, seq).await?;
    summary["stream_cursor"] = json!(seq);
    tx.commit().await?;
    Ok(Outcome::Json(summary))
}

pub(crate) async fn options(app: App, id: String, context: RequestContext) -> Result<Outcome> {
    let who = context.identity;
    let company = company(&id)?;
    let mut tx = app.pool.begin().await?;
    auth::access(&mut tx, &who, company, AccessLock::Shared)
        .await?
        .require(&[EventsRead])?;
    let dto = options_value(&mut tx, company).await?;
    tx.commit().await?;
    Ok(Outcome::cached(
        &format!(
            "\"{}\"",
            dto["revision"].as_str().ok_or_else(Error::internal)?
        ),
        dto,
        crate::application::CachePolicy::Conditional,
    ))
}
