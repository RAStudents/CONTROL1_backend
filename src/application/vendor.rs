use crate::application::ReplayStatus;
use crate::application::access::AccessLock;
use crate::application::{
    access::{self as auth, TenantPermission::DictionariesManage},
    events,
};
use crate::infrastructure::queries::vendor as queries;
use crate::{
    application::{Outcome, RequestContext},
    domain::error::{Error, Result},
    infrastructure::support as api,
    state::App,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;
const COMPANIES_ENDPOINT: &str = "GET /api/v1/vendor/companies";
const COMMAND_ENDPOINT: &str =
    "POST /api/v1/companies/{company_id}/devices/{device_id}/commands/{command}";
const COMMANDS: &[&str] = &["DEVICE_DIAGNOSTICS", "DEVICE_RESTART", "TOKEN_ROTATE"];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CompanyQuery {
    search: Option<String>,
    status: Option<String>,
    sort: Option<String>,
    limit: Option<i64>,
    cursor: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct VendorCursor {
    format_version: u8,
    key_id: String,
    endpoint_id: String,
    user_id: Uuid,
    filter_hash: String,
    limit: i64,
    sort_value: String,
    last_id: Uuid,
    issued_at_sec: i64,
}

#[derive(sqlx::FromRow)]
struct Listed {
    id: Uuid,
    name: String,
    created_at: DateTime<Utc>,
    status: String,
}

pub(crate) async fn companies(
    app: App,
    context: RequestContext,
    query: Result<CompanyQuery>,
) -> Result<Outcome> {
    let who = context.identity;
    let mut tx = app.pool.begin().await?;
    if !auth::account(&mut tx, &who, AccessLock::Shared)
        .await?
        .can_browse()
    {
        return Err(Error::new(403, "PERMISSION_DENIED"));
    }
    let q = query?;
    let search = q.search.as_deref().map(str::trim).filter(|v| !v.is_empty());
    let status = q.status.as_deref().unwrap_or("ALL");
    let sort = q.sort.as_deref().unwrap_or("NAME_ASC");
    let limit = q.limit.unwrap_or(50);
    let mut invalid = Vec::new();
    if search.is_some_and(|s| s.chars().count() > 256 || s.contains('\0')) {
        invalid.push("search");
    }
    if !matches!(status, "ACTIVE" | "RESTRICTED" | "ALL") {
        invalid.push("status");
    }
    if !matches!(sort, "NAME_ASC" | "CREATED_DESC") {
        invalid.push("sort");
    }
    if !(1..=100).contains(&limit) {
        invalid.push("limit");
    }
    if !invalid.is_empty() {
        return Err(Error::new(422, "FILTER_INVALID").details(json!({"invalid_fields":invalid})));
    }
    let hash = api::sha(api::canonical(
        &json!({"search":search,"status":status,"sort":sort}),
    )?);
    let invalid_cursor = || Error::new(422, "INVALID_PAGINATION_CURSOR");
    let mut last = None;
    let mut name = None;
    let mut created = None;
    if let Some(raw) = q.cursor {
        let token: VendorCursor = api::verify(&app.cursor_key, &raw).ok_or_else(invalid_cursor)?;
        let now = Utc::now().timestamp();
        if token.format_version != 1
            || token.key_id != app.cursor_key_id
            || token.endpoint_id != COMPANIES_ENDPOINT
            || token.user_id != who.sub
            || token.filter_hash != hash
            || token.limit != limit
            || !(now - 3599..=now).contains(&token.issued_at_sec)
        {
            return Err(invalid_cursor());
        }
        last = Some(token.last_id);
        if sort == "NAME_ASC" {
            name = Some(token.sort_value);
        } else {
            created = Some(
                token
                    .sort_value
                    .parse::<DateTime<Utc>>()
                    .map_err(|_| invalid_cursor())?,
            );
        }
    }
    let mut rows: Vec<Listed> = sqlx::query_as(queries::COMPANIES_SELECT_COMPANIES)
        .bind(search)
        .bind(status)
        .bind(last)
        .bind(sort)
        .bind(name)
        .bind(created)
        .bind(limit + 1)
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    let more = rows.len() as i64 > limit;
    rows.truncate(limit as usize);
    let cursor = if more {
        let row = rows.last().ok_or_else(Error::internal)?;
        Some(api::sign(
            &app.cursor_key,
            &VendorCursor {
                format_version: 1,
                key_id: app.cursor_key_id.clone(),
                endpoint_id: COMPANIES_ENDPOINT.into(),
                user_id: who.sub,
                filter_hash: hash,
                limit,
                sort_value: if sort == "NAME_ASC" {
                    row.name.clone()
                } else {
                    row.created_at.to_rfc3339()
                },
                last_id: row.id,
                issued_at_sec: Utc::now().timestamp(),
            },
        )?)
    } else {
        None
    };
    let items: Vec<_> = rows
        .into_iter()
        .map(|r| json!({"id":r.id,"name":r.name,"created_at":r.created_at,"status":r.status}))
        .collect();
    Ok(Outcome::Json(
        json!({"items":items,"next_cursor":cursor,"has_more":more}),
    ))
}

pub(crate) async fn health(app: App, context: RequestContext) -> Result<Outcome> {
    let who = context.identity;
    let mut tx = app.pool.begin().await?;
    if !auth::account(&mut tx, &who, AccessLock::Shared)
        .await?
        .can_browse()
    {
        return Err(Error::new(403, "PERMISSION_DENIED"));
    }
    let items: Vec<Value> = sqlx::query_scalar(queries::HEALTH_SELECT_COMPANIES)
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Outcome::Json(json!({"items":items})))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Override {
    allow_billing_override: bool,
}

pub(crate) async fn billing_override(
    app: App,
    (company, grant): (String, String),
    context: RequestContext,
    data: Result<Override>,
) -> Result<Outcome> {
    let company = events::company(&company)?;
    let grant = Uuid::parse_str(&grant).map_err(|_| Error::missing())?;
    let who = context.identity;
    let mut tx = app.pool.begin().await?;
    let account = auth::account(&mut tx, &who, AccessLock::Shared).await?;
    if !account
        .global_permissions
        .iter()
        .any(|p| p == "COMPANIES_MANAGE")
    {
        return Err(Error::new(403, "PERMISSION_DENIED"));
    }
    let old: bool = sqlx::query_scalar(queries::BILLING_OVERRIDE_SELECT_SUPPORT_GRANTS)
        .bind(company)
        .bind(grant)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(Error::missing)?;
    let new = data?.allow_billing_override;
    let tag = format!("\"{}\"", api::sha(format!("{grant}:{old}")));
    if let Some(expected) = context.if_match.as_ref()
        && expected.as_bytes() != tag.as_bytes()
        && expected.as_bytes() != b"*"
    {
        return Err(Error::new(409, "STATE_CONFLICT"));
    }
    if old != new {
        sqlx::query(queries::BILLING_OVERRIDE_UPDATE_SUPPORT_GRANTS)
            .bind(company)
            .bind(grant)
            .bind(new)
            .execute(&mut *tx)
            .await?;
        events::audit(
            &mut tx,
            who.sub,
            company,
            "SUPPORT_BILLING_OVERRIDE_CHANGED",
            json!({"grant_id":grant,"previous":old,"current":new}),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(Outcome::Json(
        json!({"grant_id":grant,"allow_billing_override":new,"revision":api::sha(format!("{grant}:{new}"))}),
    ))
}

pub(crate) async fn command(
    app: App,
    (company, device, command): (String, String, String),
    context: RequestContext,
    data: Result<Value>,
) -> Result<Outcome> {
    let company = events::company(&company)?;
    let who = context.identity;
    if !COMMANDS.contains(&command.as_str()) {
        return Err(Error::missing());
    }
    let mut tx = app.pool.begin().await?;
    auth::access(&mut tx, &who, company, AccessLock::Shared)
        .await?
        .require_support(&command)?;
    let payload = data?;
    if payload.as_object().is_none_or(|o| !o.is_empty()) {
        return Err(Error::invalid());
    }
    let key = context.key()?;
    let hash = api::sha(format!(
        "POST:/api/v1/companies/{company}/devices/{device}/commands/{command}:{{}}"
    ));
    events::lock_key(&mut tx, who.sub, company, COMMAND_ENDPOINT, &key).await?;
    if let Some((code, dto)) =
        events::saved(&mut tx, (who.sub, company, COMMAND_ENDPOINT, &key), &hash).await?
    {
        let op: Uuid =
            serde_json::from_value(dto["operation_id"].clone()).map_err(|_| Error::internal())?;
        let delivery: String = sqlx::query_scalar(queries::COMMAND_SELECT_DEVICE_OPERATIONS)
            .bind(company)
            .bind(op)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        return match delivery.as_str() {
            "UNKNOWN" => {
                Err(Error::new(409, "IDEMPOTENCY_OUTCOME_UNKNOWN")
                    .details(json!({"operation_id":op})))
            }
            "IN_PROGRESS" => Err(Error::new(409, "IDEMPOTENCY_REQUEST_IN_PROGRESS")),
            _ => Ok(events::response(code, dto, ReplayStatus::Replayed)),
        };
    }
    let op = Uuid::new_v4();
    let count = sqlx::query(queries::COMMAND_INSERT_DEVICE_OPERATIONS)
        .bind(op)
        .bind(company)
        .bind(&device)
        .bind(who.sub)
        .bind(command)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if count != 1 {
        return Err(Error::missing());
    }
    let dto = json!({"operation_id":op,"status":"PENDING_DELIVERY"});
    events::save(
        &mut tx,
        (who.sub, company, COMMAND_ENDPOINT, &key),
        &hash,
        202,
        &dto,
    )
    .await?;
    tx.commit().await?;
    Ok(events::response(202, dto, ReplayStatus::Original))
}

pub(crate) async fn operation(
    app: App,
    (company, op): (String, String),
    context: RequestContext,
) -> Result<Outcome> {
    let company = events::company(&company)?;
    let who = context.identity;
    let op = Uuid::parse_str(&op).map_err(|_| Error::missing())?;
    let mut tx = app.pool.begin().await?;
    let access = auth::access(&mut tx, &who, company, AccessLock::Shared).await?;
    let (command, status): (String, String) =
        sqlx::query_as(queries::OPERATION_SELECT_DEVICE_OPERATIONS)
            .bind(company)
            .bind(who.sub)
            .bind(op)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(Error::missing)?;
    access.require_support(&command)?;
    tx.commit().await?;
    Ok(Outcome::Json(json!({"operation_id":op,"status":status})))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventType {
    id: String,
    name: String,
    icon_key: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Location {
    id: String,
    name: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Device {
    id: String,
    name: String,
    location_id: String,
    health: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Category {
    id: String,
    name: String,
    score: i32,
    trend: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Configuration {
    #[serde(default)]
    event_types: Vec<EventType>,
    #[serde(default)]
    locations: Vec<Location>,
    #[serde(default)]
    devices: Vec<Device>,
    #[serde(default)]
    categories: Vec<Category>,
}

pub(crate) async fn event_configuration(
    app: App,
    company: String,
    context: RequestContext,
    data: Result<Configuration>,
) -> Result<Outcome> {
    let company = events::company(&company)?;
    let who = context.identity;
    let mut tx = app.pool.begin().await?;
    auth::access(&mut tx, &who, company, AccessLock::Shared)
        .await?
        .require(&[DictionariesManage])?;
    let data = data?;
    if data
        .devices
        .iter()
        .any(|d| !matches!(d.health.as_str(), "OK" | "UNKNOWN" | "WARNING" | "CRITICAL"))
        || data.categories.iter().any(|c| {
            !matches!(
                c.trend.as_str(),
                "IMPROVING" | "WORSENING" | "STABLE" | "UNKNOWN"
            )
        })
    {
        return Err(Error::invalid());
    }
    // Serialize independent projection writes with commits while checking for a no-op.
    let previous: i64 =
        sqlx::query_scalar(queries::EVENT_CONFIGURATION_SELECT_COMPANY_STREAM_COUNTERS)
            .bind(company)
            .fetch_one(&mut *tx)
            .await?;
    let before_options = events::options_value(&mut tx, company).await?;
    let before_summary = events::summary_value(&mut tx, company, previous).await?;
    let writes = [
        (
            queries::EVENT_CONFIGURATION_INSERT_EVENT_TYPES,
            json!(data.event_types),
        ),
        (
            queries::EVENT_CONFIGURATION_INSERT_LOCATIONS,
            json!(data.locations),
        ),
        (
            queries::EVENT_CONFIGURATION_INSERT_DEVICES,
            json!(data.devices),
        ),
        (
            queries::EVENT_CONFIGURATION_INSERT_EVENT_CATEGORIES,
            json!(data.categories),
        ),
    ];
    for (sql, values) in writes {
        sqlx::query(sql)
            .bind(company)
            .bind(values)
            .execute(&mut *tx)
            .await?;
    }
    let next_options = events::options_value(&mut tx, company).await?;
    let mut next_summary = events::summary_value(&mut tx, company, previous).await?;
    let options_changed = before_options != next_options;
    let summary_changed = before_summary != next_summary;
    let seq = if options_changed || summary_changed {
        let seq = events::allocate(&mut tx, company, 1, 1).await?;
        next_summary["summary_as_of_sequence"] = json!(seq);
        events::outbox(
            &mut tx,
            company,
            seq,
            crate::infrastructure::event_repository::CommitChanges {
                summary: summary_changed.then_some(next_summary),
                options: options_changed.then_some(next_options),
                ..Default::default()
            },
        )
        .await?;
        seq
    } else {
        previous
    };
    tx.commit().await?;
    Ok(Outcome::Json(json!({"stream_cursor":seq})))
}

/// Scrapes require the same global access as the vendor monitoring API.
pub(crate) async fn metrics(app: App, context: RequestContext) -> Result<Outcome> {
    let mut tx = app.pool.begin().await?;
    if !auth::account(&mut tx, &context.identity, AccessLock::Shared)
        .await?
        .can_browse()
    {
        return Err(Error::new(403, "PERMISSION_DENIED"));
    }
    tx.commit().await?;
    Ok(Outcome::Text(crate::infrastructure::metrics::render()))
}
