use crate::{
    application::{self, RequestContext},
    domain::error::{Error, Result},
    state::App,
};
use axum::{
    Json,
    extract::{
        ConnectInfo, Path, Query, RawQuery, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{HeaderMap, header},
    response::Response,
};
use serde_json::{Value, json};
use std::net::SocketAddr;
fn context(app: &App, headers: &HeaderMap) -> Result<RequestContext> {
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let token = authorization
        .split_once(' ')
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
        .map(|(_, token)| token)
        .ok_or_else(|| Error::new(401, "AUTH_REQUIRED"))?;
    Ok(RequestContext {
        identity: crate::infrastructure::tokens::decode(app, token)?,
        idempotency_key: headers
            .get("Idempotency-Key")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
        if_match: headers
            .get(header::IF_MATCH)
            .map(|v| v.to_str().unwrap_or("").to_owned()),
    })
}
fn body<T>(data: std::result::Result<Json<T>, JsonRejection>) -> Result<T> {
    data.map(|Json(v)| v).map_err(|_| Error::invalid())
}
pub(crate) async fn access_context(State(app): State<App>, headers: HeaderMap) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome = application::access::context(app, ctx).await?;
    Ok(outcome.render(&headers))
}
pub(crate) async fn access_company_access(
    State(app): State<App>,
    axum::extract::Path(company): axum::extract::Path<String>,
    headers: HeaderMap,
) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome = application::access::company_access(app, company, ctx).await?;
    Ok(outcome.render(&headers))
}
pub(crate) async fn access_login(
    State(app): State<App>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    data: std::result::Result<Json<application::access::Login>, JsonRejection>,
) -> Result<Response> {
    let outcome = application::access::login(app, peer, body(data)).await?;
    Ok(outcome.render(&HeaderMap::new()))
}
pub(crate) async fn access_refresh(
    State(app): State<App>,
    data: std::result::Result<Json<application::access::Refresh>, JsonRejection>,
) -> Result<Response> {
    let outcome = application::access::refresh(app, body(data)).await?;
    Ok(outcome.render(&HeaderMap::new()))
}
pub(crate) async fn events_bootstrap(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome = application::events::bootstrap(app, id, ctx, q).await?;
    Ok(outcome.render(&headers))
}
pub(crate) async fn events_list(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome = application::events::list(app, id, ctx, q).await?;
    Ok(outcome.render(&headers))
}
pub(crate) async fn events_summary(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome = application::events::summary(app, id, ctx).await?;
    Ok(outcome.render(&headers))
}
pub(crate) async fn events_options(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome = application::events::options(app, id, ctx).await?;
    Ok(outcome.render(&headers))
}
pub(crate) async fn events_acknowledge(
    State(app): State<App>,
    Path((id, event)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome = application::events::acknowledge(app, (id, event), ctx).await?;
    Ok(outcome.render(&headers))
}
pub(crate) async fn events_erroneous(
    State(app): State<App>,
    Path((id, event)): Path<(String, String)>,
    headers: HeaderMap,
    data: std::result::Result<Json<application::events::Mark>, JsonRejection>,
) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome = application::events::erroneous(app, (id, event), ctx, body(data)).await?;
    Ok(outcome.render(&headers))
}
pub(crate) async fn events_ingest(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
    data: std::result::Result<Json<application::events::Ingest>, JsonRejection>,
) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome = application::events::ingest(app, id, ctx, body(data)).await?;
    Ok(outcome.render(&headers))
}
pub(crate) async fn exports_create(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
    request: std::result::Result<Json<application::exports::ExportRequest>, JsonRejection>,
) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome = application::exports::create(app, id, ctx, body(request)).await?;
    Ok(outcome.render(&headers))
}
pub(crate) async fn exports_status(
    State(app): State<App>,
    Path((company, id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome = application::exports::status(app, (company, id), ctx).await?;
    Ok(outcome.render(&headers))
}
pub(crate) async fn exports_download(
    State(app): State<App>,
    Path((company, id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome = application::exports::download(app, (company, id), ctx).await?;
    Ok(outcome.render(&headers))
}
pub(crate) async fn vendor_companies(
    State(app): State<App>,
    headers: HeaderMap,
    query: std::result::Result<Query<application::vendor::CompanyQuery>, QueryRejection>,
) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome = application::vendor::companies(
        app,
        ctx,
        query.map(|Query(q)| q).map_err(|_| {
            Error::new(422, "FILTER_INVALID")
                .details(json!({"invalid_fields":["search","status","sort","limit","cursor"]}))
        }),
    )
    .await?;
    Ok(outcome.render(&headers))
}
pub(crate) async fn vendor_health(State(app): State<App>, headers: HeaderMap) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome = application::vendor::health(app, ctx).await?;
    Ok(outcome.render(&headers))
}
pub(crate) async fn vendor_billing_override(
    State(app): State<App>,
    Path((company, grant)): Path<(String, String)>,
    headers: HeaderMap,
    data: std::result::Result<Json<application::vendor::Override>, JsonRejection>,
) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome =
        application::vendor::billing_override(app, (company, grant), ctx, body(data)).await?;
    Ok(outcome.render(&headers))
}
pub(crate) async fn vendor_command(
    State(app): State<App>,
    Path((company, device, command)): Path<(String, String, String)>,
    headers: HeaderMap,
    data: std::result::Result<Json<Value>, JsonRejection>,
) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome =
        application::vendor::command(app, (company, device, command), ctx, body(data)).await?;
    Ok(outcome.render(&headers))
}
pub(crate) async fn vendor_operation(
    State(app): State<App>,
    Path((company, op)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome = application::vendor::operation(app, (company, op), ctx).await?;
    Ok(outcome.render(&headers))
}
pub(crate) async fn vendor_event_configuration(
    State(app): State<App>,
    Path(company): Path<String>,
    headers: HeaderMap,
    data: std::result::Result<Json<application::vendor::Configuration>, JsonRejection>,
) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    let outcome = application::vendor::event_configuration(app, company, ctx, body(data)).await?;
    Ok(outcome.render(&headers))
}

pub(crate) async fn vendor_metrics(State(app): State<App>, headers: HeaderMap) -> Result<Response> {
    let ctx = context(&app, &headers)?;
    Ok(application::vendor::metrics(app, ctx)
        .await?
        .render(&headers))
}
