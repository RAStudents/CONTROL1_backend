use crate::{
    infrastructure::support as api,
    state::App,
    transport::{http, websocket as ws},
};
use axum::{
    Router,
    body::Body,
    extract::{DefaultBodyLimit, Request, State},
    http::{HeaderValue, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, patch, post, put},
};
use tower_http::cors::CorsLayer;
async fn response_headers(req: Request<Body>, next: Next) -> Response {
    let mut response = next.run(req).await;
    let status = response.status();
    let json_error = response
        .headers()
        .get(header::CONTENT_TYPE)
        .is_some_and(|content| content.as_bytes().starts_with(b"application/json"));
    if (status.is_client_error() || status.is_server_error()) && !json_error {
        let code = if status.is_server_error() {
            "INTERNAL_ERROR"
        } else {
            "REQUEST_INVALID"
        };
        response = api::Error::new(status.as_u16(), code).into_response();
    }
    let headers = response.headers_mut();
    headers
        .entry(header::CACHE_CONTROL)
        .or_insert(HeaderValue::from_static("private, no-store"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    response
}
pub(crate) fn router(app: App, cors: CorsLayer) -> Router {
    let company = Router::new()
        .route("/access", get(http::access_company_access))
        .route("/events", get(http::events_list))
        .route("/events/bootstrap", get(http::events_bootstrap))
        .route("/events/summary", get(http::events_summary))
        .route("/events/filter-options", get(http::events_options))
        .route("/events/ingest", post(http::events_ingest))
        .route("/events/{event}/acknowledge", put(http::events_acknowledge))
        .route("/events/{event}/erroneous", post(http::events_erroneous))
        .route("/events/exports", post(http::exports_create))
        .route("/events/exports/{export}", get(http::exports_status))
        .route(
            "/events/exports/{export}/download",
            get(http::exports_download),
        )
        .route("/events/ws", get(ws::upgrade))
        .route(
            "/support-grants/{grant}/billing-override",
            patch(http::vendor_billing_override),
        )
        .route(
            "/devices/{device}/commands/{command}",
            post(http::vendor_command),
        )
        .route("/operations/{operation}", get(http::vendor_operation))
        .route(
            "/events/configuration",
            put(http::vendor_event_configuration),
        );
    Router::new()
        .route("/api/v1/vendor/metrics", get(http::vendor_metrics))
        .route("/healthz", get(health))
        .route("/api/auth/login", post(http::access_login))
        .route("/api/auth/refresh", post(http::access_refresh))
        .route("/api/v1/auth/context", get(http::access_context))
        .route("/api/v1/vendor/companies", get(http::vendor_companies))
        .route("/api/v1/vendor/companies/health", get(http::vendor_health))
        .nest("/api/v1/companies/{company}", company)
        .fallback(|| async { api::Error::missing() })
        .method_not_allowed_fallback(|| async { api::Error::new(405, "METHOD_NOT_ALLOWED") })
        .layer(DefaultBodyLimit::max(4 * 1024 * 1024))
        .layer(middleware::from_fn(response_headers))
        .layer(cors)
        .with_state(app)
}
async fn health(State(app): State<App>) -> api::Result<Response> {
    Ok(crate::application::gateway::health(&app)
        .await?
        .render(&axum::http::HeaderMap::new()))
}
