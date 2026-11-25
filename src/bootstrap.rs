use crate::{application::access as auth, infrastructure::stream, state::App};
use anyhow::{Context, Result};
use axum::http::{HeaderName, HeaderValue, Method, header};
use sqlx::{
    ConnectOptions, PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{env, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{Semaphore, broadcast};
use tower_http::cors::{AllowOrigin, CorsLayer};

fn secret(name: &str) -> Result<Arc<Vec<u8>>> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    anyhow::ensure!(value.len() >= 32, "{name} requires at least 32 bytes");
    Ok(Arc::new(value.into_bytes()))
}

fn cors_origins(origins: &str) -> Result<AllowOrigin> {
    if origins.trim() == "*" {
        return Ok(AllowOrigin::any());
    }

    let mut allowed = Vec::new();
    for origin in origins.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let url = url::Url::parse(origin)?;
        anyhow::ensure!(
            url.origin().ascii_serialization() == origin
                && matches!(url.scheme(), "http" | "https"),
            "invalid CORS origin"
        );
        allowed.push(HeaderValue::from_str(origin)?);
    }

    Ok(AllowOrigin::list(allowed))
}

fn cors(origins: &str) -> Result<CorsLayer> {
    Ok(CorsLayer::new()
        .allow_origin(cors_origins(origins)?)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::OPTIONS,
        ])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            header::IF_NONE_MATCH,
            header::IF_MATCH,
            HeaderName::from_static("idempotency-key"),
        ])
        .expose_headers([
            header::ETAG,
            header::CONTENT_DISPOSITION,
            HeaderName::from_static("x-idempotency-replayed"),
        ]))
}

async fn watch_access(app: App, mut listener: sqlx::postgres::PgListener) {
    loop {
        match listener.recv().await {
            Ok(_) => {
                let _ = app.access_changes.send(());
            }
            Err(_) => {
                let _ = app.access_changes.send(());
                tracing::warn!("access invalidation listener disconnected");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

// Session-specific configuration must survive both autocommit and explicit transactions.
async fn verify_affinity(pool: &PgPool) -> Result<()> {
    let mut conn = pool.acquire().await?;
    conn.close_on_drop();
    let marker = uuid::Uuid::new_v4().to_string();
    // The probe must also work through proxies that reset prepared statements.
    // Only a generated UUID is interpolated; no user input enters this SQL.
    let probe =
        format!("SELECT pg_backend_pid(),set_config('control.affinity_probe','{marker}',false)");
    let first = sqlx::raw_sql(&probe).fetch_one(&mut *conn).await?;
    let pid: i32 = first.try_get(0)?;
    let mut tx = sqlx::Acquire::begin(&mut *conn).await?;
    let second = sqlx::raw_sql(
        r#"
SELECT
    pg_backend_pid(),
    current_setting('control.affinity_probe', true)
"#,
    )
    .fetch_one(&mut *tx)
    .await?;
    let next: i32 = second.try_get(0)?;
    let value: Option<String> = second.try_get(1)?;
    anyhow::ensure!(
        next == pid && value.as_deref() == Some(marker.as_str()),
        "PostgreSQL session affinity check failed"
    );
    tx.commit().await?;
    Ok(())
}

/// Initialize dependencies and run the configured server or account command.
///
/// # Errors
/// Returns an error when configuration, dependency initialization, or serving fails.
pub async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let pool = database().await?;
    if let StartupMode::CommandCompleted = command(&pool).await? {
        return Ok(());
    }
    serve(runtime(pool).await?).await
}

enum StartupMode {
    Serve,
    CommandCompleted,
}

struct Runtime {
    app: App,
    listener: sqlx::postgres::PgListener,
    cors: CorsLayer,
}

async fn database() -> Result<PgPool> {
    anyhow::ensure!(
        matches!(
            env::var("DATABASE_SESSION_MODE").as_deref(),
            Ok("direct" | "session")
        ),
        "set DATABASE_SESSION_MODE to direct or session"
    );
    let options = if let Ok(url) = env::var("DATABASE_URL") {
        url.parse::<PgConnectOptions>()?
    } else {
        PgConnectOptions::new()
    };
    let options = options.options([
        ("statement_timeout", "8s"),
        ("idle_in_transaction_session_timeout", "15s"),
        ("lock_timeout", "5s"),
        ("TimeZone", "UTC"),
    ]);
    let pool = PgPoolOptions::new()
        .max_connections(32)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(options.disable_statement_logging())
        .await?;
    verify_affinity(&pool).await?;
    sqlx::migrate!().run(&pool).await?;
    Ok(pool)
}

async fn command(pool: &PgPool) -> Result<StartupMode> {
    if let Some(command) = env::args().nth(1) {
        match command.as_str() {
            "create-account" => {
                let email = env::var("ACCOUNT_EMAIL")?.trim().to_lowercase();
                let password = env::var("ACCOUNT_PASSWORD")?;
                anyhow::ensure!(
                    !email.is_empty() && (12..=1024).contains(&password.len()),
                    "invalid account credentials"
                );
                let permissions: Vec<String> = env::var("ACCOUNT_GLOBAL_PERMISSIONS")
                    .unwrap_or_default()
                    .split(',')
                    .map(str::trim)
                    .filter(|p| !p.is_empty())
                    .map(str::to_owned)
                    .collect();
                anyhow::ensure!(
                    permissions
                        .iter()
                        .all(|p| auth::GLOBAL.contains(&p.as_str())),
                    "unknown global permission"
                );
                let user = uuid::Uuid::new_v4();
                sqlx::query(
                    r#"
INSERT INTO accounts (id, email, password_hash, global_permissions) VALUES ($1, $2, $3, $4)
"#,
                )
                .bind(user)
                .bind(email)
                .bind(auth::password_hash(&password)?)
                .bind(permissions)
                .execute(pool)
                .await?;
                println!("{user}");
                return Ok(StartupMode::CommandCompleted);
            }
            _ => anyhow::bail!("unknown command"),
        }
    }
    Ok(StartupMode::Serve)
}

fn cdn_hosts() -> std::collections::HashSet<String> {
    env::var("CDN_HOSTS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|host| !host.is_empty())
        .map(|host| {
            if host.contains(':') {
                host.to_lowercase()
            } else {
                format!("{}:443", host.to_lowercase())
            }
        })
        .collect()
}

async fn connect_nats(
    nats_url: &str,
    nats_reconnected: &broadcast::Sender<()>,
) -> Result<(async_nats::Client, async_nats::jetstream::stream::Stream)> {
    let events = nats_reconnected.clone();
    let nats = async_nats::ConnectOptions::new()
        .retry_on_initial_connect()
        .event_callback(move |event| {
            let events = events.clone();
            async move {
                if matches!(event, async_nats::Event::Connected) {
                    let _ = events.send(());
                }
            }
        })
        .connect(nats_url)
        .await
        .context("NATS connection")?;
    let stream = loop {
        match stream::setup(&nats).await {
            Ok(s) => break s,
            Err(e) if nats.connection_state() != async_nats::connection::State::Connected => {
                tracing::warn!(error=%e,"waiting for JetStream connection");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => return Err(e),
        }
    };
    Ok((nats, stream))
}

async fn runtime(pool: PgPool) -> Result<Runtime> {
    let jwt_key = secret("JWT_SECRET")?;
    let cursor_key = secret("CURSOR_SECRET")?;
    anyhow::ensure!(
        jwt_key != cursor_key,
        "JWT_SECRET and CURSOR_SECRET must differ"
    );
    let max_batch = env::var("MAX_INGEST_BATCH_V1")?.parse()?;
    anyhow::ensure!(
        (1..=1000).contains(&max_batch),
        "MAX_INGEST_BATCH_V1 is outside 1..=1000"
    );
    let max_snapshot_items = crate::infrastructure::config::snapshot_limit(
        env::var("MAX_SNAPSHOT_ITEMS").ok().as_deref(),
    )?;
    let export_dir = PathBuf::from(env::var("EXPORT_DIR")?);
    tokio::fs::create_dir_all(&export_dir).await?;
    let cors = cors(&env::var("CORS_ORIGINS").unwrap_or_default())?;
    let (nats_reconnected, _) = broadcast::channel(16);
    let (nats, stream) = connect_nats(&env::var("NATS_URL")?, &nats_reconnected).await?;
    let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
    listener.listen("access_change").await?;
    let (access_changes, _) = broadcast::channel(64);
    let app = App {
        pool,
        jwt_key,
        cursor_key,
        cursor_key_id: env::var("CURSOR_KEY_ID").unwrap_or_else(|_| "v1".into()),
        dummy_hash: auth::password_hash(&uuid::Uuid::new_v4().to_string())?,
        password_checks: Arc::new(Semaphore::new(4)),
        nats,
        stream,
        dispatchers: Default::default(),
        cdn_hosts: Arc::new(cdn_hosts()),
        max_batch,
        max_snapshot_items,
        export_dir,
        access_changes,
        nats_reconnected,
    };
    Ok(Runtime {
        app,
        listener,
        cors,
    })
}

async fn serve(runtime: Runtime) -> Result<()> {
    let app = runtime.app;
    let cors = runtime.cors;
    tokio::spawn(watch_access(app.clone(), runtime.listener));
    tokio::spawn(stream::relay(app.clone()));
    tokio::spawn(crate::application::retention::run(app.clone()));
    tokio::spawn(crate::application::commands::deliver_commands(app.clone()));
    for _ in 0..2 {
        tokio::spawn(crate::application::export_worker::worker(app.clone()));
    }
    let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let addr = env::var("BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8080".into())
        .parse::<SocketAddr>()?;
    let tcp = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr,"CONTROL API listening");
    axum::serve(
        tcp,
        crate::transport::router::router(app, cors)
            .into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown(terminate))
    .await?;
    Ok(())
}

async fn shutdown(mut terminate: tokio::signal::unix::Signal) {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = terminate.recv() => {},
    }
}

#[cfg(test)]
mod tests {
    use super::{cors, cors_origins};
    use anyhow::Result;
    use axum::{Router, routing::get};
    use std::{future::IntoFuture, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    async fn assert_preflight(origins: &str, origin: &str, allowed: Option<&str>) -> Result<()> {
        let router = Router::new()
            .route("/", get(|| async { "ok" }))
            .layer(cors(origins)?);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let stop = async {
            let _ = stopped.await;
        };
        let server = tokio::spawn(
            axum::serve(listener, router)
                .with_graceful_shutdown(stop)
                .into_future(),
        );

        let mut stream = TcpStream::connect(address).await?;
        let request = format!(
            "OPTIONS / HTTP/1.1\r\nHost: {address}\r\nOrigin: {origin}\r\n\
             Access-Control-Request-Method: POST\r\n\
             Access-Control-Request-Headers: authorization\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).await?;
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response)).await??;
        let _ = shutdown.send(());
        server.await??;

        let response = String::from_utf8(response)?;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        let allowed_origin = response
            .lines()
            .find_map(|line| line.strip_prefix("access-control-allow-origin: "));
        assert_eq!(allowed_origin, allowed);
        assert!(response.contains("access-control-allow-methods: GET,POST,PUT,PATCH,OPTIONS"));
        assert!(response.contains("access-control-allow-headers: authorization"));
        assert!(!response.contains("access-control-allow-credentials:"));
        Ok(())
    }

    #[tokio::test]
    async fn preflight_enforces_allowlists_and_explicit_wildcard_without_credentials() -> Result<()>
    {
        let origin = "https://app.example.test";
        let unlisted = "https://unlisted.example.test";
        assert_preflight("", origin, None).await?;
        assert_preflight("*", unlisted, Some("*")).await?;
        assert_preflight(origin, origin, Some(origin)).await?;
        assert_preflight(origin, unlisted, None).await?;
        Ok(())
    }

    #[test]
    fn cors_accepts_empty_lists_exact_origins_and_explicit_wildcard() {
        for origins in [
            "",
            "  ",
            "*",
            " * ",
            "https://app.example.test",
            "http://localhost:5173, https://app.example.test",
        ] {
            assert!(cors_origins(origins).is_ok(), "{origins}");
        }
    }

    #[test]
    fn cors_rejects_paths_credentials_other_schemes_and_mixed_wildcards() {
        for origins in [
            "null",
            "ftp://app.example.test",
            "https://app.example.test/",
            "https://user@app.example.test",
            "https://app.example.test/path",
            "*,https://app.example.test",
            "https://app.example.test,*",
        ] {
            assert!(cors_origins(origins).is_err(), "{origins}");
        }
    }
}
