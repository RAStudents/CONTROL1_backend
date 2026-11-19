use crate::infrastructure::queries::commands as queries;
use crate::{infrastructure::support::Result, state::App};
use serde_json::{Value, json};
use std::time::Duration;
use uuid::Uuid;
#[derive(sqlx::FromRow)]
struct Operation {
    id: Uuid,
    company_id: Uuid,
    device_id: String,
    command: String,
}
async fn deliver(app: &App) -> Result<bool> {
    sqlx::query(queries::DELIVER_WITH_DEVICE_OPERATIONS)
        .execute(&app.pool)
        .await?;
    let row: Option<Operation> = sqlx::query_as(queries::DELIVER_WITH_DEVICE_OPERATIONS_2)
        .fetch_optional(&app.pool)
        .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let subject = format!("commands.company.{}", row.company_id);
    let body = json!({"operation_id":row.id,"company_id":row.company_id,"device_id":row.device_id,"command":row.command});
    let reply = tokio::time::timeout(
        Duration::from_secs(15),
        app.nats.request(subject, body.to_string().into()),
    )
    .await;
    let status = match reply {
        Ok(Ok(message)) => serde_json::from_slice::<Value>(&message.payload)
            .ok()
            .and_then(|v| {
                (v["operation_id"] == json!(row.id))
                    .then(|| v["status"].as_str().unwrap_or_default().to_owned())
            }),
        _ => None,
    };
    let (delivery, status) = match status.as_deref() {
        Some("EXECUTED") => ("DELIVERED", "EXECUTED"),
        Some("FAILED_ON_DEVICE") => ("DELIVERED", "FAILED_ON_DEVICE"),
        _ => ("UNKNOWN", "TIMED_OUT"),
    };
    sqlx::query(queries::DELIVER_UPDATE_DEVICE_OPERATIONS)
        .bind(row.id)
        .bind(status)
        .bind(delivery)
        .execute(&app.pool)
        .await?;
    Ok(true)
}

pub(crate) async fn deliver_commands(app: App) {
    loop {
        match deliver(&app).await {
            Ok(true) => {}
            Ok(false) => tokio::time::sleep(Duration::from_millis(300)).await,
            Err(e) => {
                tracing::warn!(code = e.code, "device command delivery paused");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
