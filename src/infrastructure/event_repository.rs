use crate::{
    domain::events::{ERRONEOUS, Filters, Validity},
    infrastructure::support::{self as api, Error, Result},
};
use serde_json::{Value, json};
use sqlx::PgConnection;
use uuid::Uuid;
pub(crate) const EVENT_FIELDS: &str = "jsonb_build_object(
    'id', e.id,
    'event_version', v.event_version,
    'classification_version', v.classification_version,
    'acknowledgement_version', v.acknowledgement_version,
    'event_type_id', e.event_type_id,
    'event_type_name', coalesce(t.name, e.event_type_id),
    'category_code', e.category_code,
    'icon_key', coalesce(t.icon_key, 'GENERIC_ALERT'),
    'location_id', e.location_id,
    'location_name', coalesce(l.name, e.location_id),
    'device_id', e.device_id,
    'device_name', coalesce(d.name, e.device_id),
    'occurred_at', e.occurred_at,
    'received_at', e.received_at,
    'status', v.status,
    'acknowledged_at', v.acknowledged_at,
    'acknowledged_by', v.acknowledged_by,
    'is_erroneous', v.is_erroneous,
    'erroneous_marked_at', v.erroneous_marked_at,
    'erroneous_marked_by', v.erroneous_marked_by,
    'preview_url', e.preview_url,
    'video_url', e.video_url
)";
pub(crate) const DICTS: &str = "LEFT JOIN event_types t
    ON (t.company_id, t.id) = (e.company_id, e.event_type_id)
LEFT JOIN locations l
    ON (l.company_id, l.id) = (e.company_id, e.location_id)
LEFT JOIN devices d
    ON (d.company_id, d.id) = (e.company_id, e.device_id)";
pub(crate) const AS_OF: &str =
    "v.valid_from_sequence <= $2 AND (v.valid_to_sequence IS NULL OR v.valid_to_sequence > $2)";
const MATCHING: &str = r"e.company_id = $1 AND e.created_sequence <= $2
    AND (
        $3::text IS NULL OR CASE
            WHEN char_length($3) < 3 THEN lower(e.id) = lower($3)
            ELSE lower(e.id) LIKE lower($4) || '%' ESCAPE '\'
        END
    )
    AND ($5::timestamptz IS NULL OR e.occurred_at >= $5)
    AND ($6::timestamptz IS NULL OR e.occurred_at < $6)
    AND (cardinality($7::text[]) = 0 OR e.event_type_id = ANY($7))
    AND (cardinality($8::text[]) = 0 OR e.device_id = ANY($8))
    AND ($9 = 'ALL' OR v.is_erroneous = ($9 = 'ERRONEOUS'))";
pub(crate) const ORDER: &str =
    "(coalesce(lower(e.id) = lower($3), false)) DESC, e.occurred_at DESC, e.id DESC";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Projection {
    Core,
    DisplayNames,
}

fn filtered_sql(select: &str, projection: Projection) -> String {
    let joins = if projection == Projection::DisplayNames {
        DICTS
    } else {
        ""
    };
    format!(
        "SELECT {select}
FROM events e
INNER JOIN event_versions v ON (v.company_id, v.event_id) = (e.company_id, e.id)
{joins}
WHERE {MATCHING} AND {AS_OF}"
    )
}

pub(crate) fn matched_sql(select: &str, projection: Projection) -> String {
    format!("{} ORDER BY {ORDER}", filtered_sql(select, projection))
}
pub(crate) fn filter_binds<'q, O>(
    query: sqlx::query::QueryAs<'q, sqlx::Postgres, O, sqlx::postgres::PgArguments>,
    company: Uuid,
    seq: i64,
    f: &'q Filters,
) -> sqlx::query::QueryAs<'q, sqlx::Postgres, O, sqlx::postgres::PgArguments> {
    query
        .bind(company)
        .bind(seq)
        .bind(f.search_id.as_deref())
        .bind(f.escaped())
        .bind(f.from)
        .bind(f.to)
        .bind(&f.event_type_ids)
        .bind(&f.device_ids)
        .bind(match f.validity {
            Validity::All => "ALL",
            Validity::Valid => "VALID",
            Validity::Erroneous => "ERRONEOUS",
        })
}

pub(crate) async fn summary_value(
    conn: &mut PgConnection,
    company: Uuid,
    seq: i64,
) -> Result<Value> {
    Ok(
        sqlx::query_scalar(include_str!("queries/sql/events_summary.sql"))
            .bind(company)
            .bind(seq)
            .fetch_one(conn)
            .await?,
    )
}

pub(crate) async fn options_value(conn: &mut PgConnection, company: Uuid) -> Result<Value> {
    let mut dto: Value = sqlx::query_scalar(include_str!("queries/sql/event_filter_options.sql"))
        .bind(company)
        .fetch_one(conn)
        .await?;
    dto["revision"] = json!(api::sha(api::canonical(&json!([company, dto]))?));
    Ok(dto)
}
pub(crate) async fn lock_key(
    conn: &mut PgConnection,
    user: Uuid,
    company: Uuid,
    endpoint: &str,
    key: &str,
) -> Result<()> {
    let lock = format!("{user}:{company}:{endpoint}:{key}");
    if endpoint == ERRONEOUS {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(lock)
            .execute(conn)
            .await?;
        return Ok(());
    }
    let locked: bool =
        sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(lock)
            .fetch_one(conn)
            .await?;
    if !locked {
        return Err(Error::new(409, "IDEMPOTENCY_REQUEST_IN_PROGRESS"));
    }
    Ok(())
}
pub(crate) async fn saved(
    conn: &mut PgConnection,
    scope: (Uuid, Uuid, &str, &str),
    hash: &str,
) -> Result<Option<(i32, Value)>> {
    let (user, company, endpoint, key) = scope;
    sqlx::query(
        r#"
DELETE FROM idempotency_keys
WHERE
    user_id = $1
    AND company_id = $2
    AND endpoint = $3
    AND idempotency_key = $4
    AND expires_at <= clock_timestamp()
"#,
    )
    .bind(user)
    .bind(company)
    .bind(endpoint)
    .bind(key)
    .execute(&mut *conn)
    .await?;
    let record: Option<(String, i32, Value)> = sqlx::query_as(
        r#"
SELECT
    payload_hash,
    response_code,
    response_body
FROM idempotency_keys
WHERE user_id = $1 AND company_id = $2 AND endpoint = $3 AND idempotency_key = $4
"#,
    )
    .bind(user)
    .bind(company)
    .bind(endpoint)
    .bind(key)
    .fetch_optional(conn)
    .await?;
    match record {
        Some((previous, _, _)) if previous != hash => {
            Err(Error::new(422, "IDEMPOTENCY_KEY_PAYLOAD_MISMATCH"))
        }
        Some((_, code, value)) => Ok(Some((code, value))),
        None => Ok(None),
    }
}
pub(crate) async fn save(
    conn: &mut PgConnection,
    scope: (Uuid, Uuid, &str, &str),
    hash: &str,
    code: i32,
    value: &Value,
) -> Result<()> {
    let (user, company, endpoint, key) = scope;
    sqlx::query(
        r#"
INSERT INTO idempotency_keys (
    user_id,
    company_id,
    endpoint,
    idempotency_key,
    payload_hash,
    response_code,
    response_body,
    expires_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, clock_timestamp() + interval '24 hours')
"#,
    )
    .bind(user)
    .bind(company)
    .bind(endpoint)
    .bind(key)
    .bind(hash)
    .bind(code)
    .bind(value)
    .execute(conn)
    .await?;
    Ok(())
}
pub(crate) async fn audit(
    conn: &mut PgConnection,
    user: Uuid,
    company: Uuid,
    action: &str,
    details: Value,
) -> Result<()> {
    sqlx::query(
        r#"
INSERT INTO audit_events (user_id, company_id, action, details) SELECT
    $1,
    $2,
    $3,
    $4
WHERE
    EXISTS (
        SELECT 1 FROM companies
        WHERE id = $2) AND EXISTS (
        SELECT 1 FROM accounts
        WHERE id = $1
    )
"#,
    )
    .bind(user)
    .bind(company)
    .bind(action)
    .bind(details)
    .execute(conn)
    .await?;
    Ok(())
}
pub(crate) async fn allocate(
    conn: &mut PgConnection,
    company: Uuid,
    size: i64,
    max: i64,
) -> Result<i64> {
    sqlx::query_scalar(
        r#"
UPDATE company_stream_counters SET last_sequence = last_sequence + $2
WHERE company_id = $1 AND $2 BETWEEN 1 AND $3 AND last_sequence <= 9007199254740991 - $2 RETURNING
    last_sequence
"#,
    )
    .bind(company)
    .bind(size)
    .bind(max)
    .fetch_optional(conn)
    .await?
    .ok_or_else(|| Error::new(422, "SEQUENCE_OUT_OF_RANGE"))
}
#[derive(Default)]
pub(crate) struct CommitChanges<'a> {
    pub(crate) created: Option<&'a Value>,
    pub(crate) updated: Option<&'a Value>,
    pub(crate) summary: Option<Value>,
    pub(crate) options: Option<Value>,
}

pub(crate) async fn outbox(
    conn: &mut PgConnection,
    company: Uuid,
    seq: i64,
    changes: CommitChanges<'_>,
) -> Result<()> {
    let CommitChanges {
        created,
        updated,
        summary,
        options,
    } = changes;
    if (created.is_some() && updated.is_some())
        || (created.is_none() && updated.is_none() && summary.is_none() && options.is_none())
    {
        return Err(Error::internal());
    }
    let payload = json!({"schema_version":1,"type":"TENANT_COMMIT","tenant_sequence":seq,
        "event_created":created,"event_updated":updated,"summary_updated":summary,"filter_options_updated":options});
    let bytes = api::canonical(&payload)?;
    // Reserve space for JetStream message headers under the default 1 MiB message limit.
    if bytes.len() > 1_000_000 {
        return Err(Error::invalid().details(json!({"reason":"COMMIT_TOO_LARGE"})));
    }
    let hash = api::sha(bytes);
    let rows = sqlx::query(
        r#"
INSERT INTO tenant_outbox (company_id, tenant_sequence, payload, payload_sha256) VALUES (
    $1, $2, $3, $4
)
"#,
    )
    .bind(company)
    .bind(seq)
    .bind(payload)
    .bind(hash)
    .execute(conn)
    .await?
    .rows_affected();
    if rows != 1 {
        return Err(Error::internal());
    }
    Ok(())
}
pub(crate) async fn current(conn: &mut PgConnection, company: Uuid, id: &str) -> Result<Value> {
    let sql = format!(
        "SELECT {EVENT_FIELDS}
FROM events e
INNER JOIN event_versions v
    ON (v.company_id, v.event_id) = (e.company_id, e.id)
    AND v.valid_to_sequence IS NULL
{DICTS}
WHERE e.company_id = $1 AND e.id = $2"
    );
    sqlx::query_scalar(&sql)
        .bind(company)
        .bind(id)
        .fetch_optional(conn)
        .await?
        .ok_or_else(Error::missing)
}

pub(crate) fn page_sql() -> String {
    format!(
        "SELECT i.position, {EVENT_FIELDS}
FROM event_snapshot_items i
INNER JOIN events e ON (e.company_id, e.id) = (i.company_id, i.event_id)
INNER JOIN event_versions v ON (v.company_id, v.event_id) = (e.company_id, e.id) AND {AS_OF}
{DICTS}
WHERE i.company_id = $1 AND i.snapshot_id = $3 AND i.position > $4
ORDER BY i.position
LIMIT $5"
    )
}
pub(crate) fn snapshot_count_sql(max: i64) -> String {
    format!(
        "SELECT count(*)::int FROM ({} LIMIT {}) matched",
        filtered_sql("e.id", Projection::Core),
        max + 1
    )
}
pub(crate) fn snapshot_materialization_sql() -> String {
    let select = filtered_sql(
        &format!("e.id, e.occurred_at, (row_number() OVER (ORDER BY {ORDER}) - 1)::int position"),
        Projection::Core,
    );
    format!(
        "WITH matches AS ({select}), inserted AS (
    INSERT INTO event_snapshot_items (snapshot_id, company_id, position, event_id, occurred_at)
    SELECT $10, $1, position, id, occurred_at FROM matches
    RETURNING 1
)
SELECT count(*)::int FROM inserted"
    )
}
pub(crate) fn export_count_sql() -> String {
    format!(
        "SELECT count(*) FROM ({} LIMIT 250001) matched",
        filtered_sql("e.id", Projection::Core)
    )
}

pub(crate) async fn stream_bounds(
    conn: &mut PgConnection,
    company: Uuid,
) -> Result<(i64, Option<i64>)> {
    Ok(sqlx::query_as(
        r#"
SELECT
    c.last_sequence,
    (
        SELECT min(tenant_sequence) FROM tenant_outbox
        WHERE company_id = $1
    )
FROM company_stream_counters c
WHERE c.company_id = $1
"#,
    )
    .bind(company)
    .fetch_one(conn)
    .await?)
}
