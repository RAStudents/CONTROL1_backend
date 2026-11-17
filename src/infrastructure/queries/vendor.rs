pub(crate) const COMPANIES_SELECT_COMPANIES: &str = include_str!("sql/vendor_companies.sql");
pub(crate) const HEALTH_SELECT_COMPANIES: &str = r#"
SELECT
    jsonb_build_object(
        'company_id', c.id, 'company_name', c.name,
        'monitoring_config_status', c.monitoring_config_status,
        'severity', CASE
            WHEN c.monitoring_config_status <> 'VALID' THEN 'UNKNOWN'
            WHEN count(*) FILTER (WHERE d.health = 'CRITICAL') > 0 THEN 'CRITICAL'
            WHEN count(*) FILTER (WHERE d.health = 'WARNING') > 0 THEN 'WARNING'
            WHEN count(*) FILTER (WHERE d.health = 'UNKNOWN') > 0 THEN 'UNKNOWN' ELSE 'OK'
        END,
        'unknown_reasons', CASE
            WHEN c.monitoring_config_status <> 'VALID'
                THEN jsonb_build_array('MONITORING_CONFIG_' || c.monitoring_config_status)
            WHEN
                count(*) FILTER (WHERE d.health = 'UNKNOWN') > 0
                THEN jsonb_build_array('DEVICE_HEALTH_UNKNOWN')
            ELSE '[]'::jsonb
        END,
        'affected_devices_count',
        count(DISTINCT d.id) FILTER (
            WHERE c.monitoring_config_status <> 'VALID' OR d.health <> 'OK'
        )
    )
FROM companies c LEFT JOIN devices d ON d.company_id = c.id
GROUP BY c.id
ORDER BY c.name, c.id
"#;
pub(crate) const BILLING_OVERRIDE_SELECT_SUPPORT_GRANTS: &str = r#"
SELECT allow_billing_override FROM support_grants
WHERE company_id = $1 AND grant_id = $2 FOR UPDATE
"#;
pub(crate) const BILLING_OVERRIDE_UPDATE_SUPPORT_GRANTS: &str = r#"
UPDATE support_grants SET allow_billing_override = $3
WHERE company_id = $1 AND grant_id = $2
"#;
pub(crate) const COMMAND_SELECT_DEVICE_OPERATIONS: &str = r#"
SELECT delivery_state FROM device_operations
WHERE company_id = $1 AND id = $2
"#;
pub(crate) const COMMAND_INSERT_DEVICE_OPERATIONS: &str = r#"
INSERT INTO device_operations (id, company_id, device_id, user_id, command)
SELECT
    $1,
    $2,
    $3,
    $4,
    $5
FROM devices
WHERE company_id = $2 AND id = $3
"#;
pub(crate) const OPERATION_SELECT_DEVICE_OPERATIONS: &str = r#"
SELECT
    command,
    status
FROM device_operations
WHERE company_id = $1 AND user_id = $2 AND id = $3
"#;
pub(crate) const EVENT_CONFIGURATION_SELECT_COMPANY_STREAM_COUNTERS: &str = r#"
SELECT last_sequence FROM company_stream_counters
WHERE company_id = $1 FOR UPDATE
"#;
pub(crate) const EVENT_CONFIGURATION_INSERT_EVENT_TYPES: &str = r#"
INSERT INTO event_types
SELECT
    $1,
    id,
    name,
    icon_key
FROM jsonb_to_recordset($2) AS d (id text, name text, icon_key text)
ON CONFLICT (company_id, id) DO UPDATE SET name = excluded.name, icon_key = excluded.icon_key
"#;
pub(crate) const EVENT_CONFIGURATION_INSERT_LOCATIONS: &str = r#"
INSERT INTO locations SELECT
    $1,
    id,
    name
FROM jsonb_to_recordset($2) AS d (id text, name text)
ON CONFLICT (company_id, id) DO UPDATE SET name = excluded.name
"#;
pub(crate) const EVENT_CONFIGURATION_INSERT_DEVICES: &str = r#"
INSERT INTO devices
SELECT
    $1,
    id,
    name,
    location_id,
    health
FROM jsonb_to_recordset($2) AS d (id text, name text, location_id text, health text)
ON CONFLICT (company_id, id) DO UPDATE
    SET name = excluded.name, location_id = excluded.location_id, health = excluded.health
"#;
pub(crate) const EVENT_CONFIGURATION_INSERT_EVENT_CATEGORIES: &str = r#"
INSERT INTO event_categories
SELECT
    $1,
    id,
    name,
    score,
    trend
FROM jsonb_to_recordset($2) AS d (id text, name text, score int, trend text)
ON CONFLICT (company_id, id) DO UPDATE
    SET name = excluded.name, score = excluded.score, trend = excluded.trend
"#;
