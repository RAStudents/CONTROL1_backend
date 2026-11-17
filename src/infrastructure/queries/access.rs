pub(crate) const ACCOUNT_SELECT_ACCOUNTS: &str = r#"
SELECT
    a.frozen,
    a.global_permissions,
    a.default_company_id,
    (s.revoked_at IS NULL AND s.expires_at > clock_timestamp()) alive
FROM accounts a JOIN sessions s ON s.user_id = a.id
WHERE a.id = $1 AND s.id = $2 FOR SHARE OF a, s
"#;
pub(crate) const ACCOUNT_SELECT_ACCOUNTS_2: &str = r#"
SELECT
    a.frozen,
    a.global_permissions,
    a.default_company_id,
    (s.revoked_at IS NULL AND s.expires_at > clock_timestamp()) alive
FROM accounts a JOIN sessions s ON s.user_id = a.id
WHERE a.id = $1 AND s.id = $2
"#;
pub(crate) const ACCESS_SELECT_COMPANIES: &str = r#"
SELECT
    name,
    restriction,
    restriction_details,
    vendor_permissions
FROM companies
WHERE id = $1
"#;
pub(crate) const ACCESS_SELECT_MEMBERSHIPS: &str = r#"
SELECT
    permissions,
    role
FROM memberships
WHERE user_id = $1 AND company_id = $2
"#;
pub(crate) const ACCESS_SELECT_TENANT_ROLES: &str = r#"
SELECT permissions FROM tenant_roles
WHERE company_id = $1 AND name = $2
"#;
pub(crate) const ACCESS_SELECT_SUPPORT_GRANTS: &str = r#"
SELECT
    grant_id,
    mode,
    ticket_reference,
    allow_billing_override,
    granted_by_user_id,
    granted_at,
    expires_at,
    revoked_at
FROM support_grants
WHERE company_id = $1 AND user_id = $2
"#;
pub(crate) const ACCESS_SELECT_TRANSACTION: &str = "SELECT clock_timestamp()";
pub(crate) const CONTEXT_SELECT_MEMBERSHIPS: &str = r#"
SELECT
    jsonb_build_object(
        'id',
        c.id,
        'name',
        c.name,
        'status',
        CASE WHEN c.restriction IS NULL THEN 'ACTIVE' ELSE 'RESTRICTED' END
    )
FROM memberships m
JOIN companies c ON c.id = m.company_id
WHERE m.user_id = $1
ORDER BY c.name, c.id
"#;
pub(crate) const LOGIN_INSERT_LOGIN_LIMITS: &str = r#"
INSERT INTO login_limits VALUES ($1, clock_timestamp() + interval '1 minute', 1)
ON CONFLICT (bucket) DO UPDATE
    SET
        attempts
        = CASE
            WHEN login_limits.until_at <= clock_timestamp() THEN 1 ELSE
                LEAST(login_limits.attempts + 1, 31)
        END,
        until_at
        = CASE
            WHEN login_limits.until_at <= clock_timestamp() THEN excluded.until_at ELSE
                login_limits.until_at
        END
RETURNING attempts
"#;
pub(crate) const LOGIN_SELECT_ACCOUNTS: &str = r#"
SELECT
    id,
    password_hash
FROM accounts
WHERE email = $1
"#;
pub(crate) const LOGIN_SELECT_ACCOUNTS_2: &str = r#"
SELECT
    frozen,
    password_hash
FROM accounts
WHERE id = $1 FOR SHARE
"#;
pub(crate) const LOGIN_INSERT_SESSIONS: &str = r#"
INSERT INTO sessions (id, user_id, refresh_hash, expires_at) VALUES (
    $1, $2, $3, clock_timestamp() + interval '30 days'
)
"#;
pub(crate) const REFRESH_SELECT_SESSIONS: &str = r#"
SELECT
    s.id,
    s.user_id,
    a.frozen
FROM sessions AS s
INNER JOIN accounts AS a
    ON a.id = s.user_id
WHERE
    s.refresh_hash = $1
    AND s.revoked_at IS NULL
    AND s.expires_at > clock_timestamp()
FOR UPDATE OF s
FOR SHARE OF a
"#;
pub(crate) const REFRESH_UPDATE_SESSIONS: &str = r#"
UPDATE sessions SET refresh_hash = $2
WHERE id = $1
"#;
