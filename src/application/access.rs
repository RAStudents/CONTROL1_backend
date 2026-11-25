pub(crate) use crate::domain::access::{GLOBAL, Identity, TENANT, TenantPermission};
use crate::infrastructure::queries::access as queries;
use crate::{
    application::{Outcome, RequestContext},
    domain::error::{Error, Result},
    infrastructure::support as api,
    state::App,
};
use argon2::{
    Argon2, PasswordHasher, PasswordVerifier,
    password_hash::{PasswordHash, SaltString, rand_core::OsRng},
};
use chrono::{DateTime, Utc};
use jsonwebtoken::{EncodingKey, Header};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgConnection;
use std::{collections::BTreeMap, net::SocketAddr};
use uuid::Uuid;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AccessLock {
    Unlocked,
    Shared,
}

#[derive(sqlx::FromRow)]
pub(crate) struct Account {
    pub(crate) frozen: bool,
    pub(crate) global_permissions: Vec<String>,
    pub(crate) default_company_id: Option<Uuid>,
    alive: bool,
}

impl Account {
    pub(crate) fn can_browse(&self) -> bool {
        self.global_permissions
            .iter()
            .any(|p| matches!(p.as_str(), "COMPANIES_MANAGE" | "COMPANIES_MONITOR"))
    }
}

#[derive(sqlx::FromRow)]
struct Company {
    name: String,
    restriction: Option<String>,
    restriction_details: Option<Value>,
    vendor_permissions: Vec<String>,
}

#[derive(sqlx::FromRow, Serialize)]
pub(crate) struct Grant {
    pub(crate) grant_id: Uuid,
    mode: String,
    ticket_reference: String,
    pub(crate) allow_billing_override: bool,
    granted_by_user_id: Uuid,
    granted_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    revoked_at: Option<DateTime<Utc>>,
}

impl Grant {
    fn status(&self, now: DateTime<Utc>) -> &'static str {
        if self.revoked_at.is_some() {
            "REVOKED"
        } else if self.expires_at <= now {
            "EXPIRED"
        } else {
            "ACTIVE"
        }
    }
}

pub(crate) struct Access {
    company: Company,
    pub(crate) grant: Option<Grant>,
    origins: Vec<&'static str>,
    tenant: BTreeMap<String, &'static str>,
    support: BTreeMap<String, &'static str>,
    status: &'static str,
    now: DateTime<Utc>,
}

impl Access {
    pub(crate) fn require(&self, permissions: &[TenantPermission]) -> Result<()> {
        if self.status == "RESTRICTED" {
            return Err(Error::new(403, "COMPANY_RESTRICTED"));
        }
        if permissions
            .iter()
            .all(|p| self.tenant.contains_key(p.name()))
        {
            return Ok(());
        }
        Err(Error::new(403, "PERMISSION_DENIED"))
    }

    pub(crate) fn require_support(&self, permission: &str) -> Result<()> {
        if self.support.contains_key(permission) {
            return Ok(());
        }
        if let Some(grant) = &self.grant {
            match grant.status(self.now) {
                "REVOKED" => return Err(Error::new(403, "SUPPORT_GRANT_REVOKED")),
                "EXPIRED" => return Err(Error::new(403, "SUPPORT_GRANT_EXPIRED")),
                _ => {}
            }
        }
        if self.status == "RESTRICTED" {
            return Err(Error::new(403, "COMPANY_RESTRICTED"));
        }
        Err(Error::new(403, "PERMISSION_DENIED"))
    }

    pub(crate) fn active_company(&self) -> Result<()> {
        if self.company.restriction.is_some() {
            return Err(Error::new(403, "PERMISSION_DENIED"));
        }
        Ok(())
    }

    pub(crate) fn grant_deadline(&self) -> Option<DateTime<Utc>> {
        self.grant
            .as_ref()
            .filter(|g| g.status(self.now) == "ACTIVE")
            .map(|g| g.expires_at)
    }

    fn dto(&self, company_id: Uuid) -> Result<(String, Value, crate::application::CachePolicy)> {
        let grant = self.grant.as_ref().map(|g| {
            let mut value = json!(g);
            value["status"] = json!(g.status(self.now));
            value["remaining_ttl_sec"] = json!(
                (g.expires_at - self.now).num_seconds().max(0)
                    * i64::from(g.status(self.now) == "ACTIVE")
            );
            value
        });
        let sources: BTreeMap<String, Vec<&'static str>> = self
            .tenant
            .iter()
            .chain(self.support.iter())
            .map(|(permission, source)| (permission.clone(), vec![*source]))
            .collect();
        let mut dto = json!({
            "company_id":company_id,"company_name":self.company.name,
            "company_status": if self.company.restriction.is_some() {"RESTRICTED"} else {"ACTIVE"},
            "access_status":self.status,"active_origins":self.origins,
            "restriction":self.company.restriction.as_ref().map(|r| json!({"type":r,"details":self.company.restriction_details})),
            "tenant_permissions":self.tenant.keys().collect::<Vec<_>>(),
            "support_permissions":self.support.keys().collect::<Vec<_>>(),"permission_sources":sources,
            "support_grant":grant
        });
        let mut stable = dto.clone();
        if let Some(grant) = stable["support_grant"].as_object_mut() {
            grant.remove("remaining_ttl_sec");
        }
        let revision = api::sha(api::canonical(&stable)?);
        dto["revision"] = json!(revision);
        dto["server_time"] = json!(self.now);
        Ok((
            format!("\"{revision}\""),
            dto,
            if self.grant_deadline().is_some() {
                crate::application::CachePolicy::AlwaysFresh
            } else {
                crate::application::CachePolicy::Conditional
            },
        ))
    }
}

pub(crate) async fn account(
    conn: &mut PgConnection,
    id: &Identity,
    lock: AccessLock,
) -> Result<Account> {
    if id.exp <= Utc::now().timestamp() as usize {
        return Err(Error::new(401, "TOKEN_EXPIRED"));
    }
    let query = if lock == AccessLock::Shared {
        queries::ACCOUNT_SELECT_ACCOUNTS
    } else {
        queries::ACCOUNT_SELECT_ACCOUNTS_2
    };
    let account: Account = sqlx::query_as(query)
        .bind(id.sub)
        .bind(id.sid)
        .fetch_optional(conn)
        .await?
        .filter(|a: &Account| a.alive)
        .ok_or_else(|| Error::new(401, "SESSION_REVOKED"))?;
    if account.frozen {
        return Err(Error::new(403, "ACCOUNT_FROZEN"));
    }
    Ok(account)
}

fn shared(query: &str, lock: AccessLock) -> String {
    if lock == AccessLock::Shared {
        format!("{query} FOR SHARE")
    } else {
        query.into()
    }
}

pub(crate) async fn access(
    conn: &mut PgConnection,
    who: &Identity,
    company: Uuid,
    lock: AccessLock,
) -> Result<Access> {
    let account = account(conn, who, lock).await?;
    let company_row: Company = sqlx::query_as(&shared(queries::ACCESS_SELECT_COMPANIES, lock))
        .bind(company)
        .fetch_optional(&mut *conn)
        .await?
        .ok_or_else(|| Error::new(403, "COMPANY_ACCESS_DENIED"))?;
    let membership: Option<(Vec<String>, Option<String>)> =
        sqlx::query_as(&shared(queries::ACCESS_SELECT_MEMBERSHIPS, lock))
            .bind(who.sub)
            .bind(company)
            .fetch_optional(&mut *conn)
            .await?;
    let direct = if let Some((mut permissions, role)) = membership {
        if let Some(role) = role {
            let role_permissions: Vec<String> =
                sqlx::query_scalar(&shared(queries::ACCESS_SELECT_TENANT_ROLES, lock))
                    .bind(company)
                    .bind(role)
                    .fetch_one(&mut *conn)
                    .await?;
            permissions.extend(role_permissions);
        }
        Some(permissions)
    } else {
        None
    };
    let grant: Option<Grant> = sqlx::query_as(&shared(queries::ACCESS_SELECT_SUPPORT_GRANTS, lock))
        .bind(company)
        .bind(who.sub)
        .fetch_optional(&mut *conn)
        .await?;
    let now: DateTime<Utc> = sqlx::query_scalar(queries::ACCESS_SELECT_TRANSACTION)
        .fetch_one(&mut *conn)
        .await?;
    let active = grant.as_ref().filter(|g| g.status(now) == "ACTIVE");
    let vendor = account.can_browse();
    if direct.is_none() && !vendor && active.is_none() {
        let code = match grant.as_ref().map(|g| g.status(now)) {
            Some("EXPIRED") => "SUPPORT_GRANT_EXPIRED",
            Some("REVOKED") => "SUPPORT_GRANT_REVOKED",
            _ => "COMPANY_ACCESS_DENIED",
        };
        return Err(Error::new(403, code));
    }
    let mut origins = Vec::new();
    if direct.is_some() {
        origins.push("DIRECT_MEMBERSHIP");
    }
    if vendor {
        origins.push("VENDOR_GLOBAL");
    }
    if active.is_some() {
        origins.push("SUPPORT_GRANT");
    }
    let (mut tenant, mut support) = (BTreeMap::new(), BTreeMap::new());
    if company_row.restriction.is_none() && vendor {
        tenant.extend(
            company_row
                .vendor_permissions
                .iter()
                .filter(|p| TENANT.contains(&p.as_str()))
                .map(|p| (p.clone(), "VENDOR_GLOBAL")),
        );
    }
    let policy = crate::domain::policy::grant_policy(
        company_row.restriction.as_deref(),
        match active {
            None => crate::domain::policy::GrantState::Inactive,
            Some(g) if g.allow_billing_override => {
                crate::domain::policy::GrantState::BillingOverride
            }
            Some(_) => crate::domain::policy::GrantState::Active,
        },
    );
    let status = policy.status;
    let grant_permissions = (policy.tenant, policy.support);
    if active.is_some() {
        tenant.extend(
            grant_permissions
                .0
                .iter()
                .map(|p| (p.to_string(), "SUPPORT_GRANT")),
        );
        support.extend(
            grant_permissions
                .1
                .iter()
                .map(|p| (p.to_string(), "SUPPORT_GRANT")),
        );
    }
    if status == "ACTIVE" {
        tenant.extend(
            direct
                .unwrap_or_default()
                .into_iter()
                .filter(|p| TENANT.contains(&p.as_str()))
                .map(|p| (p, "DIRECT_MEMBERSHIP")),
        );
    }
    Ok(Access {
        company: company_row,
        grant,
        origins,
        tenant,
        support,
        status,
        now,
    })
}

pub(crate) async fn context(app: App, context: RequestContext) -> Result<Outcome> {
    let who = context.identity;
    let mut tx = app.pool.begin().await?;
    let a = account(&mut tx, &who, AccessLock::Shared).await?;
    let companies: Vec<Value> = sqlx::query_scalar(queries::CONTEXT_SELECT_MEMBERSHIPS)
        .bind(who.sub)
        .fetch_all(&mut *tx)
        .await?;
    let result = json!({"user_id":who.sub,"server_time":Utc::now(),"global_permissions":a.global_permissions.iter().filter(|p| GLOBAL.contains(&p.as_str())).collect::<Vec<_>>(),
        "default_company_id":a.default_company_id,"can_browse_all_companies":a.can_browse(),"member_companies":companies});
    tx.commit().await?;
    Ok(Outcome::Json(result))
}

pub(crate) async fn company_access(
    app: App,
    company: String,
    context: RequestContext,
) -> Result<Outcome> {
    let company = Uuid::parse_str(&company).map_err(|_| Error::missing())?;
    let who = context.identity;
    let mut tx = app.pool.begin().await?;
    let access = access(&mut tx, &who, company, AccessLock::Shared).await?;
    let (tag, body, policy) = access.dto(company)?;
    tx.commit().await?;
    Ok(Outcome::cached(&tag, body, policy))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Login {
    email: String,
    password: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Refresh {
    refresh_token: String,
}

pub(crate) fn password_hash(password: &str) -> anyhow::Result<String> {
    Argon2::default()
        .hash_password(password.as_bytes(), &SaltString::generate(&mut OsRng))
        .map(|hash| hash.to_string())
        .map_err(|e| anyhow::anyhow!("{e}"))
}

fn token_pair(app: &App, account: Uuid, session: Uuid, refresh: &str) -> Result<Outcome> {
    let claims = json!({"sub":account,"sid":session,"exp":Utc::now().timestamp() + 900,"iss":"control","aud":"control-api"});
    let jwt = jsonwebtoken::encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(&app.jwt_key),
    )
    .map_err(|_| Error::internal())?;
    Ok(Outcome::Json(
        json!({"access_token":jwt,"refresh_token":refresh}),
    ))
}

fn refresh_secret() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

pub(crate) async fn login(app: App, peer: SocketAddr, data: Result<Login>) -> Result<Outcome> {
    let Login { email, password } = data.map_err(|_| Error::new(400, "REQUEST_INVALID"))?;
    let email = email.trim().to_lowercase();
    if email.len() > 320
        || email.chars().any(|c| c.is_whitespace() || c.is_control())
        || email.matches('@').count() != 1
        || !email.split_once('@').is_some_and(|(u, d)| {
            !u.is_empty() && d.split('.').count() > 1 && d.split('.').all(|part| !part.is_empty())
        })
        || password.is_empty()
        || password.len() > 1024
    {
        return Err(Error::new(400, "REQUEST_INVALID"));
    }
    let bucket = api::sha(peer.ip().to_string());
    let attempts: i32 = sqlx::query_scalar(queries::LOGIN_INSERT_LOGIN_LIMITS)
        .bind(bucket)
        .fetch_one(&app.pool)
        .await?;
    if attempts > 30 {
        return Err(Error::new(429, "RATE_LIMITED"));
    }
    let row: Option<(Uuid, String)> = sqlx::query_as(queries::LOGIN_SELECT_ACCOUNTS)
        .bind(email)
        .fetch_optional(&app.pool)
        .await?;
    let hash = row
        .as_ref()
        .map(|(_, h)| h.clone())
        .unwrap_or_else(|| app.dummy_hash.clone());
    let permit = std::sync::Arc::clone(&app.password_checks)
        .acquire_owned()
        .await
        .map_err(|_| Error::unavailable())?;
    let valid = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        PasswordHash::new(&hash).is_ok_and(|h| {
            Argon2::default()
                .verify_password(password.as_bytes(), &h)
                .is_ok()
        })
    })
    .await
    .map_err(|_| Error::internal())?;
    let (user, previous_hash) = row
        .filter(|_| valid)
        .ok_or_else(|| Error::new(401, "INVALID_CREDENTIALS"))?;
    let mut tx = app.pool.begin().await?;
    let current: Option<(bool, String)> = sqlx::query_as(queries::LOGIN_SELECT_ACCOUNTS_2)
        .bind(user)
        .fetch_optional(&mut *tx)
        .await?;
    let (frozen, _) = current
        .filter(|(_, hash)| hash == &previous_hash)
        .ok_or_else(|| Error::new(401, "INVALID_CREDENTIALS"))?;
    if frozen {
        return Err(Error::new(403, "ACCOUNT_FROZEN"));
    }
    let session = Uuid::new_v4();
    let refresh = refresh_secret();
    sqlx::query(queries::LOGIN_INSERT_SESSIONS)
        .bind(session)
        .bind(user)
        .bind(api::sha(&refresh))
        .execute(&mut *tx)
        .await?;
    let response = token_pair(&app, user, session, &refresh)?;
    tx.commit().await?;
    Ok(response)
}

pub(crate) async fn refresh(app: App, data: Result<Refresh>) -> Result<Outcome> {
    let rejected = || Outcome::Reply {
        code: 401,
        value: json!({"error":"invalid_grant","error_description":"Refresh token is expired or revoked"}),
        replayed: crate::application::ReplayStatus::Original,
    };
    let Ok(data) = data else {
        return Ok(rejected());
    };
    if data.refresh_token.is_empty() || data.refresh_token.len() > 256 {
        return Ok(rejected());
    }
    let hash = api::sha(data.refresh_token);
    let mut tx = app.pool.begin().await?;
    let row: Option<(Uuid, Uuid, bool)> = sqlx::query_as(queries::REFRESH_SELECT_SESSIONS)
        .bind(hash)
        .fetch_optional(&mut *tx)
        .await?;
    let Some((session, user, frozen)) = row else {
        return Ok(rejected());
    };
    if frozen {
        return Ok(rejected());
    }
    let refresh = refresh_secret();
    sqlx::query(queries::REFRESH_UPDATE_SESSIONS)
        .bind(session)
        .bind(api::sha(&refresh))
        .execute(&mut *tx)
        .await?;
    let response = token_pair(&app, user, session, &refresh)?;
    tx.commit().await?;
    Ok(response)
}
