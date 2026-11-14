//! Effective grant permissions for a company's current restriction.
const READ: &[&str] = &["EVENTS_READ", "DEVICES_READ", "AUDIT_READ"];
const SUPPORT: &[&str] = &["DEVICE_DIAGNOSTICS", "DEVICE_RESTART", "TOKEN_ROTATE"];
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct GrantPolicy {
    pub(crate) status: &'static str,
    pub(crate) tenant: &'static [&'static str],
    pub(crate) support: &'static [&'static str],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GrantState {
    Inactive,
    Active,
    BillingOverride,
}

pub(crate) fn grant_policy(restriction: Option<&str>, grant: GrantState) -> GrantPolicy {
    match (restriction, grant) {
        (None, _) => GrantPolicy {
            status: "ACTIVE",
            tenant: READ,
            support: SUPPORT,
        },
        (Some("MAINTENANCE"), GrantState::Active | GrantState::BillingOverride) => GrantPolicy {
            status: "ACTIVE_SUPPORT_OVERRIDE",
            tenant: READ,
            support: SUPPORT,
        },
        (Some("BILLING_DEBT"), GrantState::BillingOverride) => GrantPolicy {
            status: "ACTIVE_SUPPORT_OVERRIDE",
            tenant: &["EVENTS_READ", "DEVICES_READ"],
            support: &["DEVICE_DIAGNOSTICS"],
        },
        _ => GrantPolicy {
            status: "RESTRICTED",
            tenant: &[],
            support: &[],
        },
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_restriction_and_override_combination() {
        for restriction in [
            None,
            Some("MAINTENANCE"),
            Some("BILLING_DEBT"),
            Some("ADMIN_BLOCKED"),
            Some("TERMS_VIOLATION"),
            Some("FUTURE_RESTRICTION"),
        ] {
            for active in [false, true] {
                for override_ in [false, true] {
                    let grant = match (active, override_) {
                        (false, _) => GrantState::Inactive,
                        (true, false) => GrantState::Active,
                        (true, true) => GrantState::BillingOverride,
                    };
                    let p = grant_policy(restriction, grant);
                    let allowed = restriction.is_none()
                        || active
                            && (restriction == Some("MAINTENANCE")
                                || restriction == Some("BILLING_DEBT") && override_);
                    assert_eq!(
                        p.status != "RESTRICTED",
                        allowed,
                        "{restriction:?} {active} {override_}"
                    );
                    assert!(!p.tenant.contains(&"EVENTS_REVIEW"));
                    assert!(!p.tenant.contains(&"REPORTS_EXPORT"));
                    assert!(!p.tenant.contains(&"DEVICES_DELETE"));
                    assert!(!p.tenant.contains(&"USERS_MANAGE"));
                    if p.status == "RESTRICTED" {
                        assert!(p.tenant.is_empty() && p.support.is_empty());
                    }
                    if restriction == Some("BILLING_DEBT") && allowed {
                        assert_eq!(p.support, &["DEVICE_DIAGNOSTICS"]);
                        assert!(!p.tenant.contains(&"AUDIT_READ"));
                    }
                }
            }
        }
    }
}
