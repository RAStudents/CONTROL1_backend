use serde::{Deserialize, Serialize};
use uuid::Uuid;
pub(crate) const GLOBAL: &[&str] = &[
    "COMPANIES_MONITOR",
    "COMPANIES_MANAGE",
    "GLOBAL_USERS_MANAGE",
];

macro_rules! tenant_permissions {
    ($($variant:ident => $wire:literal),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
        pub(crate) enum TenantPermission { $(#[serde(rename = $wire)] $variant),+ }

        impl TenantPermission {
            pub(crate) const fn name(self) -> &'static str {
                match self { $(Self::$variant => $wire),+ }
            }
        }

        pub(crate) const TENANT: &[&str] = &[$($wire),+];
    };
}
tenant_permissions![
    EventsRead => "EVENTS_READ",
    EventsReview => "EVENTS_REVIEW",
    DevicesRead => "DEVICES_READ",
    DevicesCreate => "DEVICES_CREATE",
    DevicesUpdate => "DEVICES_UPDATE",
    DevicesDelete => "DEVICES_DELETE",
    ReportsRead => "REPORTS_READ",
    ReportsExport => "REPORTS_EXPORT",
    DictionariesRead => "DICTIONARIES_READ",
    DictionariesManage => "DICTIONARIES_MANAGE",
    AuditRead => "AUDIT_READ",
    UsersRead => "USERS_READ",
    UsersManage => "USERS_MANAGE",
];

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub(crate) struct Identity {
    pub(crate) sub: Uuid,
    pub(crate) sid: Uuid,
    pub(crate) exp: usize,
}

#[cfg(test)]
mod tests {
    use super::TenantPermission;

    #[test]
    fn permission_wire_names_survive_rust_identifier_changes() {
        for (permission, wire) in [
            (TenantPermission::EventsRead, "EVENTS_READ"),
            (TenantPermission::EventsReview, "EVENTS_REVIEW"),
            (TenantPermission::DevicesCreate, "DEVICES_CREATE"),
            (TenantPermission::ReportsExport, "REPORTS_EXPORT"),
            (TenantPermission::DictionariesManage, "DICTIONARIES_MANAGE"),
            (TenantPermission::UsersManage, "USERS_MANAGE"),
        ] {
            let json = serde_json::json!(wire);
            assert_eq!(serde_json::to_value(permission).unwrap(), json);
            assert_eq!(
                serde_json::from_value::<TenantPermission>(json).unwrap(),
                permission
            );
            assert_eq!(permission.name(), wire);
        }
        assert!(serde_json::from_str::<TenantPermission>("\"EventsRead\"").is_err());
    }
}
