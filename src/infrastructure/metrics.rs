//! Bounded-cardinality operational metrics. Names are static; tenant labels are
//! supplied only by maintenance's existing company scan, never by unauthenticated input.
use std::{
    collections::BTreeMap,
    sync::{LazyLock, Mutex},
};
use uuid::Uuid;
type MetricValues = BTreeMap<(&'static str, Option<Uuid>), f64>;

static VALUES: LazyLock<Mutex<MetricValues>> = LazyLock::new(|| Mutex::new(BTreeMap::new()));
pub(crate) fn set(name: &'static str, tenant: Option<Uuid>, value: f64) {
    if value.is_finite() {
        VALUES
            .lock()
            .expect("metrics mutex poisoned")
            .insert((name, tenant), value);
    }
}
pub(crate) fn increment(name: &'static str) {
    *VALUES
        .lock()
        .expect("metrics mutex poisoned")
        .entry((name, None))
        .or_default() += 1.0;
}
pub(crate) fn observe(name: &'static str, value: f64) {
    let mut values = VALUES.lock().expect("metrics mutex poisoned");
    *values.entry((name, None)).or_default() += value;
}
pub(crate) fn render() -> String {
    let values = VALUES.lock().expect("metrics mutex poisoned");
    let mut text = String::new();
    for ((name, tenant), value) in values.iter() {
        use std::fmt::Write;
        if let Some(id) = tenant {
            let _ = writeln!(text, "control_{name}{{company_id=\"{id}\"}} {value}");
        } else {
            let _ = writeln!(text, "control_{name} {value}");
        }
    }
    text
}
