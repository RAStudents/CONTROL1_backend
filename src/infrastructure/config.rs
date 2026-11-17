use crate::domain::limits::DEFAULT_SNAPSHOT_ITEMS;

pub(crate) fn snapshot_limit(value: Option<&str>) -> anyhow::Result<i64> {
    let n = value
        .map(str::parse)
        .transpose()?
        .unwrap_or(DEFAULT_SNAPSHOT_ITEMS);
    anyhow::ensure!(
        (1..=DEFAULT_SNAPSHOT_ITEMS).contains(&n),
        "MAX_SNAPSHOT_ITEMS must be in 1..=100000"
    );
    Ok(n)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deployment_can_lower_snapshot_ceiling() {
        assert_eq!(snapshot_limit(None).unwrap(), 100000);
        assert_eq!(snapshot_limit(Some("42")).unwrap(), 42);
        for value in ["0", "-1", "100001", "invalid"] {
            assert!(snapshot_limit(Some(value)).is_err());
        }
    }
}
