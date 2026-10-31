//! Small helpers that the `lastdb db` verbs share.

pub(crate) fn report_u64(report: &serde_json::Value, key: &str) -> u64 {
    report
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
}
