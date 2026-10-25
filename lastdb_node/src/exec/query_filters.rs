//! Data-only query rules. The route supplies the poll timeout cap.

use std::collections::HashSet;
use std::time::Duration;

use lastdb_host::wire::percent_decode;

pub(super) struct LocalWatchRequest {
    pub after_seq: u64,
    pub timeout: Duration,
    pub schemas: Option<HashSet<String>>,
    pub hash: Option<String>,
    pub range_start: Option<String>,
    pub range_end: Option<String>,
}

impl LocalWatchRequest {
    pub(super) fn parse(target: &str, max_timeout: Duration) -> Self {
        let timeout_ms = query_u64(target, "timeout_ms").unwrap_or(0);
        Self {
            after_seq: query_u64(target, "after_seq").unwrap_or(0),
            timeout: Duration::from_millis(timeout_ms.min(max_timeout.as_millis() as u64)),
            schemas: query_schema_filter(target),
            hash: query_string_filter_any(target, &["hash"]),
            range_start: query_string_filter_any(target, &["start", "range_start"]),
            range_end: query_string_filter_any(target, &["end", "range_end"]),
        }
    }
}

fn query_schema_filter(target: &str) -> Option<HashSet<String>> {
    let q = target.split_once('?')?.1;
    let mut schemas = HashSet::new();
    for pair in q.split('&') {
        let Some((k, v)) = pair.split_once('=') else {
            continue;
        };
        if k != "schema" {
            continue;
        }
        for schema in v.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            schemas.insert(schema.to_string());
        }
    }
    if schemas.is_empty() {
        None
    } else {
        Some(schemas)
    }
}

fn query_string_filter_any(target: &str, wanted_keys: &[&str]) -> Option<String> {
    let q = target.split_once('?')?.1;
    q.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (wanted_keys.contains(&key) && !value.is_empty()).then(|| percent_decode(value))
    })
}

pub(super) fn query_u64(target: &str, key: &str) -> Option<u64> {
    let q = target.split_once('?')?.1;
    for pair in q.split('&') {
        let (k, v) = pair.split_once('=')?;
        if k == key {
            return v.parse().ok();
        }
    }
    None
}
