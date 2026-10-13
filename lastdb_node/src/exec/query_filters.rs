use super::*;

pub(super) fn query_schema_filter(target: &str) -> Option<HashSet<String>> {
    query_multi_string_filter(target, "schema")
}

pub(super) fn query_multi_string_filter(target: &str, wanted_key: &str) -> Option<HashSet<String>> {
    let q = target.split_once('?')?.1;
    let mut schemas = HashSet::new();
    for pair in q.split('&') {
        let Some((k, v)) = pair.split_once('=') else {
            continue;
        };
        if k != wanted_key {
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

pub(super) fn query_string_filter(target: &str, wanted_key: &str) -> Option<String> {
    query_string_filter_any(target, &[wanted_key])
}

pub(super) fn query_string_filter_any(target: &str, wanted_keys: &[&str]) -> Option<String> {
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
