//! Conflict response rules. No host, storage, environment, or task access.

use std::collections::BTreeSet;

use fold_db::schema::types::KeyValue;
use serde_json::Value;

/// The primary key of each row of a page. `None` when a row has none.
pub(super) fn row_keys(results: &[Value]) -> Option<Vec<KeyValue>> {
    results
        .iter()
        .map(|row| serde_json::from_value(row.get("key")?.clone()).ok())
        .collect()
}

/// Set the conflict flag of each row. `has_conflicts` is true when a
/// returned field of the record has a conflict, and `conflicted_fields` then
/// names those fields. Without the flags of the page every row is
/// `"unknown"`, not clean.
pub(super) fn stamp_record_conflict_flags(
    results: &mut [Value],
    flags: Option<&[BTreeSet<String>]>,
) -> &'static str {
    for (at, row) in results.iter_mut().enumerate() {
        let Some(row) = row.as_object_mut() else {
            continue;
        };
        let Some(flags) = flags else {
            row.insert("has_conflicts".into(), Value::String("unknown".into()));
            continue;
        };
        let conflicted: Vec<Value> =
            match (flags.get(at), row.get("fields").and_then(Value::as_object)) {
                (Some(set), Some(fields)) => set
                    .iter()
                    .filter(|field| fields.contains_key(field.as_str()))
                    .map(|field| Value::String(field.clone()))
                    .collect(),
                _ => Vec::new(),
            };
        row.insert("has_conflicts".into(), Value::Bool(!conflicted.is_empty()));
        if !conflicted.is_empty() {
            row.insert("conflicted_fields".into(), Value::Array(conflicted));
        }
    }
    if flags.is_some() {
        "known"
    } else {
        "unknown"
    }
}
