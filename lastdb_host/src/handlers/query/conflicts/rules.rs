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

/// Set `has_conflicts` true on each conflicted field of each row. Without
/// the flags of the page every field is unknown, not clean.
pub(super) fn stamp_record_conflict_flags(
    results: &mut [Value],
    flags: Option<&[BTreeSet<String>]>,
) -> &'static str {
    for (at, row) in results.iter_mut().enumerate() {
        let Some(metadata) = row.get_mut("metadata").and_then(Value::as_object_mut) else {
            continue;
        };
        for (field, field_meta) in metadata.iter_mut() {
            let flag = match flags {
                Some(flags) if flags.get(at).is_some_and(|set| set.contains(field)) => {
                    Value::Bool(true)
                }
                Some(_) => continue,
                None => Value::String("unknown".to_string()),
            };
            if let Some(obj) = field_meta.as_object_mut() {
                obj.insert("has_conflicts".to_string(), flag);
            }
        }
    }
    if flags.is_some() {
        "known"
    } else {
        "unknown"
    }
}
