//! What a Search inbox batch may carry.
//!
//! Inbox files are plain JSON outside the encrypted disk runs, and a Search
//! reader can keep them after it reads them. So a batch carries only the fields
//! that the schema marks searchable, and never a file key. The live write path
//! and the offline rebuild both use these functions.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::atom::file_pointer::LASTDB_FILE_KEY;
use crate::schema::types::Schema;

/// Fields eligible for Search inbox delivery for this schema.
///
/// Returns `None` when the schema has no field classifications (the Search
/// readers then apply no allowlist). Otherwise returns the fields tagged
/// `word` that are not excluded by [`is_search_excluded`].
pub(crate) fn searchable_fields(schema: &Schema) -> Option<HashSet<String>> {
    if schema.field_classifications.is_empty() {
        return None;
    }

    Some(
        schema
            .field_classifications
            .iter()
            .filter(|(_, classifications)| !is_search_excluded(classifications))
            .filter(|(_, classifications)| {
                classifications
                    .iter()
                    .any(|classification| classification.eq_ignore_ascii_case("word"))
            })
            .map(|(field_name, _)| field_name.clone())
            .collect(),
    )
}

/// True when field classifications ban Search inbox delivery
/// (`secret`, `no_index`, or `no-index`).
pub(crate) fn is_search_excluded(classifications: &[String]) -> bool {
    classifications.iter().any(|classification| {
        classification.eq_ignore_ascii_case("secret")
            || classification.eq_ignore_ascii_case("no_index")
            || classification.eq_ignore_ascii_case("no-index")
    })
}

/// The part of `fields` that a Search inbox batch may carry: the fields in
/// `searchable` (every field when it is `None`), with file keys removed.
pub(crate) fn inbox_fields(
    fields: &HashMap<String, Value>,
    searchable: Option<&HashSet<String>>,
) -> HashMap<String, Value> {
    fields
        .iter()
        .filter(|(field_name, _)| searchable.is_none_or(|allow| allow.contains(*field_name)))
        .map(|(field_name, value)| (field_name.clone(), inbox_value(value)))
        .collect()
}

/// Copy of one field value without file keys: every `dek` inside a
/// `$lastdb_file` pointer (the file's own key and its thumbnail's key) is
/// removed. The rest of the pointer stays, so the Search text does not change.
pub(crate) fn inbox_value(value: &Value) -> Value {
    let mut value = value.clone();
    strip_file_keys(&mut value, false);
    value
}

fn strip_file_keys(value: &mut Value, in_pointer: bool) {
    match value {
        Value::Object(map) => {
            if in_pointer {
                map.remove("dek");
            }
            for (key, child) in map.iter_mut() {
                strip_file_keys(child, in_pointer || key == LASTDB_FILE_KEY);
            }
        }
        Value::Array(items) => {
            for item in items {
                strip_file_keys(item, in_pointer);
            }
        }
        _ => {}
    }
}
