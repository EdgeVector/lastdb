//! What a Search inbox batch may carry.
//!
//! Inbox files are plain JSON outside the encrypted disk runs, and a Search
//! reader can keep them after it reads them. So a batch carries only the fields
//! that the schema marks searchable, and no file key or file bytes. The live
//! write path and the offline rebuild both use these functions.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::atom::file_pointer::LASTDB_FILE_KEY;
use crate::schema::types::Schema;

/// The file value fields that a Search inbox batch keeps.
const SEARCH_FILE_FIELDS: [&str; 2] = ["name", "media_type"];

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
/// `searchable` (every field when it is `None`), each through [`inbox_value`].
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

/// Copy of one field value without file access data: at any depth, a
/// `$lastdb_file` pointer keeps only its `name` and `media_type`.
///
/// Removing only the `dek` is not enough. `file_hash` is the plaintext
/// digest, which confirms the file content, and `blob_ref` names the stored
/// object. The thumbnail goes too.
pub(crate) fn inbox_value(value: &Value) -> Value {
    let mut value = value.clone();
    strip_file_access(&mut value);
    value
}

fn strip_file_access(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                match child {
                    Value::Object(file) if key == LASTDB_FILE_KEY => {
                        file.retain(|field, _| SEARCH_FILE_FIELDS.contains(&field.as_str()));
                    }
                    _ => strip_file_access(child),
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                strip_file_access(item);
            }
        }
        _ => {}
    }
}
