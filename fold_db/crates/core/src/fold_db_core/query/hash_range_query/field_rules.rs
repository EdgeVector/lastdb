//! Storage-free projection and primary-field rules.

use crate::schema::Schema;

/// An empty projection selects every runtime field in deterministic name order.
/// An explicit projection keeps its caller order and duplicate names.
pub(super) fn selected_fields(schema: &Schema, fields: &[String]) -> Vec<String> {
    if fields.is_empty() {
        schema.runtime_field_names()
    } else {
        fields
            .iter()
            .filter(|field| schema.runtime_fields.contains_key(field.as_str()))
            .cloned()
            .collect()
    }
}

/// The declared key field establishes the record set, even when unprojected.
/// A sparse projected field must not change the read or count membership.
/// Callers check that the selected field set is nonempty first.
pub(super) fn primary_field(schema: &Schema, selected: &[String]) -> String {
    if let Some(key_field) = schema
        .key
        .as_ref()
        .and_then(|key| key.hash_field.as_ref().or(key.range_field.as_ref()))
    {
        if schema.runtime_fields.contains_key(key_field.as_str()) {
            return key_field.clone();
        }
    }
    selected[0].clone()
}
