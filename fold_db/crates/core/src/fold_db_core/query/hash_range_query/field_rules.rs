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
