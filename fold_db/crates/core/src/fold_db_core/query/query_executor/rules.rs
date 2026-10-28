//! Storage-free projection validation and request-local field scope.

use crate::schema::{Schema, SchemaError};

/// An empty projection selects all fields. Unknown names are sorted and deduplicated.
pub(super) fn reject_unknown_projection(
    schema: &Schema,
    fields: &[String],
) -> Result<(), SchemaError> {
    let mut unknown: Vec<&str> = fields
        .iter()
        .filter(|name| !schema.runtime_fields.contains_key(*name))
        .map(String::as_str)
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    unknown.sort_unstable();
    unknown.dedup();
    Err(SchemaError::InvalidField(format!(
        "schema '{}' has no field(s): {}",
        schema.name,
        unknown.join(", ")
    )))
}

/// Change only the request-local schema clone, never the schema registry.
pub(super) fn scope_fields(schema: &mut Schema, prefix: Option<&str>) {
    if let Some(prefix) = prefix {
        for field in schema.runtime_fields.values_mut() {
            field
                .common_mut()
                .set_storage_prefix(Some(prefix.to_string()));
        }
    }
}
