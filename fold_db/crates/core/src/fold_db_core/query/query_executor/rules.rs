//! Storage-free projection validation.

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
