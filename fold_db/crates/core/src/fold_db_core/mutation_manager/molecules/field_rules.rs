//! Storage-free field selection and value validation before atom creation.

use crate::schema::types::Schema;
use crate::schema::SchemaError;

/// Non-schema protein key values stay on the mutation but produce no atom.
pub(super) fn validate_field_value(
    schema_name: &str,
    schema: &Schema,
    field_name: &str,
    value: &serde_json::Value,
) -> Result<bool, SchemaError> {
    // Extra keys (e.g. sibling partition values for field_hash
    // protein fold) are not schema fields — keep them on the
    // mutation for fold field maps, but do not create atoms.
    if !schema.runtime_fields.contains_key(field_name) {
        return Ok(false);
    }
    let field_type = schema.get_field_type(field_name);
    if let Err(type_err) = field_type.validate(value) {
        return Err(SchemaError::InvalidData(format!(
            "Type error in field '{}' of schema '{}': {}. Expected {}, got {}",
            field_name,
            schema_name,
            type_err,
            field_type,
            serde_json::to_string(value).unwrap_or_else(|_| "?".to_string())
        )));
    }
    Ok(true)
}
