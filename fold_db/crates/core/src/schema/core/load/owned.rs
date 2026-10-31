//! Every installed schema owns its values. Field identity stays separate.

use crate::schema::types::{Schema, SchemaError};

/// Mark an incoming declaration as owned and check the owned-schema rules.
pub(super) fn normalize_owned_declaration(
    schema: &mut Schema,
    stored: Option<&Schema>,
) -> Result<(), SchemaError> {
    if let Some(stored) = stored {
        require_owned_schema(stored)?;
    }
    schema.owned_values = true;
    schema
        .validate_alternate_keys()
        .map_err(SchemaError::InvalidData)
}

/// Refuse a stored schema from a home that a release with cross-schema value
/// sharing wrote. Startup calls this for every stored schema before replay.
pub(in crate::schema::core) fn require_owned_schema(schema: &Schema) -> Result<(), SchemaError> {
    if !schema.has_owned_layout() {
        return Err(SchemaError::InvalidData(format!(
            "schema '{}' uses a retired shared-value layout; copy its records into a fresh home",
            schema.name
        )));
    }
    schema
        .validate_alternate_keys()
        .map_err(SchemaError::InvalidData)
}
