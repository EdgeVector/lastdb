//! Independent values are a local storage invariant, separate from field identity.

use crate::schema::types::{Schema, SchemaError};

pub(super) fn normalize_owned_declaration(
    schema: &mut Schema,
    stored: Option<&Schema>,
) -> Result<(), SchemaError> {
    if let Some(stored) = stored {
        require_owned_schema(stored)?;
    }
    schema.independent_fields = true;
    schema
        .validate_alternate_keys()
        .map_err(SchemaError::InvalidData)
}

pub(in crate::schema::core) fn require_owned_schema(schema: &Schema) -> Result<(), SchemaError> {
    if !schema.uses_independent_fields() {
        return Err(SchemaError::InvalidData(format!(
            "schema '{}' uses a retired shared-value layout; copy its records into a fresh home",
            schema.name
        )));
    }
    schema
        .validate_alternate_keys()
        .map_err(SchemaError::InvalidData)
}
