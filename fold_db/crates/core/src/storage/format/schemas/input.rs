//! Immutable, validated schema data. The encoder derives its destination.

use crate::schema::{Schema, SchemaError};

pub(crate) struct SchemaInput<'a> {
    pub(super) schema: &'a Schema,
}

pub(crate) fn schema_input<'a>(
    name: &str,
    schema: &'a Schema,
) -> Result<SchemaInput<'a>, SchemaError> {
    super::require_name(name)?;
    if schema.name != name {
        return Err(SchemaError::InvalidData(
            "schema catalog key differs from its payload name".into(),
        ));
    }
    schema
        .validate_definition()
        .map_err(SchemaError::InvalidData)?;
    schema
        .validate_key_field_types()
        .map_err(SchemaError::InvalidData)?;
    schema
        .require_record_keys()?
        .validate()
        .map_err(SchemaError::InvalidData)?;
    Ok(SchemaInput { schema })
}
