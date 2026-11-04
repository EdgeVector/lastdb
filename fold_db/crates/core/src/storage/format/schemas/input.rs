//! Immutable, validated schema data. The encoder derives its destination.

use crate::schema::{Schema, SchemaError};
use schema_types::field_range_profile::FieldRangeProfile;

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
    let range_type = schema
        .key
        .as_ref()
        .and_then(|key| key.range_field.as_deref())
        .map(|path| schema.key_field_type(path));
    FieldRangeProfile::for_field_type(range_type.as_ref()).map_err(SchemaError::InvalidData)?;
    Ok(SchemaInput { schema })
}
