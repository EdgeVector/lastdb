//! Direct catalog writes obey the node's typed logical key format.

use super::{Schema, SchemaError, SchemaStore};
use schema_types::field_range_profile::FieldRangeProfile;

impl SchemaStore {
    pub(super) fn require_schema_key_format(&self, schema: &Schema) -> Result<(), SchemaError> {
        schema
            .validate_key_field_types()
            .map_err(SchemaError::InvalidData)?;
        let range_type = schema
            .key
            .as_ref()
            .and_then(|key| key.range_field.as_deref())
            .map(|path| schema.key_field_type(path));
        FieldRangeProfile::for_field_type(range_type.as_ref()).map_err(SchemaError::InvalidData)?;
        Ok(())
    }
}
