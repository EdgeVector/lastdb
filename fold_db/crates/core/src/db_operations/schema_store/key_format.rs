//! Direct catalog writes obey the node's actual logical key format.

use super::{Schema, SchemaError, SchemaStore};
use schema_types::{key_comparison::KeyOrderVersion, molecule_range_profile::MoleculeRangeProfile};

impl SchemaStore {
    pub(super) fn require_schema_key_format(&self, schema: &Schema) -> Result<(), SchemaError> {
        if self
            .runtime
            .as_ref()
            .is_none_or(|runtime| runtime.range_order() != KeyOrderVersion::TypedScalarV1)
        {
            return schema
                .validate_legacy_key_field_types()
                .map_err(SchemaError::InvalidData);
        }
        schema
            .validate_key_field_types()
            .map_err(SchemaError::InvalidData)?;
        let range_type = schema
            .key
            .as_ref()
            .and_then(|key| key.range_field.as_deref())
            .map(|path| schema.key_field_type(path));
        MoleculeRangeProfile::for_field_type(range_type.as_ref())
            .map_err(SchemaError::InvalidData)?;
        Ok(())
    }
}
