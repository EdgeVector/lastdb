//! A range TTL needs a String range with the 20-digit timestamp prefix.

use super::{Schema, SchemaError, SchemaStore};
use crate::schema::types::schema::DeclarativeSchemaType;
use schema_types::{
    field_range_profile::FieldRangeProfile, range_storage_profile::RangeStorageProfile,
};

fn require_typed_range_retention(schema: &Schema) -> Result<(), SchemaError> {
    if !matches!(
        schema.schema_type,
        DeclarativeSchemaType::Range | DeclarativeSchemaType::HashRange
    ) {
        return Ok(());
    }
    let profile = schema
        .require_record_keys()?
        .primary
        .range_profile()
        .map_err(SchemaError::InvalidData)?;
    if !matches!(
        profile,
        FieldRangeProfile::TypedV1 {
            scalar: RangeStorageProfile::StringUtf8V1
        }
    ) {
        return Err(SchemaError::InvalidData(
            "range TTL requires a String range with the 20-digit timestamp prefix".into(),
        ));
    }
    Ok(())
}

impl SchemaStore {
    pub(super) async fn require_retention_format(
        &self,
        schema_name: &str,
    ) -> Result<(), SchemaError> {
        let schema = self
            .get_schema(schema_name)
            .await?
            .ok_or_else(|| SchemaError::NotFound(schema_name.to_owned()))?;
        require_typed_range_retention(&schema)
    }
}
