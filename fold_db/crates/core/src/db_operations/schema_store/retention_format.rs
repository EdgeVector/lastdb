//! Preserve the existing timestamp-prefix TTL rule across typed conversion.

use super::{Schema, SchemaError, SchemaStore};
use crate::schema::types::schema::DeclarativeSchemaType;
use schema_types::{key_comparison::KeyOrderVersion, range_storage_profile::RangeStorageProfile};

pub(crate) fn require_typed_range_retention(schema: &Schema) -> Result<(), SchemaError> {
    if !matches!(
        schema.schema_type,
        DeclarativeSchemaType::Range | DeclarativeSchemaType::HashRange
    ) {
        return Ok(());
    }
    let path = schema
        .key
        .as_ref()
        .and_then(|key| key.range_field.as_deref())
        .ok_or_else(|| {
            SchemaError::InvalidData("range TTL requires a declared range field".into())
        })?;
    if RangeStorageProfile::typed(&schema.key_field_type(path)).map_err(SchemaError::InvalidData)?
        != RangeStorageProfile::StringUtf8V1
    {
        return Err(SchemaError::InvalidData(
            "range TTL requires a String range with the existing 20-digit timestamp prefix; this typed scalar needs an explicit retention-policy conversion".into(),
        ));
    }
    Ok(())
}

impl SchemaStore {
    pub(super) async fn require_retention_format(
        &self,
        schema_name: &str,
    ) -> Result<(), SchemaError> {
        if self
            .runtime
            .as_ref()
            .is_none_or(|runtime| runtime.range_order() != KeyOrderVersion::TypedScalarV1)
        {
            return Ok(());
        }
        let schema = self
            .get_schema(schema_name)
            .await?
            .ok_or_else(|| SchemaError::NotFound(schema_name.to_owned()))?;
        require_typed_range_retention(&schema)
    }
}
