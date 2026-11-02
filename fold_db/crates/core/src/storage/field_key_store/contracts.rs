//! The node format and every schema must agree with the one stored profile.

use super::{
    schema_field_key_profiles, BundleMetadata, FieldKeyStore, StorageError, StorageResult,
};
use crate::schema::Schema;
use schema_types::field_range_profile::FieldRangeProfile;

pub(super) const TYPED_METADATA_VERSION: u8 = 2;

impl FieldKeyStore {
    pub(super) fn metadata_contract(
        &self,
        field_key: &str,
        metadata: &BundleMetadata,
    ) -> StorageResult<FieldRangeProfile> {
        if metadata.version != TYPED_METADATA_VERSION || metadata.field_key_id != field_key {
            return Err(invalid());
        }
        Ok(metadata.range_profile)
    }

    pub(super) fn validate_metadata(
        &self,
        field_key: &str,
        metadata: Option<&BundleMetadata>,
    ) -> StorageResult<()> {
        if let Some(metadata) = metadata {
            self.metadata_contract(field_key, metadata)?;
        }
        Ok(())
    }

    pub(crate) fn schema_profile(&self, schema: &Schema) -> StorageResult<FieldRangeProfile> {
        schema
            .validate_key_field_types()
            .map_err(StorageError::InvalidOperation)?;
        let field_type = schema
            .key
            .as_ref()
            .and_then(|key| key.range_field.as_deref())
            .map(|path| schema.key_field_type(path));
        FieldRangeProfile::for_field_type(field_type.as_ref())
            .map_err(StorageError::InvalidOperation)
    }

    pub(crate) fn canonical_key(
        &self,
        schema: &Schema,
        mut key: crate::schema::types::KeyValue,
    ) -> Result<crate::schema::types::KeyValue, crate::schema::SchemaError> {
        crate::schema::types::key_comparison::normalize::canonical_key(
            self.schema_profile(schema)?,
            &mut key,
        )?;
        Ok(key)
    }

    pub(super) fn require_schema_profiles(&self, schema: &Schema) -> StorageResult<()> {
        for (field_key, expected) in schema_field_key_profiles(schema)? {
            match self.cached_contract(&field_key)? {
                Some(contract) if contract.range_profile == expected => {}
                _ => return Err(invalid()),
            }
        }
        Ok(())
    }
}

fn invalid() -> StorageError {
    StorageError::InvalidOperation(
        "the field key profile is missing or differs from its node/schema contract".into(),
    )
}
