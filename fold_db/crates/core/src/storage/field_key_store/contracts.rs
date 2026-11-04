//! The node format and every schema must agree with the one stored profile.

use super::{
    schema_field_key_profiles, BundleMetadata, FieldKeyStore, StorageError, StorageResult,
};
use crate::schema::Schema;
use schema_types::field_range_profile::FieldRangeProfile;

use crate::storage::format::FIELD_KEY_METADATA_VERSION as TYPED_METADATA_VERSION;

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
            .require_record_keys()
            .map_err(|error| StorageError::InvalidOperation(error.to_string()))?
            .primary
            .range_profile()
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
