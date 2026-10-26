//! The node format and every schema must agree with the one stored profile.

use super::{
    schema_molecule_uuids, BundleMetadata, KeyAuthority, MoleculeKeyStore, StorageError,
    StorageResult,
};
use crate::schema::Schema;
use schema_types::key_comparison::KeyOrderVersion;
use schema_types::molecule_range_profile::MoleculeRangeProfile;

pub(super) const TYPED_METADATA_VERSION: u8 = 2;

impl MoleculeKeyStore {
    pub(super) fn is_typed(&self) -> bool {
        self.runtime
            .as_ref()
            .is_some_and(|runtime| runtime.range_order() == KeyOrderVersion::TypedScalarV1)
    }

    pub(super) fn metadata_contract(
        &self,
        molecule: &str,
        metadata: &BundleMetadata,
    ) -> StorageResult<(Option<MoleculeRangeProfile>, KeyAuthority)> {
        if !self.is_typed() {
            return if metadata.is_legacy_for(molecule) {
                Ok((None, KeyAuthority::Bundle))
            } else {
                Err(invalid())
            };
        }
        if metadata.version != TYPED_METADATA_VERSION || metadata.molecule_uuid != molecule {
            return Err(invalid());
        }
        let profile = metadata.range_profile.ok_or_else(invalid)?;
        profile.validate().map_err(|_| invalid())?;
        let authority = metadata.key_authority.ok_or_else(invalid)?;
        Ok((Some(profile), authority))
    }

    pub(super) fn validate_metadata(
        &self,
        molecule: &str,
        metadata: Option<&BundleMetadata>,
    ) -> StorageResult<()> {
        if let Some(metadata) = metadata {
            self.metadata_contract(molecule, metadata)?;
        }
        Ok(())
    }

    pub(crate) fn schema_profile(
        &self,
        schema: &Schema,
    ) -> StorageResult<Option<MoleculeRangeProfile>> {
        if !self.is_typed() {
            schema
                .validate_legacy_key_field_types()
                .map_err(|error| StorageError::InvalidOperation(error.to_string()))?;
            return Ok(None);
        }
        schema
            .validate_key_field_types()
            .map_err(StorageError::InvalidOperation)?;
        let field_type = schema
            .key
            .as_ref()
            .and_then(|key| key.range_field.as_deref())
            .map(|path| schema.key_field_type(path));
        MoleculeRangeProfile::for_field_type(field_type.as_ref())
            .map(Some)
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
        let expected = self.schema_profile(schema)?;
        for molecule in schema_molecule_uuids(schema) {
            match self.cached_contract(&molecule)? {
                Some(contract) if contract.range_profile != expected => return Err(invalid()),
                None if expected.is_some() => return Err(invalid()),
                _ => {}
            }
        }
        Ok(())
    }
}

fn invalid() -> StorageError {
    StorageError::InvalidOperation(
        "the molecule profile is missing or differs from its node/schema contract".into(),
    )
}
