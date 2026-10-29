//! The node format and every schema must agree with the one stored profile.

use super::{
    schema_molecule_profiles, BundleMetadata, ConflictKeyIdentity, KeyAuthority, MoleculeKeyStore,
    StorageError, StorageResult,
};
use crate::schema::Schema;
use schema_types::molecule_range_profile::MoleculeRangeProfile;

pub(super) const TYPED_METADATA_VERSION: u8 = 2;

impl MoleculeKeyStore {
    pub(super) fn metadata_contract(
        &self,
        molecule: &str,
        metadata: &BundleMetadata,
    ) -> StorageResult<MoleculeRangeProfile> {
        if metadata.version != TYPED_METADATA_VERSION
            || metadata.molecule_uuid != molecule
            || metadata.key_authority != Some(KeyAuthority::Bundle)
            || metadata.conflict_identity != Some(ConflictKeyIdentity::StoredV1)
        {
            return Err(invalid());
        }
        metadata.range_profile.ok_or_else(invalid)
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

    pub(crate) fn schema_profile(&self, schema: &Schema) -> StorageResult<MoleculeRangeProfile> {
        schema
            .validate_key_field_types()
            .map_err(StorageError::InvalidOperation)?;
        let field_type = schema
            .key
            .as_ref()
            .and_then(|key| key.range_field.as_deref())
            .map(|path| schema.key_field_type(path));
        MoleculeRangeProfile::for_field_type(field_type.as_ref())
            .map_err(StorageError::InvalidOperation)
    }

    pub(crate) fn canonical_key(
        &self,
        schema: &Schema,
        mut key: crate::schema::types::KeyValue,
    ) -> Result<crate::schema::types::KeyValue, crate::schema::SchemaError> {
        crate::schema::types::key_comparison::normalize::canonical_key(
            Some(self.schema_profile(schema)?),
            &mut key,
        )?;
        Ok(key)
    }

    pub(super) fn require_schema_profiles(&self, schema: &Schema) -> StorageResult<()> {
        for (molecule, expected) in schema_molecule_profiles(schema)? {
            match self.cached_contract(&molecule)? {
                Some(contract) if contract.range_profile == expected => {}
                _ => return Err(invalid()),
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
