//! Establish one complete schema contract before its native publication.

use super::{
    contracts::TYPED_METADATA_VERSION, make_wrap, metadata_key, schema_molecule_uuids,
    validate_identity, wrap_key_for, BundleMetadata, ConflictKeyIdentity, KeyAuthority,
    MoleculeKeyBundle, MoleculeKeyStore, StorageError, StorageResult, NODE_DOMAIN,
};
use crate::schema::Schema;
use crate::storage::KvMutation;
use schema_types::molecule_range_profile::MoleculeRangeProfile;

impl MoleculeKeyStore {
    pub(super) async fn establish_schema(&self, schema: &Schema) -> StorageResult<()> {
        let expected = self.schema_profile(schema)?;
        if !self.is_enabled() {
            return Ok(());
        }
        let populated;
        let schema = if schema.runtime_fields.is_empty() {
            populated = {
                let mut populated = schema.clone();
                populated.populate_runtime_fields().map_err(|error| {
                    StorageError::InvalidOperation(format!("resolve schema molecule keys: {error}"))
                })?;
                populated
            };
            &populated
        } else {
            schema
        };
        let molecules = schema_molecule_uuids(schema);
        if self.missing_contracts(&molecules, expected)?.is_empty() {
            return Ok(());
        }
        self.load_existing(&molecules).await?;
        if self.missing_contracts(&molecules, expected)?.is_empty() {
            return Ok(());
        }
        // Hot schema writes use only admitted memory contracts. Only an actual
        // mint takes the native write gate, then the shared mint lock.
        crate::storage::disk_pack::transaction::scope(self.runtime.clone(), async {
            let _guard = self.mint_lock.lock().await;
            self.load_existing(&molecules).await?;
            let missing = self.missing_contracts(&molecules, expected)?;
            self.publish_new_bundles(missing, expected).await
        })
        .await
    }

    fn missing_contracts(
        &self,
        molecules: &[String],
        expected: MoleculeRangeProfile,
    ) -> StorageResult<Vec<String>> {
        let mut missing = Vec::new();
        for molecule in molecules {
            validate_identity(molecule, NODE_DOMAIN)?;
            match self.cached_contract(molecule)? {
                Some(contract) if contract.range_profile != expected => {
                    return Err(StorageError::InvalidOperation(
                        "the schema range type differs from the stored molecule profile".into(),
                    ));
                }
                Some(_) => {}
                None => missing.push(molecule.clone()),
            }
        }
        Ok(missing)
    }

    async fn publish_new_bundles(
        &self,
        molecules: Vec<String>,
        profile: MoleculeRangeProfile,
    ) -> StorageResult<()> {
        if molecules.is_empty() {
            return Ok(());
        }
        let key = self.node_wrap_key.as_ref().ok_or_else(|| {
            StorageError::InvalidOperation("molecule keys require an account wrapping key".into())
        })?;
        let mut rows = Vec::with_capacity(molecules.len().saturating_mul(2));
        let mut bundles = Vec::with_capacity(molecules.len());
        for molecule in molecules {
            let bundle = MoleculeKeyBundle::generate();
            let wrap = make_wrap(&molecule, NODE_DOMAIN, &bundle, key)?;
            let metadata = BundleMetadata {
                version: TYPED_METADATA_VERSION,
                molecule_uuid: molecule.clone(),
                range_profile: Some(profile),
                key_authority: Some(KeyAuthority::Bundle),
                conflict_identity: Some(ConflictKeyIdentity::StoredV1),
            };
            rows.push(KvMutation::put(
                wrap_key_for(&molecule, NODE_DOMAIN).as_bytes(),
                serde_json::to_vec(&wrap)
                    .map_err(|error| StorageError::SerializationError(error.to_string()))?,
            ));
            rows.push(KvMutation::put(
                metadata_key(&molecule).as_bytes(),
                serde_json::to_vec(&metadata)
                    .map_err(|error| StorageError::SerializationError(error.to_string()))?,
            ));
            bundles.push((molecule, bundle));
        }
        self.entries.inner().batch_mutate(rows).await?;
        self.entries.inner().flush().await?;
        for (molecule, bundle) in bundles {
            self.remember_contract(
                &molecule,
                profile,
                ConflictKeyIdentity::StoredV1,
                Some(bundle),
            )?;
        }
        Ok(())
    }
}
