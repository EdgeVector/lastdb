//! Share only independent molecule keys, never the node's account keys.

use super::{
    make_wrap, schema_molecule_uuids, validate_identity, wrap_key_for, MoleculeKeyStore,
    StorageError, StorageResult,
};
use crate::{schema::Schema, storage::KvMutation};

impl MoleculeKeyStore {
    /// Resolve the complete schema before one batch publishes its domain wraps.
    pub async fn grant_schema_domain(
        &self,
        schema: &Schema,
        domain: &str,
        domain_wrap_key: &[u8; 32],
    ) -> StorageResult<usize> {
        if !self.is_enabled() {
            return Ok(0);
        }
        validate_identity(&schema.name, domain)?;
        let molecules = schema_molecule_uuids(schema, self.record_home());
        self.load_existing(&molecules).await?;
        self.require_schema_profiles(schema)?;
        let mut rows = Vec::with_capacity(molecules.len());
        for molecule in &molecules {
            let bundle = self.cached_bundle(molecule)?.ok_or_else(|| {
                StorageError::InvalidOperation("schema share requires the molecule keys".into())
            })?;
            let wrap = make_wrap(molecule, domain, &bundle, domain_wrap_key)?;
            rows.push(KvMutation::put(
                wrap_key_for(molecule, domain).as_bytes(),
                serde_json::to_vec(&wrap)
                    .map_err(|error| StorageError::SerializationError(error.to_string()))?,
            ));
        }
        self.entries.inner().batch_mutate(rows).await?;
        self.entries.inner().flush().await?;
        Ok(molecules.len())
    }
}
