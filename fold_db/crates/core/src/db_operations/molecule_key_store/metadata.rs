//! Existing molecule-key metadata, including confirmed absence, in core memory.

use super::{metadata_key, BundleMetadata, MoleculeKeyStore, StorageError, StorageResult};
use crate::storage::disk_pack::core_memory_sources;
use std::sync::Arc;

impl MoleculeKeyStore {
    /// Stage metadata, never plaintext key material, from the final private view.
    pub(crate) async fn stage_current_metadata(
        &self,
        runtime: &crate::storage::disk_pack::PackRuntime,
        molecules: &[String],
    ) -> StorageResult<()> {
        let mut molecules = molecules.to_vec();
        molecules.sort();
        molecules.dedup();
        let keys: Vec<_> = molecules
            .iter()
            .map(|molecule| metadata_key(molecule))
            .collect();
        let values = self.entries.get_items::<BundleMetadata>(&keys).await?;
        if values.len() != keys.len() {
            return Err(StorageError::BackendError(
                "molecule metadata publication read is incomplete".into(),
            ));
        }
        for ((key, molecule), metadata) in keys.into_iter().zip(molecules).zip(values) {
            if self.is_typed() && metadata.is_none() {
                return Err(StorageError::InvalidOperation(
                    "typed molecule publication requires its range contract".into(),
                ));
            }
            self.validate_metadata(&molecule, metadata.as_ref())?;
            let bytes = metadata.as_ref().map_or(256, |value| {
                value.molecule_uuid.capacity().saturating_add(256)
            });
            crate::storage::disk_pack::transaction::stage_core_memory(
                runtime,
                Arc::as_ptr(&self.cache) as usize,
                key.clone(),
                metadata,
                core_memory_sources("molecule_keys", key.as_bytes()),
                bytes,
            )?;
        }
        Ok(())
    }
}
