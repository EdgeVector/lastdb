//! Existing molecule-key metadata, including confirmed absence, in core memory.

use super::{metadata_key, BundleMetadata, MoleculeKeyStore, StorageError, StorageResult};
use crate::storage::disk_pack::{core_memory_get, core_memory_insert, core_memory_sources};
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
        for ((key, molecule), metadata) in keys.into_iter().zip(molecules).zip(values) {
            if metadata
                .as_ref()
                .is_some_and(|metadata| !metadata.is_legacy_for(&molecule))
            {
                return Err(StorageError::InvalidOperation(
                    "molecule key metadata does not match its identity".into(),
                ));
            }
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

    pub(super) async fn metadata_from_memory(
        &self,
        molecule_uuid: &str,
    ) -> StorageResult<Option<BundleMetadata>> {
        let key = metadata_key(molecule_uuid);
        let owner = Arc::as_ptr(&self.cache) as usize;
        if let Some(value) =
            core_memory_get::<Option<BundleMetadata>>(self.runtime.clone(), owner, &key)?
        {
            return Ok(value.as_ref().as_ref().cloned());
        }
        let metadata = self.entries.get_item::<BundleMetadata>(&key).await?;
        if metadata
            .as_ref()
            .is_some_and(|metadata| !metadata.is_legacy_for(molecule_uuid))
        {
            return Err(StorageError::InvalidOperation(format!(
                "molecule key metadata mismatch for {molecule_uuid}"
            )));
        }
        let sources = core_memory_sources("molecule_keys", key.as_bytes());
        let bytes = metadata.as_ref().map_or(256, |value| {
            value.molecule_uuid.capacity().saturating_add(256)
        });
        let held = core_memory_insert(self.runtime.clone(), owner, key, metadata, sources, bytes)?;
        Ok(held.as_ref().as_ref().cloned())
    }
}
