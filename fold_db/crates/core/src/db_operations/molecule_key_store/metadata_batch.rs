//! Retain positive and confirmed-absent metadata under the existing core owner.

use super::{metadata_key, BundleMetadata, MoleculeKeyStore, StorageError, StorageResult};
use crate::storage::disk_pack::{core_memory_get, core_memory_insert, core_memory_sources};
use std::sync::Arc;

impl MoleculeKeyStore {
    pub(super) async fn metadata_batch_from_memory(
        &self,
        molecules: &[&str],
    ) -> StorageResult<Vec<Option<BundleMetadata>>> {
        let owner = Arc::as_ptr(&self.cache) as usize;
        let mut output = vec![None; molecules.len()];
        let mut missing = Vec::new();
        let mut keys = Vec::new();
        for (at, molecule) in molecules.iter().enumerate() {
            let key = metadata_key(molecule);
            if let Some(value) =
                core_memory_get::<Option<BundleMetadata>>(self.runtime.clone(), owner, &key)?
            {
                validate(molecule, value.as_ref().as_ref())?;
                output[at] = value.as_ref().as_ref().cloned();
            } else {
                keys.push(key);
                missing.push((at, *molecule));
            }
        }
        if keys.is_empty() {
            return Ok(output);
        }
        let metadata = self.entries.get_items::<BundleMetadata>(&keys).await?;
        if metadata.len() != missing.len() {
            return Err(StorageError::BackendError(
                "molecule metadata batch returned an incomplete result".into(),
            ));
        }
        for ((_, molecule), value) in missing.iter().zip(&metadata) {
            validate(molecule, value.as_ref())?;
        }
        for (((at, _), key), value) in missing.into_iter().zip(keys).zip(metadata) {
            let sources = core_memory_sources("molecule_keys", key.as_bytes());
            let bytes = value.as_ref().map_or(256, |metadata| {
                metadata.molecule_uuid.capacity().saturating_add(256)
            });
            let held = core_memory_insert(self.runtime.clone(), owner, key, value, sources, bytes)?;
            output[at] = held.as_ref().as_ref().cloned();
        }
        Ok(output)
    }
}

fn validate(molecule: &str, metadata: Option<&BundleMetadata>) -> StorageResult<()> {
    if metadata.is_some_and(|metadata| !metadata.is_legacy_for(molecule)) {
        return Err(StorageError::InvalidOperation(
            "molecule key metadata does not match its identity or format".into(),
        ));
    }
    Ok(())
}
