//! Existing molecule-key metadata, including confirmed absence, in core memory.

use super::{
    metadata_key, BundleMetadata, MoleculeKeyStore, StorageError, StorageResult, BUNDLE_VERSION,
};
use crate::storage::disk_pack::{core_memory_get, core_memory_insert, core_memory_sources};
use std::sync::Arc;

impl MoleculeKeyStore {
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
        if metadata.as_ref().is_some_and(|metadata| {
            metadata.version != BUNDLE_VERSION || metadata.molecule_uuid != molecule_uuid
        }) {
            return Err(StorageError::InvalidOperation(format!(
                "molecule key metadata mismatch for {molecule_uuid}"
            )));
        }
        let sources = crate::kind_partition::read_forms(&key)
            .into_iter()
            .flat_map(|form| core_memory_sources("molecule_keys", form.as_bytes()))
            .collect();
        let bytes = metadata.as_ref().map_or(256, |value| {
            value.molecule_uuid.capacity().saturating_add(256)
        });
        let held = core_memory_insert(self.runtime.clone(), owner, key, metadata, sources, bytes)?;
        Ok(held.as_ref().as_ref().cloned())
    }
}
