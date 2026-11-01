//! Retain positive and confirmed-absent metadata under the existing core owner.

use super::{
    metadata_key, metadata_memory_bytes, metadata_memory_owner, BundleMetadata, FieldKeyStore,
    StorageError, StorageResult,
};
use crate::storage::disk_pack::{core_memory_get, core_memory_insert, core_memory_sources};

impl FieldKeyStore {
    pub(super) async fn metadata_batch_from_memory(
        &self,
        field_keys: &[&str],
    ) -> StorageResult<Vec<Option<BundleMetadata>>> {
        let owner = metadata_memory_owner(&self.cache);
        let mut output = vec![None; field_keys.len()];
        let mut missing = Vec::new();
        let mut keys = Vec::new();
        for (at, field_key) in field_keys.iter().enumerate() {
            let key = metadata_key(field_key);
            if let Some(value) =
                core_memory_get::<Option<BundleMetadata>>(self.runtime.clone(), owner, &key)?
            {
                self.validate_metadata(field_key, value.as_ref().as_ref())?;
                output[at] = value.as_ref().as_ref().cloned();
            } else {
                keys.push(key);
                missing.push((at, *field_key));
            }
        }
        if keys.is_empty() {
            return Ok(output);
        }
        let metadata = self.entries.get_items::<BundleMetadata>(&keys).await?;
        if metadata.len() != missing.len() {
            return Err(StorageError::BackendError(
                "field key metadata batch returned an incomplete result".into(),
            ));
        }
        for ((_, field_key), value) in missing.iter().zip(&metadata) {
            self.validate_metadata(field_key, value.as_ref())?;
        }
        for (((at, _), key), value) in missing.into_iter().zip(keys).zip(metadata) {
            let sources = core_memory_sources("field_keys", key.as_bytes());
            let bytes = metadata_memory_bytes(value.as_ref());
            let held = core_memory_insert(self.runtime.clone(), owner, key, value, sources, bytes)?;
            output[at] = held.as_ref().as_ref().cloned();
        }
        Ok(output)
    }
}
