//! Existing typed headers share the core publication and query lifetime.

use super::super::AtomStore;
use crate::schema::SchemaError;
use crate::storage::disk_pack::{core_memory_get, core_memory_insert, core_memory_sources};
use serde::{de::DeserializeOwned, Serialize};

static HEADER_MEMORY: u8 = 0;

impl AtomStore {
    /// Headers and generation selectors remain typed core control objects.
    pub(super) async fn molecule_control_from_memory<T>(
        &self,
        key: &str,
    ) -> Result<Option<T>, SchemaError>
    where
        T: DeserializeOwned + Serialize + Clone + Send + Sync + 'static,
    {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        let owner = header_memory_owner();
        if !crate::storage::disk_pack::read_snapshot_active(runtime.clone()) {
            return Ok(self.main_store.get_item::<T>(key).await?);
        }
        if let Some(value) = core_memory_get::<Option<T>>(runtime.clone(), owner, key)? {
            return Ok(value.as_ref().as_ref().cloned());
        }
        let control = self.main_store.get_item::<T>(key).await?;
        let bytes = header_control_bytes(&control)?;
        let sources = core_memory_sources("main", key.as_bytes());
        let held = core_memory_insert(runtime, owner, key.to_owned(), control, sources, bytes)?;
        Ok(held.as_ref().as_ref().cloned())
    }
}

fn header_memory_owner() -> usize {
    std::ptr::from_ref(&HEADER_MEMORY) as usize
}

/// The charge stays the encoded length times 8, plus 256.
fn header_control_bytes<T: Serialize>(value: &T) -> Result<usize, SchemaError> {
    Ok(serde_json::to_vec(value)
        .map_err(|error| SchemaError::InvalidData(error.to_string()))?
        .len()
        .saturating_mul(8)
        .saturating_add(256))
}
