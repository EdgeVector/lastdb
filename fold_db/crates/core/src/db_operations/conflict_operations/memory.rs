//! The existing conflict control record, including confirmed absence, stays warm.

use super::*;
use crate::storage::disk_pack::{core_memory_get, core_memory_insert, core_memory_sources};

static CONFLICT_MEMORY: u8 = 0;

impl DbOperations {
    pub(super) async fn conflict_index_from_memory(
        &self,
        storage_prefix: Option<&str>,
    ) -> Option<HomeConflictIndex> {
        let key = build_storage_key(storage_prefix, HOME_CONFLICT_INDEX_KEY);
        let runtime = self.namespaced_store().disk_pack_runtime();
        let owner = std::ptr::from_ref(&CONFLICT_MEMORY) as usize;
        if let Some(value) =
            core_memory_get::<Option<HomeConflictIndex>>(runtime.clone(), owner, &key).ok()?
        {
            return value.as_ref().as_ref().cloned();
        }
        // Preserve Unknown on unreadable or incompatible controls. Such an
        // error must not become a retained assertion of absence.
        let index = self
            .atoms()
            .raw()
            .get_item::<HomeConflictIndex>(&key)
            .await
            .ok()?;
        let bytes = serde_json::to_vec(&index)
            .ok()?
            .len()
            .saturating_mul(8)
            .saturating_add(1024);
        let held = core_memory_insert(
            runtime,
            owner,
            key.clone(),
            index,
            core_memory_sources("main", key.as_bytes()),
            bytes,
        )
        .ok()?;
        held.as_ref().as_ref().cloned()
    }
}
