//! One used membership is a typed core definition with exact invalidation.

use super::*;
use crate::storage::disk_pack::{core_memory_get, core_memory_insert, core_memory_sources};

static MEMBERSHIP_MEMORY: u8 = 0;

impl DbCatalogStore {
    pub(super) async fn get_entry_from_memory(
        &self,
        key: &str,
    ) -> StorageResult<Option<DbCatalogEntry>> {
        if !crate::storage::disk_pack::read_snapshot_active(&self.runtime) {
            return self.entries.get_item(key).await;
        }
        let owner = std::ptr::from_ref(&MEMBERSHIP_MEMORY) as usize;
        if let Some(entry) = core_memory_get::<Option<DbCatalogEntry>>(&self.runtime, owner, key)? {
            return Ok(entry.as_ref().as_ref().cloned());
        }
        let entry: Option<DbCatalogEntry> = self.entries.get_item(key).await?;
        let bytes = entry
            .as_ref()
            .map_or(0, |entry| {
                entry
                    .db_locator
                    .len()
                    .saturating_add(entry.schema_name.len())
                    .saturating_add(entry.instance_id.as_ref().map_or(0, String::len))
            })
            .saturating_mul(8)
            .saturating_add(1024);
        let retained = core_memory_insert(
            &self.runtime,
            owner,
            key.to_owned(),
            entry,
            core_memory_sources("db_catalog", key.as_bytes()),
            bytes,
        )?;
        Ok(retained.as_ref().as_ref().cloned())
    }
}
