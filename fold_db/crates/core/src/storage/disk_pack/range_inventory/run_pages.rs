//! Bound work per call, then resume at the same immutable root. Tombstones
//! never consume the separate count of captured live metadata records.

use super::{invalid, source::budget, InventoryLimits, StorageResult};
use crate::storage::disk_pack::map;
use laststore::disk_run::home::RunRead;

pub(super) fn visit(
    source: &RunRead,
    collection: &str,
    start: &[u8],
    end: &[u8],
    limits: InventoryLimits,
    mut visitor: impl FnMut(String, Vec<u8>) -> StorageResult<()>,
) -> StorageResult<()> {
    let mut after: Option<String> = None;
    loop {
        let mut read = source.fork(budget(limits.max_metadata_bytes.saturating_mul(2), 4096));
        let mut cursor = match after.as_ref() {
            None => read.administrative_range(collection, start, end),
            Some(key) => read.administrative_range_after(collection, key.as_bytes(), end),
        }
        .map_err(map)?;
        let page = cursor.next_page_yielding(128, 0, 0).map_err(map)?;
        let next = page
            .resume
            .or_else(|| page.records.last().map(|(key, _)| key.clone()));
        for (key, value) in page.records {
            visitor(key, value)?;
        }
        if page.exhausted {
            return Ok(());
        }
        let next = next
            .filter(|key| after.as_ref().is_none_or(|old| key > old))
            .ok_or_else(|| invalid("native inventory continuation did not advance"))?;
        after = Some(next);
    }
}
