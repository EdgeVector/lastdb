//! Finite receipt inventory summary. Transport metadata is outside the object totals.
use super::limits;
use crate::{DecimalU64, Digest, ProtocolError};
use serde::{Deserialize, Serialize};

const MAX_PAGE_ENTRIES: u64 = 512;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Digest and totals of one inventory index.
pub struct InventoryRoot {
    /// SHA-256 of the index's canonical JSON.
    pub sha256: Digest,
    /// Number of pages. Zero and values above `MAX_PAGES` are rejected.
    pub page_count: DecimalU64,
    /// Number of object receipts covered here.
    pub object_count: DecimalU64,
    /// Sum of stored object bytes. Transport metadata is not included.
    pub stored_bytes: DecimalU64,
}

impl InventoryRoot {
    /// Reject a bad page count, an object count outside the per-page bounds, or stored bytes below the object count.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        let pages = self.page_count.get();
        let maximum_count = pages
            .checked_mul(MAX_PAGE_ENTRIES)
            .ok_or_else(|| ProtocolError::limit("v2.inventory_root.object_count"))?;
        if pages == 0
            || pages > limits::MAX_PAGES as u64
            || self.object_count.get() < pages
            || self.object_count.get() > maximum_count
            || self.stored_bytes.get() < self.object_count.get()
        {
            return Err(ProtocolError::invalid("v2.inventory_root"));
        }
        Ok(())
    }
}
