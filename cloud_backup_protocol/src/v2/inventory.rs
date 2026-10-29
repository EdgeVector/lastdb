//! Finite receipt inventory. Transport metadata is outside the object totals.
use super::{hash, limits, transfer::ObjectReceipt, Version};
use crate::{CloudScope, DecimalU64, Digest, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// One sorted page of object receipts in an account scope.
pub struct InventoryPage {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Account id and account generation.
    pub scope: CloudScope,
    /// Zero-based page index. Validation rejects an index at or above MAX_PAGES.
    pub page_index: u32,
    /// Receipts on this page, strictly increasing by receipt id and in the page scope.
    pub objects: Vec<ObjectReceipt>,
}

impl InventoryPage {
    /// Reject a bad scope, a bad page index, an empty or oversized page, an out-of-order receipt, or oversized JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        if self.page_index as usize >= limits::MAX_PAGES
            || self.objects.is_empty()
            || self.objects.len() > limits::MAX_PAGE_ENTRIES
        {
            return Err(ProtocolError::limit("v2.inventory_page.entries"));
        }
        for (offset, receipt) in self.objects.iter().enumerate() {
            receipt.validate()?;
            if receipt.scope != self.scope
                || (offset > 0 && self.objects[offset - 1].receipt_id >= receipt.receipt_id)
            {
                return Err(ProtocolError::invalid("v2.inventory_page.order_scope"));
            }
        }
        limits::encoded(self, limits::INVENTORY_JSON_MAX_BYTES)
    }

    /// Summarize this page: canonical digest, JSON length, receipt count, stored bytes, and first and last receipt ids.
    pub fn descriptor(&self) -> Result<InventoryPageDescriptor, ProtocolError> {
        self.validate()?;
        let bytes = hash::canonical_bytes(self)?;
        let stored_bytes = self.objects.iter().try_fold(0u64, |sum, receipt| {
            sum.checked_add(receipt.object.bytes.get())
                .ok_or_else(|| ProtocolError::limit("v2.inventory_page.stored_bytes"))
        })?;
        Ok(InventoryPageDescriptor {
            page_index: self.page_index,
            sha256: hash::stored_bytes(&bytes)?,
            bytes: DecimalU64::new(bytes.len() as u64),
            object_count: DecimalU64::new(self.objects.len() as u64),
            stored_bytes: DecimalU64::new(stored_bytes),
            first_receipt: self.objects[0].receipt_id.clone(),
            last_receipt: self.objects[self.objects.len() - 1].receipt_id.clone(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Digest and bounds of one inventory page, without the page's receipts.
pub struct InventoryPageDescriptor {
    /// Zero-based page index. Validation rejects an index at or above MAX_PAGES.
    pub page_index: u32,
    /// SHA-256 of the page's canonical JSON.
    pub sha256: Digest,
    /// Length of that canonical JSON. Zero and values above the inventory cap are rejected.
    pub bytes: DecimalU64,
    /// Number of object receipts covered here.
    pub object_count: DecimalU64,
    /// Sum of stored object bytes. This excludes transport metadata bytes.
    pub stored_bytes: DecimalU64,
    /// Lowest receipt id on the page.
    pub first_receipt: Digest,
    /// Highest receipt id on the page.
    pub last_receipt: Digest,
}

impl InventoryPageDescriptor {
    /// Reject a bad index, byte count, object count, or a first/last receipt pair that disagrees with the count.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.page_index as usize >= limits::MAX_PAGES
            || self.bytes.get() == 0
            || self.bytes.get() > limits::INVENTORY_JSON_MAX_BYTES
            || self.object_count.get() == 0
            || self.object_count.get() > limits::MAX_PAGE_ENTRIES as u64
            || self.stored_bytes.get() < self.object_count.get()
            || self.first_receipt > self.last_receipt
            || ((self.first_receipt == self.last_receipt) != (self.object_count.get() == 1))
        {
            return Err(ProtocolError::invalid("v2.inventory_page_descriptor"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Ordered page summaries for one account scope.
pub struct InventoryIndex {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Account id and account generation.
    pub scope: CloudScope,
    /// Page summaries. Indexes must be 0..len-1 and receipt ranges must move forward.
    pub pages: Vec<InventoryPageDescriptor>,
}

impl InventoryIndex {
    /// Reject a bad scope, an empty or oversized page list, a bad page, or overlapping receipt ranges.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        if self.pages.is_empty() || self.pages.len() > limits::MAX_PAGES {
            return Err(ProtocolError::limit("v2.inventory_index.pages"));
        }
        for (offset, page) in self.pages.iter().enumerate() {
            page.validate()?;
            if page.page_index as usize != offset
                || (offset > 0 && self.pages[offset - 1].last_receipt >= page.first_receipt)
            {
                return Err(ProtocolError::invalid("v2.inventory_index.order"));
            }
        }
        limits::encoded(self, limits::INVENTORY_JSON_MAX_BYTES)
    }

    /// Hash the canonical index and sum page object counts and stored bytes.
    pub fn root(&self) -> Result<InventoryRoot, ProtocolError> {
        self.validate()?;
        let (count, stored_bytes) =
            self.pages
                .iter()
                .try_fold((0u64, 0u64), |(count, bytes), page| {
                    Ok::<_, ProtocolError>((
                        count.checked_add(page.object_count.get()).ok_or_else(|| {
                            ProtocolError::limit("v2.inventory_index.object_count")
                        })?,
                        bytes.checked_add(page.stored_bytes.get()).ok_or_else(|| {
                            ProtocolError::limit("v2.inventory_index.stored_bytes")
                        })?,
                    ))
                })?;
        let root = InventoryRoot {
            sha256: hash::stored_bytes(&hash::canonical_bytes(self)?)?,
            page_count: DecimalU64::new(self.pages.len() as u64),
            object_count: DecimalU64::new(count),
            stored_bytes: DecimalU64::new(stored_bytes),
        };
        root.validate()?;
        Ok(root)
    }

    /// Require `page` to sit at `page_index` in this index and match that slot's descriptor.
    pub fn validate_page(&self, page: &InventoryPage) -> Result<(), ProtocolError> {
        self.validate()?;
        if page.scope != self.scope
            || self.pages.get(page.page_index as usize) != Some(&page.descriptor()?)
        {
            return Err(ProtocolError::invalid("v2.inventory_index.page"));
        }
        Ok(())
    }
}

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
            .checked_mul(limits::MAX_PAGE_ENTRIES as u64)
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

mod proof;
pub use proof::{InventoryProof, InventoryVerifier};
