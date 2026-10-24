//! Finite receipt inventory. Transport metadata is outside the object totals.
use super::{hash, limits, transfer::ObjectReceipt, Version};
use crate::{CloudScope, DecimalU64, Digest, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryPage {
    pub version: Version,
    pub scope: CloudScope,
    pub page_index: u32,
    pub objects: Vec<ObjectReceipt>,
}

impl InventoryPage {
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
pub struct InventoryPageDescriptor {
    pub page_index: u32,
    pub sha256: Digest,
    pub bytes: DecimalU64,
    pub object_count: DecimalU64,
    /// Sum of stored object bytes. This excludes transport metadata bytes.
    pub stored_bytes: DecimalU64,
    pub first_receipt: Digest,
    pub last_receipt: Digest,
}

impl InventoryPageDescriptor {
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
pub struct InventoryIndex {
    pub version: Version,
    pub scope: CloudScope,
    pub pages: Vec<InventoryPageDescriptor>,
}

impl InventoryIndex {
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
pub struct InventoryRoot {
    pub sha256: Digest,
    pub page_count: DecimalU64,
    pub object_count: DecimalU64,
    pub stored_bytes: DecimalU64,
}

impl InventoryRoot {
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
