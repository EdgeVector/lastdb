//! Finite public receipt inventory. The encrypted manifest index is separate.

use crate::limits::{encoded_json_bytes, MAX_PAGE_ENTRIES, MAX_REFERENCE_PAGE_BYTES};
use crate::transfer::ObjectReceipt;
use crate::{
    bytes_sha256, canonical_bytes, CloudScope, DecimalU64, Digest, ObjectKind, ProtocolError,
    ReferenceRoot, Version,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferencePage {
    pub version: Version,
    pub scope: CloudScope,
    pub page_index: u32,
    /// Strictly increasing receipt identities; each receipt appears once.
    pub objects: Vec<ObjectReceipt>,
}

impl ReferencePage {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        if self.page_index >= 1024
            || self.objects.is_empty()
            || self.objects.len() as u64 > MAX_PAGE_ENTRIES
        {
            return Err(ProtocolError::limit("reference_page.entries"));
        }
        for (offset, object) in self.objects.iter().enumerate() {
            object.validate()?;
            if object.scope != self.scope
                || object.object.kind == ObjectKind::ManifestIndex
                || (offset > 0 && self.objects[offset - 1].receipt_id >= object.receipt_id)
            {
                return Err(ProtocolError::invalid("reference_page.objects"));
            }
        }
        encoded_json_bytes(self, MAX_REFERENCE_PAGE_BYTES)?;
        Ok(())
    }

    pub fn descriptor(&self) -> Result<ReferencePageDescriptor, ProtocolError> {
        self.validate()?;
        let bytes = canonical_bytes(self)?;
        let stored_bytes = self.objects.iter().try_fold(0u64, |sum, receipt| {
            sum.checked_add(receipt.object.bytes.get())
                .ok_or_else(|| ProtocolError::limit("reference_page.stored_bytes"))
        })?;
        Ok(ReferencePageDescriptor {
            page_index: self.page_index,
            sha256: bytes_sha256(&bytes),
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
pub struct ReferencePageDescriptor {
    pub page_index: u32,
    pub sha256: Digest,
    pub bytes: DecimalU64,
    pub object_count: DecimalU64,
    /// Sum of receipt object bytes; excludes public reference metadata bytes.
    pub stored_bytes: DecimalU64,
    pub first_receipt: Digest,
    pub last_receipt: Digest,
}

impl ReferencePageDescriptor {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.page_index >= 1024
            || self.bytes.get() == 0
            || self.bytes.get() > MAX_REFERENCE_PAGE_BYTES
            || self.object_count.get() == 0
            || self.object_count.get() > MAX_PAGE_ENTRIES
            || self.stored_bytes.get() < self.object_count.get()
            || self.stored_bytes.get() > self.object_count.get() * crate::limits::MAX_OBJECT_BYTES
            || self.first_receipt > self.last_receipt
            || ((self.first_receipt == self.last_receipt) != (self.object_count.get() == 1))
        {
            return Err(ProtocolError::invalid("reference_page_descriptor"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceIndex {
    pub version: Version,
    pub scope: CloudScope,
    pub pages: Vec<ReferencePageDescriptor>,
}

impl ReferenceIndex {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        if self.pages.is_empty() || self.pages.len() > 1024 {
            return Err(ProtocolError::limit("reference_index.pages"));
        }
        for (offset, page) in self.pages.iter().enumerate() {
            page.validate()?;
            if page.page_index as usize != offset
                || (offset > 0 && self.pages[offset - 1].last_receipt >= page.first_receipt)
            {
                return Err(ProtocolError::invalid("reference_index.page_order"));
            }
        }
        encoded_json_bytes(self, MAX_REFERENCE_PAGE_BYTES)?;
        Ok(())
    }

    pub fn root(&self) -> Result<ReferenceRoot, ProtocolError> {
        self.validate()?;
        let (count, bytes) = self
            .pages
            .iter()
            .try_fold((0u64, 0u64), |(count, bytes), page| {
                Ok::<_, ProtocolError>((
                    count
                        .checked_add(page.object_count.get())
                        .ok_or_else(|| ProtocolError::limit("reference_index.object_count"))?,
                    bytes
                        .checked_add(page.stored_bytes.get())
                        .ok_or_else(|| ProtocolError::limit("reference_index.stored_bytes"))?,
                ))
            })?;
        let root = ReferenceRoot {
            sha256: bytes_sha256(&canonical_bytes(self)?),
            page_count: DecimalU64::new(self.pages.len() as u64),
            object_count: DecimalU64::new(count),
            stored_bytes: DecimalU64::new(bytes),
        };
        root.validate()?;
        Ok(root)
    }

    pub fn validate_page(&self, page: &ReferencePage) -> Result<(), ProtocolError> {
        self.validate()?;
        let actual = page.descriptor()?;
        if page.scope != self.scope || self.pages.get(page.page_index as usize) != Some(&actual) {
            return Err(ProtocolError::invalid("reference_index.page"));
        }
        Ok(())
    }
}
