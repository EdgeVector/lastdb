//! Conservative physical publication dimensions for one complete stored batch.

use super::{HomeDescriptor, HomeMutation};
use crate::disk_pack::journal::JournalQuote;
use crate::disk_pack::{corrupt, PAGE_BYTES, PAGE_PAYLOAD};
use crate::Result;
use std::collections::BTreeMap;

/// A quote includes journal frames, worst-case map paths and splits, complete
/// generation receipts, and fixed checkpoint publications. It assumes no
/// immediate file deletion and grants no memory acknowledgement by itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MutationQuote {
    journal: JournalQuote,
    map_pages: u64,
    dependency_parts: u64,
    physical_bytes: u64,
}

impl MutationQuote {
    /// Extend a prefix from the new request's dimensions only. Repeated map
    /// and dependency bounds are conservative; fixed publication space is paid once.
    pub(super) fn extended(self, incoming: Self, limit: usize) -> Result<Self> {
        let journal = self.journal.extended(incoming.journal, limit)?;
        let map_pages = add(self.map_pages, incoming.map_pages)?;
        let dependency_parts = add(self.dependency_parts, incoming.dependency_parts)?;
        Ok(Self {
            journal,
            map_pages,
            dependency_parts,
            physical_bytes: physical_bytes(journal, map_pages)?,
        })
    }

    /// Exact journal dimensions for all original ordered operations.
    pub fn journal(&self) -> JournalQuote {
        self.journal
    }
    /// Worst-case private map pages, including new groups and key overflow.
    pub fn map_pages(&self) -> u64 {
        self.map_pages
    }
    /// Upper bound on additional directory, locator, and journal parts.
    pub fn dependency_parts(&self) -> u64 {
        self.dependency_parts
    }
    /// Complete worst-case bytes, without assuming space from future reclaim.
    pub fn physical_bytes(&self) -> u64 {
        self.physical_bytes
    }
}

impl HomeDescriptor {
    /// Quote the complete staged store batch before any append or publication.
    pub fn quote_mutation(&self, operations: &[HomeMutation]) -> Result<MutationQuote> {
        self.quote_mutation_with_limit(operations, crate::disk_pack::LEGACY_BATCH_OPERATIONS)
    }

    pub(super) fn quote_mutation_with_limit(
        &self,
        operations: &[HomeMutation],
        operation_limit: usize,
    ) -> Result<MutationQuote> {
        let journal = self.quote_with_limit(operations, operation_limit)?;
        let mut groups = BTreeMap::<(&str, u16), BTreeMap<&str, ()>>::new();
        for operation in operations {
            groups
                .entry((&operation.collection, self.group(&operation.key)?))
                .or_default()
                .insert(&operation.key, ());
        }
        let mut map_pages = 0u64;
        let mut dependency_parts = 0u64;
        let mut directory_pages = 0u64;
        for ((collection, _), keys) in groups {
            // Eight levels, two outputs at each split, and at most five
            // overflow pages per new key. Existing descriptors are reused.
            let locator_pages = keys.keys().try_fold(0u64, |pages, key| {
                add(pages, 16 + locator_overflow(key.len()))
            })?;
            // A new group can also require its initial empty locator part.
            map_pages = add(map_pages, add(locator_pages, 1)?)?;
            // Contiguous overflow chains can leave four unused page positions.
            dependency_parts = add(dependency_parts, locator_pages.div_ceil(508) + 1)?;
            directory_pages = add(directory_pages, 16 + overflow(collection.len() + 4))?;
        }
        map_pages = add(map_pages, directory_pages)?;
        dependency_parts = add(dependency_parts, directory_pages.div_ceil(508))?;
        let blocks = journal.logical_bytes().div_ceil(4096);
        dependency_parts = add(
            dependency_parts,
            blocks.div_ceil(2008) + blocks.div_ceil(677).div_ceil(512),
        )?;
        Ok(MutationQuote {
            journal,
            map_pages,
            dependency_parts,
            physical_bytes: physical_bytes(journal, map_pages)?,
        })
    }
}

fn physical_bytes(journal: JournalQuote, map_pages: u64) -> Result<u64> {
    // Publication and stream rotation each save a complete receipt.
    let receipt_pages = 1 + (65_536u64 - 150).div_ceil(201);
    let metadata_bytes = map_pages
        .checked_add(2 * receipt_pages)
        .and_then(|pages| pages.checked_mul(PAGE_BYTES as u64))
        .ok_or_else(|| corrupt("complete mutation metadata reserve overflow"))?;
    add(add(journal.physical_reserve(), metadata_bytes)?, 8 * 4096)
}

fn overflow(bytes: usize) -> u64 {
    bytes.saturating_sub(128).div_ceil(PAGE_PAYLOAD) as u64
}

fn locator_overflow(bytes: usize) -> u64 {
    if bytes <= crate::disk_pack::key_layout::INLINE_KEY_BYTES {
        0
    } else {
        overflow(bytes)
    }
}

fn add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| corrupt("complete mutation reserve overflow"))
}
