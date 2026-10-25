//! Conservative physical publication dimensions for one complete stored batch.

use super::{HomeDescriptor, HomeMutation};
use crate::disk_pack::snapshot::SnapshotQuote;
use crate::disk_pack::{corrupt, PAGE_BYTES, PAGE_PAYLOAD};
use crate::Result;
use std::collections::BTreeMap;

/// A quote includes payload frames, worst-case map paths and splits, complete
/// generation receipts, and one root publication. It assumes no
/// immediate file deletion and grants no memory acknowledgement by itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MutationQuote {
    payload: SnapshotQuote,
    map_pages: u64,
    dependency_parts: u64,
    physical_bytes: u64,
}

impl MutationQuote {
    pub(in crate::disk_pack) fn locator_delta(
        operations: &[crate::disk_pack::snapshot::PreparedMutation],
        features: u64,
    ) -> Result<Self> {
        let payload = SnapshotQuote::from_iter_with_features(
            operations.iter().map(|operation| {
                crate::disk_pack::snapshot::SnapshotMutationShape::new(
                    operation.collection.len(),
                    operation.group,
                    operation.key.len(),
                    None,
                )
            }),
            features,
        )?;
        let mut groups = BTreeMap::<(&str, u16), BTreeMap<&str, ()>>::new();
        for operation in operations {
            groups
                .entry((&operation.collection, operation.group))
                .or_default()
                .insert(&operation.key, ());
        }
        quote_maps(payload, groups)
    }

    /// Extend a prefix from the new request's dimensions only. Repeated map
    /// and dependency bounds are conservative; fixed publication space is paid once.
    pub(super) fn extended(self, incoming: Self, limit: usize) -> Result<Self> {
        let payload = self.payload.extended(incoming.payload, limit)?;
        let map_pages = add(self.map_pages, incoming.map_pages)?;
        let dependency_parts = add(self.dependency_parts, incoming.dependency_parts)?;
        Ok(Self {
            payload,
            map_pages,
            dependency_parts,
            physical_bytes: physical_bytes(payload, map_pages)?,
        })
    }

    /// Exact payload dimensions for all original ordered operations.
    pub fn payload(&self) -> SnapshotQuote {
        self.payload
    }
    /// Worst-case private map pages, including new groups and key overflow.
    pub fn map_pages(&self) -> u64 {
        self.map_pages
    }
    /// Upper bound on additional directory, locator, and payload parts.
    pub fn dependency_parts(&self) -> u64 {
        self.dependency_parts
    }
    /// Complete worst-case bytes, without assuming space from future reclaim.
    pub fn physical_bytes(&self) -> u64 {
        self.physical_bytes
    }
}

impl HomeDescriptor {
    /// Quote the complete staged store batch before its snapshot commit.
    pub fn quote_mutation(&self, operations: &[HomeMutation]) -> Result<MutationQuote> {
        self.quote_mutation_with_features(operations, 0)
    }

    pub(super) fn quote_mutation_with_features(
        &self,
        operations: &[HomeMutation],
        features: u64,
    ) -> Result<MutationQuote> {
        let payload = self.quote_with_features(operations, features)?;
        let mut groups = BTreeMap::<(&str, u16), BTreeMap<&str, ()>>::new();
        for operation in operations {
            groups
                .entry((&operation.collection, self.group(&operation.key)?))
                .or_default()
                .insert(&operation.key, ());
        }
        quote_maps(payload, groups)
    }
}

fn quote_maps(
    payload: SnapshotQuote,
    groups: BTreeMap<(&str, u16), BTreeMap<&str, ()>>,
) -> Result<MutationQuote> {
    let mut map_pages = 0u64;
    let mut dependency_parts = 0u64;
    let mut directory_pages = 0u64;
    for ((collection, _), keys) in groups {
        // Eight levels and two outputs at each split, plus the complete
        // feature-bound overflow chain. Existing descriptors are reused.
        let locator_pages = keys.keys().try_fold(0u64, |pages, key| {
            add(pages, 16 + locator_overflow(key.len()))
        })?;
        // A new group can also require its initial empty locator part.
        map_pages = add(map_pages, add(locator_pages, 1)?)?;
        // Retain the conservative allowance from contiguous legacy chains.
        dependency_parts = add(
            dependency_parts,
            locator_pages.div_ceil(crate::disk_pack::PAGES_PER_PART - 4) + 1,
        )?;
        directory_pages = add(directory_pages, 16 + overflow(collection.len() + 4))?;
    }
    map_pages = add(map_pages, directory_pages)?;
    dependency_parts = add(
        dependency_parts,
        directory_pages.div_ceil(crate::disk_pack::PAGES_PER_PART - 4),
    )?;
    let blocks = payload
        .logical_bytes()
        .div_ceil(crate::disk_pack::BLOCK_BYTES as u64);
    dependency_parts = add(
        dependency_parts,
        blocks.div_ceil(2008)
            + blocks
                .div_ceil(crate::disk_pack::MAP_ENTRIES)
                .div_ceil(crate::disk_pack::PAGES_PER_PART),
    )?;
    Ok(MutationQuote {
        payload,
        map_pages,
        dependency_parts,
        physical_bytes: physical_bytes(payload, map_pages)?,
    })
}

fn physical_bytes(payload: SnapshotQuote, map_pages: u64) -> Result<u64> {
    // Each direct commit saves one complete receipt.
    let receipt_pages = 1 + (65_536u64 - 150).div_ceil(201);
    let metadata_bytes = map_pages
        .checked_add(receipt_pages)
        .and_then(|pages| pages.checked_mul(PAGE_BYTES as u64))
        .ok_or_else(|| corrupt("complete mutation metadata reserve overflow"))?;
    add(add(payload.physical_reserve(), metadata_bytes)?, 2 * 4096)
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
