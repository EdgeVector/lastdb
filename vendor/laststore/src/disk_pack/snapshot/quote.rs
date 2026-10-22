use super::SnapshotMutation;
use crate::Result;

/// Complete stored-operation dimensions. No body is copied or retained.
#[derive(Clone, Copy, Debug)]
pub struct SnapshotMutationShape {
    collection_bytes: usize,
    group: u16,
    key_bytes: usize,
    body_bytes: Option<u64>,
}

impl SnapshotMutationShape {
    /// A put with an empty body differs from a tombstone (`None`).
    pub fn new(
        collection_bytes: usize,
        group: u16,
        key_bytes: usize,
        body_bytes: Option<u64>,
    ) -> Self {
        Self {
            collection_bytes,
            group,
            key_bytes,
            body_bytes,
        }
    }

    pub(super) fn encoded_bytes(self) -> Result<u64> {
        if self.collection_bytes > super::super::KEY_BYTES - 4
            || self.key_bytes > super::super::KEY_BYTES
            || self.group >= 1024
            || self
                .body_bytes
                .is_some_and(|bytes| bytes > u64::from(u32::MAX))
        {
            return Err(super::refuse(
                "unrepresentable collection, key, group, or body",
            ));
        }
        let prefix = 64 + self.collection_bytes as u64 + self.key_bytes as u64;
        let payload = self
            .body_bytes
            .map_or(0, |bytes| 96 + self.key_bytes as u64 + bytes);
        prefix
            .checked_add(payload)
            .ok_or_else(|| super::super::corrupt("operation length overflow"))
    }
}

/// Checked payload-only quota. Directory, logical-record, and disk-space
/// reservations are separate prerequisites before a memory acknowledgement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotQuote {
    pub(super) operations: usize,
    pub(super) logical_bytes: u64,
    pub(super) physical_reserve: u64,
}

impl SnapshotQuote {
    /// Extend dimensions without revisiting an admitted operation or body.
    pub(in crate::disk_pack) fn extended(self, incoming: Self, limit: usize) -> Result<Self> {
        let operations = self
            .operations
            .checked_add(incoming.operations)
            .filter(|count| *count <= limit)
            .ok_or_else(|| super::capacity("batch operation admission limit"))?;
        let logical_bytes = self
            .logical_bytes
            .checked_add(incoming.logical_bytes)
            .filter(|bytes| *bytes <= super::MAX_BATCH_BYTES)
            .ok_or_else(|| super::capacity("batch byte admission limit"))?;
        Ok(Self {
            operations,
            logical_bytes,
            physical_reserve: reserve(logical_bytes)?,
        })
    }

    /// Quote one complete batch from dimensions available before ownership moves.
    pub fn from_shapes(shapes: &[SnapshotMutationShape]) -> Result<Self> {
        Self::from_iter(shapes.iter().copied())
    }

    pub(super) fn from_iter(
        shapes: impl IntoIterator<Item = SnapshotMutationShape>,
    ) -> Result<Self> {
        Self::from_iter_with_limit(shapes, crate::disk_pack::LEGACY_BATCH_OPERATIONS)
    }

    pub(in crate::disk_pack) fn from_iter_with_limit(
        shapes: impl IntoIterator<Item = SnapshotMutationShape>,
        operation_limit: usize,
    ) -> Result<Self> {
        if ![
            crate::disk_pack::LEGACY_BATCH_OPERATIONS,
            crate::disk_pack::LARGE_BATCH_OPERATIONS,
        ]
        .contains(&operation_limit)
        {
            return Err(super::refuse("unsupported operation admission limit"));
        }
        let mut length = 0u64;
        let mut operations = 0;
        for shape in shapes {
            operations += 1;
            if operations > operation_limit {
                return Err(super::refuse("batch operation admission limit"));
            }
            length = length
                .checked_add(shape.encoded_bytes()?)
                .ok_or_else(|| super::super::corrupt("batch length overflow"))?;
        }
        if operations == 0 {
            return Err(super::refuse("batch operation admission limit"));
        }
        if length > super::MAX_BATCH_BYTES {
            return Err(super::refuse("batch byte admission limit"));
        }
        Ok(Self {
            operations,
            logical_bytes: length,
            physical_reserve: reserve(length)?,
        })
    }

    /// Exact operation count for the admitted dimensions.
    pub fn operations(&self) -> usize {
        self.operations
    }
    /// Complete payload and operation dimensions before block compression.
    pub fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }
    /// Worst-case data frames, part headers, and complete block-map pages.
    pub fn physical_reserve(&self) -> u64 {
        self.physical_reserve
    }
}

impl SnapshotMutation {
    /// Borrow only the dimensions needed for a bounded admission quote.
    pub fn shape(&self) -> SnapshotMutationShape {
        SnapshotMutationShape::new(
            self.collection.len(),
            self.group,
            self.key.len(),
            self.body.as_ref().map(|body| body.len() as u64),
        )
    }
}

fn reserve(length: u64) -> Result<u64> {
    let blocks = length.div_ceil(4096);
    length
        .checked_add(blocks * 80)
        .and_then(|n| n.checked_add(blocks.div_ceil(2008) * 128))
        .and_then(|n| n.checked_add(blocks.div_ceil(677) * 16_384))
        .ok_or_else(|| super::super::corrupt("snapshot payload reserve overflow"))
}
