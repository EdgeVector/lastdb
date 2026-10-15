use super::{BatchPlan, JournalMutation};
use crate::Result;

/// Complete stored-operation dimensions. No body is copied or retained.
#[derive(Clone, Copy, Debug)]
pub struct JournalMutationShape {
    collection_bytes: usize,
    group: u16,
    key_bytes: usize,
    body_bytes: Option<u64>,
}

impl JournalMutationShape {
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

/// Checked journal-only quota. Directory, logical-record, and disk-space
/// reservations are separate prerequisites before a memory acknowledgement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JournalQuote {
    pub(super) operations: usize,
    pub(super) logical_bytes: u64,
    pub(super) physical_reserve: u64,
}

impl JournalQuote {
    /// Quote one complete batch from dimensions available before ownership moves.
    pub fn from_shapes(shapes: &[JournalMutationShape]) -> Result<Self> {
        Self::from_iter(shapes.iter().copied())
    }

    pub(super) fn from_iter(
        shapes: impl IntoIterator<Item = JournalMutationShape>,
    ) -> Result<Self> {
        let mut length = 256u64;
        let mut operations = 0;
        for shape in shapes {
            operations += 1;
            if operations > super::MAX_OPERATIONS {
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
            physical_reserve: BatchPlan::reserve(length)?,
        })
    }

    /// Exact operation count for the admitted dimensions.
    pub fn operations(&self) -> usize {
        self.operations
    }
    /// BEGIN, complete operations, and COMMIT before block compression.
    pub fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }
    /// Worst-case data frames, part headers, and complete block-map pages.
    pub fn physical_reserve(&self) -> u64 {
        self.physical_reserve
    }

    pub(super) fn from_plan(operations: usize, plan: &BatchPlan) -> Self {
        Self {
            operations,
            logical_bytes: plan.logical_bytes,
            physical_reserve: plan.physical_reserve,
        }
    }
}

impl JournalMutation {
    /// Borrow only the dimensions needed for a bounded admission quote.
    pub fn shape(&self) -> JournalMutationShape {
        JournalMutationShape::new(
            self.collection.len(),
            self.group,
            self.key.len(),
            self.body.as_ref().map(|body| body.len() as u64),
        )
    }
}
