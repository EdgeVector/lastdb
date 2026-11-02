//! Used records and typed definitions over the actual disk view.
//!
//! The caller holds the publication lock before it locks this set. Disk I/O
//! never runs under the set lock. A home keeps one decoded record
//! for each used record key (decision D2); there are no field paths.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Weak};

mod bytes;
mod charge;
mod lifecycle;
mod memory;
mod model;
mod records;
mod sources;

pub use bytes::CoreWarmByteStats;
pub(crate) use memory::CoreMemorySource;
pub(crate) use memory::PreparedCoreMemory;
pub use memory::{CoreMemoryStats, CoreMemoryValue};
pub(crate) use model::{CoreModelStats, CoreModelUpdate};
pub(crate) use records::RecordCoordinate;

#[derive(Clone, Copy, Debug)]
pub(crate) struct CoreCapacity {
    pub(crate) detail: &'static str,
}

impl std::fmt::Display for CoreCapacity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.detail)
    }
}

impl std::error::Error for CoreCapacity {}

/// Counts of the warm set for the status reply.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub(crate) struct CoreWarmStats {
    /// Decoded records in the warm set.
    pub(crate) records: usize,
    pub(crate) held: usize,
    pub(crate) hits: u64,
    pub(crate) misses: u64,
    pub(crate) purged: u64,
}

/// One current model: warm records and typed definitions.
#[derive(Default)]
pub(crate) struct CoreWarmSet {
    memory: memory::CoreMemory,
    model: model::CoreModel,
    clock: u64,
    // Decoded records of a home, in recency order.
    records: records::RecordTable,
    hits: u64,
    misses: u64,
    purged: u64,
    bytes: bytes::WarmBytes,
}

impl CoreWarmSet {
    pub(crate) fn stats(&self) -> CoreWarmStats {
        CoreWarmStats {
            records: self.records.len(),
            held: self.records.held(),
            hits: self.hits,
            misses: self.misses,
            purged: self.purged,
        }
    }
}
