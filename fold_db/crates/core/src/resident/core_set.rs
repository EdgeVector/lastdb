//! Used records and typed definitions over the actual disk view.
//!
//! The caller holds the publication lock before it locks this set. Disk I/O
//! never runs under the set lock. A format 7 home keeps one decoded record
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

/// The path, tip and molecule counts stay in the status reply. A
/// format 7 home keeps no field paths, so they are always 0.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub(crate) struct CoreWarmStats {
    pub(crate) paths: usize,
    /// Decoded records of a format 7 home.
    pub(crate) records: usize,
    pub(crate) tip_only_paths: usize,
    pub(crate) molecules: usize,
    pub(crate) held: usize,
    pub(crate) dirty: usize,
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
    // Decoded records of a format 7 home, in recency order.
    records: records::RecordTable,
    hits: u64,
    misses: u64,
    purged: u64,
    bytes: bytes::WarmBytes,
}

impl CoreWarmSet {
    pub(crate) fn stats(&self) -> CoreWarmStats {
        CoreWarmStats {
            paths: 0,
            records: self.records.len(),
            tip_only_paths: 0,
            molecules: 0,
            held: self.records.held(),
            dirty: 0,
            hits: self.hits,
            misses: self.misses,
            purged: self.purged,
        }
    }
}
