//! Used records over the actual disk view.
//!
//! The caller holds the publication lock before it locks this set. Disk I/O
//! never runs under the set lock. A home keeps one decoded record
//! for each used record key (decision D2); there are no field paths.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

mod bytes;
mod charge;
mod lifecycle;
mod records;

pub use bytes::CoreWarmByteStats;
pub(crate) use records::RecordCoordinate;

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

/// Decoded warm records in recency order.
#[derive(Default)]
pub(crate) struct CoreWarmSet {
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
