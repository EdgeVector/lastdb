//! Bounded negative lookup hints for ids that are usually absent: exact v2 Delete
//! marker ids and the plain ids named in `storage::laststore::absence_hints`.
//!
//! These hints are not logical records. They do not enter the warm set, its
//! 10,000-record budget, or its occupancy gauges. A caller checks the marker
//! shape before it consults or admits a hint.

use std::collections::{BTreeMap, HashMap};

use super::{LogicalResidentSet, RecordKey};

const NEGATIVE_BARRIER_CAP: usize = 1_024;
// An 82-byte BoardCards range becomes a 1.6 KiB marker after OPE and hex.
// Two RecordKey copies per entry bound key text to 4 MiB, plus map overhead.
const NEGATIVE_BARRIER_MAX_KEY_BYTES: usize = 2_048;

fn admissible(collection: &str, id: &str) -> bool {
    collection.len().saturating_add(id.len()) <= NEGATIVE_BARRIER_MAX_KEY_BYTES
}

#[derive(Debug, Default)]
pub(super) struct NegativeBarrierCache {
    entries: HashMap<RecordKey, u64>,
    order: BTreeMap<u64, RecordKey>,
    clock: u64,
}

impl NegativeBarrierCache {
    fn contains(&mut self, collection: &str, id: &str) -> bool {
        let key = RecordKey {
            collection: collection.to_owned(),
            id: id.to_owned(),
        };
        if !self.entries.contains_key(&key) {
            return false;
        }
        self.touch(key);
        true
    }

    fn admit(&mut self, collection: &str, id: &str) {
        self.touch(RecordKey {
            collection: collection.to_owned(),
            id: id.to_owned(),
        });
        if self.entries.len() > NEGATIVE_BARRIER_CAP {
            if let Some((_, oldest)) = self.order.pop_first() {
                self.entries.remove(&oldest);
            }
        }
    }

    fn touch(&mut self, key: RecordKey) {
        if let Some(old) = self.entries.remove(&key) {
            self.order.remove(&old);
        }
        if self.clock == u64::MAX {
            self.clear();
        }
        self.clock += 1;
        self.order.insert(self.clock, key.clone());
        self.entries.insert(key, self.clock);
    }

    pub(super) fn remove(&mut self, collection: &str, id: &str) {
        let key = RecordKey {
            collection: collection.to_owned(),
            id: id.to_owned(),
        };
        if let Some(tick) = self.entries.remove(&key) {
            self.order.remove(&tick);
        }
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
        self.clock = 0;
    }
}

impl LogicalResidentSet {
    /// A hit means this exact id was absent at the last read and no write has touched it since.
    pub(crate) fn absent_hint(&mut self, collection: &str, id: &str) -> bool {
        admissible(collection, id) && self.negative_barriers.contains(collection, id)
    }

    /// Reject a miss if a write touched its epoch during the disk read.
    pub(crate) fn admit_absent_hint(&mut self, collection: &str, id: &str, epoch: u64) -> bool {
        if !admissible(collection, id) || self.record_epoch(id) != epoch {
            return false;
        }
        self.negative_barriers.admit(collection, id);
        true
    }
}
