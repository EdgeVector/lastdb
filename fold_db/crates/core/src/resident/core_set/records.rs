//! Decoded records of a home: one entry for each record key
//! (decision D2).
//!
//! A record enters after a read finds it on disk, or after a flush puts its
//! write on disk. It leaves when a later write changes its key, or when the
//! purge needs its bytes. The purge removes the least recently used record
//! first. A record that a call still holds stays.

use super::charge::record_charge;
use super::*;
use lastdb_model::record_value::StoredRecord;

/// The native coordinate of one record key: its collection and encoded key.
pub(crate) type RecordCoordinate = (String, String);

struct WarmRecord {
    record: Arc<StoredRecord>,
    /// The encoded length; a hit costs the call this read allowance.
    bytes: usize,
    tick: u64,
    charge: usize,
}

/// The key strings live once, shared by the map and the recency list.
#[derive(Default)]
pub(super) struct RecordTable {
    entries: HashMap<Arc<RecordCoordinate>, WarmRecord>,
    order: BTreeMap<u64, Arc<RecordCoordinate>>,
}

impl RecordTable {
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    /// A returned `Arc` is the call's hold; the table keeps one more.
    pub(super) fn held(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| Arc::strong_count(&entry.record) > 1)
            .count()
    }
}

impl CoreWarmSet {
    /// The memory copies of these records and their encoded lengths, in the
    /// order given. A hit becomes the most recently used entry.
    pub(crate) fn record_lookup(
        &mut self,
        coordinates: &[RecordCoordinate],
    ) -> Vec<Option<(Arc<StoredRecord>, usize)>> {
        coordinates
            .iter()
            .map(|coordinate| {
                let found = self.touch_record(coordinate);
                if found.is_some() {
                    self.hits = self.hits.saturating_add(1);
                } else {
                    self.misses = self.misses.saturating_add(1);
                }
                found
            })
            .collect()
    }

    /// Records that a read found on disk, or that a flush put on disk, become
    /// the most recent entries. A new value replaces the old one. The caller
    /// proved that each value is current under the publication boundary.
    pub(crate) fn admit_records(
        &mut self,
        records: Vec<(RecordCoordinate, Arc<StoredRecord>, usize)>,
    ) {
        if records.is_empty() {
            return;
        }
        for (coordinate, record, bytes) in records {
            self.remove_record(&coordinate);
            let charge = record_charge(&coordinate, &record);
            let tick = self.next_tick();
            let key = Arc::new(coordinate);
            self.records.order.insert(tick, Arc::clone(&key));
            self.records.entries.insert(
                key,
                WarmRecord {
                    record,
                    bytes,
                    tick,
                    charge,
                },
            );
            self.charge_record(charge);
        }
        self.trim_to_limit(0);
    }

    /// Call under the same publication boundary as the changed disk view.
    pub(super) fn invalidate_records(&mut self, changes: &[(String, String)]) {
        if self.records.entries.is_empty() {
            return;
        }
        for coordinate in changes {
            self.remove_record(coordinate);
        }
    }

    /// Up to `limit` unheld records, oldest first, with their ticks.
    pub(super) fn record_victims(&self, limit: usize) -> Vec<(u64, Arc<RecordCoordinate>)> {
        self.records
            .order
            .iter()
            .filter(|(_, key)| {
                self.records
                    .entries
                    .get::<RecordCoordinate>(key)
                    .is_some_and(|entry| Arc::strong_count(&entry.record) == 1)
            })
            .take(limit)
            .map(|(tick, key)| (*tick, Arc::clone(key)))
            .collect()
    }

    /// The purge removes only an unheld record; a call may take a hold after
    /// the victim list was made.
    pub(super) fn purge_record(&mut self, coordinate: &RecordCoordinate) -> bool {
        let held = self
            .records
            .entries
            .get(coordinate)
            .is_none_or(|entry| Arc::strong_count(&entry.record) > 1);
        !held && self.remove_record(coordinate)
    }

    /// The next recency tick. The rare wrap renumbers the records in their
    /// order, so the purge order stays the same.
    pub(super) fn next_tick(&mut self) -> u64 {
        if self.clock == u64::MAX {
            self.renumber();
        }
        self.clock += 1;
        self.clock
    }

    fn touch_record(
        &mut self,
        coordinate: &RecordCoordinate,
    ) -> Option<(Arc<StoredRecord>, usize)> {
        if !self.records.entries.contains_key(coordinate) {
            return None;
        }
        // A wrap renumbers every tick, so the entry's tick is read after it.
        let tick = self.next_tick();
        let entry = self
            .records
            .entries
            .get_mut(coordinate)
            .expect("warm record");
        let old = std::mem::replace(&mut entry.tick, tick);
        let found = (Arc::clone(&entry.record), entry.bytes);
        let key = self
            .records
            .order
            .remove(&old)
            .expect("ordered warm record");
        self.records.order.insert(tick, key);
        Some(found)
    }

    fn remove_record(&mut self, coordinate: &RecordCoordinate) -> bool {
        let Some(entry) = self.records.entries.remove(coordinate) else {
            return false;
        };
        self.records.order.remove(&entry.tick);
        self.release_record(entry.charge);
        true
    }

    fn renumber(&mut self) {
        // A `BTreeMap` iterates in tick order.
        let all = std::mem::take(&mut self.records.order);
        self.clock = 0;
        for (_, key) in all {
            self.clock += 1;
            self.records
                .entries
                .get_mut::<RecordCoordinate>(&key)
                .expect("ordered warm record")
                .tick = self.clock;
            self.records.order.insert(self.clock, key);
        }
    }
}
