use super::{record_charge, CoreWarmSet};
use lastdb_model::record_value::StoredRecord;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub(crate) type RecordCoordinate = (String, String);

struct WarmRecord {
    record: Arc<StoredRecord>,
    bytes: usize,
    tick: u64,
    charge: usize,
}

#[derive(Default)]
pub(super) struct RecordTable {
    entries: HashMap<Arc<RecordCoordinate>, WarmRecord>,
    order: BTreeMap<u64, Arc<RecordCoordinate>>,
}

impl RecordTable {
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn held(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| Arc::strong_count(&entry.record) > 1)
            .count()
    }
}

impl CoreWarmSet {
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

    pub(crate) fn invalidate_records(&mut self, changes: &[(String, String)]) {
        if self.records.entries.is_empty() {
            return;
        }
        for coordinate in changes {
            self.remove_record(coordinate);
        }
    }

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

    pub(super) fn purge_record(&mut self, coordinate: &RecordCoordinate) -> bool {
        let held = self
            .records
            .entries
            .get(coordinate)
            .is_none_or(|entry| Arc::strong_count(&entry.record) > 1);
        !held && self.remove_record(coordinate)
    }

    fn next_tick(&mut self) -> u64 {
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
