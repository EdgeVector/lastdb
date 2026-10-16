//! Exact-record holds share the existing logical-record cap.

use super::*;

#[derive(Debug, Default)]
pub(super) struct RecordHolds {
    pub(super) counts: HashMap<RecordKey, u32>,
    /// Held keys with no cached body or cached absence yet.
    pub(super) unfetched: usize,
}

impl LogicalResidentSet {
    /// Reserve one exact record before its fetch or copy. A repeated key only
    /// takes another hold. A new key never exceeds the logical-record cap.
    pub fn hold_record(&mut self, collection: &str, id: &str) -> bool {
        let key = RecordKey {
            collection: collection.to_string(),
            id: id.to_string(),
        };
        if let Some(count) = self.record_holds.counts.get_mut(&key) {
            let Some(next) = count.checked_add(1) else {
                return false;
            };
            *count = next;
        } else {
            if !self.records.contains_key(&key) {
                if !self.make_read_room() {
                    return false;
                }
                self.record_holds.unfetched += 1;
            }
            self.record_holds.counts.insert(key.clone(), 1);
            self.note_hold_transition(0, 1);
        }
        self.touch(ResidentKey::Record {
            collection: key.collection,
            id: key.id,
        });
        self.publish_occupancy();
        true
    }

    /// A clean fetched value remains warm after its last call returns.
    /// An invalidated or failed fetch releases its reserved slot instead.
    pub fn release_records(&mut self, keys: impl IntoIterator<Item = (String, String)>) {
        for (collection, id) in keys {
            self.release_record(RecordKey { collection, id });
        }
        self.purge_over_cap();
    }

    fn release_record(&mut self, key: RecordKey) {
        let Some(count) = self.record_holds.counts.get_mut(&key) else {
            return;
        };
        if *count > 1 {
            *count -= 1;
            return;
        }
        self.record_holds.counts.remove(&key);
        self.note_hold_transition(1, 0);
        if !self.records.contains_key(&key) {
            self.record_holds.unfetched -= 1;
            self.forget_key(&ResidentKey::Record {
                collection: key.collection,
                id: key.id,
            });
        }
    }

    pub(super) fn record_is_held(&self, key: &RecordKey) -> bool {
        self.record_holds.counts.contains_key(key)
    }
}
