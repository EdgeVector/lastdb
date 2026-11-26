//! The recompute after a lease closing or out-of-order grant changes which entries count.

use std::collections::BTreeSet;

use crate::entry::SignedEntry;
use crate::ids::{EntryHash, NodeKey};
use crate::payload::{MaterializedRecord, RecordPosition};

use super::lease::pos_key;
use super::VerifiedHistory;

/// Lightweight metadata of one write to a record key (no value bytes).
#[derive(Clone, Copy, Debug)]
pub(super) struct WriteMeta {
    pub(super) position: RecordPosition,
    pub(super) author: NodeKey,
    pub(super) entry: EntryHash,
    pub(super) tombstone: bool,
    pub(super) from_bundle: bool,
}

impl VerifiedHistory {
    pub(super) fn record_write_meta(&mut self, key: &(String, String), meta: WriteMeta) {
        self.write_meta
            .entry(key.clone())
            .or_default()
            .insert(pos_key(meta.position), meta);
    }

    pub(super) fn is_bundle_write(&self, key: &(String, String), position: RecordPosition) -> bool {
        self.write_meta
            .get(key)
            .and_then(|map| map.get(&pos_key(position)))
            .is_some_and(|meta| meta.from_bundle)
    }

    /// Creator of the current live creation epoch for `key`: the earliest
    /// non-excluded live write after the latest non-excluded tombstone before
    /// the winner.
    pub(super) fn active_creator(
        &self,
        key: &(String, String),
    ) -> Option<(NodeKey, RecordPosition)> {
        let map = self.write_meta.get(key)?;
        let mut active = map
            .values()
            .rev()
            .filter(|meta| !self.excluded.contains(&meta.entry));
        let winner = active.next()?;
        if winner.tombstone {
            return Some((winner.author, winner.position));
        }
        let mut earliest = (winner.author, winner.position);
        for meta in active {
            if meta.tombstone {
                break;
            }
            earliest = (meta.author, meta.position);
        }
        Some(earliest)
    }

    /// Recompute the records that the entries in `changed` wrote. Each such
    /// record becomes the winner among the counting data entries that wrote it
    /// and the bundle writes that a non-bundle write shadowed or displaced.
    pub(super) fn recompute_written_by(&mut self, changed: &[EntryHash]) {
        let mut keys: BTreeSet<(String, String)> = BTreeSet::new();
        for hash in changed {
            let Some(entry) = self.accepted.get(hash).map(|meta| &meta.entry) else {
                continue;
            };
            if let Some((namespace, decoded)) = self.decode_data_entry(entry) {
                keys.insert((namespace, decoded.record_key));
            }
        }
        let mut outcome: Vec<((String, String), Option<MaterializedRecord>)> = Vec::new();
        for key in keys {
            let rebuilt = self.rebuild_key(&key);
            outcome.push((key, rebuilt));
        }
        for (key, record) in outcome {
            match record {
                Some(record) => {
                    self.records.insert(key, record);
                }
                None => {
                    self.records.remove(&key);
                }
            }
        }
    }

    fn rebuild_key(&self, key: &(String, String)) -> Option<MaterializedRecord> {
        let win = self
            .write_meta
            .get(key)?
            .values()
            .rev()
            .find(|meta| !self.excluded.contains(&meta.entry))
            .copied()?;
        let (creator, creator_position) = self.active_creator(key)?;
        let value = if win.from_bundle {
            self.records
                .get(key)
                .filter(|r| r.entry == win.entry)
                .or_else(|| self.bundle_floor.get(key).filter(|r| r.entry == win.entry))
                .map(|r| r.value.clone())?
        } else {
            let entry = &self.accepted.get(&win.entry)?.entry;
            self.decode_data_entry(entry)?.1.value
        };
        Some(MaterializedRecord {
            position: win.position,
            author: win.author,
            entry: win.entry,
            creator,
            creator_position,
            value,
        })
    }
}
