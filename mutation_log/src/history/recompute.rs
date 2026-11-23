//! The recompute after a lease closing changes which entries count.

use std::collections::{BTreeMap, BTreeSet};

use crate::entry::SignedEntry;
use crate::ids::{EntryHash, NodeKey};
use crate::payload::{decode_record_payload, MaterializedRecord, RecordPosition};

use super::lease::{pos_key, PosKey};
use super::VerifiedHistory;

impl VerifiedHistory {
    /// Recompute the records that the entries in `changed` wrote. Each such
    /// record becomes the winner among the counting data entries that wrote it
    /// and the bundle writes that a non-bundle write shadowed or displaced.
    pub(super) fn recompute_written_by(&mut self, changed: &[EntryHash]) {
        let mut keys: BTreeSet<(String, String)> = BTreeSet::new();
        for hash in changed {
            let Some(SignedEntry::Data { header, body }) =
                self.accepted.get(hash).map(|meta| &meta.entry)
            else {
                continue;
            };
            let Some((namespace, payload)) = self.data_write(header, body) else {
                continue;
            };
            if let Ok(decoded) = decode_record_payload(&payload) {
                keys.insert((namespace, decoded.record_key));
            }
        }
        let excluded_at: BTreeSet<PosKey> = self
            .excluded
            .iter()
            .filter_map(|hash| self.accepted.get(hash))
            .map(|meta| pos_key(meta.position))
            .collect();
        let mut written: BTreeMap<(String, String), Vec<MaterializedRecord>> = BTreeMap::new();
        for (hash, meta) in &self.accepted {
            let SignedEntry::Data { header, body } = &meta.entry else {
                continue;
            };
            let Some((author, namespace, decoded)) =
                self.contribution(header, body, hash, meta.is_publisher)
            else {
                continue;
            };
            let key = (namespace, decoded.record_key);
            if !keys.contains(&key) {
                continue;
            }
            written.entry(key).or_default().push(MaterializedRecord {
                position: meta.position,
                author,
                entry: *hash,
                creator: author,
                creator_position: meta.position,
                value: decoded.value,
            });
        }
        let mut outcome: Vec<((String, String), Option<MaterializedRecord>)> = Vec::new();
        for key in keys {
            let mut candidates = written.remove(&key).unwrap_or_default();
            if let Some(floor) = self.bundle_floor.get(&key) {
                candidates.push(floor.clone());
            }
            if let Some(held) = self.records.get(&key) {
                if self.is_bundle(&held.entry) {
                    candidates.push(held.clone());
                }
            }
            let mut winner: Option<&MaterializedRecord> = None;
            for candidate in &candidates {
                if winner.is_none_or(|w| candidate.position.beats(w.position)) {
                    winner = Some(candidate);
                }
            }
            let mut creator: Option<(NodeKey, RecordPosition)> = None;
            for candidate in &candidates {
                let (who, at) = if excluded_at.contains(&pos_key(candidate.creator_position)) {
                    (candidate.author, candidate.position)
                } else {
                    (candidate.creator, candidate.creator_position)
                };
                if creator.is_none_or(|(_, held)| pos_key(at) < pos_key(held)) {
                    creator = Some((who, at));
                }
            }
            let rebuilt = match (winner, creator) {
                (Some(win), Some((who, at))) => {
                    let mut record = win.clone();
                    record.creator = who;
                    record.creator_position = at;
                    Some(record)
                }
                _ => None,
            };
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
}
