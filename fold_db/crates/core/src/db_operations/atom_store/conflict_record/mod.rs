//! The tip-conflict recorder.
//!
//! LastDB resolves a write from another device by last-writer-wins. The atom
//! that loses has no tip, so garbage collection would reclaim it. This module
//! keeps both atoms of a decision between writes from different sync devices:
//! it builds the history row, the conflict row, the `mcc:` entry and the `hcu:`
//! event, and the atom references that the records hold. The planner writes
//! them in the batch of the tip.
//!
//! Three call sites decide a tip:
//!
//! 1. the in-memory decision (`MoleculeHashRange`), as a pending pair that the
//!    persist step records;
//! 2. the durable decision (`retain_durable_tip_winners`), against the stored tip;
//! 3. the legacy `mk:` replay branch.
//!
//! A pair is skipped when both writes name the same sync device: that device
//! wrote them in sequence. A pair from the in-memory or the durable decision
//! is also skipped unless the incoming write came from another device than
//! this node (`AtomStore::is_peer_write`): a local write has seen every tip
//! of this node.

mod legacy_identity;
mod rows;
mod write;

pub(crate) use rows::is_counted_record_key;

use super::types::ChangedKey;
use super::AtomStore;
use crate::atom::{AtomEntry, FieldKey, PendingTipConflict, TipConflictKind};
use crate::schema::SchemaError;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

pub(crate) use write::{Recorded, TipConflictDigest};

/// One molecule of a tip store: its uuid, the molecule, and the changed keys in API form.
pub(in super::super) type StoreMolecule<'a> = (
    &'a str,
    &'a crate::db_operations::MoleculeData,
    &'a HashSet<ChangedKey>,
);

/// One tip decision that the recorder keeps.
#[derive(Debug, Clone)]
pub(crate) struct TipConflict {
    pub(crate) molecule_uuid: String,
    /// The key for display and for the history row.
    pub(crate) field_key: FieldKey,
    /// The key as stored. The digest covers this form, so every call site
    /// that sees one decision computes one digest.
    pub(crate) storage_hash: String,
    pub(crate) storage_range: String,
    /// Historical digest input only. Retention uses the current stored slot.
    pub(crate) digest_coordinate: Option<(String, String)>,
    pub(crate) kind: TipConflictKind,
    /// The entry that is the tip after the decision.
    pub(crate) winner: AtomEntry,
    /// The replaced tip, or the dropped incoming write.
    pub(crate) loser: AtomEntry,
}

/// The durable decision of one tip row, when the stored tip held a different atom.
#[derive(Debug, Clone)]
pub(crate) struct DurableTipDecision {
    /// The final `mk:` storage key of the slot.
    pub(crate) key: String,
    pub(crate) incoming: AtomEntry,
    pub(crate) durable: AtomEntry,
    pub(crate) incoming_won: bool,
}

impl TipConflict {
    /// The write that came in: the winner of a replaced tip, the loser of a dropped one.
    pub(crate) fn incoming(&self) -> &AtomEntry {
        match self.kind {
            TipConflictKind::Replaced => &self.winner,
            TipConflictKind::Dropped => &self.loser,
        }
    }

    /// Both writes name one sync device, so that device wrote them in sequence.
    pub(crate) fn same_device(&self) -> bool {
        !self.winner.origin_device.is_empty()
            && self.winner.origin_device == self.loser.origin_device
    }

    /// The idempotent identity of this pair: the molecule, the stored key, the
    /// kind, and the atom, mutation and order of both writes. A replay of the
    /// same decision hashes to the same digest.
    pub(crate) fn digest(&self) -> TipConflictDigest {
        let mut hasher = Sha256::new();
        let kind = match self.kind {
            TipConflictKind::Replaced => "replaced",
            TipConflictKind::Dropped => "dropped",
        };
        let (hash, range) = self.digest_coordinate.as_ref().map_or(
            (self.storage_hash.as_str(), self.storage_range.as_str()),
            |(hash, range)| (hash.as_str(), range.as_str()),
        );
        for part in [
            "lastdb:tip-conflict:v1",
            self.molecule_uuid.as_str(),
            hash,
            range,
            kind,
        ] {
            hasher.update((part.len() as u64).to_be_bytes());
            hasher.update(part.as_bytes());
        }
        for entry in [&self.loser, &self.winner] {
            for part in [
                entry.atom_uuid.as_str(),
                entry.mutation_uuid.as_str(),
                entry.lww_device(),
            ] {
                hasher.update((part.len() as u64).to_be_bytes());
                hasher.update(part.as_bytes());
            }
            hasher.update(entry.written_at.to_be_bytes());
            hasher.update(entry.logical_counter.to_be_bytes());
        }
        TipConflictDigest(hasher.finalize().into())
    }
}

impl AtomStore {
    /// Sync device id of this node. Empty until a sync engine attaches.
    #[must_use]
    pub(crate) fn local_origin_device(&self) -> String {
        self.local_origin_device
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The number of record calls that wrote conflict rows so far. Read it
    /// before a scan of the conflict rows, and give it to
    /// `stamp_conflict_index` after the scan.
    pub(crate) fn conflict_record_epoch(&self) -> u64 {
        self.conflict_record_epoch
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// The durable decision of one tip row, when the recorder must see it: the
    /// stored tip holds a different atom, that tip is not from the device of
    /// the incoming write, and the pair is worth a record
    /// ([`Self::is_recordable`]).
    pub(in super::super) fn durable_tip_decision(
        &self,
        key: &str,
        incoming: &AtomEntry,
        durable: &AtomEntry,
        incoming_won: bool,
    ) -> Option<DurableTipDecision> {
        let kind = if incoming_won {
            TipConflictKind::Replaced
        } else {
            TipConflictKind::Dropped
        };
        // Cheap tests first: a local rewrite of a local tip stops here.
        let in_sequence =
            !incoming.origin_device.is_empty() && incoming.origin_device == durable.origin_device;
        (incoming.atom_uuid != durable.atom_uuid
            && !in_sequence
            && self.is_recordable(kind, incoming))
        .then(|| DurableTipDecision {
            key: key.to_string(),
            incoming: incoming.clone(),
            durable: durable.clone(),
            incoming_won,
        })
    }

    /// A write from a device that is not this node. A write with no device
    /// (a local write before the engine attached, or an import path that does
    /// not know the writer) is not a peer write. This does not allocate.
    fn is_peer_write(&self, origin_device: &str) -> bool {
        !origin_device.is_empty()
            && self
                .local_origin_device
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_str()
                != origin_device
    }

    /// Whether a pair that two devices wrote needs a record.
    ///
    /// A write that lost (`Dropped`) always does, even a local one: the
    /// author clock of a local write is above every clock that the node
    /// observed, so a local write can lose last-writer-wins only to a tip
    /// that the writer never observed (a legacy `mk:` tip, a restored tip).
    /// A write that won over a tip it had seen (`Replaced`) needs a record
    /// only when it came from a peer.
    fn is_recordable(&self, kind: TipConflictKind, incoming: &AtomEntry) -> bool {
        !incoming.origin_device.is_empty()
            && (kind == TipConflictKind::Dropped || self.is_peer_write(&incoming.origin_device))
    }

    /// Whether a tip store has any pair to record. A store without one skips
    /// the recorder call, so the hot path pays no allocation for it.
    pub(in super::super) fn has_conflicts_to_record(
        molecules: &[StoreMolecule<'_>],
        decisions: &[DurableTipDecision],
    ) -> bool {
        !decisions.is_empty()
            || molecules
                .iter()
                .any(|(_, data, _)| !data.pending_tip_conflicts().is_empty())
    }

    /// Record the conflicts of one tip store, before the caller writes the tips.
    ///
    /// The pairs come from the in-memory decisions of `molecules` (pending on
    /// each molecule) and from the durable decisions of this store. Both can
    /// name one decision. The digest makes them one record.
    pub(in super::super) async fn record_store_conflicts(
        &self,
        molecules: &[StoreMolecule<'_>],
        decisions: Vec<DurableTipDecision>,
        storage_prefix: Option<&str>,
    ) -> Result<Recorded, SchemaError> {
        let mut conflicts = Vec::new();
        for (molecule_uuid, data, _) in molecules {
            for pending in data.pending_tip_conflicts() {
                conflicts.push(self.conflict_from_pending(molecule_uuid, pending)?);
            }
        }
        let api_keys = self.api_keys_of(molecules, &decisions, storage_prefix)?;
        for decision in decisions {
            if let Some(conflict) = self.conflict_from_decision(decision, &api_keys)? {
                conflicts.push(conflict);
            }
        }
        conflicts.retain(|conflict| self.is_recordable(conflict.kind, conflict.incoming()));
        self.record_tip_conflicts(conflicts, storage_prefix).await
    }

    fn conflict_from_pending(
        &self,
        molecule_uuid: &str,
        pending: &PendingTipConflict,
    ) -> Result<TipConflict, SchemaError> {
        Ok(TipConflict {
            molecule_uuid: molecule_uuid.to_string(),
            field_key: field_key_of(&pending.hash, &pending.range),
            storage_hash: self.storage_hash(molecule_uuid, &pending.hash)?,
            storage_range: self.storage_range(molecule_uuid, &pending.range)?,
            digest_coordinate: self.historical_conflict_coordinate(
                molecule_uuid,
                Some((&pending.hash, &pending.range)),
            )?,
            kind: pending.kind,
            winner: pending.winner.clone(),
            loser: pending.loser.clone(),
        })
    }

    /// The API form of the key of each durable decision. A blind or
    /// order-preserving key codec stores the hash and the range in a form that
    /// cannot be turned back, but the store call holds the changed keys in API
    /// form. The history row names its key in API form, because the purge and
    /// the history route match it against an API key.
    fn api_keys_of(
        &self,
        molecules: &[StoreMolecule<'_>],
        decisions: &[DurableTipDecision],
        storage_prefix: Option<&str>,
    ) -> Result<HashMap<String, (String, String)>, SchemaError> {
        let mut api_keys = HashMap::new();
        if decisions.is_empty() {
            return Ok(api_keys);
        }
        for (molecule_uuid, _, changed) in molecules {
            for key in *changed {
                let base = crate::atom::molecule_key_codec::hash_range_record_key(
                    molecule_uuid,
                    &self.storage_hash(molecule_uuid, key.hash_or_empty())?,
                    &self.storage_range(molecule_uuid, key.range_or_empty())?,
                );
                api_keys.insert(
                    crate::schema::types::field::build_storage_key(storage_prefix, &base),
                    (
                        key.hash_or_empty().to_string(),
                        key.range_or_empty().to_string(),
                    ),
                );
            }
        }
        Ok(api_keys)
    }

    fn conflict_from_decision(
        &self,
        decision: DurableTipDecision,
        api_keys: &HashMap<String, (String, String)>,
    ) -> Result<Option<TipConflict>, SchemaError> {
        use crate::atom::molecule_key_codec;
        let DurableTipDecision {
            key,
            incoming,
            durable,
            incoming_won,
        } = decision;
        let base_key = key.find("mk:").map_or(key.as_str(), |at| &key[at..]);
        let (Some(molecule), Some((hash, range))) = (
            molecule_key_codec::molecule_uuid_from_storage_key(base_key),
            molecule_key_codec::decode_hash_range_any(base_key),
        ) else {
            return Ok(None);
        };
        let (kind, winner, loser) = if incoming_won {
            (TipConflictKind::Replaced, incoming, durable)
        } else {
            (TipConflictKind::Dropped, durable, incoming)
        };
        let (field_key, storage_hash, storage_range) = match api_keys.get(&key) {
            Some((api_hash, api_range)) => (
                field_key_of(api_hash, api_range),
                self.storage_hash(molecule, api_hash)?,
                self.storage_range(molecule, api_range)?,
            ),
            None => (field_key_of(&hash, &range), hash, range),
        };
        let digest_coordinate = self.historical_conflict_coordinate(
            molecule,
            api_keys
                .get(&key)
                .map(|(hash, range)| (hash.as_str(), range.as_str())),
        )?;
        Ok(Some(TipConflict {
            molecule_uuid: molecule.to_string(),
            field_key,
            storage_hash,
            storage_range,
            digest_coordinate,
            kind,
            winner,
            loser,
        }))
    }
}

/// An empty hash or range names no dimension (a `Single`, `Hash` or `Range` slot).
fn field_key_of(hash: &str, range: &str) -> FieldKey {
    FieldKey {
        hash: (!hash.is_empty()).then(|| hash.to_string()),
        range: (!range.is_empty()).then(|| range.to_string()),
    }
}
