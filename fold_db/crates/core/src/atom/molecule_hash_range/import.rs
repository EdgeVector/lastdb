//! Imported (replayed or author-clocked) tip writes and the conflict pairs
//! they leave for the recorder.

use super::MoleculeHashRange;
use crate::atom::{now_nanos, AtomEntry, Provenance};
use chrono::Utc;

/// The signed origin of one tip write, for a protein sibling tip.
#[derive(Debug, Clone)]
pub struct TipAuthor {
    pub written_at: u64,
    pub logical_counter: u64,
    /// LWW device: the signer key of the author clock.
    pub device_id: String,
    pub mutation_uuid: String,
    /// Sync device that wrote the mutation. Empty when it is not known.
    pub origin_device: String,
}

/// One imported tip write. `device_id` is the LWW device and `origin_device`
/// is the sync device; see [`AtomEntry::origin_device`].
#[derive(Debug, Clone, Default)]
pub struct ImportedTip {
    pub atom_uuid: String,
    pub device_id: String,
    pub origin_device: String,
    pub writer_pubkey: String,
    pub signature: String,
    pub signature_version: u8,
    pub written_at: Option<u64>,
    pub logical_counter: u64,
    pub mutation_uuid: String,
}

/// How a tip decision treated the other device's write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TipConflictKind {
    /// The incoming write won and replaced a different atom at the tip.
    Replaced,
    /// The incoming write lost to the tip. It never became the tip.
    Dropped,
}

/// A tip decision between two writes of different atoms that came from
/// different sync devices. The persist step records it before it writes the tip.
#[derive(Debug, Clone)]
pub struct PendingTipConflict {
    pub hash: String,
    pub range: String,
    pub kind: TipConflictKind,
    /// The entry that is the tip after the decision.
    pub winner: AtomEntry,
    /// The entry that lost: the replaced tip, or the dropped incoming write.
    pub loser: AtomEntry,
}

/// A pair is a candidate only when the incoming write names a sync device and
/// that device did not write the tip. A write from the same device is in
/// sequence with the tip, so it is not a conflict.
fn may_conflict(incoming: &AtomEntry, tip: &AtomEntry) -> bool {
    !incoming.origin_device.is_empty() && incoming.origin_device != tip.origin_device
}

impl MoleculeHashRange {
    /// Import a tip while preserving its signed author-clock identity.
    ///
    /// Last-writer-wins decides the tip. A decision between different atoms
    /// from different sync devices is also kept in `pending_tip_conflicts`.
    pub fn set_atom_uuid_from_values_imported_with_author(
        &mut self,
        hash_value: String,
        range_value: String,
        tip: ImportedTip,
    ) {
        if self.get_atom_uuid(&hash_value, &range_value) == Some(&tip.atom_uuid) {
            return;
        }
        let written_at = tip.written_at.unwrap_or_else(now_nanos);
        // Imported tips may carry an external signature for data_share verify.
        // Local mutation path uses AtomEntry::thin only (no per-tip crypto).
        let provenance = if tip.signature_version > 0 {
            Some(Provenance::User {
                pubkey: tip.writer_pubkey.clone(),
                signature: tip.signature.clone(),
                signature_version: tip.signature_version,
            })
        } else {
            None
        };
        let mut entry = AtomEntry {
            atom_uuid: tip.atom_uuid,
            written_at,
            logical_counter: tip.logical_counter,
            mutation_uuid: tip.mutation_uuid,
            writer_pubkey: tip.writer_pubkey,
            signature: tip.signature,
            signature_version: tip.signature_version,
            provenance,
            device_id: tip.device_id,
            origin_device: tip.origin_device,
            prev_tip_id: String::new(),
        };
        let mut replaced = None;
        if let Some(existing) = self.get_atom_entry(&hash_value, &range_value) {
            // Pre-clock #1556 envelopes deserialize written_at=0. Treat that as
            // "no origin clock": last-write-wins among zeros so bootstrap of
            // several updates to one key cannot lose on atom-uuid lex order,
            // but still lose to any real (non-zero) tip.
            let legacy_zero_clock =
                written_at == 0 && entry.logical_counter == 0 && entry.mutation_uuid.is_empty();
            let incoming_wins = if legacy_zero_clock {
                existing.written_at == 0
            } else {
                crate::atom::incoming_wins_lww(
                    crate::atom::lww_order_key(
                        written_at,
                        entry.logical_counter,
                        entry.device_id.as_str(),
                        entry.mutation_uuid.as_str(),
                        entry.atom_uuid.as_str(),
                    ),
                    existing.lww_key(),
                )
            };
            // A zero-clock pair is in doubt, so it is queued like any other.
            let queued = may_conflict(&entry, existing);
            if !incoming_wins {
                if queued {
                    let conflict = (TipConflictKind::Dropped, existing.clone(), entry);
                    self.push_tip_conflict(&hash_value, &range_value, conflict);
                }
                return;
            }
            if queued {
                replaced = Some(existing.clone());
            }
        }
        entry.prev_tip_id = self.archive_current_tip(&hash_value, &range_value, &entry.atom_uuid);
        self.version += 1;
        if let Some(replaced) = replaced {
            let conflict = (TipConflictKind::Replaced, entry.clone(), replaced);
            self.push_tip_conflict(&hash_value, &range_value, conflict);
        }
        self.atom_uuids
            .entry(hash_value)
            .or_default()
            .insert(range_value, entry);
        self.updated_at = Utc::now();
    }

    /// Queue one pair `(kind, winner, loser)` for the recorder.
    fn push_tip_conflict(
        &mut self,
        hash: &str,
        range: &str,
        (kind, winner, loser): (TipConflictKind, AtomEntry, AtomEntry),
    ) {
        self.pending_tip_conflicts.push(PendingTipConflict {
            hash: hash.to_string(),
            range: range.to_string(),
            kind,
            winner,
            loser,
        });
    }

    /// Pairs that the recorder has not written yet.
    #[must_use]
    pub(crate) fn pending_tip_conflicts(&self) -> &[PendingTipConflict] {
        &self.pending_tip_conflicts
    }

    /// Stamp the sync device on the tip at `(hash, range)` when the tip names
    /// none. A local write calls this after it sets the tip. A tip that names
    /// a device keeps it: an unchanged tip may have come from another device.
    pub fn stamp_tip_origin_device(&mut self, hash: &str, range: &str, origin_device: &str) {
        if origin_device.is_empty() {
            return;
        }
        if let Some(entry) = self
            .atom_uuids
            .get_mut(hash)
            .and_then(|ranges| ranges.get_mut(range))
        {
            if entry.origin_device.is_empty() {
                entry.origin_device = origin_device.to_string();
            }
        }
    }
}
