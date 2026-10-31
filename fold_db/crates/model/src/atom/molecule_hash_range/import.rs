//! Imported (replayed or author-clocked) tip writes and the conflict pairs
//! they leave for the recorder.

use super::MoleculeHashRange;
use crate::atom::{AtomEntry, Provenance};
use chrono::{DateTime, Utc};

/// The clock values a caller read for one molecule change. The model reads no
/// clock; core builds this value from the system clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeTime {
    /// Unix nanoseconds: the write time of a local tip, and the write time of
    /// an imported tip that carries no signed time.
    pub written_at: u64,
    /// The molecule's `updated_at` after the change.
    pub updated_at: DateTime<Utc>,
}

/// One local tip write. The caller names the device from its signing key.
#[derive(Debug, Clone, Default)]
pub struct LocalTip {
    pub atom_uuid: String,
    /// LWW device: the base64 public key of the local signer.
    pub device_id: String,
    pub logical_counter: u64,
    pub mutation_uuid: String,
}

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

/// Only different atoms from different sync devices form a conflict pair.
/// Equal atoms can still carry a newer author clock or different key metadata.
fn may_conflict(incoming: &AtomEntry, tip: &AtomEntry) -> bool {
    incoming.atom_uuid != tip.atom_uuid
        && !incoming.origin_device.is_empty()
        && incoming.origin_device != tip.origin_device
}

impl MoleculeHashRange {
    /// Import a tip while preserving its signed author-clock identity.
    ///
    /// Last-writer-wins decides the full tip, even when its atom is unchanged.
    /// Different atoms from different sync devices also produce a conflict pair.
    /// Returns true only when the incoming tip becomes the new head.
    ///
    /// `time.written_at` stands in only for a tip without a signed write time.
    pub fn set_imported_tip_at(
        &mut self,
        hash_value: String,
        range_value: String,
        tip: ImportedTip,
        time: ChangeTime,
    ) -> bool {
        let written_at = tip.written_at.unwrap_or(time.written_at);
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
        let mut entry = AtomEntry::thin_with_author(
            tip.atom_uuid,
            written_at,
            tip.logical_counter,
            tip.device_id,
            tip.mutation_uuid,
            String::new(),
        )
        .with_origin_device(tip.origin_device);
        entry.set_signer(
            tip.writer_pubkey,
            tip.signature,
            tip.signature_version,
            provenance,
        );
        let mut replaced = None;
        if let Some(existing) = self.get_atom_entry(&hash_value, &range_value) {
            let incoming_wins = crate::atom::incoming_wins_lww(entry.lww_key(), existing.lww_key());
            let queued = may_conflict(&entry, existing);
            if !incoming_wins {
                if queued {
                    let conflict = (TipConflictKind::Dropped, existing.clone(), entry);
                    self.push_tip_conflict(&hash_value, &range_value, conflict);
                }
                return false;
            }
            if queued {
                replaced = Some(existing.clone());
            }
        }
        self.version += 1;
        if let Some(replaced) = replaced {
            let conflict = (TipConflictKind::Replaced, entry.clone(), replaced);
            self.push_tip_conflict(&hash_value, &range_value, conflict);
        }
        self.atom_uuids
            .entry(hash_value)
            .or_default()
            .insert(range_value, entry);
        self.updated_at = time.updated_at;
        true
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
    pub fn pending_tip_conflicts(&self) -> &[PendingTipConflict] {
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
