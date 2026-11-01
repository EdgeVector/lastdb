//! Warm set byte charges. The purge enforces the limit.
//!
//! A format 7 home charges only decoded records. The path, atom, slot, link
//! and held byte fields stay in the status reply and are always 0.

use super::*;

#[derive(Default)]
pub(super) struct WarmBytes {
    record_bytes: usize,
    peak_bytes: usize,
    purged_bytes: u64,
    over_limit_admissions: u64,
}

/// Charged warm set bytes. The purge keeps `used_bytes` at or under the limit
/// whenever unheld records remain.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct CoreWarmByteStats {
    /// `LASTDB_CORE_WARM_BYTES`.
    pub limit_bytes: usize,
    pub used_bytes: usize,
    /// Highest charge seen at a status read since start.
    pub peak_bytes: usize,
    /// Retired field path charge; always 0.
    pub path_bytes: usize,
    /// Retired field atom charge; always 0.
    pub atom_bytes: usize,
    /// Retired field slot charge; always 0.
    pub slot_bytes: usize,
    /// Retired source link charge; always 0.
    pub link_bytes: usize,
    /// Decoded records of a format 7 home and their keys.
    pub record_bytes: usize,
    /// Retired call-held atom charge; always 0.
    pub held_bytes: usize,
    /// Retired unflushed warm-path charge; disk-first publication keeps this zero.
    pub dirty_bytes: usize,
    /// The share of the warm byte limit available to unflushed writes.
    pub dirty_limit_bytes: usize,
    /// Retired atom tracker size; always 0.
    pub tracked_atoms: usize,
    /// Charged bytes that the purge removed since start.
    pub purged_bytes: u64,
    /// Admissions that left the charge over the limit because only held records remained.
    pub over_limit_admissions: u64,
}

impl WarmBytes {
    pub(super) fn used(&self) -> usize {
        self.record_bytes
    }

    pub(super) fn note_purged(&mut self, bytes: usize) {
        self.purged_bytes = self.purged_bytes.saturating_add(bytes as u64);
    }

    pub(super) fn note_over_limit(&mut self) {
        self.over_limit_admissions = self.over_limit_admissions.saturating_add(1);
    }

    fn note_peak(&mut self) {
        self.peak_bytes = self.peak_bytes.max(self.used());
    }
}

impl CoreWarmSet {
    pub(super) fn charge_record(&mut self, charge: usize) {
        self.bytes.record_bytes += charge;
    }

    pub(super) fn release_record(&mut self, charge: usize) {
        self.bytes.record_bytes -= charge;
    }

    /// The current charge. Status calls it, so it also notes the peak.
    pub(crate) fn byte_stats(&mut self) -> CoreWarmByteStats {
        self.bytes.note_peak();
        let limit_bytes = crate::resident::core_warm_bytes();
        CoreWarmByteStats {
            limit_bytes,
            used_bytes: self.bytes.used(),
            peak_bytes: self.bytes.peak_bytes,
            path_bytes: 0,
            atom_bytes: 0,
            slot_bytes: 0,
            link_bytes: 0,
            record_bytes: self.bytes.record_bytes,
            held_bytes: 0,
            dirty_bytes: 0,
            dirty_limit_bytes: crate::resident::core_warm_dirty_bytes(limit_bytes),
            tracked_atoms: 0,
            purged_bytes: self.bytes.purged_bytes,
            over_limit_admissions: self.bytes.over_limit_admissions,
        }
    }
}
