//! Warm set byte charges. The purge enforces the limit.
//!
//! The warm set charges only decoded records.

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
    /// Decoded records and their keys.
    pub record_bytes: usize,
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
        CoreWarmByteStats {
            limit_bytes: crate::resident::core_warm_bytes(),
            used_bytes: self.bytes.used(),
            peak_bytes: self.bytes.peak_bytes,
            record_bytes: self.bytes.record_bytes,
            purged_bytes: self.bytes.purged_bytes,
            over_limit_admissions: self.bytes.over_limit_admissions,
        }
    }
}
