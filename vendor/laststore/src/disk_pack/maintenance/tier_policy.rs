//! In-memory tier policy. Reopen conservatively starts with a full copy.
use crate::disk_pack::{generation, Current};
use crate::Result;
use std::path::Path;

#[derive(Clone, Copy, Default)]
pub(in crate::disk_pack) struct CompactionPolicy {
    base: u64,
    latest: u64,
    tiers: u8,
    base_records: u64,
    changed_records: u64,
}

impl CompactionPolicy {
    pub(super) fn boundary(&self, root: &Path, current: &Current) -> Result<u64> {
        if self.base == 0 || self.changed_records >= (self.base_records / 4).max(1) {
            return Ok(0);
        }
        let receipt = generation::read_receipt(root, current)?;
        let (mut base_bytes, mut recent_bytes) = (0u64, 0u64);
        for (coordinate, bytes) in receipt.coordinates.iter().zip(&receipt.records) {
            if coordinate.kind != 5 {
                continue;
            }
            let size = crate::disk_pack::bytes::u64_at(bytes, 32);
            if crate::disk_pack::bytes::u64_at(bytes, 72) <= self.base {
                base_bytes = base_bytes.saturating_add(size);
            } else {
                recent_bytes = recent_bytes.saturating_add(size);
            }
        }
        // Bound the retained base and obsolete payload bytes. Eight small outputs
        // merge together; growth above one quarter of the base starts a full copy.
        if base_bytes == 0 || recent_bytes.saturating_mul(4) > base_bytes {
            return Ok(0);
        }
        Ok(if self.tiers >= 8 {
            self.base
        } else {
            self.latest
        })
    }

    pub(in crate::disk_pack) fn completed(&mut self, receipt: &super::MaintenanceReceipt) {
        let copied = receipt
            .records_copied
            .saturating_add(receipt.tombstones_copied);
        if receipt.tier_boundary == 0 {
            self.base = receipt.generation;
            self.tiers = 0;
            self.base_records = copied;
            self.changed_records = 0;
        } else {
            self.changed_records = self
                .changed_records
                .saturating_add(copied)
                .saturating_add(receipt.catch_up_records);
            self.tiers = if receipt.tier_boundary == self.base {
                1
            } else {
                self.tiers.saturating_add(1)
            };
        }
        self.latest = receipt.generation;
    }
}
