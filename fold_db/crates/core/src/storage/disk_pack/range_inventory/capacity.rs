//! Read-only conversion sizing and fresh filesystem headroom checks.

use super::StorageResult;
use super::{conversion_copy, conversion_plan::ConversionPlan, invalid, source::FrozenSource};
use crate::storage::disk_pack::map;
use laststore::disk_run::home::HomeMutation;
use laststore::space::SpacePolicy;
use serde::Serialize;
use std::path::Path;

const PER_RECORD_ALLOWANCE: u64 = 128;
const FIXED_ALLOWANCE: u64 = 1024 * 1024;

/// A planning estimate, not a guaranteed peak or a reservation against other processes.
/// Existing source bytes already occupy disk and are not charged against free space twice.
#[derive(Debug, Clone, Serialize)]
pub struct ConversionSpaceEstimate {
    pub source_file_bytes: u64,
    pub source_files: u64,
    pub target_records: [u64; 2],
    pub translated_logical_bytes: [u64; 2],
    pub estimated_target_bytes: u64,
    pub estimated_merge_bytes: u64,
    pub free_space_reserve_bytes: u64,
    pub disk_cap_bytes: u64,
    pub target_cap_required_bytes: u64,
    pub additional_required_bytes: u64,
    pub retained_source_and_target_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct ConversionCapacity {
    pub estimate: ConversionSpaceEstimate,
    pub available_bytes: u64,
    pub sufficient_free_space: bool,
    pub within_disk_cap: bool,
    pub sufficient_space: bool,
}

pub(super) fn estimate(
    source_root: &Path,
    source: &FrozenSource,
    plan: &ConversionPlan,
) -> StorageResult<ConversionSpaceEstimate> {
    let policy = policy()?;
    let usage = laststore::space::space_usage(source_root, policy.max_files).map_err(map)?;
    let mut records = [0_u64; 2];
    let mut logical = [0_u64; 2];
    for (slot, input) in [&source.source.main, &source.source.cas]
        .into_iter()
        .enumerate()
    {
        let mut after = None;
        loop {
            let page = conversion_copy::page(input, after.as_deref())?;
            for row in page.records {
                add(
                    &plan.record(row, slot)?,
                    &mut records[slot],
                    &mut logical[slot],
                )?;
            }
            if page.exhausted {
                break;
            }
            after = page.resume;
        }
    }
    for row in &plan.extras {
        add(row, &mut records[0], &mut logical[0])?;
    }
    // Progress is one live record. Its repeated versions and small descriptors
    // share the fixed allowance; run framing and lookup blocks use the multiplier.
    records[0] = checked(records[0].checked_add(1))?;
    let logical_bytes = checked(logical[0].checked_add(logical[1]))?;
    let record_count = checked(records[0].checked_add(records[1]))?;
    let target = checked(
        logical_bytes
            .checked_mul(2)
            .and_then(|n| {
                record_count
                    .checked_mul(PER_RECORD_ALLOWANCE)
                    .and_then(|r| n.checked_add(r))
            })
            .and_then(|n| n.checked_add(FIXED_ALLOWANCE)),
    )?;
    let reserve = checked(
        policy
            .free_space_reserve_bytes
            .checked_add(policy.replacement_reserve_bytes),
    )?;
    source.check_binding()?;
    Ok(ConversionSpaceEstimate {
        source_file_bytes: usage.bytes,
        source_files: usage.files,
        target_records: records,
        translated_logical_bytes: logical,
        estimated_target_bytes: target,
        estimated_merge_bytes: target,
        free_space_reserve_bytes: reserve,
        disk_cap_bytes: policy.disk_cap_bytes,
        target_cap_required_bytes: checked(
            target
                .checked_mul(2)
                .and_then(|n| n.checked_add(policy.replacement_reserve_bytes)),
        )?,
        additional_required_bytes: checked(
            target.checked_mul(2).and_then(|n| n.checked_add(reserve)),
        )?,
        retained_source_and_target_bytes: checked(usage.bytes.checked_add(target))?,
    })
}

impl ConversionSpaceEstimate {
    pub(super) fn capacity(&self, target: &Path) -> StorageResult<ConversionCapacity> {
        let mut existing = target;
        while !existing.try_exists()? {
            existing = existing
                .parent()
                .ok_or_else(|| invalid("target has no existing ancestor"))?;
        }
        let stats = rustix::fs::statvfs(existing).map_err(std::io::Error::from)?;
        let available = checked((stats.f_bavail as u64).checked_mul(stats.f_frsize as u64))?;
        let sufficient_free_space = available >= self.additional_required_bytes;
        let within_disk_cap =
            self.disk_cap_bytes == 0 || self.target_cap_required_bytes <= self.disk_cap_bytes;
        Ok(ConversionCapacity {
            estimate: self.clone(),
            available_bytes: available,
            sufficient_free_space,
            within_disk_cap,
            sufficient_space: sufficient_free_space && within_disk_cap,
        })
    }

    pub(super) fn require(&self, target: &Path) -> StorageResult<ConversionCapacity> {
        let report = self.capacity(target)?;
        if !report.within_disk_cap {
            return Err(invalid(&format!(
                "estimated target and merge bytes plus replacement reserve require {} native bytes; configured disk cap is {}; the source stays unchanged",
                self.target_cap_required_bytes, self.disk_cap_bytes,
            )));
        }
        if !report.sufficient_free_space {
            return Err(invalid(&format!(
                "target filesystem has {} available bytes; conversion requires an estimated {} additional bytes including merge space and reserve; the source stays unchanged",
                report.available_bytes, self.additional_required_bytes
            )));
        }
        Ok(report)
    }
}

fn add(row: &HomeMutation, records: &mut u64, bytes: &mut u64) -> StorageResult<()> {
    *records = checked(records.checked_add(1))?;
    let length = checked(
        (row.collection.len() as u64)
            .checked_add(row.key.len() as u64)
            .and_then(|n| n.checked_add(row.body.as_ref().map_or(0, |body| body.len() as u64))),
    )?;
    *bytes = checked(bytes.checked_add(length))?;
    Ok(())
}

fn checked(value: Option<u64>) -> StorageResult<u64> {
    value.ok_or_else(|| invalid("conversion space estimate overflows"))
}

pub(super) fn batch_allowance(batch: &[HomeMutation]) -> StorageResult<u64> {
    let (mut records, mut bytes) = (0, 0);
    for row in batch {
        add(row, &mut records, &mut bytes)?;
    }
    checked(
        bytes
            .checked_mul(2)
            .and_then(|n| {
                records
                    .checked_mul(PER_RECORD_ALLOWANCE)
                    .and_then(|r| n.checked_add(r))
            })
            .and_then(|n| n.checked_add(FIXED_ALLOWANCE)),
    )
}

pub(super) fn policy() -> StorageResult<SpacePolicy> {
    crate::storage::disk_pack::space::policy_from_env()
}
