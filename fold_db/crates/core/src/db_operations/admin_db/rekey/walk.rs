//! Report, page-decode and dry-run helpers for the atom partition-prefix rekey.

use super::*;
use crate::atom::{atom_key_codec, atom_locator_codec, AtomKeyEncoding, AtomPartition};

impl AtomStore {
    /// Zeroed report for a pass that starts from `checkpoint`.
    pub(super) fn new_rekey_report(
        options: &AtomPartitionRekeyOptions,
        checkpoint: &AtomPartitionRekeyCheckpoint,
        tip_page: usize,
    ) -> AtomPartitionRekeyReport {
        AtomPartitionRekeyReport {
            dry_run: options.dry_run,
            remove_flat: options.remove_flat,
            tips_scanned: 0,
            slots_considered: 0,
            dual_written: 0,
            would_dual_write: 0,
            already_prefixed: 0,
            missing_body: 0,
            flat_removed: 0,
            flat_retained_unverified: 0,
            completed: false,
            scan_reached_end: false,
            tips_walked_total: checkpoint.tips_walked_total,
            tip_page: tip_page as u64,
            unresolved_mis_derived: 0,
            unresolved_orphan_locator: 0,
            unresolved_undecodable_locator: 0,
            unresolved_no_body: 0,
            unresolved: Vec::new(),
            unresolved_truncated: false,
            would_dual_write_tips: Vec::new(),
            would_dual_write_truncated: false,
            checkpoint: checkpoint.clone(),
        }
    }

    /// Answer a `progress_only` call from the durable checkpoint alone.
    pub(super) fn rekey_progress_report(
        options: &AtomPartitionRekeyOptions,
        checkpoint: &AtomPartitionRekeyCheckpoint,
        tip_page: usize,
    ) -> AtomPartitionRekeyReport {
        AtomPartitionRekeyReport {
            dry_run: true,
            completed: checkpoint.completed,
            scan_reached_end: checkpoint.completed,
            ..Self::new_rekey_report(options, checkpoint, tip_page)
        }
    }

    /// Decode one page of `mk:` rows into rekey slots. Advances `page_start`
    /// over every row, including rows that decode to nothing.
    pub(super) fn decode_rekey_page(
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        head: Option<&str>,
        storage_prefix: Option<&str>,
        page_start: &mut String,
    ) -> Vec<RekeySlot> {
        let mut decoded: Vec<RekeySlot> = Vec::with_capacity(rows.len());
        for (k, v) in rows {
            let tip_key = String::from_utf8_lossy(&k).into_owned();
            *page_start = tip_key.clone();
            if head == Some(tip_key.as_str()) {
                continue;
            }
            let Some(base_key) = strip_storage_prefix(storage_prefix, &tip_key) else {
                continue;
            };
            let Some(partition) = AtomPartition::from_record_key(base_key) else {
                continue;
            };
            let Ok(val) = serde_json::from_slice::<Value>(&v) else {
                continue;
            };
            let Some(uuid) = val
                .pointer("/entry/atom_uuid")
                .and_then(|x| x.as_str())
                .filter(|u| !u.is_empty())
            else {
                continue;
            };
            let uuid = uuid.to_string();
            decoded.push(RekeySlot {
                flat_key: build_storage_key(storage_prefix, &atom_key_codec::flat_key(&uuid)),
                prefixed_key: build_storage_key(
                    storage_prefix,
                    &atom_key_codec::storage_key(
                        AtomKeyEncoding::PartitionPrefix,
                        Some(&partition),
                        &uuid,
                    ),
                ),
                locator_key: build_storage_key(
                    storage_prefix,
                    &atom_locator_codec::locator_key(&uuid),
                ),
                tip_key,
                uuid,
                partition,
            });
        }
        decoded
    }

    /// A dry run reports a plan from the start and never persists a cursor;
    /// fold this pass's counters into the in-memory checkpoint only.
    pub(super) fn apply_dry_run_checkpoint(
        checkpoint: &mut AtomPartitionRekeyCheckpoint,
        baseline: &AtomPartitionRekeyCheckpoint,
        report: &AtomPartitionRekeyReport,
        last_tip: Option<String>,
    ) {
        checkpoint.after_tip_key = last_tip.or(checkpoint.after_tip_key.take());
        checkpoint.tips_scanned = report.tips_scanned;
        checkpoint.tips_walked_total = baseline
            .tips_walked_total
            .saturating_add(report.tips_scanned);
        checkpoint.already_prefixed = baseline
            .already_prefixed
            .saturating_add(report.already_prefixed);
        checkpoint.missing_body = baseline.missing_body.saturating_add(report.missing_body);
        checkpoint.flat_removed = baseline.flat_removed.saturating_add(report.flat_removed);
        checkpoint.completed = false;
        checkpoint.version = 1;
    }
}
