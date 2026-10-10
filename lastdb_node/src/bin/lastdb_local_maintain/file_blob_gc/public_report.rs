//! Public output contains fixed labels, numeric totals and booleans only.

use super::model;
use serde::Serialize;

#[derive(Serialize)]
pub(super) struct Report<'a> {
    event: &'static str,
    execute: bool,
    pub counts: &'a model::PublicCounts,
    ledger_committed: bool,
    pub file_blobs_deleted: u64,
    compactions: CompactionCounts,
    atom_retirement_state_unchanged: bool,
    fresh_snapshot_required: bool,
    pre_blob_snapshot_writer_count: u64,
    csn_before: u64,
    csn_after: u64,
}

#[derive(Default, Serialize)]
struct CompactionCounts {
    collections: u64,
    live_keys: u64,
    bytes_before: u64,
    bytes_after: u64,
}

impl<'a> From<&'a model::Report> for Report<'a> {
    fn from(report: &'a model::Report) -> Self {
        let compactions =
            report
                .compactions
                .iter()
                .fold(CompactionCounts::default(), |mut totals, compact| {
                    totals.collections = totals.collections.saturating_add(1);
                    totals.live_keys = totals.live_keys.saturating_add(compact.live_keys);
                    totals.bytes_before = totals.bytes_before.saturating_add(compact.bytes_before);
                    totals.bytes_after = totals
                        .bytes_after
                        .saturating_add(compact.bytes_after.unwrap_or_default());
                    totals
                });
        Self {
            event: "file_blob_gc_offline",
            execute: report.execute,
            counts: &report.counts,
            ledger_committed: report.ledger_committed,
            file_blobs_deleted: report.file_blobs_deleted,
            compactions,
            atom_retirement_state_unchanged: report.atom_retirement_state_unchanged,
            fresh_snapshot_required: report.fresh_snapshot_required,
            pre_blob_snapshot_writer_count: report.pre_blob_snapshot_writer_count,
            csn_before: report.csn_before,
            csn_after: report.csn_after,
        }
    }
}
