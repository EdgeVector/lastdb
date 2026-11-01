//! Retention of kept conflict rows: the queue and the release.
//!
//! The database keeps every conflicting write of a record as a conflict row
//! (`rcf:`, `record_conflicts`). The row holds no atom or file blob reference.
//! This module decides when a row ends: when its retention window passes.
//!
//! Every new row writes one entry in a time-bucketed queue (`time_queue`) in
//! the batch of the row (`rows.rs`): the entry is
//! `crq:v1:{bucket}\0{expires_at:020}:{record_id}`, and the directory row of
//! its bucket is `crb:v1\0{bucket}`. The janitor
//! (`fold_db_core::conflict_janitor`) walks the buckets that have started,
//! oldest first, in bounded pages, outside the write gate. For each page it
//! runs one release batch (`page.rs`) through a row commit
//! (`counter::commit_rows`): the row, its queue entry, and the marker of the
//! record when no live entry of it is left go in one batch. A bucket that is
//! read to its end, with nothing in it left to do, leaves the directory.
//!
//! The queue holds no reference. An entry that outlives its row is harmless: a
//! release of a missing row deletes only the entry.

mod entry;
pub(crate) use entry::RetentionEntry;
mod keys;
mod page;
mod repair;
mod report;
mod rows;
mod steps;
mod window;

pub(crate) use keys::queue_for_window;
pub(crate) use repair::RepairWindow;
pub use report::ConflictRetentionReport;
pub(crate) use report::RetentionState;
pub(crate) use steps::{RetentionCursor, StepOutcome};
