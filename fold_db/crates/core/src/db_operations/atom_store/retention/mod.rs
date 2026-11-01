//! Retention of kept conflict records: the queue, the slot directory, and the
//! release.
//!
//! The database keeps every conflicting write. A conflict record is a history
//! row that names loser atoms. It holds one counted reference to each atom it
//! names (see `counter`), so a loser keeps its body for as long as its record
//! exists. This module decides when a record ends.
//!
//! A record ends in two ways:
//!
//! - **The retention window passes.** Every new record writes one entry in a
//!   time-bucketed queue (`time_queue`): the entry is
//!   `crq:v1:{bucket}\0{expires_at:020}:{record_id}`, and the directory row of
//!   its bucket is `crb:v1\0{bucket}`. The janitor
//!   (`fold_db_core::conflict_janitor`) walks the buckets that have started,
//!   oldest first, in bounded pages, outside the write gate. For each page it
//!   runs one release batch. A bucket that is read to its end, with nothing in
//!   it left to do, leaves the directory, and the deleted keys of its records
//!   go out of reach with it.
//! - **The key goes.** Every new record also writes one entry in the slot
//!   directory (`crs:v1:{M}\0{hash}\0{range}\0{record_id}`), one hash for each
//!   scope and molecule. A purge, a TTL sweep or a delete of a key reads the
//!   directory of the slots it removes and releases the records in the same
//!   batch as the tip deletes. A slot with more records than one erase
//!   releases gets a drain marker in the queue, and the janitor drains it
//!   (`drain.rs`) in batches of its own, between the batches of ordinary work.
//!
//! A release never deletes an atom. It names the history row to the planner
//! (`counter::ReleaseRequest`). The planner reads the row in the write gate,
//! lowers one reference for each atom the row names, and deletes the row, the
//! conflict row, the directory entry and the queue entry in one batch. An atom
//! whose count reaches zero goes by the delete-on-zero rule of the planner.
//! The janitor and the purge write nothing else.
//!
//! The queue and the directory hold no atom reference. They are pointers, and
//! the history row is the only holder of the references. A pointer that outlives
//! its row is harmless: a release of a missing row lowers nothing.

mod drain;
mod entry;
pub(crate) use entry::RetentionEntry;
mod keys;
mod page;
mod repair;
mod report;
mod rows;
mod slots;
mod split;
mod steps;
mod window;

pub(crate) use keys::{queue_for_window, record_digest};
pub(crate) use repair::RepairWindow;
pub use report::ConflictRetentionReport;
pub(crate) use report::RetentionState;
pub(crate) use steps::{Lane, RetentionCursor, StepOutcome};
