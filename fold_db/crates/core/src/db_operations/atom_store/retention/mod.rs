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
//!   time-ordered queue (`crq:v1\0{expires_at:020}:{record_id}`). The janitor
//!   (`fold_db_core::conflict_janitor`) reads the queue oldest first in
//!   bounded pages, outside the write gate. For each page it runs one release
//!   batch.
//! - **The key goes.** Every new record also writes one entry in the slot
//!   directory (`crs:v1:{M}\0{hash}\0{range}\0{record_id}`), one hash for each
//!   scope and molecule. A purge, a TTL sweep or a delete of a key reads the
//!   directory of the slots it removes and releases the records in the same
//!   batch as the tip deletes.
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

mod entry;
mod keys;
mod page;
mod report;
mod rows;
mod slots;

pub use report::ConflictRetentionReport;
pub(crate) use report::RetentionState;
pub(crate) use rows::RecordSlot;
