//! Pure functional core of the unified cloud mutation log.
//!
//! One ordered log per `database_id`, for a personal database and for a
//! shareable database. Entries are tagged by writer (node key, device). This
//! crate holds the types, the read rule, and the apply step. It reads no
//! clock, opens no file or socket, and stores nothing. The shell passes time,
//! ids, and a [`NamespacePolicy`]. [`MutationLog`] is the port. The in-memory
//! adapter lives in `mutation_log_mem`. There is no flag. This crate still
//! stores nothing. After a verified per-database cutover, the daemon calls
//! this port through `mutation_log_sync`.
//!
//! The crate is a workspace leaf, next to `cloud_backup_plan`, because the
//! log is not the local record model (`lastdb_model`) and not the current
//! backup wire contract (`cloud_backup_protocol`). Those stay on their
//! current paths until a later change adopts this step.
//!
//! # Sharing
//!
//! A database is [`DatabaseKind::Personal`] or [`DatabaseKind::Shareable`].
//! A personal database accepts app-private namespaces and rejects a receiver
//! grant. A shareable database rejects an app-private namespace on append.
//! A receiver reads entries whose writer key is the publisher key. A
//! contributor's pending entry stays hidden from receivers until the publisher
//! key re-appends it.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

mod append;
mod auth;
mod error;
mod id;
mod model;
mod namespace;
mod port;
mod read;
mod retain;
mod state;
mod step;

pub use append::AppendOutcome;
pub use auth::can_read;
pub use error::LogError;
pub use id::{DatabaseId, DeviceId, Namespace, PrincipalId, SnapshotRef};
pub use model::{
    AppendBatch, Capability, DatabaseKind, Entry, Grant, Head, Incorporation, Snapshot, Stamp,
    StoredEntry, SubmissionRecord, SubmissionStatus, Writer,
};
pub use namespace::{classify_shareable, NamespaceKind, NamespacePolicy, SHAREABLE_NAMESPACES};
pub use port::MutationLog;
pub use read::{
    entry_visible, filter_readable, merge_order, ListPage, Recovery, Reset, ScopedHead,
};
pub use state::LogState;
pub use step::{step, Command, Outcome, StepResult};

/// Entries applied in one append transaction.
pub const APPEND_BATCH_LIMIT: usize = 90;
/// Entries returned from one list call.
pub const LIST_PAGE_LIMIT: usize = 90;
/// Maximum time an unacked entry stays in the log. 30 days, in milliseconds.
pub const MAX_RETENTION_MS: u64 = 30 * 24 * 60 * 60 * 1000;

#[cfg(test)]
mod tests;
