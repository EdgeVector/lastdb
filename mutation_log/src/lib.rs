//! Pure functional core of the unified cloud mutation log.
//!
//! The core models one ordered log value. Entries are tagged by writer
//! (node key, device). The caller supplies time, IDs, and a [`NamespacePolicy`].
//! [`MutationLog`] is the persistence port. The in-memory adapter lives in
//! `mutation_log_mem`. The daemon uses `mutation_log_sync` for the signed log.
//!
//! # Sharing
//!
//! A database is [`DatabaseKind::Personal`] or [`DatabaseKind::Shareable`].
//! A personal database accepts app-private namespaces and rejects a receiver
//! grant. A shareable database rejects an app-private namespace on append.
//! ExeMem gives receivers only entries written by the publisher, including
//! when filtering legacy mixed history. Whole-database shares also have a
//! separate pending log that only the publisher reads. The publisher's LastDB
//! validates those mutations and publishes accepted changes under its own key.

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
pub use namespace::{
    classify_shareable, BoundPolicy, NamespaceKind, NamespacePolicy, SHAREABLE_NAMESPACES,
};
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
