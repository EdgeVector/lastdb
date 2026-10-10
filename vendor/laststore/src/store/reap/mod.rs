//! Rules-driven key reap for a stopped store.
//!
//! A plan directory holds one rules file for each collection. Each rule names
//! raw key bytes: a prefix, one key, or a file of keys. The reap drops every
//! live key that a rule matches and keeps every other key byte for byte.
//!
//! The reap runs in two passes:
//!
//! 1. The count pass loads each group of each selected collection. It runs the
//!    matcher on every live key. It writes nothing.
//! 2. The apply pass runs only if the count of every collection passes the
//!    gate. It rewrites the groups that hold a match, one group at a time.
//!
//! The group rewrite uses the PR 1480 steps: drop the keys from the group,
//! write the new segment, then delete the older segments. After each rewrite
//! the group is read again from disk. The reap stops if the kept keys differ
//! from the kept keys before the rewrite.
//!
//! The caller must be the only process that uses the store. [`reap_home`]
//! refuses a store whose socket accepts a connection.
//!
//! What a run writes:
//!
//! - A run without `execute` changes no byte of the store. It creates no file
//!   and no directory. The files keep their bytes and their modification
//!   times. A store root without a `data` directory is refused, because the
//!   store open would make that directory.
//! - A run with `execute` creates `maintenance.lock` in the store root before
//!   the count pass. The file stays also when the gate fails and the run
//!   exits with code 4. It holds no data. Apart from that file, a run that
//!   fails the gate of the count pass changes nothing.
//!
//! The load of a plain group cuts a torn tail off the newest segment. That is
//! a write, and it drops every record after the first bad one. The reap never
//! lets that happen. Before each load, `tail` walks the newest segment. A
//! group that is not whole is refused with exit code 2 in the count pass. The
//! operator repairs it, then runs the reap again.

mod apply;
mod count;
mod guard;
mod matcher;
mod plan;
mod rules;
mod run;
mod scan;
mod tail;

pub use apply::CollectionApplied;
pub use count::{CollectionCount, GroupCount};
pub use plan::{CollectionRules, ReapPlan};
pub use run::{reap_home, ReapEvent, ReapOptions, ReapOutcome};

use std::fmt;

/// Collections that a reap may change.
///
/// `atoms` and `cas_blobs` are absent on purpose. The daemon frees them with
/// its own garbage collection after the restart.
pub const REAP_ALLOWED_COLLECTIONS: &[&str] = &[
    "tips",
    "atom_ref_edges_v2",
    "atom_ref_edges",
    "molecule_ref_edges",
    "keep_small",
    "schema_index",
    "atom_locators",
    "schema_states",
    "proteins",
];

/// A reap stopped. The variant gives the exit code of the tool.
#[derive(Debug)]
pub enum ReapError {
    /// A guard refused before any write: exit code 2. A group whose newest
    /// segment is torn is refused in the count pass.
    Refused(String),
    /// The count or a group check did not match the plan: exit code 4.
    ///
    /// A gate mismatch in the count pass means nothing was written. A
    /// mismatch in the group check means that group is in its old or new
    /// complete state.
    GateMismatch(String),
    /// A store or file system error: exit code 5.
    Internal(String),
}

impl ReapError {
    /// The exit code of the tool for this error.
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Refused(_) => 2,
            Self::GateMismatch(_) => 4,
            Self::Internal(_) => 5,
        }
    }
}

impl fmt::Display for ReapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(text) => write!(f, "refused: {text}"),
            Self::GateMismatch(text) => write!(f, "gate mismatch: {text}"),
            Self::Internal(text) => write!(f, "internal error: {text}"),
        }
    }
}

impl std::error::Error for ReapError {}

impl From<crate::Error> for ReapError {
    fn from(error: crate::Error) -> Self {
        Self::Internal(error.to_string())
    }
}

impl From<std::io::Error> for ReapError {
    fn from(error: std::io::Error) -> Self {
        Self::Internal(error.to_string())
    }
}

/// Text of a group for messages: `tips shard 3 group 01f`.
pub(crate) fn group_label(collection: &str, shard: u16, group: Option<u32>) -> String {
    match group {
        Some(group) => format!("{collection} shard {shard} group {group:03x}"),
        None => format!("{collection} shard {shard}"),
    }
}
