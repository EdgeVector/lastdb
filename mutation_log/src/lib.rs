//! Pure functional core of the unified cloud mutation log.
//!
//! The core models one ordered log value. Entries are tagged by writer
//! (node key, device). The caller supplies time, IDs, and a [`NamespacePolicy`].
//! [`MutationLog`] is the persistence port. The daemon uses
//! `mutation_log_sync` for the signed log.
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
mod error;
mod model;
mod read;
mod state;

use database_policy::{admit_namespace, NamespaceClass, SpecBinding};

pub use append::AppendOutcome;
pub use database_policy::SHAREABLE_NAMESPACES;
pub use error::LogError;
pub use model::{
    AppendBatch, Capability, DatabaseKind, Entry, Grant, Head, Incorporation, Snapshot, Stamp,
    StoredEntry, SubmissionRecord, SubmissionStatus, Writer,
};
pub use read::{can_read, entry_visible, ListPage, Recovery, Reset, ScopedHead};
pub use state::{step, Command, LogState, Outcome, StepResult};

/// Entries applied in one append transaction.
pub const APPEND_BATCH_LIMIT: usize = 90;
/// Entries returned from one list call.
pub const LIST_PAGE_LIMIT: usize = 90;

fn checked_label(value: &str, error: LogError) -> Result<String, LogError> {
    if (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Ok(value.to_owned())
    } else {
        Err(error)
    }
}

macro_rules! label_id {
    ($name:ident, $error:ident, $doc:expr) => {
        #[doc = $doc]
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            /// Reject an empty label, a label longer than 64 bytes, or a byte
            /// outside the node database-selector alphabet.
            pub fn new(value: &str) -> Result<Self, LogError> {
                checked_label(value, LogError::$error).map(Self)
            }

            /// Borrow the label.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

label_id!(
    DatabaseId,
    InvalidId,
    "Identity of one database. One log is keyed by this value."
);
label_id!(
    DeviceId,
    InvalidId,
    "Identity of one device of a node key. A 64-character hex label fits."
);
label_id!(
    PrincipalId,
    InvalidId,
    "Node key. The same key is used on every device of that node."
);

/// Opaque pointer to snapshot bytes held by the shell.
///
/// The core stores the pointer and returns it on reset. It does not store
/// the bytes. Any non-empty text without a control character is accepted.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapshotRef(String);

impl SnapshotRef {
    /// Reject an empty pointer or one that contains a control character.
    pub fn new(value: &str) -> Result<Self, LogError> {
        if !value.is_empty() && value.chars().all(|ch| !ch.is_control()) {
            Ok(Self(value.to_owned()))
        } else {
            Err(LogError::InvalidId)
        }
    }

    /// Borrow the pointer.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Schema namespace, the part before `/` in a name such as `hearth/PhotoMeta`.
///
/// The schema name stays in the entry payload. This core classifies the
/// namespace only.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Namespace(String);

impl Namespace {
    /// Reject an empty namespace or one that already includes the schema name.
    pub fn new(value: &str) -> Result<Self, LogError> {
        checked_label(value, LogError::InvalidNamespace).map(Self)
    }

    /// Borrow the namespace.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Persistence port for one database log.
///
/// `append` accepts at most [`APPEND_BATCH_LIMIT`] entries and holds one
/// in-flight batch per writer (node key, device). The other methods are the
/// read and recovery surface. Adapters implement this trait. The sync step
/// depends on it and not on an adapter.
pub trait MutationLog {
    /// Begin, apply, and finish one batch. A failed apply still finishes.
    fn append(&mut self, batch: AppendBatch) -> Result<AppendOutcome, LogError>;

    /// Last entry this reader may see.
    fn head(&self, reader: &PrincipalId) -> Result<Option<ScopedHead>, LogError>;

    /// One page of entries this reader may see.
    fn list(
        &self,
        reader: &PrincipalId,
        after: u64,
        limit: usize,
        now_ms: u64,
    ) -> Result<ListPage, LogError>;

    /// Entries after `local_cursor` when the cursor falls below the trim floor.
    ///
    /// When that start is below the trim floor, [`Recovery::reset`] names the
    /// snapshot the shell downloads before applying [`Recovery::entries`].
    fn recover(
        &self,
        reader: &Writer,
        local_cursor: u64,
        now_ms: u64,
    ) -> Result<Recovery, LogError>;

    /// Last accepted writer sequence for `writer`. `reader` must be that node key.
    fn writer_tip(&self, reader: &PrincipalId, writer: &Writer) -> Result<u64, LogError>;
}

/// Whether a namespace may travel in a shareable database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NamespaceKind {
    /// A standard schema namespace the shareable database may store.
    Official,
    /// An app-private namespace. It belongs in the personal database only.
    AppPrivate,
}

/// Namespace admission for one database spec binding.
///
/// [`BoundPolicy::absent`] classifies every namespace as app-private, so a
/// shareable log rejects the append until a binding is present.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundPolicy {
    binding: Option<SpecBinding>,
}

impl BoundPolicy {
    /// No spec binding. Share appends fail closed.
    pub fn absent() -> Self {
        Self { binding: None }
    }

    /// Admit namespaces through this catalog binding.
    ///
    /// `binding` must already be [`SpecBinding::admitted`]. The registry row,
    /// the publication marker, and this policy share that value.
    pub fn new(binding: SpecBinding) -> Self {
        Self {
            binding: Some(binding),
        }
    }

    /// Build the policy from the stored `{ "name", "version" }` object.
    ///
    /// This is the same admission as [`SpecBinding::admitted`]. A missing spec
    /// is [`Self::absent`], not this call, and is not `lastdb-files`.
    pub fn admitted(name: &str, version: u32) -> Result<Self, database_policy::PolicyError> {
        Ok(Self::new(SpecBinding::admitted(name, version)?))
    }

    /// Borrow the binding, when one was supplied.
    pub fn binding(&self) -> Option<&SpecBinding> {
        self.binding.as_ref()
    }
}

impl NamespacePolicy for BoundPolicy {
    fn classify(&self, namespace: &Namespace) -> NamespaceKind {
        if admit_namespace(self.binding.as_ref(), namespace.as_str())
            == Ok(NamespaceClass::Official)
        {
            NamespaceKind::Official
        } else {
            NamespaceKind::AppPrivate
        }
    }
}

/// Classify with [`SHAREABLE_NAMESPACES`].
///
/// A listed namespace is [`NamespaceKind::Official`]. Every other namespace
/// is [`NamespaceKind::AppPrivate`].
#[must_use]
pub fn classify_shareable(namespace: &Namespace) -> NamespaceKind {
    if SHAREABLE_NAMESPACES.contains(&namespace.as_str()) {
        NamespaceKind::Official
    } else {
        NamespaceKind::AppPrivate
    }
}

/// Classifies namespaces.
///
/// [`classify_shareable`] is the interim production list. A production
/// caller passes that function. A namespace on the list is
/// [`NamespaceKind::Official`]. Every other namespace is
/// [`NamespaceKind::AppPrivate`].
pub trait NamespacePolicy {
    /// Classify one namespace.
    fn classify(&self, namespace: &Namespace) -> NamespaceKind;
}

impl<F> NamespacePolicy for F
where
    F: Fn(&Namespace) -> NamespaceKind,
{
    fn classify(&self, namespace: &Namespace) -> NamespaceKind {
        self(namespace)
    }
}
