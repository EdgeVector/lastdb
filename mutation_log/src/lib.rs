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
mod model;
mod read;

use std::fmt;

use database_policy::{admit_namespace, NamespaceClass, SpecBinding};

pub use append::AppendOutcome;
pub use database_policy::SHAREABLE_NAMESPACES;
pub use model::{
    step, AppendBatch, Capability, Command, DatabaseKind, Entry, Grant, Head, Incorporation,
    LogState, Outcome, Snapshot, SnapshotRef, Stamp, StepResult, StoredEntry, SubmissionRecord,
    SubmissionStatus, Writer,
};
pub use read::{ListPage, Recovery, Reset, ScopedHead};

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
label_id!(
    Namespace,
    InvalidNamespace,
    "Schema namespace, the part before `/` in a name such as `hearth/PhotoMeta`."
);

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

    pub(crate) fn new(binding: SpecBinding) -> Self {
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

/// A refused log step. The caller's previous state is unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogError {
    /// An id was empty, too long, or outside the selector alphabet.
    InvalidId,
    /// A namespace was empty, too long, or contained a slash.
    InvalidNamespace,
    /// An append carried no entries.
    EmptyBatch,
    /// An append carried more than [`APPEND_BATCH_LIMIT`] entries.
    BatchTooLarge,
    /// A list limit was zero or above [`LIST_PAGE_LIMIT`].
    InvalidPage,
    /// An entry named a different database than the log.
    WrongDatabase,
    /// An entry's writer differed from the batch.
    WriterMismatch,
    /// An entry broke a structural rule, such as a re-append from a key other than the publisher.
    InvalidEntry,
    /// `incorporated_from` did not match a stored submission.
    UnknownSubmission,
    /// A batch mixes an already copied submission with a new entry.
    MixedIncorporation,
    /// A writer sequence repeated with different content.
    Conflict,
    /// A port operation received an outcome from a different command.
    UnexpectedOutcome,
    /// A writer sequence was zero, repeated, or unordered inside the batch.
    InvalidSequence,
    /// This writer has no representable successor. `u64::MAX` is never stored.
    SequenceExhausted,
    /// The next writer sequence skipped ahead of this writer's last accepted value.
    Gap {
        /// The next sequence this writer can accept.
        expected: u64,
        /// The sequence the batch offered.
        got: u64,
    },
    /// This writer already has an append open.
    AppendInFlight,
    /// Append or finish ran without an open append for this writer.
    AppendNotOpen,
    /// The principal may not perform this step.
    Unauthorized,
    /// A shareable database was asked to store an app-private namespace.
    AppPrivateNamespace,
    /// A personal database was asked to grant a receiver or a contributor.
    PersonalNotShareable,
    /// An owner grant was asked of a shareable database.
    DeviceGrantNotShareable,
    /// The cursor is below the trim floor. Load the snapshot, then list after it.
    CursorExpired {
        /// Delivery sequence the latest snapshot covers, or zero when none exists.
        snapshot_through: u64,
    },
    /// Trim would drop entries, and no snapshot pointer covers that floor.
    SnapshotRequired,
    /// A pending submission aged out before the publisher copied it.
    SubmissionExpired {
        /// Writer (node key, device) of the submission.
        writer: Writer,
        /// Writer sequence of that submission.
        writer_seq: u64,
    },
    /// The publisher rejected this pending submission.
    SubmissionRejected {
        /// Writer (node key, device) of the submission.
        writer: Writer,
        /// Writer sequence of that submission.
        writer_seq: u64,
    },
}

impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidId => f.write_str("invalid id"),
            Self::InvalidNamespace => f.write_str("invalid namespace"),
            Self::EmptyBatch => f.write_str("empty append batch"),
            Self::BatchTooLarge => f.write_str("append batch exceeds its limit"),
            Self::InvalidPage => f.write_str("invalid list page"),
            Self::WrongDatabase => f.write_str("entry database differs from the log"),
            Self::WriterMismatch => f.write_str("entry writer differs from the batch"),
            Self::InvalidEntry => f.write_str("invalid log entry"),
            Self::UnknownSubmission => {
                f.write_str("incorporation does not match a pending submission")
            }
            Self::MixedIncorporation => {
                f.write_str("batch mixes an already copied submission with a new entry")
            }
            Self::Conflict => f.write_str("writer sequence repeats different content"),
            Self::UnexpectedOutcome => {
                f.write_str("log step returned an outcome this operation does not use")
            }
            Self::InvalidSequence => f.write_str("invalid writer sequence"),
            Self::SequenceExhausted => {
                f.write_str("writer sequence space is exhausted at u64::MAX")
            }
            Self::Gap { expected, got } => {
                write!(f, "writer sequence gap: expected {expected}, got {got}")
            }
            Self::AppendInFlight => f.write_str("append already in flight for this writer"),
            Self::AppendNotOpen => f.write_str("append is not open for this writer"),
            Self::Unauthorized => f.write_str("not authorized for this log step"),
            Self::AppPrivateNamespace => {
                f.write_str("app-private namespace cannot enter a shareable database")
            }
            Self::PersonalNotShareable => {
                f.write_str("a personal database cannot be granted to a receiver")
            }
            Self::DeviceGrantNotShareable => {
                f.write_str("an owner grant belongs on a personal database")
            }
            Self::CursorExpired { snapshot_through } => {
                write!(f, "cursor expired; snapshot covers {snapshot_through}")
            }
            Self::SnapshotRequired => f.write_str("trim needs a snapshot that covers the floor"),
            Self::SubmissionExpired { writer, writer_seq } => write!(
                f,
                "pending submission expired before it was copied ({}, {}, seq {writer_seq})",
                writer.key.as_str(),
                writer.device.as_str()
            ),
            Self::SubmissionRejected { writer, writer_seq } => write!(
                f,
                "pending submission was rejected ({}, {}, seq {writer_seq})",
                writer.key.as_str(),
                writer.device.as_str()
            ),
        }
    }
}

impl std::error::Error for LogError {}
