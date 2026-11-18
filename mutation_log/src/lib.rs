//! Pure functional core of one mutation log.
//! Decided state: docs/designs/shared-database-mutation-logs.md (mutation log, proposal log, share settings).
//!
//! The core models one ordered log value. Entries are tagged by writer
//! (node key, device). The caller supplies time, IDs, and a [`BoundPolicy`].
//! The same value models a personal database, a share's mutation log, or that
//! share's proposal log.
//!
//! # Sharing
//!
//! A database is [`DatabaseKind::Personal`] or [`DatabaseKind::Shareable`].
//! A personal database accepts app-private namespaces and rejects a receiver
//! grant. A shareable database rejects an app-private namespace on append.
//! A personal database has one mutation log. With the default settings, a
//! shared database has two: the mutation log, which only the publisher writes
//! and which members read, and the proposal log, which only the publisher reads.
//! The publisher's LastDB checks proposal-log entries and appends accepted
//! changes to the mutation log. Those settings give members only entries
//! written by the publisher.
//!
//! [`BoundPolicy::with_share`] carries the two settings. The default is
//! members and proposals, which is absent share settings. `read: anyone`
//! lets a caller with no grant read publisher-written entries and does not
//! let that caller append. `write: direct` keeps the existing Submit append
//! check and also shows entries written by keys that hold Submit.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

mod append;
mod authorship;
mod model;
mod read;
mod share;

use std::fmt;

use database_policy::{admit_namespace, valid_label, NamespaceClass, SpecBinding};

pub use append::AppendOutcome;
pub use authorship::{
    attach_writer_signature, carried_payload, sealed_sha256, writer_entry_digest, CarriedPayload,
    WriterEntryBinding, SEALED_ENVELOPE_VERSION, WRITER_ENTRY_DOMAIN, WRITER_FRAME_PREFIX,
    WRITER_FRAME_VERSION, WRITER_SIGNATURE_LEN,
};
pub use model::{
    AppendBatch, Capability, DatabaseKind, Entry, Grant, Head, Incorporation, LogState, Stamp,
    StoredEntry, Writer,
};
pub use read::{ListPage, ScopedHead};
pub use share::{ShareAdmission, ShareRead, ShareWrite};

/// Entries applied in one append transaction.
pub const APPEND_BATCH_LIMIT: usize = 90;
/// Entries returned from one list call.
pub const LIST_PAGE_LIMIT: usize = 90;

fn checked_label(value: &str, error: LogError) -> Result<String, LogError> {
    if valid_label(value) {
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

/// Namespace admission for one database spec binding.
///
/// [`BoundPolicy::absent`] classifies every namespace as app-private, so a
/// shareable log rejects the append until a binding is present.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundPolicy {
    binding: Option<SpecBinding>,
    share: ShareAdmission,
}

impl BoundPolicy {
    /// No spec binding. Share appends fail closed.
    ///
    /// Share admission is [`ShareAdmission::default`]: members and proposals.
    #[must_use]
    pub fn absent() -> Self {
        Self {
            binding: None,
            share: ShareAdmission::default(),
        }
    }

    pub(crate) fn new(binding: SpecBinding) -> Self {
        Self {
            binding: Some(binding),
            share: ShareAdmission::default(),
        }
    }

    /// Use these share settings for later append and read calls.
    ///
    /// Pass [`ShareAdmission::default`] when the database has no settings.
    /// That is `absent_share_settings` in `cloud_backup_protocol`: members and
    /// proposals. Map `ShareRead` and `ShareWrite` by variant name.
    #[must_use]
    pub fn with_share(mut self, share: ShareAdmission) -> Self {
        self.share = share;
        self
    }

    /// Share settings supplied for this call. The default is members and proposals.
    #[must_use]
    pub fn share(&self) -> ShareAdmission {
        self.share
    }

    /// Build the policy from the stored `{ "name", "version" }` object.
    ///
    /// This is the same admission as [`SpecBinding::admitted`]. A missing spec
    /// is [`Self::absent`], not this call, and is not `lastdb-files`.
    pub fn admitted(name: &str, version: u32) -> Result<Self, database_policy::PolicyError> {
        Ok(Self::new(SpecBinding::admitted(name, version)?))
    }

    /// Borrow the binding, when one was supplied.
    #[must_use]
    pub fn binding(&self) -> Option<&SpecBinding> {
        self.binding.as_ref()
    }

    pub(crate) fn allows_in_share(&self, namespace: &Namespace) -> bool {
        admit_namespace(self.binding.as_ref(), namespace.as_str()) == Ok(NamespaceClass::Official)
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
    /// The principal may not perform this step.
    Unauthorized,
    /// A shareable database was asked to store an app-private namespace.
    AppPrivateNamespace,
    /// `read: anyone` was combined with `write: direct`.
    InvalidShareSettings,
    /// A personal database was asked to grant a receiver or a contributor.
    PersonalNotShareable,
    /// An owner grant was asked of a shareable database.
    DeviceGrantNotShareable,
    /// The cursor is below the trim floor. Load the snapshot, then list after it.
    CursorExpired {
        /// Delivery sequence the latest snapshot covers, or zero when none exists.
        snapshot_through: u64,
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
            Self::Unauthorized => f.write_str("not authorized for this log step"),
            Self::AppPrivateNamespace => {
                f.write_str("app-private namespace cannot enter a shareable database")
            }
            Self::InvalidShareSettings => {
                f.write_str("read anyone cannot combine with write direct")
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
        }
    }
}

impl std::error::Error for LogError {}
