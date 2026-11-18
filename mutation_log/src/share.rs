//! Share settings for one read or append call.
//!
//! The settings are not a log entry, and [`crate::LogState`] does not store
//! them. [`crate::BoundPolicy`] carries them, the same way it carries namespace
//! admission. [`crate::BoundPolicy::absent`] and [`crate::BoundPolicy::admitted`]
//! start from [`ShareAdmission::default`]: members and proposals. That is the
//! log as it behaves today. A caller that has a statement passes
//! [`crate::BoundPolicy::with_share`].
//!
//! `read` and `write` are independent. Every pair is valid. Share, Deliver,
//! and Org are names for three of the pairs. This crate does not treat a pair
//! as a mode, and it does not look at what the log already holds.
//!
//! [`ShareRead`] and [`ShareWrite`] use the same variant names as
//! `cloud_backup_protocol`. This crate does not depend on that crate. Map one
//! enum to the other by variant name. A personal database ignores this value.

/// Who may read the mutation log, the snapshot, and file ranges.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ShareRead {
    /// An active member reads. This is the absent value.
    #[default]
    Members,
    /// A caller with no grant reads what a member reads and cannot append.
    Anyone,
}

/// Where a Submit member's append goes from now on.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ShareWrite {
    /// The proposal log. Readers of the mutation log see publisher-written
    /// entries and entries already marked as direct writes. This is the absent value.
    #[default]
    Proposals,
    /// The mutation log. The append path records that choice on the stored entry.
    Direct,
}

/// The two settings for one shared database.
///
/// [`Default`] is members and proposals, the same pair as
/// `absent_share_settings` in `cloud_backup_protocol`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShareAdmission {
    read: ShareRead,
    write: ShareWrite,
}

impl ShareAdmission {
    /// Accept every pair. The two settings do not constrain each other.
    #[must_use]
    pub fn new(read: ShareRead, write: ShareWrite) -> Self {
        Self { read, write }
    }

    /// Who may read.
    #[must_use]
    pub fn read(self) -> ShareRead {
        self.read
    }

    /// Where a Submit append goes from now on.
    #[must_use]
    pub fn write(self) -> ShareWrite {
        self.write
    }

    /// `read: anyone`. A caller with no grant may read and may not append.
    #[must_use]
    pub fn reads_without_grant(self) -> bool {
        self.read == ShareRead::Anyone
    }
}
