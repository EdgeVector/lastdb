//! Share settings for one read or append call.
//!
//! The settings are not a log entry, and [`crate::LogState`] does not store
//! them. [`crate::BoundPolicy`] carries them, the same way it carries namespace
//! admission. [`crate::BoundPolicy::absent`] and [`crate::BoundPolicy::admitted`]
//! start from [`ShareAdmission::default`]: members and proposals. That is the
//! log as it behaves today. A caller that has a statement passes
//! [`crate::BoundPolicy::with_share`].
//!
//! [`ShareRead`] and [`ShareWrite`] use the same variant names as
//! `cloud_backup_protocol`. This crate does not depend on that crate. Map one
//! enum to the other by variant name. A personal database ignores this value.

use crate::LogError;

/// Who may read the mutation log.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ShareRead {
    /// An active member reads the mutation log. This is the absent value.
    #[default]
    Members,
    /// A caller with no grant reads publisher-written entries and cannot append.
    Anyone,
}

/// Who may write the mutation log.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ShareWrite {
    /// Members append to the proposal log. Readers see publisher-written entries.
    /// This is the absent value.
    #[default]
    Proposals,
    /// An active Submit holder appends to the mutation log. Readers also see those entries.
    Direct,
}

/// The two settings for one shared database.
///
/// [`Default`] is members and proposals, the same pair as
/// `absent_share_settings` in `cloud_backup_protocol`. Fields stay private so
/// `read: anyone` cannot be paired with `write: direct`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShareAdmission {
    read: ShareRead,
    write: ShareWrite,
}

impl ShareAdmission {
    /// Accept every pair except `anyone` with `direct`.
    pub fn new(read: ShareRead, write: ShareWrite) -> Result<Self, LogError> {
        if read == ShareRead::Anyone && write == ShareWrite::Direct {
            return Err(LogError::InvalidShareSettings);
        }
        Ok(Self { read, write })
    }

    /// Who may read.
    #[must_use]
    pub fn read(self) -> ShareRead {
        self.read
    }

    /// Who may write.
    #[must_use]
    pub fn write(self) -> ShareWrite {
        self.write
    }

    /// `read: anyone`. A caller with no grant may read and may not append.
    #[must_use]
    pub fn reads_without_grant(self) -> bool {
        self.read == ShareRead::Anyone
    }

    /// `write: direct`. Readers see entries written by keys that hold Submit.
    #[must_use]
    pub fn shows_submit_writers(self) -> bool {
        self.write == ShareWrite::Direct
    }
}
