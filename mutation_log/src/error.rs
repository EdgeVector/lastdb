use std::fmt;

/// A refused log step. The caller's previous state is unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogError {
    /// An id was empty, too long, or outside the selector alphabet.
    InvalidId,
    /// A namespace was empty, too long, or contained a slash.
    InvalidNamespace,
    /// An append carried no entries.
    EmptyBatch,
    /// An append carried more than [`crate::APPEND_BATCH_LIMIT`] entries.
    BatchTooLarge,
    /// A list limit was zero or above [`crate::LIST_PAGE_LIMIT`].
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
    /// An ack named a delivery sequence the log has not reached.
    AckAhead,
}

impl fmt::Display for LogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidId => write!(formatter, "invalid id"),
            Self::InvalidNamespace => write!(formatter, "invalid namespace"),
            Self::EmptyBatch => write!(formatter, "empty append batch"),
            Self::BatchTooLarge => write!(formatter, "append batch exceeds its limit"),
            Self::InvalidPage => write!(formatter, "invalid list page"),
            Self::WrongDatabase => write!(formatter, "entry database differs from the log"),
            Self::WriterMismatch => write!(formatter, "entry writer differs from the batch"),
            Self::InvalidEntry => write!(formatter, "invalid log entry"),
            Self::UnknownSubmission => {
                write!(
                    formatter,
                    "incorporation does not match a pending submission"
                )
            }
            Self::MixedIncorporation => {
                write!(
                    formatter,
                    "batch mixes an already copied submission with a new entry"
                )
            }
            Self::Conflict => {
                write!(formatter, "writer sequence repeats different content")
            }
            Self::UnexpectedOutcome => {
                write!(
                    formatter,
                    "log step returned an outcome this operation does not use"
                )
            }
            Self::InvalidSequence => write!(formatter, "invalid writer sequence"),
            Self::SequenceExhausted => {
                write!(formatter, "writer sequence space is exhausted at u64::MAX")
            }
            Self::Gap { expected, got } => {
                write!(
                    formatter,
                    "writer sequence gap: expected {expected}, got {got}"
                )
            }
            Self::AppendInFlight => write!(formatter, "append already in flight for this writer"),
            Self::AppendNotOpen => write!(formatter, "append is not open for this writer"),
            Self::Unauthorized => write!(formatter, "not authorized for this log step"),
            Self::AppPrivateNamespace => {
                write!(
                    formatter,
                    "app-private namespace cannot enter a shareable database"
                )
            }
            Self::PersonalNotShareable => {
                write!(
                    formatter,
                    "a personal database cannot be granted to a receiver"
                )
            }
            Self::DeviceGrantNotShareable => {
                write!(formatter, "an owner grant belongs on a personal database")
            }
            Self::CursorExpired { snapshot_through } => {
                write!(
                    formatter,
                    "cursor expired; snapshot covers {snapshot_through}"
                )
            }
            Self::AckAhead => write!(formatter, "ack is ahead of the log head"),
        }
    }
}

impl std::error::Error for LogError {}
