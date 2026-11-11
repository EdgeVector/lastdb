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
    /// An entry's stream or author differed from the batch.
    StreamMismatch,
    /// An entry broke a structural rule, such as incorporation off the publisher stream.
    InvalidEntry,
    /// A writer sequence was zero, repeated, or unordered inside the batch.
    InvalidSequence,
    /// The next writer sequence skipped ahead of the stream's last accepted value.
    Gap {
        /// The next sequence the stream can accept.
        expected: u64,
        /// The sequence the batch offered.
        got: u64,
    },
    /// This stream already has an append open.
    AppendInFlight,
    /// Append or finish ran without an open append on this stream.
    AppendNotOpen,
    /// The principal may not perform this step.
    Unauthorized,
    /// A shareable database was asked to store an app-private namespace.
    AppPrivateNamespace,
    /// A personal database was asked to grant a receiver or a contributor.
    PersonalNotShareable,
    /// A device grant was asked of a shareable database.
    DeviceGrantNotShareable,
    /// The cursor is below the trim floor. Load the snapshot, then list after it.
    CursorExpired {
        /// Delivery sequence the latest snapshot covers, or zero when none exists.
        snapshot_through: u64,
    },
    /// An ack named a delivery sequence the log has not reached.
    AckAhead,
    /// An ack moved backward.
    AckRegress,
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
            Self::StreamMismatch => {
                write!(formatter, "entry stream or author differs from the batch")
            }
            Self::InvalidEntry => write!(formatter, "invalid log entry"),
            Self::InvalidSequence => write!(formatter, "invalid writer sequence"),
            Self::Gap { expected, got } => {
                write!(
                    formatter,
                    "writer sequence gap: expected {expected}, got {got}"
                )
            }
            Self::AppendInFlight => write!(formatter, "append already in flight for this stream"),
            Self::AppendNotOpen => write!(formatter, "append is not open for this stream"),
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
                write!(formatter, "a device grant belongs on a personal database")
            }
            Self::CursorExpired { snapshot_through } => {
                write!(
                    formatter,
                    "cursor expired; snapshot covers {snapshot_through}"
                )
            }
            Self::AckAhead => write!(formatter, "ack is ahead of the log head"),
            Self::AckRegress => write!(formatter, "ack moved backward"),
        }
    }
}

impl std::error::Error for LogError {}
