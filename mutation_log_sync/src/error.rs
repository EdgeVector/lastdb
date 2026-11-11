use mutation_log::LogError;
use std::fmt;

/// A refused sync step. The caller's previous participant state is unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncError {
    /// The log refused the effect.
    Log(LogError),
    /// A receiver has nothing to append.
    ReadOnly,
    /// Only the publisher re-appends a submission.
    NotPublisher,
    /// Re-append only copies a submission stream, not the publisher stream.
    NotSubmission,
}

impl From<LogError> for SyncError {
    fn from(error: LogError) -> Self {
        Self::Log(error)
    }
}

impl fmt::Display for SyncError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Log(error) => write!(formatter, "{error}"),
            Self::ReadOnly => write!(formatter, "a receiver cannot append"),
            Self::NotPublisher => write!(formatter, "only the publisher can re-append"),
            Self::NotSubmission => {
                write!(
                    formatter,
                    "re-append copies a submission, not the publisher stream"
                )
            }
        }
    }
}

impl std::error::Error for SyncError {}
