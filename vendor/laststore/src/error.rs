//! Error types for Last Store.

use std::fmt;

/// Result alias for Last Store operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by the disk format v2 engine ([`crate::disk_run`]).
#[derive(Debug)]
pub enum Error {
    /// Underlying filesystem or IO failure.
    Io(std::io::Error),
    /// On-disk format is truncated, truncated mid-record, or inconsistent.
    Corrupt(String),
    /// AEAD authentication failed while decrypting a frame.
    AeadAuthFail,
    /// Invalid configuration (e.g. `shard_bits` out of range).
    Config(String),
    /// A disk-run call exhausted its explicit work/byte budget or could not
    /// reserve its bounded buffer. This is never an absent-key result.
    ReadBudgetExceeded,
    /// A complete mutation cannot reserve its bounded resources before append.
    WriteCapacity(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Corrupt(s) => write!(f, "corrupt: {s}"),
            Self::AeadAuthFail => write!(f, "aead authentication failed"),
            Self::Config(s) => write!(f, "config: {s}"),
            Self::ReadBudgetExceeded => write!(f, "read resource limit exceeded"),
            Self::WriteCapacity(reason) => write!(f, "write capacity unavailable: {reason}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
