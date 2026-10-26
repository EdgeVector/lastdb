use thiserror::Error;

/// Errors of the retained local sync readers: the receiver binding read, the
/// retired-intent verify stack and the boot guard. No cloud engine builds one.
#[derive(Debug, Error)]
pub enum SyncError {
    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// A permanent account marker holds cloud work until an exact replacement
    /// is installed. This never disables local reads, writes, or durable queues.
    #[error("cloud purge holds cloud sync: {0}")]
    CloudPurgeHeld(String),

    #[error("storage error: {0}")]
    Storage(String),
}

pub type SyncResult<T> = Result<T, SyncError>;

impl From<crate::storage::error::StorageError> for SyncError {
    fn from(e: crate::storage::error::StorageError) -> Self {
        Self::Storage(e.to_string())
    }
}

impl From<serde_json::Error> for SyncError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(e.to_string())
    }
}
