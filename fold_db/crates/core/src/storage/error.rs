use thiserror::Error;

/// Comprehensive storage error type supporting multiple backends
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Storage backend error: {0}")]
    BackendError(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    /// A named database attempted to resolve a schema that its catalog does
    /// not contain. Keep this distinct from backend failures so the data path
    /// can reject the request without falling back to another keyspace.
    #[error(
        "database catalog membership denied: database '{db_locator}' does not contain schema '{schema_name}'"
    )]
    CatalogMembershipDenied {
        db_locator: String,
        schema_name: String,
    },

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Download failed: {0}")]
    DownloadFailed(String),

    #[error("Upload failed: {0}")]
    UploadFailed(String),

    #[error("Sled error: {0}")]
    SledError(String),

    /// The filesystem holding the database is out of space, so the write did
    /// not land.
    ///
    /// Carried as its own variant (not a stringly-typed [`Self::BackendError`])
    /// for the same reason as [`Self::LockContention`]: a full disk is not a
    /// backend defect and not something the caller can fix by changing its
    /// request. Every retry fails identically until an operator frees space, so
    /// the condition needs an operator-facing message and a status of its own.
    ///
    /// Flattening it is what let a disk-full episode reach the caller as
    /// `400 Bad Request` / "Invalid data" — telling the writer its payload was
    /// malformed — and mint one Sentry issue per failed write (issue
    /// `7620061902`: 207 error events from one 8-hour episode, 0 users).
    ///
    /// `detail` carries the underlying IO error for the log; the message above
    /// is what an operator needs to act.
    #[error(
        "no space left on device — the filesystem holding the database is \
         full, so the write did not land. Free disk space and retry; nothing \
         already stored was lost. Underlying error: {detail}"
    )]
    StorageFull { detail: String },

    /// The bounded mutation-capture queue stayed full for the whole admission
    /// window, so the request was refused before the local commit.
    ///
    /// Carried as its own variant for the same reason as [`Self::StorageFull`]
    /// and [`Self::LockContention`]: it is not a backend defect and not
    /// something the caller can fix by changing its request. The difference is
    /// that this one clears itself — the capture worker drains, and the same
    /// write succeeds moments later with no operator action at all.
    ///
    /// Flattening it into [`Self::BackendError`] is what made a burst of
    /// transient backpressure reach the caller as `400 Bad Request` / "Invalid
    /// data" and mint one ERROR — one Sentry event — per refused write (issue
    /// `7699865707`: 123 error events in 13 hours on one node, 0 users
    /// affected). That is the disk-full storm of issue `7620061902` again, in
    /// a different lane.
    ///
    /// `waited_ms` is the admission window that elapsed, for the operator log.
    /// It is a `u64`, not the `u128` `Duration::as_millis` hands back: this
    /// variant is reachable from `FoldDbError`, and a `u128` payload widens
    /// that enum's alignment past clippy's `result_large_err` bar. Milliseconds
    /// of an admission window never need 128 bits.
    #[error(
        "mutation capture queue remained full for {waited_ms}ms, so the write \
         did not land and nothing already stored was lost. This is transient \
         backpressure from the capture worker — retry the same write."
    )]
    CaptureQueueFull { waited_ms: u64 },

    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    /// The database file lock could not be acquired before the retry
    /// deadline because another process holds it. Almost always a duplicate
    /// node (a second `lastdbd` / sidebin primary / stray Mini) running
    /// against the same data dir. Callers should surface the message rather
    /// than panic.
    #[error(
        "database is locked by another process at {path} — another LastDB \
         instance is already running and holding the database lock. Quit the \
         duplicate instance(s) (look for extra 'fold-app', 'lastdb_server', or \
         'folddb_server' processes — e.g. `pgrep -fl fold-app` / Activity \
         Monitor) and try again. Waited {waited_ms}ms for the lock to release."
    )]
    LockContention { path: String, waited_ms: u128 },

    #[error("Encryption error: {0}")]
    EncryptionError(String),
}

/// `ENOSPC`. Identical on Linux and macOS/BSD, and checked directly because a
/// backend can surface a raw OS error that `std` left
/// [`std::io::ErrorKind::Uncategorized`].
const ENOSPC: i32 = 28;

impl StorageError {
    /// True when `error` reports that the filesystem is out of space.
    ///
    /// Checks the categorized kind first and falls back to the raw OS code, so
    /// the classification does not depend on `std` having categorized this
    /// platform's `ENOSPC`.
    #[must_use]
    pub fn is_storage_full_io(error: &std::io::Error) -> bool {
        error.kind() == std::io::ErrorKind::StorageFull || error.raw_os_error() == Some(ENOSPC)
    }

    /// True when this error is the host running out of disk space.
    ///
    /// Callers use this to keep a full disk out of the paths meant for code
    /// faults: it is an operator condition, so it must not be reported as bad
    /// caller data and must not raise one alert per failed write.
    #[must_use]
    pub const fn is_storage_full(&self) -> bool {
        matches!(self, Self::StorageFull { .. })
    }

    /// True when this error is transient mutation-capture backpressure.
    ///
    /// Callers use this to keep self-clearing backpressure out of the paths
    /// meant for code faults: the request was well formed, and retrying it
    /// unchanged is the correct response.
    #[must_use]
    pub const fn is_capture_queue_full(&self) -> bool {
        matches!(self, Self::CaptureQueueFull { .. })
    }
}

impl From<tokio::task::JoinError> for StorageError {
    fn from(e: tokio::task::JoinError) -> Self {
        Self::BackendError(e.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for StorageError {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        Self::BackendError(format!("Lock poisoned: {e}"))
    }
}

pub type StorageResult<T> = Result<T, StorageError>;
