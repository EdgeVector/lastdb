//! Shared backup admission, typed failures and resource-drained completion.
mod error;
mod runtime;
mod source_diagnostic;

pub use error::{BackupError, Disposition, PauseReason, PauseState};
pub use runtime::{
    BackupRuntime, Completion, DrainError, Invocation, InvocationOutcome, RunOrigin,
    RuntimeSnapshot,
};
pub type Result<T> = std::result::Result<T, BackupError>;

/// Phase of one V2 byte backup job. The status model reports it, so it sits
/// here with the shared backup runtime, below the host.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Preparing,
    Capturing,
    Uploading,
    Inventory,
    Committing,
    Published,
    Closed,
}
