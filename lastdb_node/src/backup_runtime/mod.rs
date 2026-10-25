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
