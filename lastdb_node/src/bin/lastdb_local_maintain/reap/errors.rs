//! Exit-code classes of the `reap` verbs.

use std::fmt;

/// Why a `reap` verb stopped.
///
/// The class decides the process exit code. The contract is:
/// - exit 2: a usage error or a guard refusal. No store read has happened.
/// - exit 3: a plan abort rule fired. The home is unchanged.
/// - exit 1: any other failure.
#[derive(Debug)]
pub(crate) enum ReapError {
    /// Exit 2.
    Refused(String),
    /// Exit 3. `gate` names the rule. Tests and mutation probes match on it.
    Abort { gate: &'static str, message: String },
    /// Exit 1.
    Failed(String),
}

impl ReapError {
    pub(crate) fn abort(gate: &'static str, message: impl Into<String>) -> Self {
        Self::Abort {
            gate,
            message: message.into(),
        }
    }

    pub(crate) fn exit_code(&self) -> i32 {
        match self {
            Self::Refused(_) => 2,
            Self::Abort { .. } => 3,
            Self::Failed(_) => 1,
        }
    }
}

impl fmt::Display for ReapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(message) => write!(f, "refused: {message}"),
            Self::Abort { gate, message } => write!(f, "plan abort [{gate}]: {message}"),
            Self::Failed(message) => write!(f, "{message}"),
        }
    }
}

impl From<String> for ReapError {
    fn from(message: String) -> Self {
        Self::Failed(message)
    }
}

impl From<std::io::Error> for ReapError {
    fn from(error: std::io::Error) -> Self {
        Self::Failed(error.to_string())
    }
}
