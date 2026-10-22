use crate::Text;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidInput,
    UnsupportedVersion,
    UnsupportedCapability,
    UnsupportedFormat,
    UnsupportedKeyMode,
    Unauthorized,
    NotFound,
    GenerationConflict,
    StreamAuthorityConflict,
    JobClosed,
    IdentityConflict,
    PredecessorConflict,
    ResourceLimit,
    QuotaExceeded,
    MissingDependency,
    IntegrityMismatch,
    RecoveryKeyMismatch,
    SourceUnavailable,
    ProviderPending,
    OutcomeUnknown,
    RetryLater,
}

/// A safe error classification. No credential, URL, key, or record body is present.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolError {
    pub code: ErrorCode,
    pub field: String,
}

impl ProtocolError {
    pub fn invalid(field: &'static str) -> Self {
        Self {
            code: ErrorCode::InvalidInput,
            field: field.into(),
        }
    }

    pub fn limit(field: &'static str) -> Self {
        Self {
            code: ErrorCode::ResourceLimit,
            field: field.into(),
        }
    }
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.field)
    }
}

impl std::error::Error for ProtocolError {}

/// Pending outcomes are never definite failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingReason {
    AwaitingBytes,
    Verifying,
    OutcomeUnknown,
}

/// The exact operation cannot succeed under its frozen input.
/// This classification does not authorize deletion or release unknown byte reservations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalFailureCode {
    InvalidInput,
    UnsupportedFormat,
    UnsupportedKeyMode,
    IntegrityMismatch,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalFailure {
    pub code: TerminalFailureCode,
    pub field: Text<128>,
}

impl TerminalFailure {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        crate::limits::encoded_json_bytes(self, crate::limits::RECEIPT_JSON_MAX_BYTES).map(|_| ())
    }
}
