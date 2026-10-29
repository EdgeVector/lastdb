use crate::Text;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Safe wire error class. The value carries no credential, URL, key, or record body.
pub enum ErrorCode {
    /// A field failed a protocol check. `ProtocolError::invalid` uses this code.
    InvalidInput,
    /// The peer sent a protocol version this side does not accept.
    UnsupportedVersion,
    /// The requested capability is not available.
    UnsupportedCapability,
    /// The format is not supported.
    UnsupportedFormat,
    /// The key mode is not supported.
    UnsupportedKeyMode,
    /// The caller is not authorized for this operation.
    Unauthorized,
    /// The named object is absent. Callers distinguish a missing stream by `field`.
    NotFound,
    /// The account generation does not match.
    GenerationConflict,
    /// Stream authority does not match the caller's claim.
    StreamAuthorityConflict,
    /// The job is already closed.
    JobClosed,
    /// An identity does not match the frozen input.
    IdentityConflict,
    /// The predecessor head does not match.
    PredecessorConflict,
    /// A size or count limit was exceeded. `ProtocolError::limit` uses this code.
    ResourceLimit,
    /// The account quota was exceeded.
    QuotaExceeded,
    /// A required dependency is absent.
    MissingDependency,
    /// Stored bytes do not match the declared digest.
    IntegrityMismatch,
    /// The recovery public key does not match the account.
    RecoveryKeyMismatch,
    /// The client source is unavailable.
    SourceUnavailable,
    /// The provider has not finished. This is not a terminal failure.
    ProviderPending,
    /// The provider outcome is not known. This is not a terminal failure.
    OutcomeUnknown,
    /// The operation can be retried. This is not a terminal failure.
    RetryLater,
}

/// A safe error classification. No credential, URL, key, or record body is present.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolError {
    /// Which class of failure this is.
    pub code: ErrorCode,
    /// Protocol field the error refers to. This is a static name, not a caller value.
    pub field: String,
}

impl ProtocolError {
    /// Build an `InvalidInput` error naming `field`.
    pub fn invalid(field: &'static str) -> Self {
        Self {
            code: ErrorCode::InvalidInput,
            field: field.into(),
        }
    }

    /// Build a `ResourceLimit` error naming `field`.
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
    /// Bytes for this operation have not arrived.
    AwaitingBytes,
    /// The service is still checking the bytes.
    Verifying,
    /// The provider outcome is not known yet.
    OutcomeUnknown,
}

/// The exact operation cannot succeed under its frozen input.
/// This classification does not authorize deletion or release unknown byte reservations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalFailureCode {
    /// The frozen input failed a field check and cannot succeed.
    InvalidInput,
    /// The frozen input uses an unsupported format.
    UnsupportedFormat,
    /// The frozen input uses an unsupported key mode.
    UnsupportedKeyMode,
    /// The frozen input does not match its byte digest.
    IntegrityMismatch,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// A terminal failure of one frozen operation. It does not delete data or release unknown reservations.
pub struct TerminalFailure {
    /// Why this frozen operation cannot succeed.
    pub code: TerminalFailureCode,
    /// Protocol field the failure refers to, at most 128 bytes.
    pub field: Text<128>,
}

impl TerminalFailure {
    /// Reject a failure whose canonical JSON exceeds `RECEIPT_JSON_MAX_BYTES`.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        crate::limits::encoded_json_bytes(self, crate::limits::RECEIPT_JSON_MAX_BYTES).map(|_| ())
    }
}
