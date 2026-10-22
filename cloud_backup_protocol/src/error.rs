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
    GenerationConflict,
    PublisherFenced,
    IdentityConflict,
    RevisionConflict,
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
