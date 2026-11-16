use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Safe wire error class. The value carries no credential, URL, key, or record body.
pub enum ErrorCode {
    /// A field failed a protocol check. `ProtocolError::invalid` uses this code.
    InvalidInput,
    /// The requested capability is not available.
    UnsupportedCapability,
    /// The caller is not authorized for this operation.
    Unauthorized,
    /// The named object is absent. Callers distinguish a missing stream by `field`.
    NotFound,
    /// The account generation does not match.
    GenerationConflict,
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

    /// Build an `Unauthorized` error naming `field`.
    pub fn unauthorized(field: &'static str) -> Self {
        Self {
            code: ErrorCode::Unauthorized,
            field: field.into(),
        }
    }

    /// True when the service refused a paid cloud operation for an inactive subscription.
    #[must_use]
    pub fn is_payment_subscription(&self) -> bool {
        self.code == ErrorCode::Unauthorized && self.field == "payment.subscription"
    }
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.field)
    }
}

impl std::error::Error for ProtocolError {}
