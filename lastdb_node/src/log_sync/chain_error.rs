//! Failures of signed-history grant, relay, and proposal calls.

use super::CloudState;

pub(crate) const NO_ACCOUNT: &str = "this node has no cloud account";
pub(crate) const UNCONFIRMED: &str = "signed grant was not confirmed";

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ChainError {
    /// No cloud credentials. Final: the caller does not retry.
    NoAccount,
    /// The signed entry did not reach local AcceptEntry and the signed log.
    Unconfirmed,
    /// The request cannot be a grant or relay call (400).
    Invalid(&'static str),
    /// The database, grantee, or signed entry is not held (404).
    NotFound(&'static str),
    /// The request contradicts the signed chain on this node (409).
    Conflict(&'static str),
    /// This node's key may not author that entry (403).
    Forbidden(&'static str),
}

/// What the app API answers: status, a code that names the cause when the
/// status alone does not, and the message.
pub(crate) struct Reply {
    pub(crate) status: u16,
    pub(crate) code: Option<&'static str>,
    pub(crate) message: &'static str,
    pub(crate) retry: bool,
}

impl ChainError {
    pub(crate) fn reply(&self) -> Reply {
        let (status, code, message, retry) = match self {
            Self::NoAccount => (409, Some("no_cloud_account"), NO_ACCOUNT, false),
            Self::Unconfirmed => (503, None, UNCONFIRMED, true),
            Self::Invalid(message) => (400, None, *message, false),
            Self::NotFound(message) => (404, None, *message, false),
            Self::Conflict(message) => (409, None, *message, false),
            Self::Forbidden(message) => (403, None, *message, false),
        };
        Reply {
            status,
            code,
            message,
            retry,
        }
    }
}

/// Gate a call on the node's cloud enrollment for `database_id`.
pub(crate) fn require_enrolled(
    host: &crate::host::Host,
    database_id: &str,
) -> Result<(), ChainError> {
    match super::wait_for_enrollment(host, database_id) {
        CloudState::Enrolled => Ok(()),
        CloudState::NoAccount => Err(ChainError::NoAccount),
    }
}
