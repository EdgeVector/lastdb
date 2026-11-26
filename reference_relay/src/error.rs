//! Refusals, answered as the protocol's [`ErrorAnswer`] with the status of its code.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use relay_protocol::{ErrorAnswer, ErrorCode};

/// The lowest protocol version this relay serves, named on a 426.
const MINIMUM_VERSION: u32 = 1;

/// A relay refusal: the protocol error answer, sent with the status its code carries.
#[derive(Debug, Clone)]
pub(crate) struct ApiError(ErrorAnswer);

impl ApiError {
    pub(crate) fn unauthorized(field: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unauthorized, field)
    }

    pub(crate) fn forbidden(field: impl Into<String>) -> Self {
        Self::new(ErrorCode::Forbidden, field)
    }

    pub(crate) fn invalid(field: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidRequest, field)
    }

    pub(crate) fn not_found(field: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, field)
    }

    pub(crate) fn conflict(field: impl Into<String>) -> Self {
        Self::new(ErrorCode::Conflict, field)
    }

    /// A stream that a `blob/drop` named as its condition is not as named: its head moved, or it
    /// holds an entry above its head. `field` is `snapshot` for the snapshot stream and `heads`
    /// for a listed one.
    pub(crate) fn heads_moved(field: &'static str) -> Self {
        Self::new(ErrorCode::Heads, field)
    }

    pub(crate) fn rate_limited(field: impl Into<String>) -> Self {
        Self::new(ErrorCode::RateLimited, field)
    }

    /// The entries asked for were dropped by a trim after a snapshot named them.
    pub(crate) fn trimmed(field: impl Into<String>) -> Self {
        Self::new(ErrorCode::Trimmed, field)
    }

    pub(crate) fn batch_too_large(field: impl Into<String>) -> Self {
        Self::new(ErrorCode::BatchTooLarge, field)
    }

    pub(crate) fn upgrade_required(field: impl Into<String>) -> Self {
        let mut answer = ErrorAnswer::new(ErrorCode::UpgradeRequired, field);
        answer.minimum_version = Some(MINIMUM_VERSION);
        Self(answer)
    }

    fn new(code: ErrorCode, field: impl Into<String>) -> Self {
        Self(ErrorAnswer::new(code, field))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status =
            StatusCode::from_u16(self.0.code.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (status, Json(self.0)).into_response()
    }
}

pub(crate) type ApiResult<T> = Result<T, ApiError>;
