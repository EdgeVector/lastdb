//! JSON error responses matching the relay OpenAPI `Error` schema.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

/// Relay API failure with OpenAPI `{code, field}` body.
#[derive(Debug, Clone)]
pub(crate) struct ApiError {
    status: StatusCode,
    code: &'static str,
    field: String,
    minimum_version: Option<u32>,
}

impl ApiError {
    pub(crate) fn unauthorized(field: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", field)
    }

    pub(crate) fn forbidden(field: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", field)
    }

    pub(crate) fn invalid(field: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", field)
    }

    pub(crate) fn not_found(field: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", field)
    }

    pub(crate) fn conflict(field: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", field)
    }

    /// A stream that a `blob/drop` named as its condition is not as named: its head moved, or it
    /// holds an entry above its head. `field` is `snapshot` for the snapshot stream and `heads`
    /// for a listed one.
    pub(crate) fn heads_moved(field: &'static str) -> Self {
        Self::new(StatusCode::CONFLICT, "heads", field)
    }

    pub(crate) fn rate_limited(field: impl Into<String>) -> Self {
        Self::new(StatusCode::TOO_MANY_REQUESTS, "rate_limited", field)
    }

    /// The entries asked for were dropped by a trim after a snapshot named them.
    pub(crate) fn trimmed(field: impl Into<String>) -> Self {
        Self::new(StatusCode::GONE, "trimmed", field)
    }

    pub(crate) fn batch_too_large(field: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "batch_too_large", field)
    }

    pub(crate) fn upgrade_required(field: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UPGRADE_REQUIRED,
            code: "upgrade_required",
            field: field.into(),
            minimum_version: Some(1),
        }
    }

    fn new(status: StatusCode, code: &'static str, field: impl Into<String>) -> Self {
        Self {
            status,
            code,
            field: field.into(),
            minimum_version: None,
        }
    }
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    minimum_version: Option<u32>,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            code: self.code,
            field: self.field,
            minimum_version: self.minimum_version,
        };
        (self.status, Json(body)).into_response()
    }
}

pub(crate) type ApiResult<T> = Result<T, ApiError>;
