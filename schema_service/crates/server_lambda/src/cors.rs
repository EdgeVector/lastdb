//! CORS response header values for the Lambda.
//!
//! The API gateway in front of the Lambda sets `Access-Control-Allow-Origin`.
//! The Lambda adds only the method and header lists.

pub(crate) const ACCESS_CONTROL_ALLOW_METHODS: &str = "GET, POST, OPTIONS";
pub(crate) const ACCESS_CONTROL_ALLOW_HEADERS: &str = "Content-Type, Authorization";
