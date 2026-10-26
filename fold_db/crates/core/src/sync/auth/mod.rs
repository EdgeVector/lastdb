//! Immutable API-key credentials and separately versioned opaque byte transport.
use std::sync::Arc;
use std::time::Duration;

mod backup_transport;
pub mod byte_backup;

pub(super) const DEFAULT_AUTH_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Explicit account credentials only. Construction starts no request or claim.
#[derive(Clone)]
pub struct AuthClient {
    base_url: String,
    api_key: Arc<str>,
}

impl AuthClient {
    pub fn new(base_url: String, api_key: String) -> Self {
        Self {
            base_url,
            api_key: api_key.into(),
        }
    }

    pub(super) fn apply_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request.header("X-API-Key", self.api_key.as_ref())
    }
}
