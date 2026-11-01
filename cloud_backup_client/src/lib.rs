//! HTTPS client for the native LastDB cloud backup service.
//!
//! Immutable API-key credentials and a separately versioned opaque byte
//! transport over the `cloud_backup_protocol` wire contract. The crate knows
//! no database type: the core (`fold_db`) does no network I/O, and the node
//! (`lastdb_node`) owns every caller.
use std::sync::Arc;
use std::time::Duration;

mod backup_transport;
pub mod byte_backup;

pub(crate) const DEFAULT_AUTH_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const SHARED_HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

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

    pub(crate) fn apply_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request.header("X-API-Key", self.api_key.as_ref())
    }
}

/// Protected HTTP construction for explicit account and V2 byte operations.
pub fn build_shared_http_client() -> Result<reqwest::Client, reqwest::Error> {
    // trace-egress: propagate — AuthClient injects W3C on its service requests.
    // trace-egress: skip-s3 — byte transport preserves signed provider request headers.
    reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(SHARED_HTTP_CONNECT_TIMEOUT)
        .build()
}
