//! Account credentials and separately versioned opaque byte transport.
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

mod backup_transport;
pub mod byte_backup;
mod http;
pub mod ops;

pub(super) const DEFAULT_AUTH_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub(super) const MAX_AUTH_REFRESH_RETRIES: u32 = 5;

#[derive(Clone)]
pub enum SyncAuth {
    ApiKey(String),
    BearerToken(String),
}

impl std::fmt::Debug for SyncAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApiKey(_) => f.write_str("SyncAuth::ApiKey(****)"),
            Self::BearerToken(_) => f.write_str("SyncAuth::BearerToken(****)"),
        }
    }
}

/// Callback type for refreshing authentication credentials.
///
/// Called when the explicit V2 byte transport receives an authentication refusal.
/// Should return a fresh `SyncAuth` (e.g., by re-registering with Exemem).
pub type AuthRefreshCallback =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<SyncAuth, String>> + Send>> + Send + Sync>;

/// Explicit account credentials only. Construction starts no request or claim.
#[derive(Clone)]
pub struct AuthClient {
    base_url: String,
    auth: Arc<RwLock<SyncAuth>>,
    auth_refresh: Option<AuthRefreshCallback>,
    request_timeout: Duration,
}
