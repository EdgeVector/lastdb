use super::s3::PresignedUrl;
use reqwest::Client;
use serde::Deserialize;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Default bound on a single auth-Lambda request — the whole connect + send +
/// full-response-read cycle.
///
/// Without it, `post_once` does a bare `req.send().await` on the shared
/// `reqwest::Client`, so a TCP black-hole mid-call (connection accepted but
/// never answered, or a stalled response body) leaves the future awaiting
/// forever. That pins [`super::SyncEngine::sync`] in the `Syncing` state for
/// the process lifetime: every later cycle and `force_sync` short-circuits on
/// "already syncing", so cloud sync dies silently until a restart. Mirrors
/// [`super::s3::S3Client`]'s per-op timeout so the auth POSTs are as bounded as
/// the S3 transfers.
pub(super) const DEFAULT_AUTH_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Max credential-refresh retries on a repeated 401 for a single request.
/// A peer device re-registering (deactivating prior keys) can evict a freshly
/// minted key mid-request; a bounded backoff loop rides through that churn
/// while still failing fast on a genuinely bad credential.
pub(super) const MAX_AUTH_REFRESH_RETRIES: u32 = 5;

/// Authentication method for the sync auth Lambda.
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
/// Called when the sync engine receives a 401 from the auth Lambda.
/// Should return a fresh `SyncAuth` (e.g., by re-registering with Exemem).
pub type AuthRefreshCallback =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<SyncAuth, String>> + Send>> + Send + Sync>;

/// Response from the auth Lambda listing available S3 objects.
#[derive(Debug, Deserialize)]
pub struct ListObjectsResponse {
    pub ok: bool,
    #[serde(default)]
    pub objects: Vec<S3ObjectInfo>,
    #[serde(default)]
    pub continuation_token: Option<String>,
    #[serde(default)]
    pub has_more: Option<bool>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct S3ObjectInfo {
    pub key: String,
    pub size: u64,
    pub last_modified: String,
}

/// Response from the auth Lambda with presigned URLs.
#[derive(Debug, Deserialize)]
pub struct PresignedResponse {
    pub ok: bool,
    #[serde(default)]
    pub urls: Vec<PresignedUrl>,
    /// Sequence numbers assigned by the server (only populated for
    /// server-allocated scoped uploads). Ordered alongside `urls`.
    #[serde(default)]
    pub seq_numbers: Vec<u64>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub already_present: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Response from the auth Lambda for lock operations.
#[derive(Debug, Deserialize)]
pub struct LockResponse {
    pub ok: bool,
    #[serde(default)]
    pub locked_by: Option<String>,
    #[serde(default)]
    pub expires_at: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Client for the sync auth Lambda.
///
/// The auth Lambda:
/// 1. Validates authentication (API key or bearer token)
/// 2. Returns presigned S3 URLs scoped to the user's prefix
/// 3. Manages device locks
///
/// The client never gets AWS credentials — only time-limited presigned URLs.
#[derive(Clone)]
pub struct AuthClient {
    http: Arc<Client>,
    base_url: String,
    auth: Arc<RwLock<SyncAuth>>,
    /// Optional callback to refresh the credential on a 401, invoked inside
    /// [`AuthClient::post`]. When set, a 401 triggers one refresh-and-retry
    /// before the error surfaces — the same recovery the [`super::SyncEngine`]
    /// runs at its layer, but pushed down to the HTTP chokepoint so any caller
    /// that drives an `AuthClient` directly recovers too. Without it, an
    /// `AuthClient` built from a credential that later rotates (re-auth,
    /// bootstrap key issuance) is pinned to the stale key for the process
    /// lifetime and every call 401s.
    auth_refresh: Option<AuthRefreshCallback>,
    /// Optional database-root scope for new Mini cloud object keys. When set,
    /// request bodies that do not already name `share_prefix`, `org_hash`, or
    /// `db_hash` are sent with this `db_hash` so storage objects root at the
    /// database identity instead of the authenticated principal.
    db_hash: Option<String>,
    /// Outcome of this client's owner-claim attempt for [`Self::db_hash`],
    /// `None` until a scoped request has actually come back unregistered *and*
    /// the claim produced a definitive result.
    ///
    /// Held behind an async mutex rather than an atomic flag so concurrent
    /// scoped requests that all 403 at once do not each fire their own
    /// `register_db`: the first takes the lock and claims, the rest await that
    /// one result and then retry.
    ///
    /// - `Some(true)` — claim succeeded; no further `register_db` calls.
    /// - `Some(false)` — definitive refusal (another principal owns the db /
    ///   ban); do not re-attempt for the life of the client.
    /// - `None` — not yet tried, **or** last attempt was a transient
    ///   network/timeout/5xx failure; a later unregistered-403 may retry the
    ///   claim (must not permanently disable auto-claim after a blip).
    db_claim: Arc<tokio::sync::Mutex<Option<bool>>>,
    /// Whether a scoped 403 may claim an unregistered database root.
    ///
    /// Normal sync enables this recovery. Restore disables it because an
    /// authenticated read must never become a remote registry write.
    db_auto_claim: bool,
    /// Upper bound on any single auth-Lambda request. Enforced in
    /// [`AuthClient::post_once`] via a `tokio::time::timeout` around the whole
    /// request/response future so a black-holed transport cannot wedge the
    /// engine. Defaults to [`DEFAULT_AUTH_REQUEST_TIMEOUT`].
    request_timeout: Duration,
}

mod helpers;
mod http;
pub mod ops;
