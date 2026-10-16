//! S3-based sync for encrypted Sled databases.
//!
//! The sync module replicates a local Sled database to S3 as encrypted blobs.
//! The server never sees plaintext data — only opaque ciphertext.
//!
//! ## Architecture
//!
//! ```text
//! fold_db (local Sled)
//!       │
//!       ▼
//! SyncEngine
//!   ├── Records KvStore ops as encrypted log entries
//!   ├── Uploads log entries to S3 via presigned URLs
//!   ├── Compacts logs into snapshots every 100 entries
//!   └── Manages single-device write lock
//!       │
//!       ▼
//! Auth Lambda (thin)
//!   ├── Validates API key / bearer token
//!   ├── Returns presigned S3 URLs scoped to /{user_hash}/*
//!   └── Manages device lock (S3 object)
//!       │
//!       ▼
//! S3 Bucket
//!   /{user_hash}/
//!     snapshots/
//!       latest.enc          — most recent snapshot
//!       {seq}.enc           — historical snapshots
//!     log/
//!       {seq}.enc           — individual encrypted log entries
//!     lock.json             — device lock file
//! ```
//!
//! ## Data Flow
//!
//! **Write path:** mutation → encrypting store → Sled. Cloud upload staging is
//!   populated by cold store-level capture/snapshot paths, never as a
//!   precondition of local mutation.
//!
//! **Read path (bootstrap):** download latest.enc → decrypt → restore to Sled →
//!   list log entries after snapshot seq → download + replay each
//!
//! ## Security
//!
//! - All data encrypted client-side with AES-256-GCM before upload
//! - E2E key never leaves the client device
//! - Server sees only opaque encrypted blobs
//! - Presigned URLs expire after 15 minutes
//! - No AWS credentials on the client
//!
//! ## Conflict detection
//!
//! FoldDB's data model prevents divergence — atoms are content-addressed,
//! history is append-only, and molecule merge uses per-key LWW to converge
//! deterministically. However, when two nodes write different values to the
//! same molecule key, the losing write is silently overridden. This is
//! correct for convergence but the user may want to know it happened.
//!
//! The sync engine detects these "soft conflicts" during molecule merge and
//! stores them as `SyncConflict` records. These are queryable via the
//! conflict API so users can review what was overridden and acknowledge it.

pub mod auth;
pub(crate) mod capture;
pub mod cloud_purge;
pub mod cloud_purge_peer;
pub mod cloud_share;
pub mod engine;
pub mod error;
pub mod log;
pub(crate) mod mutation_intent;
pub mod org_sync;
pub(crate) mod policy;
mod replay_diagnosis;
pub use replay_diagnosis::{
    MutationIntentReplayError, ReplayApplyDiagnosis, ReplayCause, ReplayOperation,
};
pub(crate) mod delete_completion;
pub mod s3;
pub mod snapshot;
/// Continuous cloud sync v1 object model (snapshot + log + frontier + CAS).
/// Design: brain `design-lastdb-cloud-sync-snapshot-log`.
pub mod snapshot_log;
pub(crate) mod upload_completion;

pub use crate::backup_progress::{
    format_backup_progress_line, BackupCycleSample, BackupProgressSnapshot, BackupProgressTracker,
};

pub use auth::AuthRefreshCallback;

/// Upper bound on establishing a TCP connection to the sync/auth or S3
/// endpoints. A connect-phase black-hole (SYN accepted but the handshake never
/// completes, or an address routed to nowhere) is otherwise unbounded.
///
/// This is a connection-establishment cap only — it deliberately does NOT set a
/// request-level `timeout()`, which would abort long but healthy S3 blob
/// transfers. Per-request bounds live where they belong: `S3Client`'s per-op
/// `tokio::time::timeout` and `AuthClient`'s per-request timeout.
const SHARED_HTTP_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Build the shared `reqwest::Client` used by both `s3::S3Client` and
/// `auth::AuthClient`. HTTPS and no redirects protect credentials and signed URLs.
/// Construction failure reaches the caller; it never weakens transport policy.
pub fn build_shared_http_client() -> Result<reqwest::Client, reqwest::Error> {
    // trace-egress: propagate — AuthClient injects W3C on its service requests.
    // trace-egress: skip-s3 — S3Client preserves signed provider request headers.
    reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(SHARED_HTTP_CONNECT_TIMEOUT)
        .build()
}
pub use crate::SyncConflict;
pub use engine::{
    BackupUploadConcurrencyStatus, BootstrapOutcome, CloudSyncReenableOutcome, ForegroundPressure,
    PurgeOutcome, ReplayBlockerQuarantineOutcome, SyncBackupBlocker, SyncConfig, SyncEngine,
    SyncReplayBlocker, SyncState, SyncStatus,
};
pub use error::{SyncError, SyncResult};
pub use org_sync::{
    storage_prefix_for_key, strip_storage_prefix, SyncDestination, SyncPartitioner, SyncTarget,
};

/// Configuration needed to enable S3 sync.
///
/// Derived automatically from the Exemem credentials — no extra config needed.
/// The sync auth Lambda shares the same API URL and API key as the Exemem platform.
#[derive(Clone)]
pub struct SyncSetup {
    /// Exemem API base URL (sync routes live at /api/sync/*).
    pub auth_url: String,
    /// Authentication credential (same as Exemem auth).
    pub auth: auth::SyncAuth,
    /// Unique identifier for this device (auto-generated if not set).
    pub device_id: String,
    /// Sync tuning parameters. Uses defaults if None.
    pub config: Option<SyncConfig>,
    /// Optional callback to refresh authentication on 401.
    /// When provided, the sync engine will attempt to refresh credentials
    /// and retry once before giving up.
    pub auth_refresh: Option<AuthRefreshCallback>,
    /// True only for the local proof engine ([`SyncSetup::local_proof`]):
    /// capture and replay are wired, but nothing contacts a cloud.
    pub local_proof_engine: bool,
}

impl std::fmt::Debug for SyncSetup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncSetup")
            .field("auth_url", &self.auth_url)
            .field("auth", &self.auth)
            .field("device_id", &self.device_id)
            .field("config", &self.config)
            .field("auth_refresh", &self.auth_refresh.as_ref().map(|_| "..."))
            .finish()
    }
}

impl SyncSetup {
    /// Create SyncSetup from Exemem credentials.
    ///
    /// The sync auth Lambda is part of the Exemem platform, so the same
    /// `api_url` and `api_key` are reused. Device ID is read from the
    /// `FOLD_SYNC_DEVICE_ID` env var, or persisted to a `.device_id` file
    /// in `data_dir` so it survives restarts.
    pub fn from_exemem(api_url: &str, api_key: &str, data_dir: &str) -> Self {
        let device_id = std::env::var("FOLD_SYNC_DEVICE_ID")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| get_or_create_device_id(data_dir));

        Self {
            auth_url: api_url.to_string(),
            auth: auth::SyncAuth::ApiKey(api_key.to_string()),
            device_id,
            config: None,
            auth_refresh: None,
            local_proof_engine: false,
        }
    }

    /// Setup for the local proof engine: a sync engine that captures writes
    /// into the durable mutation log and replays segments, with no cloud.
    ///
    /// The auth URL is an unresolvable reserved name and the transport is
    /// HTTPS only, so no call can leave the machine. The device id comes
    /// from `FOLD_SYNC_DEVICE_ID`, else the `.device_id` file in `data_dir`.
    pub fn local_proof(data_dir: &str) -> Self {
        let mut setup = Self::from_exemem(LOCAL_PROOF_AUTH_URL, LOCAL_PROOF_API_KEY, data_dir);
        setup.local_proof_engine = true;
        setup
    }
}

/// Reserved `.invalid` name (RFC 2606): never resolves, so the local proof
/// engine cannot reach any service.
const LOCAL_PROOF_AUTH_URL: &str = "https://local-proof-no-cloud.invalid";
/// Placeholder credential. The local proof engine never presents it.
const LOCAL_PROOF_API_KEY: &str = "local-proof-no-credential";

/// Read a persisted device ID from `<data_dir>/.device_id`, or generate a new
/// UUID and write it there so the same ID is used across restarts.
pub fn get_or_create_device_id(data_dir: &str) -> String {
    let device_id_path = std::path::Path::new(data_dir).join(".device_id");

    // Try to read existing device ID
    if let Ok(id) = std::fs::read_to_string(&device_id_path) {
        let id = id.trim().to_string();
        if !id.is_empty() {
            return id;
        }
    }

    // Generate and persist new device ID
    let id = uuid::Uuid::new_v4().to_string();
    if let Some(parent) = device_id_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Err(e) = std::fs::write(&device_id_path, &id) {
        tracing::warn!(
            "Failed to persist device ID to {}: {}. \
             A new device ID will be generated on next restart.",
            device_id_path.display(),
            e
        );
    }
    id
}
