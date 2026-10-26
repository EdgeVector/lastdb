//! V2 account byte transport, local maintenance and passive retired metadata.
//! No legacy cloud engine, provider admission, replay worker or scheduler exists.
pub mod auth;
pub(crate) mod capture;
pub mod cloud_purge;
pub mod cloud_share;
pub mod engine;
pub mod error;
pub mod log;
pub(crate) mod mutation_intent;
pub mod policy;
mod replay_diagnosis;
mod retired_cloud_repair;
pub mod scope_alias;
pub mod snapshot;
pub mod snapshot_log;
pub use crate::backup_progress::{format_backup_progress_line, BackupProgressSnapshot};
pub use crate::SyncConflict;
pub use auth::AuthRefreshCallback;
pub use error::{SyncError, SyncResult};
pub use replay_diagnosis::{
    MutationIntentReplayError, ReplayApplyDiagnosis, ReplayCause, ReplayOperation,
};
pub use retired_cloud_repair::check_retired_cloud_repair;

const SHARED_HTTP_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

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
