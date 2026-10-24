//! Foreground V2 backup from an existing home. No node or local writer starts.
pub(in crate::byte_cloud_backup) mod identity;
mod operation;
mod paths;
mod report;
mod transport;

use super::{BackupError, Result};
use cloud_backup_protocol::Id;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

pub use report::{BoundaryReport, StandaloneReport};

pub struct Options {
    pub source_home: PathBuf,
    pub state_dir: PathBuf,
    pub credentials_home: PathBuf,
    pub environment: folddb_profile::endpoints::Environment,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Action {
    Start { request_id: Id },
    Resume,
    Status,
    Close,
}

/// Status is local and read-only. Other actions own one foreground invocation.
pub async fn request(
    options: &Options,
    action: Action,
    pause: &AtomicBool,
) -> Result<StandaloneReport> {
    if !options.credentials_home.is_absolute() {
        return Err(BackupError::Identity);
    }
    let identity = identity::Identity::open(&options.source_home)?;
    let paths = paths::Paths::open(identity.home(), &options.state_dir, false)?;
    if matches!(action, Action::Status) {
        return report::read(&identity, &paths, options.environment);
    }
    operation::run(options, action, identity, paths, pause).await
}

/// Observe an existing stopped home. The returned account comes from identity.key,
/// independently of the metadata-only native boundary; no account proof is added.
pub fn boundary(source_home: &std::path::Path) -> Result<BoundaryReport> {
    let identity = identity::Identity::open(source_home)?;
    let root = identity.home().join("data/disk-pack").canonicalize()?;
    let native_root = fold_db::storage::disk_pack::byte_backup::read_stopped_native_root(&root)?;
    identity.check()?;
    Ok(BoundaryReport {
        version: 1,
        account_id: identity.account().clone(),
        identity_public_key: identity.recovery().account_public_key.clone(),
        native_root,
        metadata_only: true,
        writer_fence_held: false,
    })
}

fn select(base: &std::path::Path, action: &Action) -> Result<Id> {
    match action {
        Action::Start { request_id } => Ok(request_id.clone()),
        Action::Resume | Action::Close => {
            super::store::Store::active_at(base)?.ok_or(BackupError::Missing)
        }
        Action::Status => Err(BackupError::Invalid),
    }
}
