use super::{identity::Identity, paths::Paths, transport, BackupError, Result};
use crate::byte_cloud_backup::{model::Saved, store, Phase};
use cloud_backup_protocol::{v2::AcceptedBackup, AccountId, Digest, Id, NativeRoot};
use folddb_profile::endpoints::Environment;
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct StandaloneReport {
    pub version: u32,
    pub account_id: AccountId,
    pub identity_public_key: Digest,
    pub source_home: Id,
    pub request_id: Option<Id>,
    pub phase: Option<Phase>,
    pub captured_native_root: Option<NativeRoot>,
    pub capture_started_at: Option<u64>,
    pub accepted_backup: Option<AcceptedBackup>,
    pub retained_source_bytes: Option<u64>,
    pub terminal_clean: bool,
    pub close_requested: bool,
    pub objects_verified: u64,
    pub native_files: u64,
    pub native_bytes: u64,
    pub metadata_pack_objects: u64,
    pub multi_file_pack_objects: u64,
    pub blocked_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct BoundaryReport {
    pub version: u32,
    pub account_id: AccountId,
    pub identity_public_key: Digest,
    pub native_root: NativeRoot,
    pub metadata_only: bool,
    pub writer_fence_held: bool,
}

pub(super) fn read(
    identity: &Identity,
    paths: &Paths,
    env: Environment,
) -> Result<StandaloneReport> {
    let id = store::Store::active_at(&paths.coordinator)?;
    read_for(identity, paths, env, id)
}

pub(super) fn read_for(
    identity: &Identity,
    paths: &Paths,
    env: Environment,
    id: Option<Id>,
) -> Result<StandaloneReport> {
    paths.check()?;
    let saved = id
        .as_ref()
        .map(|id| store::state_at(&paths.coordinator, id))
        .transpose()?
        .flatten();
    let native_path = identity.home().join("data/disk-pack").canonicalize()?;
    let home = laststore::disk_pack::home::DiskHome::open(&native_path)
        .map_err(|_| BackupError::Source)?;
    let source_home = Id::try_from(home.descriptor().identity.to_string())?;
    if let Some(saved) = &saved {
        validate(identity, &source_home, saved, env)?;
    }
    let close = id
        .as_ref()
        .map(|id| {
            store::read::<Id>(
                &paths
                    .coordinator
                    .join("jobs")
                    .join(id.as_str())
                    .join("close.json"),
            )
            .and_then(|marker| match marker {
                Some(marker) if &marker != id => Err(BackupError::Identity),
                marker => Ok(marker.is_some()),
            })
        })
        .transpose()?
        .unwrap_or(false);
    identity.check()?;
    paths.check()?;
    let mut report = build(identity, source_home, saved.as_ref());
    report.close_requested = close;
    Ok(report)
}

pub(super) fn validate(
    identity: &Identity,
    source_home: &Id,
    saved: &Saved,
    env: Environment,
) -> Result<()> {
    saved.validate()?;
    let binding = saved.standalone.as_ref().ok_or(BackupError::Identity)?;
    if &saved.account != identity.account()
        || &saved.source_home != source_home
        || binding.source_home.as_path() != identity.home()
        || saved.endpoint != transport::endpoint(env)?
    {
        return Err(BackupError::Identity);
    }
    if let Some(begin) = &saved.begin {
        let expected = fold_db::storage::disk_pack::byte_backup::Bootstrap {
            recovery: identity.recovery().clone(),
        }
        .descriptor()?;
        if begin.descriptor != expected {
            return Err(BackupError::Identity);
        }
    }
    Ok(())
}

fn build(identity: &Identity, source_home: Id, saved: Option<&Saved>) -> StandaloneReport {
    StandaloneReport {
        version: 1,
        account_id: identity.account().clone(),
        identity_public_key: identity.recovery().account_public_key.clone(),
        source_home,
        request_id: saved.map(|s| s.request_id.clone()),
        phase: saved.map(|s| s.phase),
        captured_native_root: saved.and_then(|s| s.standalone.as_ref()?.native_root.clone()),
        capture_started_at: saved.and_then(|s| s.standalone.as_ref()?.capture_started_at),
        accepted_backup: saved.and_then(|s| s.accepted.clone()),
        retained_source_bytes: saved.and_then(|s| s.retained_source_bytes),
        terminal_clean: saved.is_some_and(Saved::terminal),
        close_requested: false,
        objects_verified: saved.map_or(0, |s| s.objects_verified),
        native_files: saved.map_or(0, |s| s.native_files),
        native_bytes: saved.map_or(0, |s| s.native_bytes),
        metadata_pack_objects: saved.map_or(0, |s| s.metadata_pack_objects),
        multi_file_pack_objects: saved.map_or(0, |s| s.multi_file_pack_objects),
        blocked_reason: saved.and_then(|s| s.blocked_reason.clone()),
    }
}
