//! A final backup of a clean, stopped home through the ordinary V2 worker.
use super::{
    capture::Capture, model::Saved, source::Source, store::Store, BackupError, Phase, Result,
};
use cloud_backup_protocol::v2::AcceptedBackup;
use cloud_backup_protocol::{AccountId, Id};
use fold_db::storage::disk_pack::byte_backup::RunBackupProfile;
use fold_db::storage::disk_pack::PackNamespacedStore;
use fold_db::storage::traits::NamespacedStore;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::AtomicBool;

mod guard;
mod manifest;
mod marker;
mod verify;
pub(super) use manifest::retain_manifest;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoppedBackupRequest {
    pub request_id: Id,
    pub account_id: AccountId,
    pub stopped_pid: u32,
    pub stopped_start_ts: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoppedBackupReport {
    pub version: u16,
    pub request: StoppedBackupRequest,
    pub accepted_backup: AcceptedBackup,
    pub run_profile: RunBackupProfile,
    pub native_files: u64,
    pub native_bytes: u64,
    pub verified_remote_objects: u64,
    pub all_remote_file_hashes_verified: bool,
    pub source_released: bool,
    pub native_cut_unchanged: bool,
    pub automatic_backup_enabled: bool,
}

pub async fn backup_stopped(
    home: &Path,
    request: StoppedBackupRequest,
) -> Result<StoppedBackupReport> {
    let ownership = guard::admit(home, &request)?;
    if super::automatic::reserved(&request.request_id) {
        return Err(BackupError::Invalid);
    }
    let permit = Store::lock(home)?;
    super::control::ensure_previous(home, &request.request_id)?;
    let seed = super::connection::identity_seed(home)?;
    let pair = fold_db::security::Ed25519KeyPair::from_secret_key(seed.as_ref())
        .map_err(|_| BackupError::Identity)?;
    let account = lastdb_identity::user_hash_from_pubkey(&pair.public_key_base64())
        .map_err(|_| BackupError::Identity)?;
    if account != request.account_id.as_str() {
        return Err(BackupError::Identity);
    }
    let keys =
        fold_db::crypto::E2eKeys::from_ed25519_seed(&seed).map_err(|_| BackupError::Identity)?;
    let node_key = zeroize::Zeroizing::new(keys.encryption_key());
    let storage = PackNamespacedStore::open_existing_for_backup(&home.join("data"), &node_key)?;
    let source = Source::from_stopped(
        storage.disk_pack_runtime(),
        &pair,
        request.account_id.clone(),
    )?;
    let client = super::connection::client_for_stopped(home, &source.account, &source.source_home)?;
    let endpoint = client.canonical_endpoint();
    super::automatic::check_stopped(home, &source.account, &source.source_home, &endpoint)?;
    let store = Store::open(home, &request.request_id)?;
    manifest::same(&store, "stopped-session.json", &request)?;
    let mut saved: Saved = store
        .state()?
        .unwrap_or_else(|| source.saved(request.request_id.clone(), client.canonical_endpoint()));
    source.validate(&saved)?;
    if saved.endpoint != client.canonical_endpoint()
        || saved.automatic_revision.is_some()
        || !matches!(saved.work, super::work::Work::Snapshot { .. })
    {
        return Err(BackupError::Identity);
    }
    ownership.check(home, &request)?;
    saved.blocked_reason = None;
    store.save(&saved)?;
    store.select(&request.request_id)?;
    let pause = AtomicBool::new(false);
    if let Err(error) = super::worker::run(&source, &store, &mut saved, &client, &pause).await {
        saved.blocked_reason = Some(error.to_string());
        store.write("pause.json", &error.pause_state())?;
        store.save(&saved)?;
        return Err(error);
    }
    if saved.phase != Phase::Published || !saved.terminal() {
        return Err(BackupError::Source);
    }
    let accepted = saved.accepted.clone().ok_or(BackupError::Missing)?;
    let manifest: manifest::Manifest = store
        .read("stopped-source.json")?
        .ok_or(BackupError::Missing)?;
    if manifest.begin != accepted.begin || manifest.version != 1 {
        return Err(BackupError::Identity);
    }
    ownership.check_profile(home, &request, &manifest.run_profile)?;
    let remote = verify::remote(&client, &accepted, &manifest, &store).await?;
    ownership.check_profile(home, &request, &manifest.run_profile)?;
    let point: super::recovery::RecoveryPoint =
        store.read("recovery.json")?.ok_or(BackupError::Missing)?;
    point.validate()?;
    if point.accepted != accepted {
        return Err(BackupError::Identity);
    }
    let automatic_backup_enabled = super::automatic::record_stopped(
        home,
        &source.account,
        &source.source_home,
        &endpoint,
        point.frontiers,
    )?;
    let report = StoppedBackupReport {
        version: 1,
        request,
        accepted_backup: accepted,
        run_profile: manifest.run_profile,
        native_files: manifest.native_files,
        native_bytes: manifest.native_bytes,
        verified_remote_objects: remote,
        all_remote_file_hashes_verified: true,
        source_released: true,
        native_cut_unchanged: true,
        automatic_backup_enabled,
    };
    manifest::same(&store, "stopped-report.json", &report)?;
    drop(source);
    drop(storage);
    drop(permit);
    drop(ownership);
    Ok(report)
}
