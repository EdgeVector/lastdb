//! Stopped-home backup source markers.

use super::*;

/// Durable pause marker: `<home>/cloud_sync.json.paused`.
///
/// Ops used to `mv cloud_sync.json cloud_sync.json.paused` by hand. The
/// `lastdb cloud off` / `on` commands use this path so reboots keep pause
/// intent without deleting credentials.
pub const CLOUD_SYNC_PAUSED_FILE: &str = "cloud_sync.json.paused";

/// A durable barrier for local writes accepted during a boot without sync.
/// Cloud Sync cannot turn On while this marker exists.
pub const CLOUD_RESUME_REQUIRED_FILE: &str = ".cloud_resume_required";
/// The owner requested one backup from paused credentials on the next boot.
/// The old daemon still sees the paused configuration.
pub const CLOUD_RESUME_REQUESTED_FILE: &str = ".cloud_resume_requested";
/// One paused-home backup committed in this process. This receipt does not
/// permit Cloud Sync to turn On.
pub const CLOUD_RESUME_READY_FILE: &str = ".cloud_resume_ready";
/// The supervised stop-and-copy action writes this only in a stopped copy.
pub const CLOUD_BACKUP_SOURCE_COPY_FILE: &str = ".cloud_backup_source_copy";
pub const CLOUD_BACKUP_UNPROVED_FLUSH_CLAIM_FILE: &str = ".cloud_backup_unproved_flush_claim";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupSourceCopyMarker {
    pub version: u32,
    pub source_pid: u32,
    pub source_start_ts: u64,
    pub copied_at_unix_s: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_proof: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_approved: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_proof: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BackupUnprovedFlushClaim {
    version: u32,
    source_pid: u32,
    source_start_ts: u64,
    flush_proof: String,
    owner_approved: String,
    decision_slug: String,
    copy_path: PathBuf,
}

pub(super) fn read_regular_small_file(path: &Path, max_bytes: u64) -> Result<Vec<u8>, String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("read {} metadata: {error}", path.display()))?;
    if !metadata.file_type().is_file() || metadata.len() > max_bytes {
        return Err(format!("{} must be a small regular file", path.display()));
    }
    std::fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))
}

/// Check an inert stopped copy before an offline cloud operation opens a store.
pub fn validate_stopped_backup_source_copy(home: &Path) -> Result<BackupSourceCopyMarker, String> {
    let marker_path = cloud_backup_source_copy_path(home);
    let marker: BackupSourceCopyMarker =
        serde_json::from_slice(&read_regular_small_file(&marker_path, 4096)?)
            .map_err(|error| format!("invalid stopped-copy marker: {error}"))?;
    if marker.source_pid == 0
        || marker.source_start_ts == 0
        || marker.copied_at_unix_s < marker.source_start_ts
    {
        return Err("invalid stopped-copy marker fields".into());
    }

    let receipt_path = crate::session_ledger::shutdown_flush_receipt_path(home);
    match marker.version {
        1 if marker.flush_proof.is_none()
            && marker.owner_approved.is_none()
            && marker.stop_proof.is_none() =>
        {
            read_regular_small_file(&receipt_path, 4096)?;
            let receipt = crate::session_ledger::read_shutdown_flush_receipt(home)
                .map_err(|error| format!("invalid shutdown flush proof: {error}"))?;
            if receipt.pid != marker.source_pid || receipt.start_ts != marker.source_start_ts {
                return Err("stopped-copy marker does not match shutdown flush proof".into());
            }
        }
        2 if marker.flush_proof.as_deref() == Some("absent")
            && marker.owner_approved.as_deref() == Some("2026-10-06")
            && marker.stop_proof.as_deref() == Some("supervised_sigterm_no_forced_kill") =>
        {
            let claim: BackupUnprovedFlushClaim = serde_json::from_slice(&read_regular_small_file(
                &home.join(CLOUD_BACKUP_UNPROVED_FLUSH_CLAIM_FILE),
                4096,
            )?)
            .map_err(|error| format!("invalid unproved flush claim: {error}"))?;
            let exact_home = home
                .canonicalize()
                .map_err(|error| format!("resolve stopped-copy home: {error}"))?;
            if claim.version != 1
                || claim.source_pid != marker.source_pid
                || claim.source_start_ts != marker.source_start_ts
                || claim.flush_proof != "absent"
                || claim.owner_approved != "2026-10-06"
                || claim.decision_slug != "decision-2026-10-06-cloud-sync-rescue-risk-acceptance"
                || claim.copy_path != exact_home
            {
                return Err("unproved flush claim does not match this stopped copy".into());
            }
            if std::fs::symlink_metadata(&receipt_path).is_ok() {
                return Err("waived stopped copy must not claim a shutdown flush receipt".into());
            }
        }
        _ => return Err("unsupported stopped-copy proof or owner waiver".into()),
    }
    if std::fs::symlink_metadata(crate::session_ledger::Ledger::current_session_path(home)).is_ok()
    {
        return Err("a live session file is present in the stopped copy".into());
    }
    for path in [
        lastdb_uds::uds::socket_path(&home.join("data")),
        home.join("data/folddb-full.sock"),
    ] {
        if std::fs::symlink_metadata(&path).is_ok() {
            return Err(format!(
                "a socket path is present in the stopped copy: {}",
                path.display()
            ));
        }
    }

    let (active, paused) = cloud_sync_paths(home);
    if std::fs::symlink_metadata(&active).is_ok()
        || std::fs::symlink_metadata(cloud_resume_ready_path(home)).is_ok()
    {
        return Err("stopped-copy backup requires Cloud Sync Off and a pending backup".into());
    }
    if marker.version == 1 {
        read_regular_small_file(&cloud_resume_required_path(home), 4096)?;
    }
    let bytes = read_regular_small_file(&paused, 65_536)?;
    let _: fold_db::storage::config::CloudSyncConfig = serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid paused cloud configuration: {error}"))?;
    lastdb_identity::load_seed(home)?
        .ok_or_else(|| "stopped copy has no identity key".to_string())?;

    let store_root = home.join("data");
    if !host::data_dir_has_existing_store(&store_root)
        || laststore::describe_home(&store_root)
            .map_err(|error| format!("invalid stopped-copy store layout: {error}"))?
            .is_none()
        || fold_db::storage::laststore::read_cloud_db_hash(&store_root).is_none()
    {
        return Err("stopped copy has no complete LastStore identity".into());
    }
    Ok(marker)
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PausedHomeBackupReceipt {
    pub version: u32,
    pub manifest_sha256: String,
    pub manifest_counter: u64,
}

impl PausedHomeBackupReceipt {
    pub(super) fn validate(&self) -> Result<(), String> {
        if self.version != 1
            || self.manifest_counter == 0
            || self.manifest_sha256.len() != 64
            || !self
                .manifest_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err("invalid paused-home backup receipt".into());
        }
        Ok(())
    }
}
