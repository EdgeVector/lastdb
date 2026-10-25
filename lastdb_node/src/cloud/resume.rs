//! Cloud resume and pause marker files.

use super::*;

pub fn cloud_resume_required_path(home: &Path) -> PathBuf {
    home.join(CLOUD_RESUME_REQUIRED_FILE)
}

pub fn cloud_resume_requested_path(home: &Path) -> PathBuf {
    home.join(CLOUD_RESUME_REQUESTED_FILE)
}

pub fn cloud_resume_ready_path(home: &Path) -> PathBuf {
    home.join(CLOUD_RESUME_READY_FILE)
}

pub fn cloud_backup_source_copy_path(home: &Path) -> PathBuf {
    home.join(CLOUD_BACKUP_SOURCE_COPY_FILE)
}

pub(super) fn mark_durable_file(home: &Path, path: &Path) -> Result<(), String> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|error| format!("create {}: {error}", path.display()))?;
    file.sync_all()
        .map_err(|error| format!("sync {}: {error}", path.display()))?;
    std::fs::File::open(home)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| format!("sync {}: {error}", home.display()))?;
    Ok(())
}

pub(super) fn clear_durable_file(home: &Path, path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => sync_cloud_home_dir(home),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("remove {}: {error}", path.display())),
    }
}

pub(super) fn sync_cloud_home_dir(home: &Path) -> Result<(), String> {
    std::fs::File::open(home)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| format!("sync {}: {error}", home.display()))
}

pub fn mark_cloud_resume_required(home: &Path) -> Result<(), String> {
    mark_durable_file(home, &cloud_resume_required_path(home))
}

/// Native cloud activation is disabled. Refuse before a resume marker or
/// active configuration can change, including offline command callers.
pub fn prepare_primary_resume_file(_home: &Path) -> Result<bool, String> {
    Err(fold_db::FoldDB::CLOUD_SYNC_DISABLED.into())
}

pub fn read_paused_home_backup_receipt(home: &Path) -> Result<PausedHomeBackupReceipt, String> {
    let path = cloud_resume_ready_path(home);
    let bytes =
        std::fs::read(&path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let receipt: PausedHomeBackupReceipt = serde_json::from_slice(&bytes)
        .map_err(|error| format!("decode {}: {error}", path.display()))?;
    receipt.validate()?;
    Ok(receipt)
}

pub fn clear_cloud_resume_ready(home: &Path) -> Result<(), String> {
    clear_durable_file(home, &cloud_resume_ready_path(home))
}

pub fn clear_cloud_resume_required(home: &Path) -> Result<(), String> {
    clear_durable_file(home, &cloud_resume_required_path(home))
}

pub fn clear_cloud_resume_requested(home: &Path) -> Result<(), String> {
    clear_durable_file(home, &cloud_resume_requested_path(home))
}

/// Paths for L2 cloud intent + durable pause marker.
pub fn cloud_sync_paths(home: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
    (
        home.join(CLOUD_SYNC_CONFIG_FILE),
        home.join(CLOUD_SYNC_PAUSED_FILE),
    )
}

/// Move `cloud_sync.json` → `cloud_sync.json.paused` (idempotent if already paused).
///
/// Returns whether a rename was performed.
pub fn pause_cloud_sync_file(home: &Path) -> Result<bool, String> {
    let (active, paused) = cloud_sync_paths(home);
    if !active.exists() {
        if paused.exists() {
            return Ok(false); // already paused
        }
        return Err("no saved cloud credentials are present".into());
    }
    if paused.exists() {
        return Err("both active and paused cloud credentials exist; refusing replacement".into());
    }
    std::fs::rename(&active, &paused)
        .map_err(|e| format!("rename {} → {}: {e}", active.display(), paused.display()))?;
    sync_cloud_home_dir(home)?;
    Ok(true)
}

/// Refuse an active configuration rename until native cloud activation exists.
pub fn resume_cloud_sync_file(_home: &Path) -> Result<bool, String> {
    Err(fold_db::FoldDB::CLOUD_SYNC_DISABLED.into())
}

/// Whether L2 intent is present (active) or only paused on disk.
pub fn cloud_sync_file_state(home: &Path) -> &'static str {
    let (active, paused) = cloud_sync_paths(home);
    match (active.exists(), paused.exists()) {
        (true, _) => "on",
        (false, true) => "off",
        (false, false) => "unset",
    }
}
