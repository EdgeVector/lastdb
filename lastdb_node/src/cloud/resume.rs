//! Cloud resume and pause marker files.

use super::*;

pub fn cloud_resume_required_path(home: &Path) -> PathBuf {
    home.join(CLOUD_RESUME_REQUIRED_FILE)
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

pub(super) fn sync_cloud_home_dir(home: &Path) -> Result<(), String> {
    std::fs::File::open(home)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| format!("sync {}: {error}", home.display()))
}

pub fn mark_cloud_resume_required(home: &Path) -> Result<(), String> {
    mark_durable_file(home, &cloud_resume_required_path(home))
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
