//! Saved credential and Cloud Sync Off marker files.

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

pub fn mark_cloud_resume_required(home: &Path) -> Result<(), String> {
    mark_durable_file(home, &cloud_resume_required_path(home))
}

/// The saved account credentials. Explicit enrollment writes only this paused file.
pub fn cloud_credentials_path(home: &Path) -> PathBuf {
    home.join(CLOUD_SYNC_PAUSED_FILE)
}
