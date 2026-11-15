//! Durable on-disk sync markers under `<home>` and `<home>/log-sync/`.
//!
//! Keeps the canonical filenames for wake requests, initial personal backup
//! intent, and per-database snapshot lifecycle files in one module while
//! running daemons wake immediately through `Host::cloud_sync`.

use std::fs::{self, File};
use std::path::{Path, PathBuf};

use crate::cloud::{first_backup_intent, FIRST_BACKUP_INTENT, FIRST_BACKUP_INTENT_FILE};
use crate::offline_home::Directory;

const LOG_SYNC_DIR: &str = "log-sync";
const WAKE_FILE: &str = "wake";
const SNAPSHOT_REQUEST_FILE: &str = "snapshot-request";
const SNAPSHOT_RESULT_FILE: &str = "snapshot-result";
const SNAPSHOT_ERROR_FILE: &str = "snapshot-error";
const SNAPSHOT_FIRST_FILE: &str = "snapshot-first";
const SNAPSHOT_BOOTSTRAP_FILE: &str = "snapshot-bootstrap.json";

fn log_sync_dir(home: &Path) -> PathBuf {
    home.join(LOG_SYNC_DIR)
}

fn database_sync_dir(home: &Path, database_id: &str) -> PathBuf {
    log_sync_dir(home).join(database_id)
}

fn wake_path(home: &Path) -> PathBuf {
    log_sync_dir(home).join(WAKE_FILE)
}

/// Persist `<home>/log-sync/wake` so the next sync turn clears holds and runs.
pub fn request_wake(home: &Path) {
    if !home.is_dir() {
        return;
    }
    let _ = super::disk::write_private(&wake_path(home), b"wake\n");
}

/// Ensure `<home>/log-sync/wake` is present before signalling an in-process wake.
pub(crate) fn ensure_wake(home: &Path) {
    if !wake_requested(home) {
        request_wake(home);
    }
}

pub(super) fn wake_requested(home: &Path) -> bool {
    wake_path(home).exists()
}

pub(super) fn consume_wake(home: &Path) -> bool {
    fs::remove_file(wake_path(home)).is_ok()
}

/// Publish `<home>/.native-first-backup` inside an opened offline home directory.
pub fn publish_first_backup_intent(dir: &Directory) -> Result<(), String> {
    dir.publish(FIRST_BACKUP_INTENT_FILE, None, FIRST_BACKUP_INTENT)
        .map_err(|error| error.to_string())
}

pub(super) fn has_first_backup_intent(home: &Path) -> bool {
    first_backup_intent(home)
}

pub(super) fn clear_first_backup_intent(home: &Path) -> Result<(), String> {
    match fs::remove_file(home.join(FIRST_BACKUP_INTENT_FILE)) {
        Ok(()) => File::open(home)
            .and_then(|file| file.sync_all())
            .map_err(|err| err.to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

pub(super) fn snapshot_request_path(home: &Path, database_id: &str) -> PathBuf {
    database_sync_dir(home, database_id).join(SNAPSHOT_REQUEST_FILE)
}

pub(super) fn snapshot_result_path(home: &Path, database_id: &str) -> PathBuf {
    database_sync_dir(home, database_id).join(SNAPSHOT_RESULT_FILE)
}

pub(super) fn snapshot_error_path(home: &Path, database_id: &str) -> PathBuf {
    database_sync_dir(home, database_id).join(SNAPSHOT_ERROR_FILE)
}

pub(super) fn snapshot_first_path(home: &Path, database_id: &str) -> PathBuf {
    database_sync_dir(home, database_id).join(SNAPSHOT_FIRST_FILE)
}

pub(super) fn snapshot_bootstrap_path(home: &Path, database_id: &str) -> PathBuf {
    database_sync_dir(home, database_id).join(SNAPSHOT_BOOTSTRAP_FILE)
}

pub(super) fn snapshot_pending(home: &Path, database_id: &str) -> bool {
    snapshot_request_path(home, database_id).exists()
}

pub(super) fn any_wake_marker(home: &Path) -> bool {
    wake_requested(home)
        || super::registry::databases(home)
            .iter()
            .any(|database| snapshot_pending(home, &database.id))
}
