//! Device id, delivery cursor, and durable on-disk sync markers under `<home>/log-sync/`.

use std::fs::{self, File};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::cloud::{first_backup_intent, FIRST_BACKUP_INTENT, FIRST_BACKUP_INTENT_FILE};
use crate::offline_home::Directory;

use super::disk;

const LIMIT: usize = 4096;
const LOG_SYNC_DIR: &str = "log-sync";
const WAKE_FILE: &str = "wake";
const SNAPSHOT_REQUEST_FILE: &str = "snapshot-request";
const SNAPSHOT_RESULT_FILE: &str = "snapshot-result";
const SNAPSHOT_ERROR_FILE: &str = "snapshot-error";
const SNAPSHOT_FIRST_FILE: &str = "snapshot-first";
const SNAPSHOT_BOOTSTRAP_FILE: &str = "snapshot-bootstrap.json";

#[derive(Serialize, Deserialize)]
struct SessionFile {
    cursor: u64,
}

#[derive(Serialize)]
struct PrefixFile<'a> {
    database_id: &'a str,
    through: u64,
}

pub(super) fn label_ok(value: &str) -> bool {
    crate::profile::valid_label_64(value)
}

pub(crate) fn device_id(home: &Path) -> Result<String, String> {
    if let Some(device) = saved_device_id(home)? {
        return Ok(device);
    }
    let fresh = uuid::Uuid::new_v4().to_string();
    disk::write_private(&device_path(home), format!("{fresh}\n").as_bytes())
        .map_err(|_| "cannot write device id")?;
    Ok(fresh)
}

pub(super) fn saved_device_id(home: &Path) -> Result<Option<String>, String> {
    let path = device_path(home);
    if let Some(bytes) = disk::read_limited(&path, LIMIT).map_err(|_| "cannot read device id")? {
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| "device id is not text")?
            .trim();
        if label_ok(text) {
            return Ok(Some(text.to_owned()));
        }
        return Err("device id is not a log label".into());
    }
    Ok(None)
}

pub(crate) fn load_cursor(home: &Path, database_id: &str) -> Result<u64, String> {
    let Some(bytes) = disk::read_limited(&session_path(home, database_id), LIMIT)
        .map_err(|_| "cannot read log sync session")?
    else {
        return Ok(0);
    };
    let file: SessionFile =
        serde_json::from_slice(&bytes).map_err(|_| "log sync session is invalid")?;
    Ok(file.cursor)
}

pub(crate) fn store_cursor(home: &Path, database_id: &str, cursor: u64) -> Result<(), String> {
    let bytes = serde_json::to_vec(&SessionFile { cursor })
        .map_err(|_| "log sync session encoding failed")?;
    disk::write_private(&session_path(home, database_id), &bytes)
        .map_err(|_| "cannot write log sync session".to_owned())
}

/// Record a durable first prefix, including an empty log with no snapshot.
pub(crate) fn mark_prefix(home: &Path, database_id: &str, through: u64) -> Result<(), String> {
    let bytes = serde_json::to_vec(&PrefixFile {
        database_id,
        through,
    })
    .map_err(|_| "log prefix encoding failed")?;
    disk::write_private(&prefix_path(home, database_id), &bytes)
        .map_err(|_| "cannot write log prefix".to_owned())
}

fn log_sync_dir(home: &Path) -> PathBuf {
    home.join(LOG_SYNC_DIR)
}

fn database_sync_dir(home: &Path, database_id: &str) -> PathBuf {
    log_sync_dir(home).join(database_id)
}

fn device_path(home: &Path) -> PathBuf {
    log_sync_dir(home).join("device-id")
}

fn session_path(home: &Path, database_id: &str) -> PathBuf {
    database_sync_dir(home, database_id).join("session.json")
}

fn prefix_path(home: &Path, database_id: &str) -> PathBuf {
    database_sync_dir(home, database_id).join("prefix.json")
}

fn wake_path(home: &Path) -> PathBuf {
    log_sync_dir(home).join(WAKE_FILE)
}

/// Persist `<home>/log-sync/wake` so the next sync turn clears holds and runs.
pub fn request_wake(home: &Path) {
    if !home.is_dir() {
        return;
    }
    let _ = disk::write_private(&wake_path(home), b"wake\n");
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
        || crate::host::owned::registered_databases(home)
            .iter()
            .any(|database| snapshot_pending(home, &database.id))
}
