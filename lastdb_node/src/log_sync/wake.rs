//! Wake markers and first-sync intent (signed history, not snapshots).

use std::path::Path;

use crate::cloud::{FIRST_BACKUP_INTENT, FIRST_BACKUP_INTENT_FILE};
use crate::offline_home::Directory;
use crate::profile::private_fs as disk;

const WAKE_FILE: &str = "wake";

pub fn request_wake(home: &Path) {
    let _ = disk::write_private(&home.join("log-sync").join(WAKE_FILE), b"1");
}

pub(crate) fn ensure_wake(home: &Path) {
    request_wake(home);
}

pub(super) fn consume_wake(home: &Path) -> bool {
    let path = home.join("log-sync").join(WAKE_FILE);
    match std::fs::remove_file(&path) {
        Ok(()) => true,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => false,
    }
}

/// Record that this home should bootstrap signed history on next sync.
pub fn publish_first_backup_intent(dir: &Directory) -> Result<(), String> {
    dir.publish(FIRST_BACKUP_INTENT_FILE, None, FIRST_BACKUP_INTENT)
        .map_err(|error| error.to_string())
}
