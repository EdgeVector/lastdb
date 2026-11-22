//! Wake marker that ends the sync loop's sleep early, and the first-sync
//! intent file.

use std::path::Path;

use crate::cloud::{SIGNED_HISTORY_INTENT, SIGNED_HISTORY_INTENT_FILE};
use crate::offline_home::Directory;
use crate::profile::private_fs as disk;

const WAKE_FILE: &str = "wake";

pub fn request_wake(home: &Path) {
    let _ = disk::write_private(&home.join("log-sync").join(WAKE_FILE), b"1");
}

pub fn ensure_wake(home: &Path) {
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
pub fn publish_signed_history_intent(dir: &Directory) -> Result<(), String> {
    dir.publish(SIGNED_HISTORY_INTENT_FILE, None, SIGNED_HISTORY_INTENT)
        .map_err(|error| error.to_string())
}
