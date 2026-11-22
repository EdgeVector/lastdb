//! Wake marker that ends the sync loop's sleep early, and the first-sync
//! intent file.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::cloud::{SIGNED_HISTORY_INTENT, SIGNED_HISTORY_INTENT_FILE};
use crate::offline_home::Directory;
use crate::profile::private_fs as disk;

const WAKE_FILE: &str = "wake";
/// How often a sleeping sync turn rechecks the wake file. Authors only write
/// this file; without a poll the 2s ACTIVE sleep would hold a new tip until
/// the timer ended (People circles saw grant revokes authored but not uploaded).
const WAKE_POLL: Duration = Duration::from_millis(50);

pub fn request_wake(home: &Path) {
    let _ = disk::write_private(&home.join("log-sync").join(WAKE_FILE), b"1");
}

pub fn ensure_wake(home: &Path) {
    request_wake(home);
}

pub(super) fn consume_wake(home: &Path) -> bool {
    let path = wake_path(home);
    match std::fs::remove_file(&path) {
        Ok(()) => true,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => false,
    }
}

fn wake_path(home: &Path) -> PathBuf {
    home.join("log-sync").join(WAKE_FILE)
}

fn wake_pending(home: &Path) -> bool {
    wake_path(home).is_file()
}

/// Sleep up to `limit`, returning early when a wake marker appears.
pub(super) async fn pause_unless_wake(home: &Path, limit: Duration) {
    let mut left = limit;
    while left > Duration::ZERO {
        if wake_pending(home) {
            return;
        }
        let step = WAKE_POLL.min(left);
        tokio::time::sleep(step).await;
        left = left.saturating_sub(step);
    }
}

/// Record that this home should bootstrap signed history on next sync.
pub fn publish_signed_history_intent(dir: &Directory) -> Result<(), String> {
    dir.publish(SIGNED_HISTORY_INTENT_FILE, None, SIGNED_HISTORY_INTENT)
        .map_err(|error| error.to_string())
}
