//! Wake signal that ends the sync loop's sleep early, and the first-sync
//! intent file.
//!
//! A wake needs no durability: a lost wake waits for the next timed turn, and
//! the loop runs a turn when it starts. So nothing here waits for the drive.
//! - Inside the daemon, [`wake_loop`] sets a flag and wakes the sleeping loop
//!   through a `Notify`. A committed write calls it before its reply, so it
//!   touches no file.
//! - Another process (the `lastdb` command) calls [`request_wake`], which
//!   creates the empty `log-sync/wake` marker. The sleeping loop checks for it
//!   every `WAKE_POLL`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::LazyLock;
use std::time::Duration;

use tokio::sync::Notify;

use crate::cloud::{SIGNED_HISTORY_INTENT, SIGNED_HISTORY_INTENT_FILE};
use crate::offline_home::Directory;
use crate::profile::private_fs as disk;

const WAKE_FILE: &str = "wake";
/// How often a sleeping sync turn checks for the marker of another process.
const WAKE_POLL: Duration = Duration::from_millis(50);

/// A wake asked inside this process and not yet taken by a turn.
static WOKEN: AtomicBool = AtomicBool::new(false);
/// Ends the sleep of the loop as soon as [`WOKEN`] is set.
static WAKE: LazyLock<Notify> = LazyLock::new(Notify::new);

/// Ask the sync loop of this daemon to run a turn now. No file, no drive flush.
pub(crate) fn wake_loop() {
    WOKEN.store(true, Ordering::Release);
    WAKE.notify_waiters();
}

/// Ask the sync loop of the daemon that serves `home` to run a turn, from another process.
/// Creates the empty marker when it is absent; nothing waits for the drive.
pub fn request_wake(home: &Path) {
    let _ = disk::touch_marker(&wake_path(home));
}

/// Take every pending wake, from this process and from the marker.
pub(super) fn consume_wake(home: &Path) -> bool {
    let woken = WOKEN.swap(false, Ordering::AcqRel);
    let marked = std::fs::remove_file(wake_path(home)).is_ok();
    woken || marked
}

fn wake_path(home: &Path) -> PathBuf {
    home.join("log-sync").join(WAKE_FILE)
}

fn wake_pending(home: &Path) -> bool {
    WOKEN.load(Ordering::Acquire) || wake_path(home).is_file()
}

/// Sleep up to `limit`, returning early when a wake arrives.
pub(super) async fn pause_unless_wake(home: &Path, limit: Duration) {
    let mut left = limit;
    while left > Duration::ZERO {
        let woken = WAKE.notified();
        tokio::pin!(woken);
        // Registered before the check, so a wake between the check and the wait still ends it.
        woken.as_mut().enable();
        if wake_pending(home) {
            return;
        }
        let step = WAKE_POLL.min(left);
        tokio::select! {
            () = &mut woken => return,
            () = tokio::time::sleep(step) => {}
        }
        left = left.saturating_sub(step);
    }
}

/// Record that this home should bootstrap signed history on next sync.
pub fn publish_signed_history_intent(dir: &Directory) -> Result<(), String> {
    dir.publish(SIGNED_HISTORY_INTENT_FILE, None, SIGNED_HISTORY_INTENT)
        .map_err(|error| error.to_string())
}
