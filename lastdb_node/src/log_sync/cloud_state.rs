//! One database's cloud enrollment, from the sync loop's existing state.
//!
//! `NoAccount`: verified credentials are missing, or they have no account connection.
//! `Enrolling`: a usable account exists, and the cloud log is not in native's confirmed set.
//! `Enrolled`: `native::database_confirmed`. There is no second tracker.

use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;

use tokio::sync::Notify;

use crate::host::Host;

const WAIT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(200);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CloudState {
    NoAccount,
    Enrolling,
    Enrolled,
}

pub(crate) fn has_cloud_account(home: &Path) -> bool {
    match crate::cloud::load_verified_cloud_credentials(home) {
        Ok(Some(creds)) => creds.connection.is_some(),
        _ => false,
    }
}

pub(crate) fn database_cloud_state(home: &Path, database_id: &str) -> CloudState {
    if !has_cloud_account(home) {
        CloudState::NoAccount
    } else if super::native::database_confirmed(database_id) {
        CloudState::Enrolled
    } else {
        CloudState::Enrolling
    }
}

/// Wakes every waiter that is already registered. A waiter that has not
/// registered yet misses this signal. The poll covers that miss.
pub(crate) fn note_enrolled() {
    signal().notify_waiters();
}

/// `NoAccount` returns at once. `Enrolling` waits at most 30 seconds.
/// The wait is async, so it stays inside the handler timeout.
pub(crate) async fn wait_for_enrollment(host: &Host, database_id: &str) -> CloudState {
    let state = database_cloud_state(&host.home, database_id);
    if !matches!(state, CloudState::Enrolling) {
        return state;
    }
    super::session::ensure_wake(&host.home);
    host.cloud_sync.wake();
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let state = database_cloud_state(&host.home, database_id);
        if !matches!(state, CloudState::Enrolling) {
            return state;
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return CloudState::Enrolling;
        }
        let slice = POLL.min(deadline - now);
        tokio::select! {
            () = signal().notified() => {}
            () = tokio::time::sleep(slice) => {}
        }
    }
}

fn signal() -> &'static Notify {
    static SIGNAL: OnceLock<Notify> = OnceLock::new();
    SIGNAL.get_or_init(Notify::new)
}
