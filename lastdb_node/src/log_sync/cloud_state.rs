//! Wait until the sync loop records this database, or the wait bound ends.
//!
//! The enrollment set lives in `named_grant::cloud_state`. This wait stays
//! here because it wakes the sync loop.

use std::time::Duration;

use crate::host::Host;
use crate::named_grant::cloud_state::{database_cloud_state, notified, CloudState};

const WAIT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(200);

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
            () = notified() => {}
            () = tokio::time::sleep(slice) => {}
        }
    }
}
