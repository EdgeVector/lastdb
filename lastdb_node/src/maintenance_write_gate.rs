//! Cancel node maintenance that would wait behind a retained selected-source cut.

use crate::Host;
use std::future::Future;
use std::time::Duration;

/// A selected source keeps mutation admission closed until its exact cut is
/// adopted. Cancel only the wait for admission. Once admitted, finish the full
/// operation before the source fence can acquire its exclusive guard.
pub async fn run<F: Future>(host: &Host, future: F) -> Option<F::Output> {
    if writes_paused(host) {
        return None;
    }
    let admission = tokio::select! {
        admission = host.db.acquire_local_mutation_admission() => admission,
        () = wait_for_pause(host) => return None,
    };
    if writes_paused(host) {
        return None;
    }
    Some(admission.run(Box::pin(future)).await)
}

fn writes_paused(host: &Host) -> bool {
    crate::cloud_purge_runtime::local_writes_paused(host).unwrap_or(true)
}

async fn wait_for_pause(host: &Host) {
    loop {
        if writes_paused(host) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
