//! Optional per-query disk receipts. Task scopes explicitly attach blocking work.

use std::future::Future;

pub use laststore::{DiskReadContext, DiskReadReceipt};

tokio::task_local! {
    static DISK_READS: DiskReadContext;
}

/// Run one awaited query with its own receipt. Background tasks do not inherit it.
pub async fn observe<F: Future>(work: F) -> (F::Output, DiskReadReceipt) {
    let context = current().map_or_else(DiskReadContext::new, |parent| parent.child());
    DISK_READS
        .scope(context.clone(), async move {
            let result = work.await;
            (result, context.snapshot())
        })
        .await
}

/// Capture on the caller side of a blocking hop.
pub(crate) fn current() -> Option<DiskReadContext> {
    DISK_READS
        .try_with(Clone::clone)
        .ok()
        .or_else(DiskReadContext::capture)
}

/// Attach only for synchronous work, without leaving a thread scope across await.
pub(crate) fn with_sync<T>(context: Option<DiskReadContext>, work: impl FnOnce() -> T) -> T {
    match context {
        Some(context) => context.with(work),
        None => work(),
    }
}

/// Count warm plaintext hits above the blocking storage boundary.
pub(crate) fn warm_hit() {
    with_sync(current(), || {
        laststore::record_requested_keys(1);
        laststore::record_logical_warm_hits(1);
    });
}

/// At-rest value open time includes envelope decode and decompression.
pub(crate) fn decrypted(bytes: usize, elapsed: std::time::Duration) {
    with_sync(current(), || {
        laststore::record_decryption(bytes as u64, elapsed)
    });
}

/// Observe the warm-set lock without changing its guard or poison policy.
pub(crate) fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    let Some(context) = current() else {
        return mutex.lock().expect("poison");
    };
    let started = std::time::Instant::now();
    let guard = mutex.lock().expect("poison");
    context.with(|| laststore::record_lock_wait(started.elapsed()));
    guard
}
