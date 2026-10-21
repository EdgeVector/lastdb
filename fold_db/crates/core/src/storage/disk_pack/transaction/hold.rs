//! The time that a job holds the native write gate.
//!
//! A background job (the retention janitor, the blob dwell pass) paces itself so
//! that it takes only a share of the gate. The share is of the time that its batch
//! holds the gate, because that is the time a writer waits. The wall time of a
//! step is a poor measure: it also holds the wait for the flush, which comes
//! after the gate is free, and a rest that follows the wall time is many times
//! too long.
//!
//! [`measure`] runs a job and adds up the hold of every write scope that the job
//! opens in its own task. The hold of a scope is the time from the moment the
//! scope has the gate to the moment it frees it (`request.rs`). The wait for the
//! gate is not part of it. A scope that an outer scope contains adds nothing: the
//! outer scope holds the gate for both.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

tokio::task_local! {
    /// Microseconds of gate hold that the running job has added up.
    static HELD: Arc<AtomicU64>;
}

/// Run `work` and return its output with the time its write scopes held the
/// native write gate. Work that opens no write scope holds the gate for zero
/// time.
pub(crate) async fn measure<F: Future>(work: F) -> (F::Output, Duration) {
    let held = Arc::new(AtomicU64::new(0));
    let output = HELD.scope(Arc::clone(&held), work).await;
    (output, Duration::from_micros(held.load(Ordering::Acquire)))
}

/// Add one finished hold to the job that runs in this task, if there is one.
pub(super) fn note(held: Duration) {
    let micros = u64::try_from(held.as_micros()).unwrap_or(u64::MAX);
    let _ = HELD.try_with(|total| total.fetch_add(micros, Ordering::AcqRel));
}
