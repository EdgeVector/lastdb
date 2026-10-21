//! The time that a job keeps other writers waiting, and how many write scopes
//! it opened.
//!
//! A background job (the retention janitor, the blob dwell pass) paces itself so
//! that it takes only a share of the time. The share is of the time its batch
//! keeps writers waiting, not of the wall time of the step: a step also reads
//! pages of its queue before it takes the write gate, and a writer does not wait
//! for that.
//!
//! A batch keeps a writer waiting in two ways. It holds the write gate while it
//! reads in the gate, stages its operations and is accepted. And a batch that
//! seals the write queue (every durable batch that is not a memory acknowledgement
//! does) makes every later writer wait in `enter_prepare` until the batch is
//! durable, though the gate is free. So the time of a sealing batch runs from the
//! moment it has the gate to the moment its flush is done. The time of a batch
//! that does not seal is the hold of the gate. The wait for the gate itself is
//! not part of it. A scope that an outer scope contains adds nothing: the outer
//! scope holds the gate for both.
//!
//! [`measure`] runs a job and adds up the time and the count of every write scope
//! that the job opens in its own task (`request.rs`). The process also counts
//! every scope that any task opens ([`write_scopes`]), so a job can tell whether
//! another writer was there while it worked (`WriterWatch`).

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Write scopes that any task of this process has opened, for the life of the
/// process.
static WRITE_SCOPES: AtomicU64 = AtomicU64::new(0);

/// What a job cost the writers.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Cost {
    /// How long the job kept writers waiting.
    pub(crate) blocked: Duration,
    /// Write scopes the job opened.
    pub(crate) scopes: u64,
}

#[derive(Default)]
struct JobCost {
    blocked_us: AtomicU64,
    scopes: AtomicU64,
}

tokio::task_local! {
    /// What the running job has cost the writers so far.
    static JOB: Arc<JobCost>;
}

/// Run `work` and return its output with what its write scopes cost the
/// writers. Work that opens no write scope blocks nobody and reports zero.
pub(crate) async fn measure<F: Future>(work: F) -> (F::Output, Cost) {
    let job = Arc::new(JobCost::default());
    let output = JOB.scope(Arc::clone(&job), work).await;
    let cost = Cost {
        blocked: Duration::from_micros(job.blocked_us.load(Ordering::Acquire)),
        scopes: job.scopes.load(Ordering::Acquire),
    };
    (output, cost)
}

/// A write scope has the gate. Count it for the process and for the job that runs
/// in this task, if there is one.
pub(super) fn opened() {
    WRITE_SCOPES.fetch_add(1, Ordering::AcqRel);
    let _ = JOB.try_with(|job| job.scopes.fetch_add(1, Ordering::AcqRel));
}

/// Add the time of one finished write scope to the job that runs in this task,
/// if there is one.
pub(super) fn note(blocked: Duration) {
    let micros = u64::try_from(blocked.as_micros()).unwrap_or(u64::MAX);
    let _ = JOB.try_with(|job| job.blocked_us.fetch_add(micros, Ordering::AcqRel));
}

/// Write scopes that any task of this process has opened.
pub(crate) fn write_scopes() -> u64 {
    WRITE_SCOPES.load(Ordering::Acquire)
}
