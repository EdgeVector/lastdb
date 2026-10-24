//! The time that a job keeps other writers waiting, and how many write scopes
//! it opened.
//!
//! A background job (the retention janitor, the blob dwell pass) paces itself so
//! that it takes only a share of the time. The share is of the time its batch
//! keeps writers waiting, not of the wall time of the step: a step also reads
//! pages of its queue before it takes the write gate, and a writer does not wait
//! for that.
//!
//! A batch keeps a writer waiting during logical preparation and acceptance.
//! The immediate disk worker then seals its complete cohort and keeps later
//! preparation behind it until the flush completes. A background batch's cost
//! includes its own preparation hold plus that shared flush hold. Client cost
//! runs from preparation admission through its durable result. The wait for
//! admission itself is excluded. There is no collection timer or memory ack.
//! Nested scopes add nothing: their outer scope owns the complete batch.
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

/// A write scope has finished and staged something. Count it for the process and for
/// the job that runs in this task, if there is one. A scope that staged nothing (an
/// ordering scope, such as the one `verify-refs` opens at the start of a run) is not
/// counted, so a poll cannot make a background job treat the node as busy.
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
