//! Time one write request spends in the write scope, by phase. Off unless the caller runs the
//! request inside [`measure`]; then each phase adds a few clock reads.
//!
//! The three phases follow one another. Each is split further, so a slow phase names its part:
//! `read_and_stage` by the steps of a native mutation batch, and `commit` by the steps of the
//! queue turn that commits the batch. A part that no step names is the rest of its phase.

use std::cell::Cell;
use std::future::Future;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

/// The phases of the write scopes one request ran. Nested scopes add nothing: the outer scope
/// holds their time.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WritePhases {
    /// Waiting for the prepare gate and then for the publication gate.
    pub lock_wait: Duration,
    /// Under the prepare gate: reading the old records, checking conditions, signing the spool
    /// entry and staging the batch.
    pub read_and_stage: Duration,
    /// Part of `read_and_stage`: taking the read view of the scope on a blocking thread.
    pub capture: Duration,
    /// Part of `read_and_stage`: reading the old records and checking the conditions.
    pub old_records: Duration,
    /// Part of `read_and_stage`: the local replay head, the author clocks and the field stamps.
    pub stamps: Duration,
    /// Part of `read_and_stage`: checking that the mutation id of a captured write is new (the
    /// only read of the stored ids), and encoding and signing its spool entry.
    pub spool_entry: Duration,
    /// Part of `read_and_stage`: staging the new records and their alternate entries.
    pub stage: Duration,
    /// Queueing the batch, the commit log write and its ordered flush, until the commit that
    /// holds the batch completes.
    pub commit: Duration,
    /// Part of `commit`: the space and queue checks on a blocking thread, the mutation id check
    /// against the queued writes, and joining the queue. It does not read the stored ids again.
    pub queue: Duration,
    /// Part of `commit`: from joining the queue until the queue turn that commits the batch
    /// holds both gates. A commit of another caller (a spool confirmation) runs first here.
    pub turn_wait: Duration,
    /// Part of `commit`: preparing the spool records of the captured writes. It does not read
    /// the stored ids again.
    pub spool_records: Duration,
    /// Part of `commit`: the commit log append and its ordered flush.
    pub log_flush: Duration,
    /// Part of `commit`: publishing the spool head, the read view and the metadata.
    pub install: Duration,
    /// Part of `commit`: post-commit work of the batches in the turn.
    pub deliver: Duration,
    /// Part of `commit`: from the end of the turn until this request runs again.
    pub wake: Duration,
}

/// The steps of `read_and_stage` that a native mutation batch names.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Step {
    OldRecords,
    Stamps,
    SpoolEntry,
    Stage,
}

tokio::task_local! { static PHASES: Cell<WritePhases>; }

/// Run `work` and return the time its write scopes spent in each phase.
pub async fn measure<F: Future>(work: F) -> (F::Output, WritePhases) {
    PHASES
        .scope(Cell::new(WritePhases::default()), async move {
            let output = work.await;
            (output, PHASES.with(Cell::get))
        })
        .await
}

fn update(change: impl FnOnce(&mut WritePhases)) {
    let _ = PHASES.try_with(|cell| {
        let mut phases = cell.get();
        change(&mut phases);
        cell.set(phases);
    });
}

/// Run one step of a write scope and add its time to that step.
pub(crate) async fn step<F: Future>(step: Step, work: F) -> F::Output {
    let started = Instant::now();
    let output = work.await;
    let spent = started.elapsed();
    update(|phases| {
        let slot = match step {
            Step::OldRecords => &mut phases.old_records,
            Step::Stamps => &mut phases.stamps,
            Step::SpoolEntry => &mut phases.spool_entry,
            Step::Stage => &mut phases.stage,
        };
        *slot += spent;
    });
    output
}

pub(super) fn add_capture(capture: Duration) {
    update(|phases| phases.capture += capture);
}

pub(super) fn add(lock_wait: Duration, read_and_stage: Duration, commit: Duration) {
    update(|phases| {
        phases.lock_wait += lock_wait;
        phases.read_and_stage += read_and_stage;
        phases.commit += commit;
    });
}

/// The moments of the queue turn that committed one batch.
#[derive(Debug, Clone, Copy)]
pub(in crate::storage::disk_pack) enum Mark {
    /// The turn holds both gates and starts the commit.
    Turn,
    /// The spool records are ready.
    SpoolRecords,
    /// The commit log append and its ordered flush are done.
    LogFlush,
    /// The commit is installed.
    Install,
    /// The post-commit work of the turn is done.
    Delivered,
}

/// The moments a queue turn reached for one batch. The turn runs in its own task, so it marks
/// the batch and the request reads the marks when it runs again.
#[derive(Default)]
pub(in crate::storage::disk_pack) struct TurnClock {
    marks: Mutex<[Option<Instant>; 5]>,
}

impl TurnClock {
    pub(in crate::storage::disk_pack) fn mark(&self, mark: Mark) {
        let now = Instant::now();
        let mut marks = self.marks.lock().unwrap_or_else(PoisonError::into_inner);
        marks[mark as usize] = Some(now);
    }

    /// Split `commit` of one request: `queued` is when the request had joined the queue,
    /// `resumed` when it ran again after the turn.
    pub(super) fn add_commit_parts(&self, queue: Duration, queued: Instant, resumed: Instant) {
        let marks = *self.marks.lock().unwrap_or_else(PoisonError::into_inner);
        let mut last = queued;
        let mut parts = [Duration::ZERO; 5];
        for (part, mark) in parts.iter_mut().zip(marks) {
            if let Some(at) = mark.filter(|at| *at >= last) {
                *part = at.duration_since(last);
                last = at;
            }
        }
        let wake = resumed.saturating_duration_since(last);
        update(|phases| {
            phases.queue += queue;
            phases.turn_wait += parts[0];
            phases.spool_records += parts[1];
            phases.log_flush += parts[2];
            phases.install += parts[3];
            phases.deliver += parts[4];
            phases.wake += wake;
        });
    }
}
