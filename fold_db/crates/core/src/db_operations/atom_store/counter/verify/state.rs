//! Memory of the verifier. Nothing here is stored on disk: a run and the last
//! result.

use super::report::{
    Mismatches, Phase, Sample, VerifyLast, VerifyProgress, VerifyReport, VerifyRequest,
    VerifySummary,
};
use crate::db_operations::atom_store::time_queue::QueueSurvey;
use crate::db_operations::atom_store::{RangeWalk, WalkStats};
use crate::schema::SchemaError;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// A run that no step touched for this long is dropped.
const RUN_IDLE_LIMIT: Duration = Duration::from_secs(600);
/// The most samples of one kind in a report.
const SAMPLES_PER_KIND: usize = 8;

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One run of the verifier.
pub(super) struct Run {
    pub phase: Phase,
    /// The range walk of a blob phase. Empty in the queue phase.
    pub walk: RangeWalk,
    /// The survey of the retention queue, while the queue phase runs.
    pub survey: Option<QueueSurvey>,
    pub report: VerifyReport,
    pub page_rows: usize,
    started: Instant,
    touched_at: Instant,
}

impl Run {
    pub(super) fn new(request: &VerifyRequest, page_rows: usize) -> Self {
        let id = uuid::Uuid::new_v4().simple().to_string();
        Self {
            phase: Phase::Queue,
            walk: RangeWalk::new(&[], &[], page_rows),
            survey: None,
            report: VerifyReport {
                run_id: id,
                scope: request.scope.clone(),
                phase: Phase::Queue,
                done: false,
                next_cursor: None,
                started_at: chrono::Utc::now(),
                finished_at: None,
                elapsed_ms: 0,
                steps: 0,
                pages: 0,
                queue_entries_read: 0,
                blob_counters_read: 0,
                blob_queue_read: 0,
                page_retries: 0,
                range_reads: WalkStats::default(),
                mismatches: Mismatches::default(),
                mismatches_total: 0,
                samples: Vec::new(),
                notes: Vec::new(),
            },
            page_rows,
            started: Instant::now(),
            touched_at: Instant::now(),
        }
    }

    /// Where the run stands, for the status.
    fn progress(&self) -> VerifyProgress {
        VerifyProgress {
            run_id: self.report.run_id.clone(),
            phase: self.phase,
            pages: self.report.pages,
            started_at: self.report.started_at,
        }
    }

    /// The cursor of the next step. It names the run and the number of steps
    /// done, so a repeated or stale step is refused.
    pub(super) fn cursor(&self) -> String {
        format!("{}.{}", self.report.run_id, self.report.steps)
    }

    /// Record one fault: the count, and a sample while the kind has room.
    pub(super) fn note_fault(&mut self, kind: super::report::Kind, subject: &str, detail: String) {
        self.note_faults(kind, subject, detail, 1);
    }

    /// Record `count` faults of one kind with one sample.
    pub(super) fn note_faults(
        &mut self,
        kind: super::report::Kind,
        subject: &str,
        detail: String,
        count: u64,
    ) {
        self.report.mismatches.add_many(kind, count);
        let name = kind.name();
        let held = self
            .report
            .samples
            .iter()
            .filter(|s| s.kind == name)
            .count();
        if held < SAMPLES_PER_KIND {
            self.report.samples.push(Sample {
                kind: name.to_string(),
                atom: subject.to_string(),
                detail,
            });
        }
    }

    /// Copy the running totals into the report before it leaves the node.
    pub(super) fn refresh_report(&mut self) {
        let report = &mut self.report;
        report.phase = self.phase;
        report.mismatches_total = report.mismatches.total();
        report.elapsed_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.touched_at = Instant::now();
    }
}

#[derive(Default)]
struct Inner {
    run: Option<Run>,
    /// A step holds the run. A second step is refused.
    busy: bool,
    last: Option<VerifyLast>,
    /// The run in progress, as of the last finished page. A step that holds
    /// the run keeps this value, so the status shows a run during a step.
    progress: Option<VerifyProgress>,
}

/// The verifier state of one atom store.
#[derive(Default)]
pub(crate) struct VerifyState {
    inner: Mutex<Inner>,
}

impl VerifyState {
    /// What `lastdb status` shows.
    pub(crate) fn summary(&self) -> VerifySummary {
        let mut inner = lock(&self.inner);
        Self::expire(&mut inner);
        VerifySummary {
            last: inner.last.clone(),
            running: inner.progress.clone(),
        }
    }

    fn expire(inner: &mut Inner) {
        let idle = inner
            .run
            .as_ref()
            .is_some_and(|run| !inner.busy && run.touched_at.elapsed() > RUN_IDLE_LIMIT);
        if idle {
            inner.run = None;
            inner.progress = None;
        }
    }

    /// Take the run for one step. Without a cursor, start a new run with `new`.
    pub(super) fn checkout(
        &self,
        request: &VerifyRequest,
        new: impl FnOnce() -> Run,
    ) -> Result<StepGuard<'_>, SchemaError> {
        let mut inner = lock(&self.inner);
        Self::expire(&mut inner);
        if inner.busy {
            return Err(SchemaError::InvalidData(
                "a verify step is already running; wait for it, then continue with the cursor"
                    .into(),
            ));
        }
        let run = match request.cursor.as_deref() {
            None => new(),
            Some(cursor) => {
                let run = inner.run.take().ok_or_else(|| {
                    SchemaError::InvalidCursor(
                        "no verify run in progress; start a new run without a cursor".into(),
                    )
                })?;
                if run.cursor() != cursor {
                    let expected = run.cursor();
                    inner.run = Some(run);
                    return Err(SchemaError::InvalidCursor(format!(
                        "the cursor does not match the run in progress (expected {expected})"
                    )));
                }
                run
            }
        };
        inner.run = None;
        inner.busy = true;
        inner.progress = Some(run.progress());
        Ok(StepGuard {
            state: self,
            run: Some(run),
        })
    }
}

/// Holds the run during a step. Dropping it, also after a cancelled request,
/// gives the run back in the state of the last finished page.
pub(super) struct StepGuard<'a> {
    state: &'a VerifyState,
    run: Option<Run>,
}

impl StepGuard<'_> {
    pub(super) fn run(&mut self) -> &mut Run {
        self.run.as_mut().expect("the guard holds a run")
    }

    /// End the run: keep its result as the last result, drop its memory.
    pub(super) fn finish(mut self) {
        let Some(run) = self.run.take() else {
            return;
        };
        let mut inner = lock(&self.state.inner);
        inner.last = run.report.last();
        inner.busy = false;
        inner.progress = None;
    }
}

impl Drop for StepGuard<'_> {
    fn drop(&mut self) {
        let mut inner = lock(&self.state.inner);
        inner.busy = false;
        if let Some(run) = self.run.take() {
            inner.progress = Some(run.progress());
            inner.run = Some(run);
        }
    }
}
