//! Memory of the verifier. Nothing here is a counter and nothing is stored on
//! disk: a run, the last result, and the set of atoms that writers touched.

use super::report::{
    Mismatches, Phase, Sample, VerifyLast, VerifyProgress, VerifyReport, VerifyRequest,
    VerifySummary,
};
use super::totals::{lock, Tally, Watch};
use crate::db_operations::atom_store::time_queue::QueueSurvey;
use crate::db_operations::atom_store::{RangeWalk, WalkStats};
use crate::schema::SchemaError;
use std::collections::HashSet;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// A run that no step touched for this long is dropped.
const RUN_IDLE_LIMIT: Duration = Duration::from_secs(600);
/// The most atoms the below-set holds (atoms a page reported as below).
const MAX_REPORTED_BELOW: usize = 100_000;
/// The most samples of one kind in a report.
const SAMPLES_PER_KIND: usize = 8;

/// One run of the verifier.
pub(super) struct Run {
    pub phase: Phase,
    pub walk: RangeWalk,
    pub tally: Tally,
    /// Tips per molecule, over the first phase.
    pub molecule_tips: Tally,
    /// Conflict records per molecule, over the second phase.
    pub molecule_records: Tally,
    /// Molecules whose header the run has read.
    pub headers_seen: HashSet<u128>,
    pub reported_below: HashSet<u128>,
    /// The ids of the counted records (first 128 bits), over the second phase.
    pub record_ids: HashSet<u128>,
    /// False when the run met more records than `max_atoms`: the ids are a
    /// part, and the queue phase judges nothing.
    pub ids_complete: bool,
    /// The most ids the run keeps.
    pub max_ids: usize,
    /// The survey of the retention queue, while the queue phase runs.
    pub survey: Option<QueueSurvey>,
    /// Ids of records that no queue entry names, for the last phase.
    pub suspects: HashSet<u128>,
    pub report: VerifyReport,
    pub page_rows: usize,
    started: Instant,
    touched_at: Instant,
}

impl Run {
    pub(super) fn new(
        request: &VerifyRequest,
        walk: RangeWalk,
        page_rows: usize,
        max_atoms: usize,
    ) -> Self {
        let id = uuid::Uuid::new_v4().simple().to_string();
        Self {
            phase: Phase::Tips,
            walk,
            tally: Tally::new(max_atoms),
            molecule_tips: Tally::new(max_atoms),
            molecule_records: Tally::new(max_atoms),
            headers_seen: HashSet::new(),
            reported_below: HashSet::new(),
            record_ids: HashSet::new(),
            ids_complete: true,
            max_ids: max_atoms,
            survey: None,
            suspects: HashSet::new(),
            report: VerifyReport {
                run_id: id,
                scope: request.scope.clone(),
                phase: Phase::Tips,
                done: false,
                next_cursor: None,
                started_at: chrono::Utc::now(),
                finished_at: None,
                elapsed_ms: 0,
                steps: 0,
                pages: 0,
                tips_read: 0,
                records_read: 0,
                counters_read: 0,
                bodies_read: 0,
                headers_read: 0,
                binding_counters_read: 0,
                binding_records_read: 0,
                queue_entries_read: 0,
                blob_counters_read: 0,
                blob_queue_read: 0,
                page_retries: 0,
                range_reads: WalkStats::default(),
                tally_atoms: 0,
                tally_complete: true,
                touched_atoms: 0,
                mismatches: Mismatches::default(),
                mismatches_total: 0,
                suspect_atoms: 0,
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
    pub(super) fn note_fault(&mut self, kind: super::report::Kind, atom: &str, detail: String) {
        self.note_faults(kind, atom, detail, 1);
    }

    /// Record `count` faults of one kind with one sample.
    pub(super) fn note_faults(
        &mut self,
        kind: super::report::Kind,
        atom: &str,
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
                atom: atom.to_string(),
                detail,
            });
        }
    }

    /// Remember that a page reported this atom as below its holders.
    /// Returns false when an earlier page did, so one atom counts once.
    pub(super) fn first_below(&mut self, atom: u128) -> bool {
        if self.reported_below.len() >= MAX_REPORTED_BELOW {
            return !self.reported_below.contains(&atom);
        }
        self.reported_below.insert(atom)
    }

    /// Copy the running totals into the report before it leaves the node.
    pub(super) fn refresh_report(&mut self, touched_atoms: u64) {
        let report = &mut self.report;
        report.phase = self.phase;
        report.tally_atoms = self.tally.len() as u64;
        report.tally_complete = self.tally.complete
            && self.molecule_tips.complete
            && self.molecule_records.complete
            && self.ids_complete;
        report.touched_atoms = touched_atoms;
        report.mismatches_total = report.mismatches.total();
        report.suspect_atoms = report.mismatches.suspect_counter;
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
    watch: Watch,
}

impl VerifyState {
    pub(crate) fn watch(&self) -> &Watch {
        &self.watch
    }

    /// What `lastdb status` shows.
    pub(crate) fn summary(&self) -> VerifySummary {
        let mut inner = lock(&self.inner);
        self.expire(&mut inner);
        VerifySummary {
            last: inner.last.clone(),
            running: inner.progress.clone(),
        }
    }

    fn expire(&self, inner: &mut Inner) {
        let idle = inner
            .run
            .as_ref()
            .is_some_and(|run| !inner.busy && run.touched_at.elapsed() > RUN_IDLE_LIMIT);
        if idle {
            inner.run = None;
            inner.progress = None;
            self.watch.stop();
        }
    }

    /// Take the run for one step. Without a cursor, start a new run with `new`.
    pub(super) fn checkout(
        &self,
        request: &VerifyRequest,
        new: impl FnOnce() -> Run,
    ) -> Result<StepGuard<'_>, SchemaError> {
        let mut inner = lock(&self.inner);
        self.expire(&mut inner);
        if inner.busy {
            return Err(SchemaError::InvalidData(
                "a verify step is already running; wait for it, then continue with the cursor"
                    .into(),
            ));
        }
        let run = match request.cursor.as_deref() {
            None => {
                self.watch.start();
                new()
            }
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

    pub(super) fn watch(&self) -> &Watch {
        &self.state.watch
    }

    /// The run and the watch together, for a caller that needs both.
    pub(super) fn split(&mut self) -> (&mut Run, &Watch) {
        (
            self.run.as_mut().expect("the guard holds a run"),
            &self.state.watch,
        )
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
        self.state.watch.stop();
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
