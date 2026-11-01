//! The driver of the verifier: steps with a page budget and a time budget.

use super::pages::phase_range;
use super::report::{Phase, VerifyReport, VerifyRequest, VerifySummary};
use super::state::{Run, StepGuard};
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::schema::SchemaError;
use std::time::{Duration, Instant};

const DEFAULT_STEP_PAGES: u32 = 64;
const MAX_STEP_PAGES: u32 = 1_024;
const DEFAULT_STEP_MS: u64 = 3_000;
const MAX_STEP_MS: u64 = 30_000;
const DEFAULT_PAUSE_MS: u64 = 2;
const MAX_PAUSE_MS: u64 = 1_000;
const DEFAULT_PAGE_ROWS: usize = 1_000;
const MAX_PAGE_ROWS: usize = 4_000;

impl AtomStore {
    /// What `lastdb status` shows: the last complete run and a run in progress.
    pub fn verify_refs_summary(&self) -> VerifySummary {
        self.counter_verify().summary()
    }

    /// Run one step of the verifier. It reads pages until the page budget or
    /// the time budget of the step is spent, or the run is complete.
    ///
    /// The verifier writes nothing and takes no write gate. A step ends between
    /// pages, and the next step continues from the cursor.
    pub async fn verify_refs_step(
        &self,
        request: VerifyRequest,
    ) -> Result<VerifyReport, SchemaError> {
        let max_pages = request
            .max_pages
            .unwrap_or(DEFAULT_STEP_PAGES)
            .clamp(1, MAX_STEP_PAGES);
        let max_time = Duration::from_millis(
            request
                .max_ms
                .unwrap_or(DEFAULT_STEP_MS)
                .clamp(100, MAX_STEP_MS),
        );
        let pause = Duration::from_millis(
            request
                .pause_ms
                .unwrap_or(DEFAULT_PAUSE_MS)
                .min(MAX_PAUSE_MS),
        );
        if request.cursor.is_none() {
            Self::validate_scope(request.scope.as_deref())?;
        }
        let mut guard = self
            .counter_verify()
            .checkout(&request, || new_run(&request))?;
        if request.cursor.is_none() {
            self.order_run_after_writers().await?;
        }
        let started = Instant::now();
        let mut pages = 0;
        while guard.run().phase != Phase::Done && pages < max_pages && started.elapsed() < max_time
        {
            self.verify_page(&mut guard).await?;
            pages += 1;
            if pause.is_zero() {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(pause).await;
            }
        }
        Ok(close_step(guard))
    }

    /// Order a new run after every write in flight. A batch that planned before
    /// is accepted before this empty scope takes the write gate, and the scope
    /// waits until it is flushed, so the first page already shows it. The scope
    /// holds the gate for no read and writes no batch.
    async fn order_run_after_writers(&self) -> Result<(), SchemaError> {
        self.in_write_scope(async { Ok(()) }).await
    }

    /// Read and judge one page, or move to the next phase.
    async fn verify_page(&self, guard: &mut StepGuard<'_>) -> Result<(), SchemaError> {
        let phase = guard.run().phase;
        let walk_done = guard.run().walk.done();
        match phase {
            Phase::Queue => self.verify_queue_page(guard).await,
            Phase::Blobs | Phase::BlobQueue if !walk_done => self.verify_blob_page(guard).await,
            Phase::Blobs | Phase::BlobQueue => {
                next_phase(guard.run());
                Ok(())
            }
            Phase::Done => Ok(()),
        }
    }

    /// Record one page attempt. A read that hits the limit asks for fewer rows
    /// or a narrower key range. The next call uses a new read scope. A range
    /// that cannot shrink returns the same limit error.
    pub(super) fn settle_verify_page(
        &self,
        run: &mut Run,
        outcome: Result<(), SchemaError>,
    ) -> Result<(), SchemaError> {
        match outcome {
            Ok(()) => {}
            Err(SchemaError::ReadResourceLimit { .. }) => {
                // The read hit the limit of one call. Ask for fewer rows or a
                // narrower key range. The next call uses a new read scope.
                run.report.page_retries += 1;
                if !run.walk.read_too_big() {
                    return Err(SchemaError::ReadResourceLimit {
                        detail: "a key range is too dense to read in pieces".into(),
                    });
                }
            }
            Err(error) => return Err(error),
        }
        let stats = run.walk.take_stats();
        run.report.range_reads.absorb(stats);
        self.range_walks().add(stats);
        Ok(())
    }
}

/// A new run. Its first phase surveys the retention queue.
fn new_run(request: &VerifyRequest) -> Run {
    let page_rows = request
        .page_rows
        .unwrap_or(DEFAULT_PAGE_ROWS)
        .clamp(16, MAX_PAGE_ROWS);
    Run::new(request, page_rows)
}

/// Move a run to `phase`, with a fresh walk of its range.
pub(super) fn enter_phase(run: &mut Run, phase: Phase) {
    run.phase = phase;
    if let Some((start, end)) = phase_range(phase) {
        run.walk = RangeWalk::new(&start, &end, run.page_rows);
    }
}

/// Move a run whose walk is done to its next phase.
fn next_phase(run: &mut Run) {
    let next = match run.phase {
        Phase::Blobs => Phase::BlobQueue,
        // The queue phase ends in `finish_queue`, which picks the next phase.
        Phase::Queue | Phase::BlobQueue | Phase::Done => Phase::Done,
    };
    enter_phase(run, next);
}

/// End a step: stamp the report, and complete the run when it is done.
fn close_step(mut guard: StepGuard<'_>) -> VerifyReport {
    let run = guard.run();
    run.report.steps += 1;
    run.refresh_report();
    let done = run.phase == Phase::Done;
    run.report.done = done;
    run.report.finished_at = done.then(chrono::Utc::now);
    run.report.next_cursor = (!done).then(|| run.cursor());
    let report = run.report.clone();
    if done {
        guard.finish();
    }
    report
}
