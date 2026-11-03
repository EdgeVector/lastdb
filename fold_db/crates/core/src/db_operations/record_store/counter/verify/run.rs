//! The driver of the verifier: steps with a page budget and a time budget.

use super::report::{Phase, VerifyReport, VerifyRequest, VerifySummary};
use super::state::{Run, StepGuard};
use crate::db_operations::record_store::RecordStore;
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

impl RecordStore {
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

    /// Read and judge one page.
    async fn verify_page(&self, guard: &mut StepGuard<'_>) -> Result<(), SchemaError> {
        match guard.run().phase {
            Phase::Queue => self.verify_queue_page(guard).await,
            Phase::Done => Ok(()),
        }
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
