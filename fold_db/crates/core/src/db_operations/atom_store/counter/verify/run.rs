//! The driver of the verifier: steps with a page budget and a time budget.

use super::check::{judge_pair, judge_referenced, judge_total, Holder, Seen};
use super::pages::{phase_range, AtomsPage, ReferencedPage};
use super::report::{Kind, Phase, VerifyReport, VerifyRequest, VerifySummary};
use super::state::{Run, StepGuard};
use super::totals::{digest, Watch};
use crate::db_operations::atom_store::counter::scan::CounterRead;
use crate::db_operations::atom_store::{unsealed_run_error, AtomStore, Progress, RangeWalk};
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
const DEFAULT_MAX_ATOMS: usize = 1_000_000;

/// What one page read gave.
enum Page {
    Referenced(ReferencedPage),
    Atoms(AtomsPage),
}

impl AtomStore {
    /// What `lastdb status` shows: the last complete run and a run in progress.
    pub fn verify_refs_summary(&self) -> VerifySummary {
        self.counter_verify().summary()
    }

    /// Run one step of the verifier. It reads pages until the page budget or
    /// the time budget of the step is spent, or the run is complete.
    ///
    /// The verifier writes nothing and takes no write gate. Each page is one
    /// captured read scope, so a page sees one committed root. A step ends
    /// between pages, and the next step continues from the cursor.
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

    /// Order a new run after every write in flight. The watch is already on,
    /// so a batch that plans from now on is noted. A batch that planned before
    /// is accepted before this empty scope takes the write gate, and the scope
    /// waits until it is flushed, so the first page already shows it. The scope
    /// holds the gate for no read and writes no batch.
    async fn order_run_after_writers(&self) -> Result<(), SchemaError> {
        self.in_write_scope(async { Ok(()) }).await
    }

    /// Read and judge one page, or move to the next phase.
    async fn verify_page(&self, guard: &mut StepGuard<'_>) -> Result<(), SchemaError> {
        let (run, watch) = guard.split();
        if run.walk.done() {
            next_phase(run);
            return Ok(());
        }
        let request = run
            .walk
            .request()
            .expect("a walk that is not done has a read");
        let scope = run.report.scope.clone();
        let page = match run.phase {
            Phase::Atoms => self
                .read_atoms_page(scope.as_deref(), request)
                .await
                .map(Page::Atoms),
            Phase::Tips | Phase::Records => self
                .read_referenced_page(run.phase, scope.as_deref(), request)
                .await
                .map(Page::Referenced),
            Phase::Done => return Ok(()),
        };
        match page {
            Ok(Page::Referenced(page)) => {
                run.note_unsealed(page.scan_unsealed + page.point_unsealed);
                match run
                    .walk
                    .finish_read(page.rows, page.scan_unsealed, page.last_key.as_deref())
                {
                    Progress::Moved => apply_referenced(run, page),
                    Progress::Widened => run.report.page_retries += 1,
                    Progress::Stuck => return Err(unsealed_run_error()),
                }
            }
            Ok(Page::Atoms(page)) => {
                run.note_unsealed(page.scan_unsealed + page.point_unsealed);
                match run
                    .walk
                    .finish_read(page.rows, page.scan_unsealed, page.last_key.as_deref())
                {
                    Progress::Moved => apply_atoms(run, watch, page),
                    Progress::Widened => run.report.page_retries += 1,
                    Progress::Stuck => return Err(unsealed_run_error()),
                }
            }
            Err(SchemaError::ReadResourceLimit { .. }) => {
                // The read hit the limit of one call. Ask for fewer rows or a
                // narrower key range, in a new read scope.
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

/// A new run. Its first phase reads the tips.
fn new_run(request: &VerifyRequest) -> Run {
    let page_rows = request
        .page_rows
        .unwrap_or(DEFAULT_PAGE_ROWS)
        .clamp(16, MAX_PAGE_ROWS);
    let max_atoms = request.max_atoms.unwrap_or(DEFAULT_MAX_ATOMS).max(1);
    let walk = phase_range(Phase::Tips, request.scope.as_deref())
        .map(|(start, end)| RangeWalk::new(&start, &end, page_rows))
        .expect("the tips phase has a range");
    Run::new(request, walk, page_rows, max_atoms)
}

/// Move a run whose walk is done to its next phase.
fn next_phase(run: &mut Run) {
    run.phase = match run.phase {
        Phase::Tips => Phase::Records,
        Phase::Records => Phase::Atoms,
        Phase::Atoms | Phase::Done => Phase::Done,
    };
    let range = phase_range(run.phase, run.report.scope.as_deref());
    if let Some((start, end)) = range {
        run.walk = RangeWalk::new(&start, &end, run.page_rows);
    }
    if run.phase == Phase::Done && !run.tally.complete {
        run.report.notes.push(format!(
            "more than {} atoms are referenced; counters were not compared with the totals of the pass",
            run.tally.len()
        ));
    }
}

/// End a step: stamp the report, and complete the run when it is done.
fn close_step(mut guard: StepGuard<'_>) -> VerifyReport {
    let touched = guard.watch().count();
    let run = guard.run();
    run.report.steps += 1;
    run.refresh_report(touched);
    let done = run.phase == Phase::Done;
    if done && run.report.tips_read == 0 {
        run.report.notes.push(format!(
            "0 tips read in scope {}: check the scope. A scope other than the personal one needs --scope PREFIX or --all-scopes.",
            run.report.scope.as_deref().unwrap_or("personal")
        ));
    }
    run.report.done = done;
    run.report.finished_at = done.then(chrono::Utc::now);
    run.report.next_cursor = (!done).then(|| run.cursor());
    let report = run.report.clone();
    if done {
        guard.finish();
    }
    report
}

/// Judge a page of tips or records and add its holders to the totals.
fn apply_referenced(run: &mut Run, page: ReferencedPage) {
    let (holder, unreadable_kind) = match run.phase {
        Phase::Records => (Holder::Record, Kind::UnreadableRecord),
        _ => (Holder::Tip, Kind::UnreadableTip),
    };
    run.report.pages += 1;
    match holder {
        Holder::Tip => run.report.tips_read += page.rows as u64,
        Holder::Record => run.report.records_read += page.rows as u64,
    }
    for key in &page.unreadable_keys {
        run.note_fault(unreadable_kind, key, "the row does not decode".into());
    }
    for _ in page.unreadable_keys.len() as u64..page.unreadable {
        run.report.mismatches.add(unreadable_kind);
    }
    for read in page.atoms {
        run.report.counters_read += u64::from(!matches!(read.counter, CounterRead::Absent));
        run.report.bodies_read += u64::from(read.body);
        let key = digest(&read.atom);
        run.tally.add(key, read.holders);
        for finding in judge_referenced(holder, read.holders, &read.counter, read.body) {
            if finding.kind == Kind::CounterBelowTips && !run.first_below(key) {
                continue;
            }
            run.note_fault(finding.kind, &read.atom, finding.detail);
        }
    }
}

/// Judge a page of atoms: the pairing, and the counter against the totals.
fn apply_atoms(run: &mut Run, watch: &Watch, page: AtomsPage) {
    run.report.pages += 1;
    if page.foreign > 0 {
        run.report.notes.push(format!(
            "{} keys under atom: are neither a body nor a counter",
            page.foreign
        ));
    }
    for pair in page.pairs {
        run.report.counters_read += u64::from(!matches!(pair.counter, CounterRead::Absent));
        run.report.bodies_read += u64::from(pair.body);
        for finding in judge_pair(pair.body, &pair.counter) {
            run.note_fault(finding.kind, &pair.atom, finding.detail);
        }
        let CounterRead::Present(counter) = &pair.counter else {
            continue;
        };
        // A counter without a body is one fault, already reported by the
        // pairing check. Its count says nothing more.
        if !run.tally.complete || !pair.body {
            continue;
        }
        let key = digest(&pair.atom);
        let seen = Seen {
            holders: run.tally.holders(key),
            touched: watch.touched(key),
            reported_below: run.reported_below.contains(&key),
        };
        if let Some(finding) = judge_total(counter, &seen) {
            run.note_fault(finding.kind, &pair.atom, finding.detail);
        }
    }
}
