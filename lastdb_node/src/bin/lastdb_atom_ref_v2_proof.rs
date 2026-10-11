//! One-shot compact atom reverse-edge proof on a temporary real-data copy.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use fold_db::db_operations::atom_store::{
    AtomRefBackfillPhase, AtomRefCrashResumeProof, AtomRefV1DrainStatus, AtomRefV2AuditReport,
    AtomRefV2LookupBenchmark, ATOM_REF_MANIFEST_VERSION_HISTORY,
};
use fold_db::storage::laststore::CollectionCompactReport;
use serde::Serialize;

#[path = "lastdb_atom_ref_v2_proof/run.rs"]
mod run;
#[path = "lastdb_atom_ref_v2_proof/support.rs"]
mod support;

use run::run;
use support::*;

const V1_COLLECTION: &str = "atom_ref_edges";
const V2_COLLECTION: &str = "atom_ref_edges_v2";
const ATOMS_COLLECTION: &str = "atoms";
const MAX_DRAINED_V2_BYTES: u64 = 1_342_177_280;
const MAX_DRAINED_V1_BYTES: u64 = 16 * 1024 * 1024;
const MIN_RECLAIM_BYTES: u64 = 10 * 1024 * 1024 * 1024;
const MAX_V2_BYTES_PER_EDGE: u64 = 320;
const MAX_V2_TO_ATOMS_BASIS_POINTS: u64 = 5_000;
const DEFAULT_DEADLINE_SECS: u64 = 7200;
const PROGRESS_INTERVAL_SECS: u64 = 30;

#[derive(Debug, Parser)]
#[command(about = "Backfill and audit atom reverse-edge v2 on an isolated copy")]
struct Args {
    /// Temporary LastDB home copied from the primary.
    #[arg(long)]
    home: PathBuf,
    /// Source rows per durable page.
    #[arg(long, default_value_t = 256)]
    page: usize,
    /// Maximum durable pages before the proof fails.
    #[arg(long, default_value_t = 1_000_000)]
    max_pages: usize,
    /// Active atoms in each paired v1/v2 latency sample.
    #[arg(long, default_value_t = 256)]
    lookup_samples: usize,
    /// Wall-clock bound in seconds. When it expires, stop and report timeout.
    #[arg(long, default_value_t = DEFAULT_DEADLINE_SECS)]
    deadline_secs: u64,
    /// Audit at most this many active atoms. Omit for a full run.
    #[arg(long)]
    max_atoms: Option<u64>,
    /// Required acknowledgement for an isolated copy.
    #[arg(long)]
    ack_isolated_copy: bool,
    /// After the exact audit, drain v1 and compact both reverse-edge planes.
    /// The 10 GiB reclaim floor does not apply when the copy is already
    /// v1_drained and v1 allocated bytes are already at leftover size.
    #[arg(long)]
    drain_v1_and_compact: bool,
}

#[derive(Debug, Serialize)]
struct PrefixProof {
    storage_prefix: Option<String>,
    pages: usize,
    v2_bytes: u64,
    audit: Option<AtomRefV2AuditReport>,
    lookup: Option<AtomRefV2LookupBenchmark>,
}

#[derive(Debug, Serialize)]
struct ProofReport {
    status: &'static str,
    scope: &'static str,
    phase: String,
    pages: usize,
    atoms_audited: u64,
    elapsed_secs: u64,
    home: PathBuf,
    max_v2_bytes: u64,
    prefixes: Vec<PrefixProof>,
    crash: Option<AtomRefCrashResumeProof>,
    reclaim: Option<ReclaimProof>,
}

#[derive(Debug, Serialize)]
struct ReclaimProof {
    v1_allocated_before: u64,
    v2_allocated_before: u64,
    v1_allocated_after: u64,
    v2_allocated_after: u64,
    atoms_allocated_after: u64,
    allocated_bytes_reclaimed: u64,
    active_edges: u64,
    v2_bytes_per_edge: u64,
    v2_to_atoms_basis_points: u64,
    drain_pages: usize,
    drain_status: AtomRefV1DrainStatus,
    v1_keys_remain: bool,
    v1_compaction: CollectionCompactReport,
    v2_compaction: CollectionCompactReport,
    reclaim_floor_skipped: bool,
}

#[derive(Clone)]
struct WatchState {
    phase: String,
    pages: usize,
    atoms_audited: u64,
    estimated_remaining_pages: Option<usize>,
    scope_partial: bool,
    home: PathBuf,
    max_v2_bytes: u64,
    started: Instant,
}

struct Lifetime {
    started: Instant,
    deadline: Duration,
    last_progress: Instant,
    phase: &'static str,
    pages: usize,
    atoms_audited: u64,
    estimated_remaining_pages: Option<usize>,
    scope_partial: bool,
    home: PathBuf,
    max_v2_bytes: u64,
    watch: Arc<Mutex<WatchState>>,
}

enum RunError {
    Timeout,
    Fail(String),
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut life = Lifetime::new(args.deadline_secs, args.max_atoms.is_some());
    let watch = life.watch.clone();
    let deadline = Duration::from_secs(args.deadline_secs);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(PROGRESS_INTERVAL_SECS));
        ticker.tick().await;
        let deadline_sleep = tokio::time::sleep(deadline);
        tokio::pin!(deadline_sleep);
        loop {
            tokio::select! {
                _ = &mut deadline_sleep => {
                    let state = watch.lock().expect("watch lock").clone();
                    emit_report(&timeout_from_watch(&state), 2);
                }
                _ = ticker.tick() => {
                    let state = watch.lock().expect("watch lock").clone();
                    eprintln!(
                        "phase={} pages={} atoms={} elapsed_secs={} remaining_pages={}",
                        state.phase,
                        state.pages,
                        state.atoms_audited,
                        state.started.elapsed().as_secs(),
                        state
                            .estimated_remaining_pages
                            .map_or_else(|| "unknown".to_string(), |n| n.to_string())
                    );
                }
            }
        }
    });
    match run(args, &mut life).await {
        Ok(report) => emit_report(&report, if report.status == "timeout" { 2 } else { 0 }),
        Err(RunError::Timeout) => emit_report(
            &life.timeout_report(life.home.clone(), life.max_v2_bytes, Vec::new(), None),
            2,
        ),
        Err(RunError::Fail(error)) => {
            eprintln!("atom reverse-edge v2 proof failed: {error}");
            std::process::exit(1);
        }
    }
}

fn timeout_from_watch(state: &WatchState) -> ProofReport {
    ProofReport {
        status: "timeout",
        scope: if state.scope_partial {
            "partial"
        } else {
            "full"
        },
        phase: state.phase.clone(),
        pages: state.pages,
        atoms_audited: state.atoms_audited,
        elapsed_secs: state.started.elapsed().as_secs(),
        home: state.home.clone(),
        max_v2_bytes: state.max_v2_bytes,
        prefixes: Vec::new(),
        crash: None,
        reclaim: None,
    }
}

fn emit_report(report: &ProofReport, code: i32) {
    match serde_json::to_string_pretty(report) {
        Ok(json) => {
            println!("{json}");
            std::process::exit(code);
        }
        Err(error) => {
            eprintln!("encode proof report: {error}");
            std::process::exit(1);
        }
    }
}

impl Lifetime {
    fn new(deadline_secs: u64, scope_partial: bool) -> Self {
        let now = Instant::now();
        let watch = Arc::new(Mutex::new(WatchState {
            phase: "start".to_string(),
            pages: 0,
            atoms_audited: 0,
            estimated_remaining_pages: None,
            scope_partial,
            home: PathBuf::new(),
            max_v2_bytes: 0,
            started: now,
        }));
        Self {
            started: now,
            deadline: Duration::from_secs(deadline_secs),
            last_progress: now,
            phase: "start",
            pages: 0,
            atoms_audited: 0,
            estimated_remaining_pages: None,
            scope_partial,
            home: PathBuf::new(),
            max_v2_bytes: 0,
            watch,
        }
    }

    fn publish(&self) {
        let mut state = self.watch.lock().expect("watch lock");
        state.phase = self.phase.to_string();
        state.pages = self.pages;
        state.atoms_audited = self.atoms_audited;
        state.estimated_remaining_pages = self.estimated_remaining_pages;
        state.scope_partial = self.scope_partial;
        state.home = self.home.clone();
        state.max_v2_bytes = self.max_v2_bytes;
    }

    fn remaining(&self) -> Duration {
        self.deadline.saturating_sub(self.started.elapsed())
    }

    fn timed_out(&self) -> bool {
        self.remaining().is_zero()
    }

    fn elapsed_secs(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    fn set_phase(&mut self, phase: &'static str) {
        self.phase = phase;
        self.publish();
        self.progress(true);
    }

    fn progress(&mut self, force: bool) {
        self.publish();
        if !force && self.last_progress.elapsed() < Duration::from_secs(PROGRESS_INTERVAL_SECS) {
            return;
        }
        let remaining_pages = self
            .estimated_remaining_pages
            .map_or_else(|| "unknown".to_string(), |n| n.to_string());
        eprintln!(
            "phase={} pages={} atoms={} elapsed_secs={} remaining_pages={}",
            self.phase,
            self.pages,
            self.atoms_audited,
            self.elapsed_secs(),
            remaining_pages
        );
        self.last_progress = Instant::now();
    }

    fn timeout_report(
        &self,
        home: PathBuf,
        max_v2_bytes: u64,
        prefixes: Vec<PrefixProof>,
        crash: Option<AtomRefCrashResumeProof>,
    ) -> ProofReport {
        ProofReport {
            status: "timeout",
            scope: if self.scope_partial {
                "partial"
            } else {
                "full"
            },
            phase: self.phase.to_string(),
            pages: self.pages,
            atoms_audited: self.atoms_audited,
            elapsed_secs: self.elapsed_secs(),
            home,
            max_v2_bytes,
            prefixes,
            crash,
            reclaim: None,
        }
    }

    fn finish_report(
        &self,
        status: &'static str,
        home: PathBuf,
        max_v2_bytes: u64,
        prefixes: Vec<PrefixProof>,
        crash: Option<AtomRefCrashResumeProof>,
        reclaim: Option<ReclaimProof>,
    ) -> ProofReport {
        ProofReport {
            status,
            scope: if self.scope_partial {
                "partial"
            } else {
                "full"
            },
            phase: self.phase.to_string(),
            pages: self.pages,
            atoms_audited: self.atoms_audited,
            elapsed_secs: self.elapsed_secs(),
            home,
            max_v2_bytes,
            prefixes,
            crash,
            reclaim,
        }
    }
}

fn decide_status(timed_out: bool, scope_partial: bool) -> &'static str {
    if timed_out {
        "timeout"
    } else if scope_partial {
        "partial"
    } else {
        "PASS"
    }
}

fn atom_budget_remaining(max_atoms: Option<u64>, atoms_audited: u64) -> Option<u64> {
    max_atoms.map(|limit| limit.saturating_sub(atoms_audited))
}

fn atom_budget_exhausted(max_atoms: Option<u64>, atoms_audited: u64) -> bool {
    matches!(atom_budget_remaining(max_atoms, atoms_audited), Some(0))
}

async fn await_with_progress<T, E>(
    life: &mut Lifetime,
    fut: impl Future<Output = Result<T, E>>,
) -> Result<T, RunError>
where
    E: ToString,
{
    tokio::pin!(fut);
    loop {
        if life.timed_out() {
            return Err(RunError::Timeout);
        }
        let slice = life
            .remaining()
            .min(Duration::from_secs(PROGRESS_INTERVAL_SECS));
        if slice.is_zero() {
            return Err(RunError::Timeout);
        }
        match tokio::time::timeout(slice, &mut fut).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) => return Err(RunError::Fail(error.to_string())),
            Err(_) => {
                life.progress(true);
                if life.timed_out() {
                    return Err(RunError::Timeout);
                }
            }
        }
    }
}
