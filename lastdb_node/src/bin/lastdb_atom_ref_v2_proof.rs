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

async fn run(args: Args, life: &mut Lifetime) -> Result<ProofReport, RunError> {
    if !args.ack_isolated_copy {
        return Err(RunError::Fail(
            "pass --ack-isolated-copy after the safe copy step".to_string(),
        ));
    }
    let home = canonical_dir(&args.home).map_err(RunError::Fail)?;
    life.home = home.clone();
    life.publish();
    let temp = canonical_dir(&std::env::temp_dir()).map_err(RunError::Fail)?;
    if !home.starts_with(&temp) {
        return Err(RunError::Fail(format!(
            "proof home must stay under the system temporary directory {} (got {})",
            temp.display(),
            home.display()
        )));
    }
    if let Ok(live_home) = lastdb_node::host::resolve_home(None) {
        if live_home.canonicalize().ok().as_ref() == Some(&home) {
            return Err(RunError::Fail(
                "refusing to run the proof on the live LastDB home".to_string(),
            ));
        }
    }
    let cloud_config = home.join(lastdb_node::host::CLOUD_SYNC_CONFIG_FILE);
    if cloud_config.exists() {
        return Err(RunError::Fail(format!(
            "remove {} from the copy before the proof",
            cloud_config.display()
        )));
    }
    if args.drain_v1_and_compact && !env_falsey("LASTDB_ATOM_REF_BACKFILL") {
        return Err(RunError::Fail(
            "--drain-v1-and-compact requires LASTDB_ATOM_REF_BACKFILL=0".to_string(),
        ));
    }
    if life.timed_out() {
        return Ok(life.timeout_report(home, 0, Vec::new(), None));
    }

    life.set_phase("boot");
    life.estimated_remaining_pages = Some(args.max_pages);
    let mut host = match await_with_progress(life, lastdb_node::Host::boot(&home)).await {
        Ok(host) => host,
        Err(RunError::Timeout) => return Ok(life.timeout_report(home, 0, Vec::new(), None)),
        Err(error) => return Err(error),
    };
    let v1_allocated_before = collection_allocated_bytes(&host, V1_COLLECTION);
    let v2_allocated_before = collection_allocated_bytes(&host, V2_COLLECTION);
    let mut molecules_by_prefix = collect_molecules_by_prefix(&host).map_err(RunError::Fail)?;
    let crash_prefix = molecules_by_prefix.keys().next().cloned().flatten();
    life.set_phase("crash_resume");
    let crash_resume = match interrupt_and_resume_first_page(
        host,
        &home,
        crash_prefix.as_deref(),
        args.page,
        life,
    )
    .await
    {
        Ok(pair) => pair,
        Err(RunError::Timeout) => return Ok(life.timeout_report(home, 0, Vec::new(), None)),
        Err(error) => return Err(error),
    };
    host = crash_resume.0;
    let mut crash = crash_resume.1;
    molecules_by_prefix = collect_molecules_by_prefix(&host).map_err(RunError::Fail)?;

    let max_v2_bytes = lastdb_node::atom_ref_backfill::atom_ref_v2_max_bytes();
    life.max_v2_bytes = max_v2_bytes;
    life.publish();
    let mut proofs = Vec::new();
    'prefixes: for (storage_prefix, molecules) in molecules_by_prefix {
        if life.timed_out() {
            return Ok(life.timeout_report(home, max_v2_bytes, proofs, Some(crash)));
        }
        if atom_budget_exhausted(args.max_atoms, life.atoms_audited) {
            break;
        }
        let prefix = storage_prefix.as_deref();
        let mut pages = 0usize;
        life.set_phase("backfill");
        loop {
            if life.timed_out() {
                return Ok(life.timeout_report(home, max_v2_bytes, proofs, Some(crash)));
            }
            let status = match await_with_progress(
                life,
                host.db.db_ops().atoms().atom_ref_v2_backfill_status(prefix),
            )
            .await
            {
                Ok(status) => status,
                Err(RunError::Timeout) => {
                    return Ok(life.timeout_report(home, max_v2_bytes, proofs, Some(crash)));
                }
                Err(error) => return Err(error),
            };
            require_growth_budget(&host, max_v2_bytes, status.edges_written)
                .map_err(RunError::Fail)?;
            match status.phase {
                AtomRefBackfillPhase::Complete => break,
                AtomRefBackfillPhase::Blocked => {
                    return Err(RunError::Fail(format!(
                        "compact rebuild blocked after {} skipped row(s)",
                        status.skipped_rows
                    )));
                }
                AtomRefBackfillPhase::Backfill | AtomRefBackfillPhase::Replay => {}
            }
            if pages >= args.max_pages {
                return Err(RunError::Fail(format!(
                    "compact rebuild exceeded {} pages",
                    args.max_pages
                )));
            }
            match await_with_progress(
                life,
                host.db
                    .db_ops()
                    .atoms()
                    .reindex_atom_ref_v2_edges(prefix, Some(args.page)),
            )
            .await
            {
                Ok(_) => {}
                Err(RunError::Timeout) => {
                    return Ok(life.timeout_report(home, max_v2_bytes, proofs, Some(crash)));
                }
                Err(error) => return Err(error),
            }
            pages = pages.saturating_add(1);
            life.pages = life.pages.saturating_add(1);
            life.estimated_remaining_pages = Some(args.max_pages.saturating_sub(life.pages));
            life.progress(false);
        }

        life.set_phase("history");
        for molecule_uuid in molecules {
            if life.timed_out() {
                return Ok(life.timeout_report(home, max_v2_bytes, proofs, Some(crash)));
            }
            if atom_budget_exhausted(args.max_atoms, life.atoms_audited) {
                break 'prefixes;
            }
            let Some(manifest) = (match await_with_progress(
                life,
                host.db
                    .db_ops()
                    .atoms()
                    .ensure_atom_ref_v2_molecule_manifest_after_reindex(&molecule_uuid, prefix),
            )
            .await
            {
                Ok(manifest) => manifest,
                Err(RunError::Timeout) => {
                    return Ok(life.timeout_report(home, max_v2_bytes, proofs, Some(crash)));
                }
                Err(error) => return Err(error),
            }) else {
                return Err(RunError::Fail(format!(
                    "missing compact manifest for {molecule_uuid}"
                )));
            };
            if manifest.version == ATOM_REF_MANIFEST_VERSION_HISTORY && manifest.replay_complete {
                continue;
            }
            loop {
                if life.timed_out() {
                    return Ok(life.timeout_report(home, max_v2_bytes, proofs, Some(crash)));
                }
                let active_edges = match await_with_progress(
                    life,
                    host.db.db_ops().atoms().atom_ref_v2_backfill_status(prefix),
                )
                .await
                {
                    Ok(status) => status.edges_written,
                    Err(RunError::Timeout) => {
                        return Ok(life.timeout_report(home, max_v2_bytes, proofs, Some(crash)));
                    }
                    Err(error) => return Err(error),
                };
                require_growth_budget(&host, max_v2_bytes, active_edges).map_err(RunError::Fail)?;
                if pages >= args.max_pages {
                    return Err(RunError::Fail(format!(
                        "compact history upgrade exceeded {} pages",
                        args.max_pages
                    )));
                }
                let report = match await_with_progress(
                    life,
                    host.db
                        .db_ops()
                        .atoms()
                        .upgrade_atom_ref_v2_molecule_history_page(
                            &molecule_uuid,
                            prefix,
                            args.page,
                        ),
                )
                .await
                {
                    Ok(report) => report,
                    Err(RunError::Timeout) => {
                        return Ok(life.timeout_report(home, max_v2_bytes, proofs, Some(crash)));
                    }
                    Err(error) => return Err(error),
                };
                pages = pages.saturating_add(1);
                life.pages = life.pages.saturating_add(1);
                life.estimated_remaining_pages = Some(args.max_pages.saturating_sub(life.pages));
                life.progress(false);
                if report.complete {
                    break;
                }
            }
            let molecule_audit = match await_with_progress(
                life,
                host.db
                    .db_ops()
                    .atoms()
                    .audit_atom_ref_v2_molecule(&molecule_uuid, prefix),
            )
            .await
            {
                Ok(audit) => audit,
                Err(RunError::Timeout) => {
                    return Ok(life.timeout_report(home, max_v2_bytes, proofs, Some(crash)));
                }
                Err(error) => return Err(error),
            };
            let added = molecule_audit.expected_active_edges.max(1);
            if let Some(remaining) = atom_budget_remaining(args.max_atoms, life.atoms_audited) {
                life.atoms_audited = life.atoms_audited.saturating_add(added.min(remaining));
            } else {
                life.atoms_audited = life.atoms_audited.saturating_add(added);
            }
            life.progress(false);
        }

        if args.max_atoms.is_some() {
            proofs.push(PrefixProof {
                storage_prefix,
                pages,
                v2_bytes: v2_bytes(&host),
                audit: None,
                lookup: None,
            });
            continue;
        }

        life.set_phase("mark_complete");
        match await_with_progress(
            life,
            host.db
                .db_ops()
                .atoms()
                .mark_atom_ref_v2_history_complete(prefix),
        )
        .await
        {
            Ok(()) => {}
            Err(RunError::Timeout) => {
                return Ok(life.timeout_report(home, max_v2_bytes, proofs, Some(crash)));
            }
            Err(error) => return Err(error),
        }

        life.set_phase("audit");
        let audit = match await_with_progress(
            life,
            host.db
                .db_ops()
                .atoms()
                .audit_atom_ref_v2_edges_on_isolated_copy(prefix),
        )
        .await
        {
            Ok(audit) => audit,
            Err(RunError::Timeout) => {
                return Ok(life.timeout_report(home, max_v2_bytes, proofs, Some(crash)));
            }
            Err(error) => return Err(error),
        };
        if !audit.complete
            || audit.missing_edges != 0
            || audit.unexpected_edges != 0
            || audit.invalid_live_keys != 0
            || audit.false_zero_reference_atoms != 0
            || audit.indexed_active_edges != audit.expected_active_edges
        {
            return Err(RunError::Fail(format!(
                "compact audit is not exact: {audit:?}"
            )));
        }
        life.atoms_audited = life
            .atoms_audited
            .saturating_add(audit.expected_active_edges.max(audit.molecules_audited));
        let lookup = if audit.expected_active_edges == 0 {
            None
        } else {
            life.set_phase("lookup");
            match await_with_progress(
                life,
                host.db
                    .db_ops()
                    .atoms()
                    .benchmark_atom_ref_v2_lookups_on_isolated_copy(prefix, args.lookup_samples),
            )
            .await
            {
                Ok(benchmark) => Some(benchmark),
                Err(RunError::Timeout) => {
                    return Ok(life.timeout_report(home, max_v2_bytes, proofs, Some(crash)));
                }
                Err(error) => return Err(error),
            }
        };
        proofs.push(PrefixProof {
            storage_prefix,
            pages,
            v2_bytes: v2_bytes(&host),
            audit: Some(audit),
            lookup,
        });
    }

    if life.timed_out() {
        return Ok(life.timeout_report(home, max_v2_bytes, proofs, Some(crash)));
    }
    if args.max_atoms.is_some() {
        life.set_phase("partial");
        return Ok(life.finish_report(
            decide_status(false, true),
            home,
            max_v2_bytes,
            proofs,
            Some(crash),
            None,
        ));
    }

    crash.resumed_to_complete = proofs
        .iter()
        .all(|proof| proof.audit.as_ref().is_some_and(|audit| audit.complete));
    if !crash.cursor_survived || !crash.resumed_to_complete {
        return Err(RunError::Fail(format!(
            "compact crash resume did not complete with a surviving cursor: {crash:?}"
        )));
    }

    let reclaim = if args.drain_v1_and_compact {
        life.set_phase("drain");
        match drain_and_compact(
            &host,
            &proofs,
            args.page,
            args.max_pages,
            v1_allocated_before,
            v2_allocated_before,
            life,
        )
        .await
        {
            Ok(reclaim) => Some(reclaim),
            Err(RunError::Timeout) => {
                return Ok(life.timeout_report(home, max_v2_bytes, proofs, Some(crash)));
            }
            Err(error) => return Err(error),
        }
    } else {
        None
    };

    if life.timed_out() {
        return Ok(life.timeout_report(home, max_v2_bytes, proofs, Some(crash)));
    }
    life.set_phase("complete");
    Ok(life.finish_report(
        decide_status(false, false),
        home,
        max_v2_bytes,
        proofs,
        Some(crash),
        reclaim,
    ))
}

fn collect_molecules_by_prefix(
    host: &lastdb_node::Host,
) -> Result<BTreeMap<Option<String>, BTreeSet<String>>, String> {
    let mut molecules_by_prefix: BTreeMap<Option<String>, BTreeSet<String>> =
        BTreeMap::from([(None, BTreeSet::new())]);
    for schema in host
        .db
        .schema_manager()
        .get_schemas()
        .map_err(|error| error.to_string())?
        .into_values()
    {
        for field in schema.runtime_fields.values() {
            let Some(molecule_uuid) = field.common().molecule_uuid() else {
                continue;
            };
            molecules_by_prefix
                .entry(field.common().storage_prefix().map(str::to_string))
                .or_default()
                .insert(molecule_uuid.clone());
        }
    }
    Ok(molecules_by_prefix)
}

async fn interrupt_and_resume_first_page(
    host: lastdb_node::Host,
    home: &Path,
    storage_prefix: Option<&str>,
    page: usize,
    life: &mut Lifetime,
) -> Result<(lastdb_node::Host, AtomRefCrashResumeProof), RunError> {
    let before = await_with_progress(
        life,
        host.db
            .db_ops()
            .atoms()
            .atom_ref_v2_backfill_status(storage_prefix),
    )
    .await?;
    if !matches!(
        before.phase,
        AtomRefBackfillPhase::Complete | AtomRefBackfillPhase::Blocked
    ) {
        await_with_progress(
            life,
            host.db
                .db_ops()
                .atoms()
                .reindex_atom_ref_v2_edges(storage_prefix, Some(page)),
        )
        .await?;
    }
    let interrupted = await_with_progress(
        life,
        host.db
            .db_ops()
            .atoms()
            .atom_ref_v2_backfill_status(storage_prefix),
    )
    .await?;
    let interrupted_phase = interrupted.phase;
    let interrupted_slots_walked = interrupted.slots_walked;
    let interrupted_edges_written = interrupted.edges_written;
    let interrupted_cursor = interrupted.after_tip_key.clone();
    drop(host);
    life.set_phase("crash_reboot");
    let host = await_with_progress(life, lastdb_node::Host::boot(home)).await?;
    let resumed = await_with_progress(
        life,
        host.db
            .db_ops()
            .atoms()
            .atom_ref_v2_backfill_status(storage_prefix),
    )
    .await?;
    let cursor_survived = resumed.phase == interrupted_phase
        && resumed.slots_walked == interrupted_slots_walked
        && resumed.edges_written == interrupted_edges_written
        && resumed.after_tip_key == interrupted_cursor;
    if !cursor_survived {
        return Err(RunError::Fail(format!(
            "compact rebuild cursor did not survive the crash interrupt: before={interrupted:?} after={resumed:?}"
        )));
    }
    Ok((
        host,
        AtomRefCrashResumeProof {
            interrupted_phase,
            interrupted_slots_walked,
            interrupted_edges_written,
            cursor_survived,
            resumed_to_complete: false,
        },
    ))
}

async fn drain_and_compact(
    host: &lastdb_node::Host,
    proofs: &[PrefixProof],
    page: usize,
    max_pages: usize,
    v1_allocated_before: u64,
    v2_allocated_before: u64,
    life: &mut Lifetime,
) -> Result<ReclaimProof, RunError> {
    let readiness_prefixes = proofs
        .iter()
        .map(|proof| proof.storage_prefix.clone())
        .collect::<Vec<_>>();
    let mut total_pages = 0usize;
    loop {
        if life.timed_out() {
            return Err(RunError::Timeout);
        }
        let status =
            await_with_progress(life, host.db.db_ops().atoms().atom_ref_v1_drain_status()).await?;
        if status.completed {
            break;
        }
        if total_pages >= max_pages {
            return Err(RunError::Fail(format!(
                "legacy drain exceeded {max_pages} pages"
            )));
        }
        await_with_progress(
            life,
            host.db
                .db_ops()
                .atoms()
                .drain_atom_ref_v1_page(page, &readiness_prefixes),
        )
        .await?;
        total_pages = total_pages.saturating_add(1);
        life.pages = life.pages.saturating_add(1);
        life.progress(false);
    }
    let drain_status =
        await_with_progress(life, host.db.db_ops().atoms().atom_ref_v1_drain_status()).await?;
    let v1_keys_remain =
        await_with_progress(life, host.db.db_ops().atoms().atom_ref_v1_keys_remain()).await?;
    if v1_keys_remain {
        return Err(RunError::Fail(
            "legacy reverse-edge keys remain after the physical drain".to_string(),
        ));
    }

    life.set_phase("compact");
    let v1_compaction =
        await_with_progress(life, host.db.compact_collection(V1_COLLECTION, false)).await?;
    if !v1_compaction.executed {
        return Err(RunError::Fail(format!(
            "legacy reverse-edge compaction did not execute: {v1_compaction:?}"
        )));
    }
    let v2_compaction =
        await_with_progress(life, host.db.compact_collection(V2_COLLECTION, false)).await?;
    if !v2_compaction.executed {
        return Err(RunError::Fail(format!(
            "compact reverse-edge compaction did not execute: {v2_compaction:?}"
        )));
    }

    let v1_allocated_after = collection_allocated_bytes(host, V1_COLLECTION);
    let v2_allocated_after = collection_allocated_bytes(host, V2_COLLECTION);
    let atoms_allocated_after = collection_allocated_bytes(host, ATOMS_COLLECTION);
    let before = v1_allocated_before.saturating_add(v2_allocated_before);
    let after = v1_allocated_after.saturating_add(v2_allocated_after);
    let allocated_bytes_reclaimed = before.saturating_sub(after);
    let active_edges = proofs
        .iter()
        .filter_map(|proof| proof.audit.as_ref())
        .map(|audit| audit.expected_active_edges)
        .sum::<u64>();
    let v2_bytes_per_edge = v2_allocated_after.checked_div(active_edges).unwrap_or(0);
    let v2_to_atoms_basis_points = v2_allocated_after
        .saturating_mul(10_000)
        .checked_div(atoms_allocated_after)
        .unwrap_or(u64::MAX);

    if v1_compaction.live_keys != 0 || v1_allocated_after > MAX_DRAINED_V1_BYTES {
        return Err(RunError::Fail(format!(
            "legacy reverse-edge plane retained {} live keys and {v1_allocated_after} allocated bytes",
            v1_compaction.live_keys
        )));
    }
    if v2_allocated_after > MAX_DRAINED_V2_BYTES {
        return Err(RunError::Fail(format!(
            "compact reverse-edge plane retained {v2_allocated_after} bytes, above {MAX_DRAINED_V2_BYTES}"
        )));
    }
    if v2_bytes_per_edge > MAX_V2_BYTES_PER_EDGE {
        return Err(RunError::Fail(format!(
            "compact reverse-edge plane uses {v2_bytes_per_edge} bytes per edge, above {MAX_V2_BYTES_PER_EDGE}"
        )));
    }
    if v2_to_atoms_basis_points > MAX_V2_TO_ATOMS_BASIS_POINTS {
        return Err(RunError::Fail(format!(
            "compact index-to-atoms ratio is {v2_to_atoms_basis_points} basis points, above {MAX_V2_TO_ATOMS_BASIS_POINTS}"
        )));
    }
    let reclaim_floor_skipped = !reclaim_floor_applies(total_pages, v1_allocated_before);
    if allocated_bytes_reclaimed < MIN_RECLAIM_BYTES && !reclaim_floor_skipped {
        return Err(RunError::Fail(format!(
            "reverse-edge drain reclaimed {allocated_bytes_reclaimed} bytes, below {MIN_RECLAIM_BYTES}"
        )));
    }

    Ok(ReclaimProof {
        v1_allocated_before,
        v2_allocated_before,
        v1_allocated_after,
        v2_allocated_after,
        atoms_allocated_after,
        allocated_bytes_reclaimed,
        active_edges,
        v2_bytes_per_edge,
        v2_to_atoms_basis_points,
        drain_pages: total_pages,
        drain_status,
        v1_keys_remain,
        v1_compaction,
        v2_compaction,
        reclaim_floor_skipped,
    })
}

/// The 10 GiB reclaim floor applies when this run still had v1 to drain, or
/// when v1 was still larger than the leftover cap. An already-drained copy
/// with v1 at leftover size cannot reclaim 10 GiB and must not fail that
/// clause.
fn reclaim_floor_applies(drain_pages: usize, v1_allocated_before: u64) -> bool {
    drain_pages > 0 || v1_allocated_before > MAX_DRAINED_V1_BYTES
}

fn canonical_dir(path: &Path) -> Result<PathBuf, String> {
    path.canonicalize()
        .map_err(|error| format!("canonicalize {}: {error}", path.display()))
}

fn env_falsey(name: &str) -> bool {
    matches!(
        std::env::var(name).ok().as_deref().map(str::trim),
        Some("0" | "false" | "no" | "off")
    )
}

fn v2_bytes(host: &lastdb_node::Host) -> u64 {
    host.db
        .db_ops()
        .namespaced_store()
        .collection_disk_bytes(V2_COLLECTION)
        .unwrap_or(0)
}

fn collection_allocated_bytes(host: &lastdb_node::Host, collection: &str) -> u64 {
    host.db
        .db_ops()
        .namespaced_store()
        .collection_disk_usage(collection)
        .map_or(0, |usage| usage.allocated_bytes)
}

fn require_growth_budget(
    host: &lastdb_node::Host,
    max_bytes: u64,
    active_edges: u64,
) -> Result<(), String> {
    let bytes = v2_bytes(host);
    let projected =
        lastdb_node::atom_ref_backfill::projected_atom_ref_v2_bytes(bytes, active_edges);
    if bytes > max_bytes || projected > max_bytes {
        Err(format!(
            "compact plane reached {bytes} bytes and projects to {projected}, above the {max_bytes}-byte proof limit"
        ))
    } else {
        Ok(())
    }
}
