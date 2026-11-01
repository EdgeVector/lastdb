//! Task-local accumulation of per-request phase timings.
//!
//! The request-ops telemetry (`lastdb_node::request_telemetry::PhaseTimings`)
//! wants to know *where inside the node* a request spent its time. For a
//! mutation that is QoS admission wait, parse, schema resolve, validate,
//! write-gate `lock_wait`, `molecule_gate`, `cas_precondition`, apply,
//! persist, flush, cloud-sync capture,
//! background index wait, change recording, and response envelope rendering.
//! For a query it is admission wait, parse, schema resolve, the index
//! `count`, row `hydrate`, and conflict `annotate`. For status it is the
//! snapshot components that can otherwise hide synchronous ops-surface work:
//! backup progress, durability marker evaluation, data-dir sizing,
//! and request-ops snapshot copying.
//! Those phases are measured in three different
//! crates (`lastdb_node` parses, `lastdb_host` resolves/validates/admits,
//! this crate applies/persists), so threading a collector parameter through
//! every signature would churn public APIs for a diagnostic.
//!
//! Instead, the socket executor wraps request dispatch in
//! [`run_with_phases`] and every recording site calls [`add_phase`], which
//! silently no-ops when no scope is present (background jobs, tests, the
//! desktop node's non-instrumented surfaces). Precedent:
//! [`crate::user_context::run_with_user`], which propagates the request's
//! user id over exactly the same task-local seam.
//!
//! `tokio::spawn` does NOT inherit task-locals, which is load-bearing here:
//! spawned background work (e.g. index embedding) must not attribute its
//! time to whatever request happens to be in scope. Only the awaited
//! request path accumulates.
//!
//! # Counts, alongside the timings
//!
//! A phase total answers "how long", never "how much work". That gap is not
//! academic: measured on the primary 2026-08-17, `persist_molecules` for
//! kanban BoardCards mutations read 7.80 s/req at 15:0xZ and 2.27 s/req at
//! 17:1xZ — a 3.4x move with NO change to that code path in the running
//! binary (fold #1546, which restructures it, is not an ancestor of the
//! running build). Node state alone moves the wall clock by more than the
//! optimisation being evaluated is expected to save, so a before/after read
//! of the phase cannot attribute anything.
//!
//! [`RequestCounter`] is the companion: a small set of *counts of work
//! issued*, accumulated over the same task-local seam. A count is immune to
//! IO contention, page-cache state and concurrent load, so it answers
//! structural questions ("did this mutation issue one durable molecule
//! commit or twenty-four?") that the clock cannot answer at all.

use std::cell::RefCell;
use std::future::Future;
use std::time::Duration;

mod counter;
mod phase;
mod tip_sketch;
mod totals;

pub use counter::{RequestCounter, RequestCounts};
pub use phase::RequestPhase;
pub use tip_sketch::TipKeySketch;
pub use totals::RequestPhaseTotals;

/// Everything one request accumulates: the phase clock and the work counts.
///
/// ONE task-local cell rather than two, for two reasons. It makes it
/// impossible for a request path to be inside one scope and outside the other
/// — [`add_counter`] would then report a structural zero indistinguishable
/// from "this path issues no durable commits". And nesting a second
/// `task_local::scope` around request dispatch pushed the compiler past its
/// query-depth limit on two `lastdb_node` integration tests (`local_watch_admission`,
/// `mutation_phase_instrumentation`): each `scope` wraps the whole request
/// future in another generic future type, and that future is already ~130
/// layers deep. Widening the payload costs no type depth at all.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RequestMetrics {
    phases: RequestPhaseTotals,
    counts: RequestCounts,
    tip_keys: TipKeySketch,
}

tokio::task_local! {
    static REQUEST_METRICS: RefCell<RequestMetrics>;
}

/// Saturating `Duration` → whole microseconds for phase fields.
#[must_use]
pub fn duration_us(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
}

/// Run `f` with fresh phase and count accumulators in scope and return its
/// output alongside whatever the recording sites accumulated.
///
/// One scope covers both — see [`RequestMetrics`] for why that is a
/// correctness property and not just tidiness.
pub async fn run_with_phases<F>(f: F) -> (F::Output, RequestPhaseTotals, RequestCounts)
where
    F: Future,
{
    let (output, phases, counts, _) = run_with_phases_and_keys(f).await;
    (output, phases, counts)
}

/// Return the request's distinct-tip sketch beside its existing metrics.
pub async fn run_with_phases_and_keys<F>(
    f: F,
) -> (F::Output, RequestPhaseTotals, RequestCounts, TipKeySketch)
where
    F: Future,
{
    REQUEST_METRICS
        .scope(RefCell::new(RequestMetrics::default()), async move {
            let output = f.await;
            let metrics = REQUEST_METRICS.with(|cell| cell.borrow().clone());
            (output, metrics.phases, metrics.counts, metrics.tip_keys)
        })
        .await
}

/// Accumulate `n` into `counter` for the request in scope. Outside a
/// [`run_with_phases`] scope this is a silent no-op, for the same reason
/// [`add_phase_us`] is: recording sites sit deep in the storage path and must
/// not care whether their caller is an instrumented socket request.
pub fn add_counter(counter: RequestCounter, n: u64) {
    // Adding zero is the identity; skip the task-local access entirely so a
    // no-op mutation (nothing modified) costs nothing.
    if n == 0 {
        return;
    }
    let _ = REQUEST_METRICS.try_with(|cell| {
        let mut metrics = cell.borrow_mut();
        let slot = match counter {
            RequestCounter::MoleculesPersisted => &mut metrics.counts.molecules_persisted,
            RequestCounter::MoleculeStoreCommits => &mut metrics.counts.molecule_store_commits,
            RequestCounter::ResidentCommits => &mut metrics.counts.resident_commits,
            RequestCounter::ResidentOperations => &mut metrics.counts.resident_operations,
        };
        *slot = slot.saturating_add(n);
    });
}

/// Accumulate `elapsed` into `phase` for the request in scope. Outside a
/// [`run_with_phases`] scope this is a silent no-op — recording sites never
/// need to know whether the caller is an instrumented socket request.
pub fn add_phase(phase: RequestPhase, elapsed: Duration) {
    add_phase_us(phase, duration_us(elapsed));
}

/// [`add_phase`] with a pre-converted microsecond value. Saturating: a
/// request cannot wrap its own phase counter no matter how long it runs.
pub fn add_phase_us(phase: RequestPhase, us: u64) {
    // Sub-microsecond measurements truncate to 0 and adding 0 is the
    // identity, but skipping the task-local access entirely keeps the
    // common "phase too fast to matter" case free.
    if us == 0 {
        return;
    }
    let _ = REQUEST_METRICS.try_with(|cell| {
        let mut metrics = cell.borrow_mut();
        let totals = &mut metrics.phases;
        let slot = match phase {
            RequestPhase::AdmissionWait => &mut totals.admission_wait_us,
            RequestPhase::Parse => &mut totals.parse_us,
            RequestPhase::SchemaResolve => &mut totals.schema_resolve_us,
            RequestPhase::Validate => &mut totals.validate_us,
            RequestPhase::LockWait => &mut totals.lock_wait_us,
            RequestPhase::PurgeBarrier => &mut totals.purge_barrier_us,
            RequestPhase::PurgePlan => &mut totals.purge_plan_us,
            RequestPhase::PurgeCommit => &mut totals.purge_commit_us,
            RequestPhase::PurgeMaterialize => &mut totals.purge_materialize_us,
            RequestPhase::PurgeTrace => &mut totals.purge_trace_us,
            RequestPhase::PurgeRetentionGuard => &mut totals.purge_retention_guard_us,
            RequestPhase::PurgeDelete => &mut totals.purge_delete_us,
            RequestPhase::PurgeFinalize => &mut totals.purge_finalize_us,
            RequestPhase::MoleculeGate => &mut totals.molecule_gate_us,
            RequestPhase::CasPrecondition => &mut totals.cas_precondition_us,
            RequestPhase::Count => &mut totals.count_us,
            RequestPhase::Hydrate => &mut totals.hydrate_us,
            RequestPhase::HydrateAtoms => &mut totals.hydrate_atoms_us,
            RequestPhase::HydrateFormat => &mut totals.hydrate_format_us,
            RequestPhase::HydrateSort => &mut totals.hydrate_sort_us,
            RequestPhase::HydrateFilter => &mut totals.hydrate_filter_us,
            RequestPhase::Annotate => &mut totals.annotate_us,
            RequestPhase::Apply => &mut totals.apply_us,
            RequestPhase::ApplyMemory => &mut totals.apply_memory_us,
            RequestPhase::SchemaLoad => &mut totals.schema_load_us,
            RequestPhase::SchemaReload => &mut totals.schema_reload_us,
            RequestPhase::RestoreMolecules => &mut totals.restore_molecules_us,
            RequestPhase::Grouping => &mut totals.grouping_us,
            RequestPhase::IdempotencyCheck => &mut totals.idempotency_check_us,
            RequestPhase::SyncUuids => &mut totals.sync_uuids_us,
            RequestPhase::SpawnIndexing => &mut totals.spawn_indexing_us,
            RequestPhase::Persist => &mut totals.persist_us,
            RequestPhase::PersistMolecules => &mut totals.persist_molecules_us,
            RequestPhase::PersistSchema => &mut totals.persist_schema_us,
            RequestPhase::PersistIdempotency => &mut totals.persist_idempotency_us,
            RequestPhase::Flush => &mut totals.flush_us,
            RequestPhase::IndexWait => &mut totals.index_wait_us,
            RequestPhase::ChangeRecord => &mut totals.change_record_us,
            RequestPhase::ChangeRecordLockWait => &mut totals.change_record_lock_wait_us,
            RequestPhase::ChangeRecordWrite => &mut totals.change_record_write_us,
            RequestPhase::ResponseEnvelope => &mut totals.response_envelope_us,
            RequestPhase::StatusBackup => &mut totals.status_backup_us,
            RequestPhase::StatusDurability => &mut totals.status_durability_us,
            RequestPhase::StatusDataDir => &mut totals.status_data_dir_us,
            RequestPhase::StatusRequestOps => &mut totals.status_request_ops_us,
        };
        *slot = slot.saturating_add(us);
    });
}
