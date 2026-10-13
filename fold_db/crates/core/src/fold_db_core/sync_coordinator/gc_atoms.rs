//! Automatic `gc-atoms` trigger: settings, step state machine and runtime.

use super::*;

mod audit;
mod phases;

use audit::*;
use phases::*;

/// Unreferenced-atom bytes tolerated before the automatic delete pass arms.
///
/// The primary accumulated 344 MiB of deletable bodies in under five hours
/// (measured 2026-08-18), so a 128 MiB cap arms inside a normal session. A cap
/// that only a multi-day uptime can reach is the defect this trigger fixes, not
/// a safety margin.
pub(super) const DEFAULT_GC_ATOMS_MAX_UNREFERENCED_BYTES: u64 = 128 * 1024 * 1024;

/// Minimum spacing between automatic gc-atoms steps.
///
/// The local cadence owns this interval. The due-check still refuses a second
/// step inside one interval if a tick overlaps a slow previous step.
pub(super) const DEFAULT_GC_ATOMS_STEP_INTERVAL_SECS: u64 = 30;

/// Wall-clock bound on one automatic gc-atoms step.
///
/// Each step advances the resumable probe or the bounded delete pass by whole
/// pages and then stops. The manual admin verb keeps its unbounded behavior.
pub(super) const DEFAULT_GC_ATOMS_STEP_BUDGET_SECS: u64 = 5;

/// Spacing between probe laps once a lap reaches its terminal result.
pub(super) const DEFAULT_GC_ATOMS_RELAP_INTERVAL_SECS: u64 = 60 * 60;

/// Orphan candidates locked and revalidated in one automatic delete batch.
pub(super) const DEFAULT_GC_ATOMS_DELETE_CANDIDATE_CAP: usize = 1_000;

/// Safety bound on bounded calls issued inside one step, so a state machine
/// that stops advancing cannot spin against the step deadline.
pub(super) const GC_ATOMS_MAX_CALLS_PER_STEP: usize = 512;

/// Tunables for the automatic `gc-atoms` trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AutomaticGcAtomsSettings {
    pub(super) max_unreferenced_bytes: u64,
    pub(super) step_interval_secs: u64,
    pub(super) step_budget_secs: u64,
    pub(super) relap_interval_secs: u64,
    pub(super) delete_candidate_cap: usize,
}

impl Default for AutomaticGcAtomsSettings {
    fn default() -> Self {
        Self {
            max_unreferenced_bytes: DEFAULT_GC_ATOMS_MAX_UNREFERENCED_BYTES,
            step_interval_secs: DEFAULT_GC_ATOMS_STEP_INTERVAL_SECS,
            step_budget_secs: DEFAULT_GC_ATOMS_STEP_BUDGET_SECS,
            relap_interval_secs: DEFAULT_GC_ATOMS_RELAP_INTERVAL_SECS,
            delete_candidate_cap: DEFAULT_GC_ATOMS_DELETE_CANDIDATE_CAP,
        }
    }
}

impl AutomaticGcAtomsSettings {
    /// Read the env overrides once, at background-sync start.
    ///
    /// Naming matches the existing plane caps
    /// (`LASTDB_PIN_LOG_COMPACT_MAX_BYTES`,
    /// `LASTDB_ATOM_LOCATORS_COMPACT_MAX_BYTES`).
    pub(super) fn from_env() -> Self {
        let defaults = Self::default();
        Self {
            max_unreferenced_bytes: gc_atoms_config_u64(
                "LASTDB_GC_ATOMS_MAX_UNREFERENCED_BYTES",
                defaults.max_unreferenced_bytes,
            ),
            step_interval_secs: gc_atoms_config_u64(
                "LASTDB_GC_ATOMS_STEP_INTERVAL_SECS",
                defaults.step_interval_secs,
            ),
            step_budget_secs: gc_atoms_config_u64(
                "LASTDB_GC_ATOMS_STEP_BUDGET_SECS",
                defaults.step_budget_secs,
            ),
            relap_interval_secs: gc_atoms_config_u64(
                "LASTDB_GC_ATOMS_RELAP_INTERVAL_SECS",
                defaults.relap_interval_secs,
            ),
            delete_candidate_cap: gc_atoms_config_u64(
                "LASTDB_GC_ATOMS_DELETE_CANDIDATE_CAP",
                defaults.delete_candidate_cap as u64,
            ) as usize,
        }
    }

    /// A zero byte cap turns the trigger off. The manual verb still works.
    pub(super) fn enabled(&self) -> bool {
        self.max_unreferenced_bytes > 0
    }
}

/// What one automatic step did. Returned so a test can assert the decision
/// instead of reading log lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AutomaticGcAtomsStep {
    /// Disabled, not due yet, or another step claimed this slot.
    Skipped,
    /// The probe lap is still walking planes.
    ProbeAdvanced,
    /// A lap completed under the byte cap. Nothing is deleted.
    BelowThreshold { unreferenced_bytes: u64 },
    /// A backup cut is held, so no body may be deleted this step.
    CutHeld,
    /// Bounded delete batches ran.
    Deleted {
        atoms_deleted: u64,
        bytes_freed_approx: u64,
        drained: bool,
    },
    /// The probe or the delete pass returned an error.
    Failed,
}

pub(super) fn gc_atoms_config_u64(name: &str, default: u64) -> u64 {
    env_flag::var_or(name, default)
}

/// True when the step spacing has elapsed. `last_step == 0` means never run.
pub(super) fn automatic_gc_atoms_due(last_step: u64, now: u64, interval_secs: u64) -> bool {
    last_step == 0 || now.saturating_sub(last_step) >= interval_secs
}

/// True when a completed lap justifies deleting bodies.
///
/// A held backup cut refuses the pass. `gc-atoms` deletes are captured appends
/// that compete with the cut for upload, and a body named by an in-flight
/// manifest must survive until the cut publishes.
pub(super) fn automatic_gc_atoms_delete_armed(
    unreferenced_bytes: u64,
    max_unreferenced_bytes: u64,
    backup_cut_held: bool,
) -> bool {
    max_unreferenced_bytes > 0 && !backup_cut_held && unreferenced_bytes >= max_unreferenced_bytes
}

/// Advance the automatic `gc-atoms` state machine by one bounded step.
///
/// The probe writes only checkpoints and keyed membership markers, so it runs
/// without the packing lock and keeps making progress while a cut drains. Only
/// the delete phase takes that lock, and it holds it across the whole batch
/// sequence: a boolean pre-check would race a new cut and remove bodies its
/// manifest already names.
pub(super) struct AutomaticGcAtomsRuntime<'a> {
    pub(super) atoms: &'a AtomStore,
    pub(super) engine: Option<&'a SyncEngine>,
}

/// Run one probe step. `Err` carries the finished step when the lap is not
/// complete yet (probe failed or only progressed).
async fn run_probe_stage(
    runtime: &AutomaticGcAtomsRuntime<'_>,
    restart_lap_at: &AtomicU64,
    restart_completed: bool,
    deadline: tokio::time::Instant,
) -> Result<AutomaticGcAtomsProbeResult, AutomaticGcAtomsStep> {
    match advance_gc_atoms_probe(runtime, restart_lap_at, restart_completed, deadline).await {
        ProbeStepOutcome::Failed => Err(AutomaticGcAtomsStep::Failed),
        ProbeStepOutcome::Progressed {
            generation,
            phase,
            rows_scanned,
        } => {
            info!(
                target: "fold_db::gc_atoms",
                generation,
                phase = ?phase,
                rows_scanned,
                "automatic gc-atoms probe progressed"
            );
            Err(AutomaticGcAtomsStep::ProbeAdvanced)
        }
        ProbeStepOutcome::LapComplete(lap) => Ok(lap),
    }
}

/// The lap measured less than the byte cap: schedule the next measurement.
fn step_below_cap(
    lap: &AutomaticGcAtomsProbeResult,
    settings: &AutomaticGcAtomsSettings,
    restart_lap_at: &AtomicU64,
    now: u64,
) -> AutomaticGcAtomsStep {
    info!(
        target: "fold_db::gc_atoms",
        generation = lap.generation,
        unreferenced_bytes = lap.unreferenced_bytes_approx,
        threshold_bytes = settings.max_unreferenced_bytes,
        "automatic gc-atoms below cap"
    );
    restart_lap_at.store(
        now.saturating_add(settings.relap_interval_secs).max(1),
        Ordering::Relaxed,
    );
    AutomaticGcAtomsStep::BelowThreshold {
        unreferenced_bytes: lap.unreferenced_bytes_approx,
    }
}

/// Run the delete batches under the packing lock the caller already holds.
async fn delete_pass_step(
    atoms: &AtomStore,
    lap: &AutomaticGcAtomsProbeResult,
    settings: &AutomaticGcAtomsSettings,
    restart_lap_at: &AtomicU64,
    now: u64,
    deadline: tokio::time::Instant,
) -> AutomaticGcAtomsStep {
    let Some(DeletePassOutcome {
        atoms_deleted,
        bytes_freed_approx,
        drained,
    }) = run_gc_atoms_delete_pass(atoms, settings, deadline).await
    else {
        return AutomaticGcAtomsStep::Failed;
    };

    if drained {
        // The generation is drained. Measure again after a lap interval.
        restart_lap_at.store(
            now.saturating_add(settings.relap_interval_secs).max(1),
            Ordering::Relaxed,
        );
    }
    tracing::info!(
        target: "fold_db::gc_atoms",
        generation = lap.generation,
        threshold_bytes = settings.max_unreferenced_bytes,
        reported_unreferenced_bytes = lap.unreferenced_bytes_approx,
        atoms_deleted,
        bytes_freed_approx,
        drained,
        "automatic gc-atoms step returned bytes"
    );
    AutomaticGcAtomsStep::Deleted {
        atoms_deleted,
        bytes_freed_approx,
        drained,
    }
}

pub(super) async fn advance_automatic_gc_atoms<F, Fut, G>(
    runtime: AutomaticGcAtomsRuntime<'_>,
    last_step: &AtomicU64,
    restart_lap_at: &AtomicU64,
    settings: AutomaticGcAtomsSettings,
    now: u64,
    deadline: tokio::time::Instant,
    lock_backup_cut: F,
) -> AutomaticGcAtomsStep
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Option<G>>,
{
    let atoms = runtime.atoms;
    if !settings.enabled() || !claim_automatic_gc_atoms_slot(last_step, &settings, now) {
        return AutomaticGcAtomsStep::Skipped;
    }

    // A terminal lap stays terminal until a lap interval elapses. Without this
    // the trigger would either re-walk the whole plane every step or never
    // refresh a measurement it already took.
    let scheduled_restart = restart_lap_at.load(Ordering::Relaxed);
    let restart_completed = scheduled_restart != 0 && now >= scheduled_restart;

    let lap = match run_probe_stage(&runtime, restart_lap_at, restart_completed, deadline).await {
        Ok(lap) => lap,
        Err(step) => return step,
    };

    // Audit first. It deletes nothing, so it needs no backup cut. A failure
    // is logged and never stops the byte-cap check or the delete pass below.
    audit_lap_zero_ref_candidates(atoms, &lap, &settings).await;

    // The candidate reaper deletes bodies and compacts the atom plane, so it
    // runs under the same backup cut as the delete pass. Take the cut only when
    // a delete may happen.
    let mut lock_cut = Some(lock_backup_cut);
    let mut cut: Option<Option<G>> = None;
    if candidate_reap_ready(atoms, &lap, &settings, now).await {
        if cut_is_held(&mut cut, &mut lock_cut).await {
            info!(
                target: "fold_db::gc_atoms",
                "automatic gc-atoms candidate reaper cut held"
            );
            return AutomaticGcAtomsStep::CutHeld;
        }
        reap_lap_zero_ref_candidates(atoms, &lap, &settings, now).await;
    }

    // Ask the byte cap before taking any new lock. A lap under the cap must
    // not stall a backup cut just to find out it has nothing to delete.
    if !automatic_gc_atoms_delete_armed(
        lap.unreferenced_bytes_approx,
        settings.max_unreferenced_bytes,
        false,
    ) {
        return step_below_cap(&lap, &settings, restart_lap_at, now);
    }

    // Packing lock held from here through every delete batch in this step. The
    // guard taken for the reaper, if any, is reused.
    if cut_is_held(&mut cut, &mut lock_cut).await {
        info!(
            target: "fold_db::gc_atoms",
            "automatic gc-atoms cut held"
        );
        return AutomaticGcAtomsStep::CutHeld;
    }

    delete_pass_step(atoms, &lap, &settings, restart_lap_at, now, deadline).await
}
