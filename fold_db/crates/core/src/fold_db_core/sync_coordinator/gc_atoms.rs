//! Automatic `gc-atoms` trigger: settings, step state machine and runtime.

use super::*;

mod delete_pass;
mod peer_interlock;
mod phases;
mod reap;
mod settings;
mod skip_reason;
mod status;
mod status_step;

use delete_pass::*;
use phases::*;
use reap::*;
pub(super) use settings::*;
use status::unix_secs_of;
pub use status::AutomaticGcAtomsStatus;
pub(super) use status::AutomaticGcAtomsStatusCell;

/// What one automatic step did. Returned so a caller can assert the decision
/// instead of reading log lines, and recorded in the status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AutomaticGcAtomsStep {
    /// Disabled, not due yet, or another step claimed this slot.
    Skipped(&'static str),
    /// The probe lap is still walking planes.
    ProbeAdvanced,
    /// A lap completed under the byte cap. Nothing is deleted.
    BelowThreshold { unreferenced_bytes: u64 },
    /// A backup cut is held, so no body may be deleted this step.
    CutHeld,
    /// Another task held the packing lock for the whole wait.
    LockBusy,
    /// The lap is over the cap but the reverse-edge plane is not proven
    /// complete, so the delete pass would refuse every candidate.
    ReadsNotReady,
    /// The audit stopped at the step deadline. No atom is deleted until the
    /// current lap has judged every candidate in the page.
    AuditIncomplete { deferred: u64 },
    /// Bounded delete batches ran.
    Deleted {
        atoms_deleted: u64,
        bytes_freed_approx: u64,
        drained: bool,
        candidates_revalidated: u64,
        retained: crate::db_operations::GcAtomsRevalidationTally,
    },
    /// The peer-apply interlock holds delete off: this node applied a peer
    /// mutation, or cannot prove it never did. The probe still ran.
    PeerInterlock(&'static str),
    /// The probe or the delete pass returned an error.
    Failed,
}

/// Result of asking for the packing lock before a delete pass.
pub(super) enum CutLock<G> {
    Acquired(G),
    CutHeld,
    Busy,
}

/// Longest wait for the packing lock. A longer hold is reported, never waited
/// out: a wait with no bound looked exactly like a stopped cadence.
pub(super) const GC_ATOMS_PACKING_LOCK_WAIT_SECS: u64 = 60;

/// Take the engine's packing lock for one delete pass, waiting at most `wait`.
pub(super) async fn lock_engine_backup_cut(
    engine: &SyncEngine,
    wait: std::time::Duration,
) -> CutLock<
    tokio::sync::MutexGuard<'_, Option<crate::sync::engine::backup_uploader::BackupPublishTarget>>,
> {
    match tokio::time::timeout(wait, engine.lock_backup_publish_target()).await {
        Err(_) => CutLock::Busy,
        Ok(guard) if guard.is_some() => CutLock::CutHeld,
        Ok(guard) => CutLock::Acquired(guard),
    }
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

/// What one automatic step works on: the atom store, the optional sync engine
/// and the status cell that records the outcome.
pub(super) struct AutomaticGcAtomsRuntime<'a> {
    pub(super) atoms: &'a AtomStore,
    pub(super) engine: Option<&'a SyncEngine>,
    pub(super) status: &'a AutomaticGcAtomsStatusCell,
}

/// Advance the automatic `gc-atoms` state machine by one bounded step.
///
/// The probe writes only checkpoints and keyed membership markers, so it runs
/// without the packing lock and keeps making progress while a cut drains. Only
/// the delete phase takes that lock, and it holds it across the whole batch
/// sequence: a boolean pre-check would race a new cut and remove bodies its
/// manifest already names.
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
    Fut: std::future::Future<Output = CutLock<G>>,
{
    let step = if !settings.enabled() {
        AutomaticGcAtomsStep::Skipped(skip_reason::DISABLED)
    } else if runtime
        .status
        .in_start_delay(now, settings.start_delay_secs)
    {
        AutomaticGcAtomsStep::Skipped(skip_reason::START_DELAY)
    } else if !claim_automatic_gc_atoms_slot(last_step, &settings, now) {
        AutomaticGcAtomsStep::Skipped(skip_reason::NOT_DUE)
    } else {
        runtime.status.mark_step_started(now);
        let step = run_claimed_step(
            &runtime,
            restart_lap_at,
            &settings,
            now,
            deadline,
            lock_backup_cut,
        )
        .await;
        runtime.record_probe_progress().await;
        step
    };
    runtime.status.record_step(now, &step);
    step
}

/// Hold the packing lock for the rest of the step. The lock closure runs at
/// most once; a later call finds the guard in `held` and returns at once. `Err`
/// carries the finished step when the cut is held or the wait ran out.
async fn hold_backup_cut<F, Fut, G>(
    runtime: &AutomaticGcAtomsRuntime<'_>,
    lock: &mut Option<F>,
    held: &mut Option<G>,
) -> Result<(), AutomaticGcAtomsStep>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = CutLock<G>>,
{
    let Some(lock) = lock.take() else {
        return Ok(());
    };
    match lock().await {
        CutLock::Acquired(guard) => {
            *held = Some(guard);
            Ok(())
        }
        CutLock::CutHeld => {
            // One line when the hold begins; the status carries the rest.
            if runtime.status.snapshot().last_skip_reason.as_deref() != Some(skip_reason::CUT_HELD)
            {
                info!(target: "fold_db::gc_atoms", "automatic gc-atoms cut held");
            }
            Err(AutomaticGcAtomsStep::CutHeld)
        }
        CutLock::Busy => {
            warn!(
                target: "fold_db::gc_atoms",
                wait_secs = GC_ATOMS_PACKING_LOCK_WAIT_SECS,
                "automatic gc-atoms could not take the packing lock; another task holds it"
            );
            Err(AutomaticGcAtomsStep::LockBusy)
        }
    }
}

/// One claimed step: probe, reap, then the delete pass when the lap is over
/// the cap and nothing blocks it.
async fn run_claimed_step<F, Fut, G>(
    runtime: &AutomaticGcAtomsRuntime<'_>,
    restart_lap_at: &AtomicU64,
    settings: &AutomaticGcAtomsSettings,
    now: u64,
    deadline: tokio::time::Instant,
    lock_backup_cut: F,
) -> AutomaticGcAtomsStep
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = CutLock<G>>,
{
    let peer_state = runtime.peer_apply_gate(settings).await;
    let Ok(restart_completed) = runtime
        .restart_completed_lap(settings, restart_lap_at, now)
        .await
    else {
        return AutomaticGcAtomsStep::Failed;
    };

    let lap = match run_probe_stage(runtime, restart_lap_at, restart_completed, deadline).await {
        Ok(lap) => lap,
        Err(step) => return step,
    };
    runtime.status.record_lap(&lap, settings);

    // Peer-apply interlock: delete only on a node that never applied a peer
    // mutation (or with the operator override). The probe and the figure above
    // keep running; nothing below deletes, audits or takes a lock.
    if let Some(reason) = peer_state.interlock_reason {
        schedule_next_lap(restart_lap_at, settings, unix_secs_of(&lap.completed_at));
        return AutomaticGcAtomsStep::PeerInterlock(reason);
    }

    // Audit first. It deletes nothing, so it needs no backup cut.
    match audit_lap_zero_ref_candidates(runtime, &lap, settings).await {
        Err(()) => return AutomaticGcAtomsStep::Failed,
        Ok(0) => {}
        Ok(deferred) => return AutomaticGcAtomsStep::AuditIncomplete { deferred },
    }

    // The candidate reaper deletes bodies, so it runs under the same backup cut
    // as the delete pass. Take the cut only when a delete may happen; the guard
    // is reused by the delete pass below.
    let mut lock_cut = Some(lock_backup_cut);
    let mut cut: Option<G> = None;
    if candidate_reap_ready(runtime, &lap, settings, now).await {
        if let Err(step) = hold_backup_cut(runtime, &mut lock_cut, &mut cut).await {
            return step;
        }
        if reap_lap_zero_ref_candidates(runtime, &lap, settings, now)
            .await
            .is_err()
        {
            return AutomaticGcAtomsStep::Failed;
        }
    }

    // Ask the byte cap before taking any new lock. A lap under the cap must not
    // stall a backup cut just to find out it has nothing to delete.
    if !automatic_gc_atoms_delete_armed(
        lap.unreferenced_bytes_approx,
        settings.max_unreferenced_bytes,
        false,
    ) {
        return step_below_cap(&lap, settings, restart_lap_at);
    }
    if let Some(step) = delete_blocked_by_edge_reads(runtime, &lap, settings, restart_lap_at).await
    {
        return step;
    }

    // Packing lock held from here through every delete batch in this step. The
    // guard taken for the reaper, if any, is reused.
    if let Err(step) = hold_backup_cut(runtime, &mut lock_cut, &mut cut).await {
        return step;
    }
    let _cut_guard = cut;

    // The budget starts after the lock wait: a wait of the whole budget used
    // to leave the pass no time to make one call.
    let deadline = tokio::time::Instant::now()
        + tokio::time::Duration::from_secs(settings.step_budget_secs.max(1));
    delete_pass_step(runtime, &lap, settings, restart_lap_at, now, deadline).await
}
