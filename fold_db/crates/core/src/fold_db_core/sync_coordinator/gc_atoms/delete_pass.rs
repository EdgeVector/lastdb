//! The delete half of an automatic `gc-atoms` step: cap check, edge-read gate and
//! the bounded delete batches.

use super::*;

/// Totals from the bounded delete pass of one step.
pub(super) struct DeletePassOutcome {
    pub(super) atoms_deleted: u64,
    pub(super) bytes_freed_approx: u64,
    pub(super) drained: bool,
    /// Orphan candidates rechecked under their locks, and why the pass kept
    /// the ones it did not delete.
    pub(super) candidates_revalidated: u64,
    pub(super) retained: crate::db_operations::GcAtomsRevalidationTally,
}

/// Run delete batches under the already-held packing lock. `None` means a
/// batch failed and was logged.
pub(super) async fn run_gc_atoms_delete_pass(
    runtime: &AutomaticGcAtomsRuntime<'_>,
    settings: &AutomaticGcAtomsSettings,
    lap_generation: u64,
    now: u64,
    deadline: tokio::time::Instant,
) -> Option<DeletePassOutcome> {
    let atoms = runtime.atoms;
    let mut outcome = DeletePassOutcome {
        atoms_deleted: 0,
        bytes_freed_approx: 0,
        drained: false,
        candidates_revalidated: 0,
        retained: crate::db_operations::GcAtomsRevalidationTally::default(),
    };
    for _ in 0..GC_ATOMS_MAX_CALLS_PER_STEP {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        let options = match settings.grace_gate(lap_generation, now) {
            Some(gate) => AutomaticGcAtomsDeleteOptions::gated(gate, settings.delete_candidate_cap),
            None => AutomaticGcAtomsDeleteOptions::ungated_emergency(settings.delete_candidate_cap),
        }
        .with_allow_after_peer_apply(settings.allow_after_peer_apply);
        let result = atoms.delete_orphan_atoms_from_probe(options).await;
        if let Ok(report) = &result {
            runtime
                .status
                .record_delete_batch(report.locks_held_ms_this_call);
            if report.rows_scanned_this_call > 0 || report.atoms_deleted_this_call > 0 {
                runtime.status.record_progress();
            }
        }
        match result {
            Ok(report) => {
                outcome.atoms_deleted = outcome
                    .atoms_deleted
                    .saturating_add(report.atoms_deleted_this_call);
                outcome.bytes_freed_approx = outcome
                    .bytes_freed_approx
                    .saturating_add(report.bytes_freed_approx_this_call);
                outcome.candidates_revalidated = outcome
                    .candidates_revalidated
                    .saturating_add(report.candidates_revalidated_this_call);
                outcome.retained.add(&report.candidates_retained_this_call);
                if report.result.is_some() {
                    outcome.drained = true;
                    break;
                }
            }
            Err(error) => {
                warn!(
                    target: "fold_db::gc_atoms",
                    %error,
                    "automatic gc-atoms delete failed"
                );
                runtime.status.record_error(&error.to_string());
                return None;
            }
        }
    }
    Some(outcome)
}

/// The lap measured less than the byte cap: schedule the next measurement.
pub(super) fn step_below_cap(
    lap: &AutomaticGcAtomsProbeResult,
    settings: &AutomaticGcAtomsSettings,
    restart_lap_at: &AtomicU64,
) -> AutomaticGcAtomsStep {
    debug!(
        target: "fold_db::gc_atoms",
        generation = lap.generation,
        unreferenced_bytes = lap.unreferenced_bytes_approx,
        threshold_bytes = settings.max_unreferenced_bytes,
        "automatic gc-atoms below cap"
    );
    schedule_next_lap(restart_lap_at, settings, unix_secs_of(&lap.completed_at));
    AutomaticGcAtomsStep::BelowThreshold {
        unreferenced_bytes: lap.unreferenced_bytes_approx,
    }
}

/// Schedule the next lap one lap interval after `measured_at`, unless a restart
/// is already scheduled.
///
/// Every step over a finished lap lands here. The old code stored the deadline
/// again on each step, so the deadline moved one interval ahead of `now` on
/// every step and never arrived: the first lap that completed was the last lap
/// the cadence ever ran, in every process lifetime. A lap that finished long
/// before this process started (`measured_at` far in the past) restarts at the
/// next step.
pub(super) fn schedule_next_lap(
    restart_lap_at: &AtomicU64,
    settings: &AutomaticGcAtomsSettings,
    measured_at: u64,
) {
    let at = measured_at
        .saturating_add(settings.relap_interval_secs)
        .max(1);
    let _ = restart_lap_at.compare_exchange(0, at, Ordering::Relaxed, Ordering::Relaxed);
}

/// The delete pass checks every candidate against the reverse-edge plane and
/// keeps all of them while the plane is not proven complete. Say so once
/// instead of walking the atom plane to delete nothing.
pub(super) async fn delete_blocked_by_edge_reads(
    runtime: &AutomaticGcAtomsRuntime<'_>,
    lap: &AutomaticGcAtomsProbeResult,
    settings: &AutomaticGcAtomsSettings,
    restart_lap_at: &AtomicU64,
) -> Option<AutomaticGcAtomsStep> {
    match runtime.atoms.atom_ref_v2_reads_ready(None).await {
        Ok(true) => None,
        Ok(false) => {
            let repeated = runtime.status.snapshot().last_outcome == skip_reason::READS_NOT_READY;
            if repeated {
                debug!(target: "fold_db::gc_atoms", "automatic gc-atoms still blocked: atom reverse-edge reads are not ready");
            } else {
                warn!(
                    target: "fold_db::gc_atoms",
                    generation = lap.generation,
                    unreferenced_bytes = lap.unreferenced_bytes_approx,
                    "automatic gc-atoms is armed but atom reverse-edge reads are not ready; \
                     no body is deleted until both aref:v2:m completion markers exist"
                );
            }
            schedule_next_lap(restart_lap_at, settings, unix_secs_of(&lap.completed_at));
            Some(AutomaticGcAtomsStep::ReadsNotReady)
        }
        Err(error) => {
            warn!(
                target: "fold_db::gc_atoms",
                %error,
                "automatic gc-atoms could not read the atom reverse-edge readiness markers"
            );
            runtime.status.record_error(&error.to_string());
            Some(AutomaticGcAtomsStep::Failed)
        }
    }
}

/// Run the delete batches under the packing lock the caller already holds.
pub(super) async fn delete_pass_step(
    runtime: &AutomaticGcAtomsRuntime<'_>,
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
        candidates_revalidated,
        retained,
    }) = run_gc_atoms_delete_pass(runtime, settings, lap.generation, now, deadline).await
    else {
        return AutomaticGcAtomsStep::Failed;
    };

    if drained {
        // The generation is drained. Measure again after a lap interval.
        schedule_next_lap(restart_lap_at, settings, now);
    }

    if atoms_deleted > 0 {
        info!(
            target: "fold_db::gc_atoms",
            generation = lap.generation,
            threshold_bytes = settings.max_unreferenced_bytes,
            reported_unreferenced_bytes = lap.unreferenced_bytes_approx,
            atoms_deleted,
            bytes_freed_approx,
            drained,
            candidates_revalidated,
            "automatic gc-atoms step returned bytes"
        );
    }
    AutomaticGcAtomsStep::Deleted {
        atoms_deleted,
        bytes_freed_approx,
        drained,
        candidates_revalidated,
        retained,
    }
}
