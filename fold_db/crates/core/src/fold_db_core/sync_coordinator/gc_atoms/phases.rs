//! Phases of one automatic `gc-atoms` step: slot claim, probe, reap, delete.

use super::*;

/// Claim the step slot. `false` means another wake owns this interval.
pub(super) fn claim_automatic_gc_atoms_slot(
    last_step: &AtomicU64,
    settings: &AutomaticGcAtomsSettings,
    now: u64,
) -> bool {
    let previous = last_step.load(Ordering::Relaxed);
    if !automatic_gc_atoms_due(previous, now, settings.step_interval_secs) {
        info!(
            target: "fold_db::gc_atoms",
            reason = "not-due",
            last_step = previous,
            interval_secs = settings.step_interval_secs,
            "automatic gc-atoms Skipped"
        );
        return false;
    }
    // Claim the slot before any measurement, so two wakes in one interval
    // cannot both walk the plane.
    if last_step
        .compare_exchange(previous, now.max(1), Ordering::SeqCst, Ordering::Relaxed)
        .is_err()
    {
        info!(
            target: "fold_db::gc_atoms",
            reason = "slot-taken",
            "automatic gc-atoms Skipped"
        );
        return false;
    }
    true
}

/// Where the resumable probe stopped after this step's call budget.
pub(super) enum ProbeStepOutcome {
    Failed,
    Progressed {
        generation: u64,
        phase: Option<crate::db_operations::AutomaticGcAtomsProbePhase>,
        rows_scanned: u64,
    },
    LapComplete(crate::db_operations::AutomaticGcAtomsProbeResult),
}

/// Pin-log references the engine owes the probe for the checkpoint's current
/// phase. `Err(())` means the pin-log read failed and was logged.
pub(super) async fn pin_log_reference_page_for(
    engine: Option<&SyncEngine>,
    checkpoint: &crate::db_operations::AutomaticGcAtomsProbeCheckpoint,
) -> Result<Option<crate::db_operations::AutomaticGcAtomsPinLogReferencePage>, ()> {
    if checkpoint.version != 2
        || checkpoint.phase != crate::db_operations::AutomaticGcAtomsProbePhase::PinLogReferences
    {
        return Ok(None);
    }
    match engine {
        Some(engine) => match engine
            .pin_log
            .pending_pin_log_atom_uuids_page(
                checkpoint.pin_log_after_key.as_deref(),
                crate::db_operations::AtomStore::GC_PLANE_SCAN_PAGE,
            )
            .await
        {
            Ok(page) => Ok(Some(page)),
            Err(error) => {
                warn!(
                    target: "fold_db::gc_atoms",
                    %error,
                    "automatic gc-atoms pin-log reference read failed"
                );
                Err(())
            }
        },
        None => Ok(Some(
            crate::db_operations::AutomaticGcAtomsPinLogReferencePage {
                scan_complete: true,
                ..Default::default()
            },
        )),
    }
}

/// Advance the resumable probe by whole pages until it finishes a lap, the
/// call budget ends, or the deadline passes.
pub(super) async fn advance_gc_atoms_probe(
    runtime: &AutomaticGcAtomsRuntime<'_>,
    restart_lap_at: &AtomicU64,
    mut restart_completed: bool,
    deadline: tokio::time::Instant,
) -> ProbeStepOutcome {
    let atoms = runtime.atoms;
    let engine = runtime.engine;
    let mut last_generation = 0u64;
    let mut last_phase = None;
    let mut last_rows_scanned = 0u64;
    for _ in 0..GC_ATOMS_MAX_CALLS_PER_STEP {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        let checkpoint = match atoms.automatic_gc_atoms_probe_checkpoint(None).await {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                warn!(target: "fold_db::gc_atoms", %error, "automatic gc-atoms checkpoint read failed");
                return ProbeStepOutcome::Failed;
            }
        };
        if let Some(engine) = engine {
            if checkpoint.version != 2
                || checkpoint.phase
                    == crate::db_operations::AutomaticGcAtomsProbePhase::ClearReferenceMarkers
                || (checkpoint.phase == crate::db_operations::AutomaticGcAtomsProbePhase::Complete
                    && restart_completed)
            {
                engine
                    .automatic_gc_pin_log_activation_pending
                    .store(true, Ordering::Release);
            }
        }
        let Ok(pin_log_reference_page) = pin_log_reference_page_for(engine, &checkpoint).await
        else {
            return ProbeStepOutcome::Failed;
        };
        let options = AutomaticGcAtomsProbeOptions {
            restart_completed,
            pin_log_reference_page,
            ..Default::default()
        };
        match atoms.probe_orphan_atoms_from_checkpoint(options).await {
            Ok(report) => {
                if report.phase
                    == crate::db_operations::AutomaticGcAtomsProbePhase::ClearTipVersionSkipMarkers
                    && atoms.automatic_gc_atoms_generation() == 0
                {
                    if let Some(engine) = engine {
                        let _activation = engine.automatic_gc_pin_log_barrier.lock().await;
                        atoms.set_automatic_gc_atoms_generation(report.generation);
                        engine
                            .automatic_gc_pin_log_activation_pending
                            .store(false, Ordering::Release);
                    } else {
                        atoms.set_automatic_gc_atoms_generation(report.generation);
                    }
                }
                last_generation = report.generation;
                last_phase = Some(report.phase);
                last_rows_scanned = report.rows_scanned_this_call;
                if restart_completed {
                    restart_lap_at.store(0, Ordering::Relaxed);
                    restart_completed = false;
                }
                if let Some(result) = report.result {
                    return ProbeStepOutcome::LapComplete(result);
                }
            }
            Err(error) => {
                warn!(
                    target: "fold_db::gc_atoms",
                    %error,
                    "automatic gc-atoms probe failed"
                );
                return ProbeStepOutcome::Failed;
            }
        }
    }
    ProbeStepOutcome::Progressed {
        generation: last_generation,
        phase: last_phase,
        rows_scanned: last_rows_scanned,
    }
}

/// Audit and reap the zero-count candidates a finished lap proved. `false`
/// means the audit or reaper failed and was logged.
pub(super) async fn reap_lap_zero_ref_candidates(
    atoms: &AtomStore,
    lap: &crate::db_operations::AutomaticGcAtomsProbeResult,
    settings: &AutomaticGcAtomsSettings,
    now: u64,
) -> bool {
    let now_unix_nanos = now.saturating_mul(1_000_000_000);
    match audit_and_reap_zero_ref_candidates(
        atoms,
        lap,
        settings.delete_candidate_cap,
        now_unix_nanos,
        DEFAULT_ATOM_GC_GRACE_WINDOW,
        true,
    )
    .await
    {
        Ok(report) if report.atoms_deleted > 0 => {
            info!(
                target: "fold_db::gc_atoms",
                generation = lap.generation,
                candidates_examined = report.candidates_examined,
                atoms_deleted = report.atoms_deleted,
                bytes_deleted_approx = report.bytes_deleted_approx,
                truncated = report.truncated,
                "automatic gc-atoms reaped audited zero-count candidates"
            );
            true
        }
        Ok(_) => true,
        Err(error) => {
            warn!(
                target: "fold_db::gc_atoms",
                generation = lap.generation,
                %error,
                "automatic gc-atoms candidate audit or reaper failed"
            );
            false
        }
    }
}

/// Totals from the bounded delete pass of one step.
pub(super) struct DeletePassOutcome {
    pub(super) atoms_deleted: u64,
    pub(super) bytes_freed_approx: u64,
    pub(super) drained: bool,
}

/// Run delete batches under the already-held packing lock. `None` means a
/// batch failed and was logged.
pub(super) async fn run_gc_atoms_delete_pass(
    atoms: &AtomStore,
    settings: &AutomaticGcAtomsSettings,
    deadline: tokio::time::Instant,
) -> Option<DeletePassOutcome> {
    let mut outcome = DeletePassOutcome {
        atoms_deleted: 0,
        bytes_freed_approx: 0,
        drained: false,
    };
    for _ in 0..GC_ATOMS_MAX_CALLS_PER_STEP {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        let options = AutomaticGcAtomsDeleteOptions {
            candidate_cap: settings.delete_candidate_cap,
            ..Default::default()
        };
        match atoms.delete_orphan_atoms_from_probe(options).await {
            Ok(report) => {
                outcome.atoms_deleted = outcome
                    .atoms_deleted
                    .saturating_add(report.atoms_deleted_this_call);
                outcome.bytes_freed_approx = outcome
                    .bytes_freed_approx
                    .saturating_add(report.bytes_freed_approx_this_call);
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
                return None;
            }
        }
    }
    Some(outcome)
}
