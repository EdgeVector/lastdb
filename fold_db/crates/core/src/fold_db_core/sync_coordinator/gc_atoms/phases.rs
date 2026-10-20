//! Probe-side phases of one automatic `gc-atoms` step: slot claim and the
//! resumable probe. The audit, reap and delete halves are in `reap` and
//! `delete_pass`.

use super::*;

/// Claim the step slot. `false` means another wake owns this interval.
pub(super) fn claim_automatic_gc_atoms_slot(
    last_step: &AtomicU64,
    settings: &AutomaticGcAtomsSettings,
    now: u64,
) -> bool {
    let previous = last_step.load(Ordering::Relaxed);
    if !automatic_gc_atoms_due(previous, now, settings.step_interval_secs) {
        debug!(
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
        debug!(
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
/// phase. `Err` carries the text of a pin-log read failure that was logged.
pub(super) async fn pin_log_reference_page_for(
    engine: Option<&SyncEngine>,
    checkpoint: &crate::db_operations::AutomaticGcAtomsProbeCheckpoint,
) -> Result<Option<crate::db_operations::AutomaticGcAtomsPinLogReferencePage>, String> {
    if checkpoint.version != 3
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
                Err(error)
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
                runtime.status.record_error(&error.to_string());
                return ProbeStepOutcome::Failed;
            }
        };
        if let Some(engine) = engine {
            if checkpoint.version != 3
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
        let pin_log_reference_page = match pin_log_reference_page_for(engine, &checkpoint).await {
            Ok(page) => page,
            Err(error) => {
                runtime.status.record_error(&error);
                return ProbeStepOutcome::Failed;
            }
        };
        let options = AutomaticGcAtomsProbeOptions {
            restart_completed,
            pin_log_reference_page,
            ..Default::default()
        };
        let call_started = std::time::Instant::now();
        let call = atoms.probe_orphan_atoms_from_checkpoint(options).await;
        runtime.status.record_probe_call(call_started.elapsed());
        match call {
            Ok(report) => {
                if report.phase
                    == crate::db_operations::AutomaticGcAtomsProbePhase::PinLogReferences
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
                runtime.status.record_error(&error.to_string());
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

impl AutomaticGcAtomsRuntime<'_> {
    /// Why the completed lap may no longer authorize a delete, `None` when it
    /// may (or when no lap has completed), `Err` when the checkpoint could not
    /// be read. A read error is never a pass: the caller deletes nothing.
    ///
    /// A lap's verdicts say what the node looked like while the lap ran. Four
    /// things make that view too old to delete on:
    /// - the lap finished before this process started, so no writer marked a
    ///   reference while the process was down, and a boot may follow an
    ///   offline tool that installed rows;
    /// - the lap claims to have finished after `now` (the clock moved back
    ///   since), so its age cannot be trusted at all; and
    /// - the lap is older than the maximum lap age (the grace window, never less
    ///   than the relap interval). Nothing waives this one: a pass that is
    ///   still making progress restarts the lap and deletes again only on the
    ///   new lap's audit; and
    /// - the lap is older than the relap interval, so the same gap has had a
    ///   whole interval to open (the scheduled restart only covers a lap that
    ///   some step has scheduled).
    ///
    /// Either way the lap restarts first, and only the new lap's audit counts.
    /// The caller decides whether the relap-interval reason is waived while a
    /// reap or delete pass is still making progress.
    pub(super) async fn stale_completed_lap(
        &self,
        settings: &AutomaticGcAtomsSettings,
        now: u64,
    ) -> Result<Option<&'static str>, String> {
        let checkpoint = self
            .atoms
            .automatic_gc_atoms_probe_checkpoint(None)
            .await
            .map_err(|error| error.to_string())?;
        if checkpoint.version != 2
            || checkpoint.phase != crate::db_operations::AutomaticGcAtomsProbePhase::Complete
        {
            return Ok(None);
        }
        let completed = checkpoint.completed_at.as_deref().map_or(0, unix_secs_of);
        Ok(if completed > now {
            Some(skip_reason::LAP_FROM_FUTURE)
        } else if completed < self.status.started_at_unix_secs() {
            Some(skip_reason::LAP_BEFORE_BOOT)
        } else if completed.saturating_add(settings.max_lap_age_secs()) <= now {
            Some(skip_reason::LAP_OLDER_THAN_MAX_AGE)
        } else if completed.saturating_add(settings.relap_interval_secs) <= now {
            Some(skip_reason::LAP_OLDER_THAN_RELAP)
        } else {
            None
        })
    }

    /// Decide whether this step restarts the completed lap, and say so when the
    /// lap is stale. `Err` means the lap's age could not be judged: delete
    /// nothing (the failure is logged and recorded).
    ///
    /// A terminal lap stays terminal until a lap interval elapses. Without this
    /// the trigger would either re-walk the whole plane every step or never
    /// refresh a measurement it already took.
    pub(super) async fn restart_completed_lap(
        &self,
        settings: &AutomaticGcAtomsSettings,
        restart_lap_at: &AtomicU64,
        now: u64,
    ) -> Result<bool, ()> {
        let scheduled_restart = restart_lap_at.load(Ordering::Relaxed);
        let scheduled_due = scheduled_restart != 0 && now >= scheduled_restart;
        // A lap whose reap or delete pass progressed within one relap interval
        // is not cut by the interval rule: at a large scale the pass needs
        // several intervals, and a cut would reset its cursor and starve the
        // late keys. The writers keep marking for the lap's generation for as
        // long as it is active, and every delete still passes the grace, audit
        // and edge gates. A lap from before this boot, from the future, or
        // older than the maximum lap age is always restarted, progress or not.
        let working = self
            .status
            .progressed_within(now, settings.relap_interval_secs);
        let stale = match self.stale_completed_lap(settings, now).await {
            Ok(stale) => stale.filter(|reason| {
                *reason != skip_reason::LAP_OLDER_THAN_RELAP || !(scheduled_due || working)
            }),
            Err(error) => {
                warn!(
                    target: "fold_db::gc_atoms",
                    %error,
                    "automatic gc-atoms could not read the probe checkpoint to judge the lap's age"
                );
                self.status.record_error(&error);
                return Err(());
            }
        };
        if let Some(reason) = stale {
            self.status.record_stale_lap_restart(reason);
            info!(
                target: "fold_db::gc_atoms",
                reason,
                "automatic gc-atoms restarts a completed lap that is too old to delete on"
            );
        }
        Ok(stale.is_some() || scheduled_due)
    }

    /// Publish where the probe stands. One keyed read per claimed step.
    pub(super) async fn record_probe_progress(&self) {
        if let Ok(checkpoint) = self.atoms.automatic_gc_atoms_probe_checkpoint(None).await {
            self.status.record_probe_progress(&checkpoint);
        }
    }
}

/// Run one probe step. `Err` carries the finished step when the lap is not
/// complete yet (probe failed or only progressed).
pub(super) async fn run_probe_stage(
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
            debug!(
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
