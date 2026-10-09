// lint:file-size-ok verbatim move from the original module; one long function remains, splitting it is separate work
//! Automatic `gc-atoms` trigger: settings, step state machine and runtime.

use super::*;

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

/// Bind one completed reachability generation to exact zero-count epochs,
/// then run the bounded grace-window reaper.
///
/// A candidate created after the probe began stays unaudited. The trace did
/// not observe that zero epoch, so absence from its marker set proves nothing.
pub(super) async fn audit_and_reap_zero_ref_candidates(
    atoms: &AtomStore,
    lap: &AutomaticGcAtomsProbeResult,
    max_candidates: usize,
    now_unix_nanos: u64,
    grace_window: std::time::Duration,
    compact_after_delete: bool,
) -> Result<AtomGcReapReport, crate::schema::SchemaError> {
    let started_at = chrono::DateTime::parse_from_rfc3339(&lap.started_at)
        .map_err(|error| {
            crate::schema::SchemaError::InvalidData(format!(
                "invalid automatic gc-atoms candidate audit start time: {error}"
            ))
        })?
        .timestamp_nanos_opt()
        .and_then(|nanos| u64::try_from(nanos).ok())
        .ok_or_else(|| {
            crate::schema::SchemaError::InvalidData(
                "automatic gc-atoms candidate audit start time is out of range".to_string(),
            )
        })?;
    let candidates = atoms
        .list_atom_gc_candidates(None, max_candidates)
        .await?
        .candidates
        .into_iter()
        .filter(|candidate| candidate.zero_since_unix_nanos <= started_at)
        .collect::<Vec<_>>();
    let uuids = candidates
        .iter()
        .map(|candidate| candidate.atom_uuid.clone())
        .collect::<Vec<_>>();
    let referenced = atoms
        .automatic_gc_atoms_referenced_candidates(None, lap.generation, &uuids)
        .await?;
    let audit_id = format!("automatic-gc-atoms:{}", lap.generation);
    for candidate in candidates {
        match atoms
            .record_atom_gc_audit(
                &candidate.atom_uuid,
                candidate.zero_since_unix_nanos,
                &audit_id,
                referenced.contains(&candidate.atom_uuid),
                None,
            )
            .await?
        {
            AtomGcAuditDecision::Cleared
            | AtomGcAuditDecision::StillReachable
            | AtomGcAuditDecision::StaleEpoch
            | AtomGcAuditDecision::PendingReference => {}
        }
    }
    atoms
        .reap_audited_atom_gc_candidates(
            None,
            AtomGcReapOptions {
                now_unix_nanos,
                grace_window,
                max_candidates,
                dry_run: false,
                compact_after_delete,
            },
        )
        .await
}

// lint:fn-size-ok moved verbatim from the original module; splitting it is a separate change
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
    let engine = runtime.engine;
    if !settings.enabled() {
        return AutomaticGcAtomsStep::Skipped;
    }
    let previous = last_step.load(Ordering::Relaxed);
    if !automatic_gc_atoms_due(previous, now, settings.step_interval_secs) {
        info!(
            target: "fold_db::gc_atoms",
            reason = "not-due",
            last_step = previous,
            interval_secs = settings.step_interval_secs,
            "automatic gc-atoms Skipped"
        );
        return AutomaticGcAtomsStep::Skipped;
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
        return AutomaticGcAtomsStep::Skipped;
    }

    // A terminal lap stays terminal until a lap interval elapses. Without this
    // the trigger would either re-walk the whole plane every step or never
    // refresh a measurement it already took.
    let scheduled_restart = restart_lap_at.load(Ordering::Relaxed);
    let mut restart_completed = scheduled_restart != 0 && now >= scheduled_restart;

    let mut lap = None;
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
                return AutomaticGcAtomsStep::Failed;
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
        let pin_log_reference_page = if checkpoint.version == 2
            && checkpoint.phase
                == crate::db_operations::AutomaticGcAtomsProbePhase::PinLogReferences
        {
            match engine {
                Some(engine) => match engine
                    .pin_log
                    .pending_pin_log_atom_uuids_page(
                        checkpoint.pin_log_after_key.as_deref(),
                        crate::db_operations::AtomStore::GC_PLANE_SCAN_PAGE,
                    )
                    .await
                {
                    Ok(page) => Some(page),
                    Err(error) => {
                        warn!(
                            target: "fold_db::gc_atoms",
                            %error,
                            "automatic gc-atoms pin-log reference read failed"
                        );
                        return AutomaticGcAtomsStep::Failed;
                    }
                },
                None => Some(crate::db_operations::AutomaticGcAtomsPinLogReferencePage {
                    scan_complete: true,
                    ..Default::default()
                }),
            }
        } else {
            None
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
                    lap = Some(result);
                    break;
                }
            }
            Err(error) => {
                warn!(
                    target: "fold_db::gc_atoms",
                    %error,
                    "automatic gc-atoms probe failed"
                );
                return AutomaticGcAtomsStep::Failed;
            }
        }
    }

    let Some(lap) = lap else {
        info!(
            target: "fold_db::gc_atoms",
            generation = last_generation,
            phase = ?last_phase,
            rows_scanned = last_rows_scanned,
            "automatic gc-atoms probe progressed"
        );
        return AutomaticGcAtomsStep::ProbeAdvanced;
    };

    let now_unix_nanos = now.saturating_mul(1_000_000_000);
    match audit_and_reap_zero_ref_candidates(
        atoms,
        &lap,
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
        }
        Ok(_) => {}
        Err(error) => {
            warn!(
                target: "fold_db::gc_atoms",
                generation = lap.generation,
                %error,
                "automatic gc-atoms candidate audit or reaper failed"
            );
            return AutomaticGcAtomsStep::Failed;
        }
    }

    // Ask the byte cap before taking any lock. A lap under the cap must not
    // stall a backup cut just to find out it has nothing to delete.
    if !automatic_gc_atoms_delete_armed(
        lap.unreferenced_bytes_approx,
        settings.max_unreferenced_bytes,
        false,
    ) {
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
        return AutomaticGcAtomsStep::BelowThreshold {
            unreferenced_bytes: lap.unreferenced_bytes_approx,
        };
    }

    // Packing lock held from here through every delete batch in this step.
    let cut_guard = lock_backup_cut().await;
    if !automatic_gc_atoms_delete_armed(
        lap.unreferenced_bytes_approx,
        settings.max_unreferenced_bytes,
        cut_guard.is_none(),
    ) {
        info!(
            target: "fold_db::gc_atoms",
            "automatic gc-atoms cut held"
        );
        return AutomaticGcAtomsStep::CutHeld;
    }
    let _cut_guard = cut_guard;

    let mut atoms_deleted = 0u64;
    let mut bytes_freed_approx = 0u64;
    let mut drained = false;
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
                atoms_deleted = atoms_deleted.saturating_add(report.atoms_deleted_this_call);
                bytes_freed_approx =
                    bytes_freed_approx.saturating_add(report.bytes_freed_approx_this_call);
                if report.result.is_some() {
                    drained = true;
                    break;
                }
            }
            Err(error) => {
                warn!(
                    target: "fold_db::gc_atoms",
                    %error,
                    "automatic gc-atoms delete failed"
                );
                return AutomaticGcAtomsStep::Failed;
            }
        }
    }

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
