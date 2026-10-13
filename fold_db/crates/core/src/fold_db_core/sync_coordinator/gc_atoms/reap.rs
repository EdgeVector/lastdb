//! Audit and reap of zero-count atom candidates inside one automatic step.

use super::*;
use crate::db_operations::AtomGcCandidate;

/// True when this generation already recorded its verdict for the candidate's
/// current zero epoch. Recording it again writes the same two rows.
fn audited_by_generation(candidate: &AtomGcCandidate, audit_id: &str) -> bool {
    candidate.audit.as_ref().is_some_and(|audit| {
        audit.audit_id == audit_id && audit.zero_since_unix_nanos == candidate.zero_since_unix_nanos
    })
}

/// How far one step got through the audit.
pub(super) struct AuditProgress {
    /// Candidates this step recorded a verdict for.
    pub(super) audited: u64,
    /// Candidates whose verdict could not be recorded. They stay unaudited and
    /// the reaper keeps them; one failing audit does not stop the rest.
    pub(super) failed: u64,
    /// Candidates the step deadline left without an attempt. While any remain,
    /// nothing may be deleted: the reaper trusts only a verdict from the
    /// current lap, so a delete now would rest on an older lap's view.
    pub(super) deferred: u64,
}

/// Record this generation's verdict for the oldest zero-count candidates.
///
/// A candidate created after the probe began stays unaudited. The trace did
/// not observe that zero epoch, so absence from its marker set proves nothing.
///
/// Every step over a finished lap lands here. A candidate this generation
/// already judged is skipped: the old loop wrote two durable rows per
/// candidate on every step, 400 candidates took longer than the whole step
/// budget, and the delete pass behind it never started. The loop stops at
/// `deadline` after at least one audit, and reports what it left for the next
/// step.
async fn audit_zero_ref_candidates(
    atoms: &AtomStore,
    lap: &AutomaticGcAtomsProbeResult,
    audit_id: &str,
    max_candidates: usize,
    deadline: tokio::time::Instant,
) -> Result<AuditProgress, crate::schema::SchemaError> {
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
        .filter(|candidate| {
            candidate.zero_since_unix_nanos <= started_at
                && !audited_by_generation(candidate, audit_id)
        })
        .collect::<Vec<_>>();
    let uuids = candidates
        .iter()
        .map(|candidate| candidate.atom_uuid.clone())
        .collect::<Vec<_>>();
    let referenced = atoms
        .automatic_gc_atoms_referenced_candidates(None, lap.generation, &uuids)
        .await?;
    let total = candidates.len() as u64;
    let (mut audited, mut failed) = (0u64, 0u64);
    for candidate in candidates {
        match atoms
            .record_atom_gc_audit(
                &candidate.atom_uuid,
                candidate.zero_since_unix_nanos,
                audit_id,
                referenced.contains(&candidate.atom_uuid),
                None,
            )
            .await
        {
            Ok(_) => audited += 1,
            Err(error) => {
                failed += 1;
                warn!(
                    target: "fold_db::gc_atoms",
                    atom = %candidate.atom_uuid,
                    %error,
                    "automatic gc-atoms audit failed; candidate kept unaudited"
                );
            }
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
    }
    Ok(AuditProgress {
        audited,
        failed,
        deferred: total - audited - failed,
    })
}

/// Options for the grace-window reaper of one finished lap. Only a verdict that
/// carries this lap's id may clear a delete.
fn lap_reap_options(
    lap: &AutomaticGcAtomsProbeResult,
    settings: &AutomaticGcAtomsSettings,
    now: u64,
    deadline: Option<tokio::time::Instant>,
) -> AtomGcReapOptions {
    AtomGcReapOptions {
        now_unix_nanos: now.saturating_mul(1_000_000_000),
        grace_window: std::time::Duration::from_secs(settings.grace_window_secs),
        max_candidates: settings.reap_candidate_cap,
        dry_run: false,
        // The plane compactor returns the bytes on its overhang trigger. A full
        // rewrite of `atoms` per reaping step is not a step-sized cost.
        compact_after_delete: false,
        required_audit_id: Some(crate::db_operations::automatic_gc_audit_id(lap.generation)),
        deadline,
        allow_after_peer_apply: settings.allow_after_peer_apply,
    }
}

/// Audit the zero-count candidates a finished lap proved. It deletes nothing,
/// so it needs no backup cut.
///
/// Returns the number of audit-eligible candidates left for a later step. A
/// nonzero value means the reap must not run this step: the reaper deletes
/// only on a verdict from this lap, and a cut-short audit leaves candidates
/// without one. `Err` means the audit failed and was logged and recorded.
pub(super) async fn audit_lap_zero_ref_candidates(
    runtime: &AutomaticGcAtomsRuntime<'_>,
    lap: &AutomaticGcAtomsProbeResult,
    settings: &AutomaticGcAtomsSettings,
) -> Result<u64, ()> {
    let deadline = tokio::time::Instant::now()
        + tokio::time::Duration::from_secs(settings.step_budget_secs.max(1));
    let audit_id = crate::db_operations::automatic_gc_audit_id(lap.generation);
    match audit_zero_ref_candidates(
        runtime.atoms,
        lap,
        &audit_id,
        settings.reap_candidate_cap,
        deadline,
    )
    .await
    {
        Ok(progress) => {
            runtime.status.record_audit(&progress);
            if progress.deferred > 0 {
                debug!(
                    target: "fold_db::gc_atoms",
                    generation = lap.generation,
                    audited = progress.audited,
                    deferred = progress.deferred,
                    "automatic gc-atoms audit stopped at the step deadline; no reap this step"
                );
            }
            Ok(progress.deferred)
        }
        Err(error) => {
            warn!(
                target: "fold_db::gc_atoms",
                generation = lap.generation,
                %error,
                "automatic gc-atoms candidate audit failed"
            );
            runtime.status.record_error(&error.to_string());
            Err(())
        }
    }
}

/// True when the oldest candidate page holds a delete that could run now. The
/// caller takes the backup cut only then.
pub(super) async fn candidate_reap_ready(
    runtime: &AutomaticGcAtomsRuntime<'_>,
    lap: &AutomaticGcAtomsProbeResult,
    settings: &AutomaticGcAtomsSettings,
    now: u64,
) -> bool {
    runtime
        .atoms
        .atom_gc_reap_ready(None, &lap_reap_options(lap, settings, now, None))
        .await
        .unwrap_or(false)
}

/// Reap the audited zero-count candidates of a finished lap, under the backup
/// cut the caller holds. The budget starts here, after any wait for the cut.
/// `Err` means the reaper failed and was logged and recorded.
pub(super) async fn reap_lap_zero_ref_candidates(
    runtime: &AutomaticGcAtomsRuntime<'_>,
    lap: &AutomaticGcAtomsProbeResult,
    settings: &AutomaticGcAtomsSettings,
    now: u64,
) -> Result<(), ()> {
    let deadline = tokio::time::Instant::now()
        + tokio::time::Duration::from_secs(settings.step_budget_secs.max(1));
    let options = lap_reap_options(lap, settings, now, Some(deadline));
    match runtime
        .atoms
        .reap_audited_atom_gc_candidates(None, options)
        .await
    {
        Ok(report) => {
            log_reap(lap.generation, &report);
            runtime.status.record_reap(now, &report);
            Ok(())
        }
        Err(error) => {
            warn!(
                target: "fold_db::gc_atoms",
                generation = lap.generation,
                %error,
                "automatic gc-atoms candidate reaper failed"
            );
            runtime.status.record_error(&error.to_string());
            Err(())
        }
    }
}

fn log_reap(generation: u64, report: &AtomGcReapReport) {
    if report.atoms_deleted > 0 {
        info!(
            target: "fold_db::gc_atoms",
            generation,
            candidates_examined = report.candidates_examined,
            atoms_deleted = report.atoms_deleted,
            bytes_deleted_approx = report.bytes_deleted_approx,
            truncated = report.truncated,
            "automatic gc-atoms reaped audited zero-count candidates"
        );
    }
    if report.retained_error > 0 {
        warn!(
            target: "fold_db::gc_atoms",
            generation,
            retained_error = report.retained_error,
            "automatic gc-atoms kept candidates after errors"
        );
    }
}
