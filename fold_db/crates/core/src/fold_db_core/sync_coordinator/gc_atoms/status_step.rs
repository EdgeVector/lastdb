//! Folding one finished step into the status.

use super::*;

impl AutomaticGcAtomsStatusCell {
    /// Fold one finished step into the status.
    pub(super) fn record_step(&self, now: u64, step: &AutomaticGcAtomsStep) {
        use AutomaticGcAtomsStep as Step;
        self.update(|status| {
            status.last_tick_at_unix_secs = now;
            status.step_in_progress = false;
            if let Step::Skipped(reason) = step {
                status.last_outcome = "skipped".to_string();
                status.last_skip_reason = Some((*reason).to_string());
                return;
            }
            status.steps = status.steps.saturating_add(1);
            status.last_step_at_unix_secs = now;
            let (outcome, reason) = match step {
                Step::Skipped(_) => unreachable!("handled above"),
                Step::ProbeAdvanced => ("probe-advanced", Some(skip_reason::PROBE_RUNNING)),
                Step::BelowThreshold { .. } => {
                    ("below-threshold", Some(skip_reason::BELOW_THRESHOLD))
                }
                Step::CutHeld => ("cut-held", Some(skip_reason::CUT_HELD)),
                Step::LockBusy => ("packing-lock-busy", Some(skip_reason::PACKING_LOCK_BUSY)),
                Step::AuditIncomplete { .. } => (
                    skip_reason::AUDIT_INCOMPLETE,
                    Some(skip_reason::AUDIT_INCOMPLETE),
                ),
                Step::ReadsNotReady => (
                    skip_reason::READS_NOT_READY,
                    Some(skip_reason::READS_NOT_READY),
                ),
                Step::PeerInterlock(reason) => {
                    status.peer_interlock_skipped_steps =
                        status.peer_interlock_skipped_steps.saturating_add(1);
                    // Reads as `skipped: peer-applied` / `skipped: peer-state-unknown`.
                    ("skipped", Some(*reason))
                }
                Step::Failed => ("failed", Some(skip_reason::FAILED)),
                Step::Deleted {
                    atoms_deleted,
                    bytes_freed_approx,
                    drained,
                    candidates_revalidated,
                    retained,
                } => {
                    status.note_delete(now, *atoms_deleted, *bytes_freed_approx);
                    if *candidates_revalidated > 0 {
                        status.last_pass_candidates_revalidated = *candidates_revalidated;
                        status.last_pass_candidates_retained = *retained;
                    }
                    if *atoms_deleted > 0 {
                        ("deleted", None)
                    } else if *drained {
                        ("no-eligible-candidates", Some(skip_reason::NO_CANDIDATES))
                    } else {
                        (
                            skip_reason::DELETE_PASS_RUNNING,
                            Some(skip_reason::DELETE_PASS_RUNNING),
                        )
                    }
                }
            };
            status.last_outcome = outcome.to_string();
            status.last_skip_reason = reason.map(str::to_string);
        });
    }
}
