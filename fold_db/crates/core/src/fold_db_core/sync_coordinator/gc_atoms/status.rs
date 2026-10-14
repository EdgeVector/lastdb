//! Observable state of the automatic `gc-atoms` cadence.
//!
//! The cadence used to leave no trace outside its log. A lap that never
//! finished, a delete pass that refused every candidate and a loop that had
//! stopped all read the same from outside: no figure, no age, no reason. This
//! cell keeps the facts an operator needs and the node serves them in
//! `lastdb status` and `GET /api/status`.

use super::*;
use serde::{Deserialize, Serialize};

/// Longest error text kept in the status. The full text is in the log.
const MAX_ERROR_CHARS: usize = 240;

/// A snapshot of the automatic `gc-atoms` cadence. All times are unix seconds;
/// zero means never.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AutomaticGcAtomsStatus {
    /// The cadence task was started at boot.
    pub running: bool,
    /// A cloud sync engine is attached. The cadence runs either way: this is
    /// context for a held backup cut, never a reason to skip.
    #[serde(default)]
    pub sync_engine_attached: bool,
    pub max_unreferenced_bytes: u64,
    pub step_interval_secs: u64,
    /// Quiet time after boot before the cadence works.
    #[serde(default)]
    pub start_delay_secs: u64,
    /// Zero-epoch age a delete needs. 86400 unless the operator lengthened it.
    #[serde(default)]
    pub grace_window_secs: u64,
    /// The emergency flag is on: the cap-armed pass deletes with no grace
    /// window and no audit.
    #[serde(default)]
    pub grace_bypass: bool,
    /// What this node knows about peer data: `never`, `seen` or `unknown`
    /// (empty until the first step reads it). Automatic delete runs only on
    /// `never`, or with the operator override below.
    #[serde(default)]
    pub peer_apply_state: String,
    /// `LASTDB_GC_ATOMS_ALLOW_AFTER_PEER_APPLY` is set: automatic delete runs
    /// even after a peer mutation, so the atoms of a lost multi-device write
    /// can be deleted.
    #[serde(default)]
    pub allow_after_peer_apply: bool,
    /// Steps (since start) whose delete the peer-apply interlock held off.
    #[serde(default)]
    pub peer_interlock_skipped_steps: u64,
    pub started_at_unix_secs: u64,
    /// Newest timer tick, including ticks that skipped. A value far older than
    /// `step_interval_secs` means the cadence itself has stopped.
    pub last_tick_at_unix_secs: u64,
    /// A claimed step is running now and began at `step_started_at_unix_secs`.
    /// One probe call can take many seconds under write load, so a long step
    /// is busy, not stopped.
    #[serde(default)]
    pub step_in_progress: bool,
    #[serde(default)]
    pub step_started_at_unix_secs: u64,
    pub last_step_at_unix_secs: u64,
    pub steps: u64,
    /// What the last step did: `skipped`, `probe-advanced`, `below-threshold`,
    /// `deleted`, `cut-held`, `packing-lock-busy`, `atom-ref-reads-not-ready`
    /// or `failed`.
    pub last_outcome: String,
    /// Why the last step deleted nothing (see [`skip_reason`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_skip_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// When `last_error` was recorded. The error is never cleared: a healthy
    /// loop shows an old age, a failing one a young age.
    #[serde(default)]
    pub last_error_at_unix_secs: u64,
    /// Probe lap in progress (or the last finished one): generation and phase.
    pub lap_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lap_phase: Option<String>,
    pub lap_pages_completed: u64,
    /// Times a completed lap was restarted because it was too old to delete
    /// on, and why the newest one was.
    #[serde(default)]
    pub stale_lap_restarts: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_stale_lap_restart_reason: Option<String>,
    /// Last time the audit recorded a verdict, the reaper deleted, or a delete
    /// batch scanned the atom plane. A lap whose work progressed within one
    /// relap interval is not cut short by the interval rule.
    #[serde(default)]
    pub last_progress_at_unix_secs: u64,
    /// Longest delete batch, in milliseconds: how long the per-atom body and
    /// count locks of one cap-armed batch were held.
    #[serde(default)]
    pub slowest_delete_batch_ms: u64,
    /// Longest single probe call since the process started, in milliseconds.
    /// The step deadline is checked only between calls, so this is the most a
    /// step has overrun its budget by one call.
    #[serde(default)]
    pub slowest_probe_call_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lap_started_at: Option<String>,
    /// The last completed lap. The unreferenced figure is its measurement.
    pub last_lap_generation: u64,
    pub last_lap_completed_at_unix_secs: u64,
    pub last_lap_duration_secs: u64,
    pub last_lap_atoms_scanned: u64,
    pub last_lap_unreferenced_atoms: u64,
    pub last_lap_unreferenced_bytes: u64,
    /// Last bounded reap of zero-count candidates (24 h grace window).
    pub last_reap_at_unix_secs: u64,
    pub last_reap_examined: u64,
    pub last_reap_deleted: u64,
    pub last_reap_retained_before_grace: u64,
    pub last_reap_retained_without_audit: u64,
    pub last_reap_retained_audit_reachable: u64,
    #[serde(default)]
    pub last_reap_retained_stale_audit: u64,
    /// Candidates the last reap kept after an error (or an undecodable queue
    /// record). One bad candidate does not stop the page.
    #[serde(default)]
    pub last_reap_retained_error: u64,
    /// Candidates the newest step still owed an audit verdict. While nonzero,
    /// that step deleted nothing.
    #[serde(default)]
    pub last_audit_deferred: u64,
    /// Audits (since start) that could not record a verdict. Each candidate
    /// stays unaudited and the reaper keeps it.
    #[serde(default)]
    pub audit_failed: u64,
    /// Steps (since start) whose audit stopped at the step deadline.
    #[serde(default)]
    pub audit_cut_short_steps: u64,
    /// Newest delete of any kind by this cadence, and process-lifetime totals.
    pub last_delete_at_unix_secs: u64,
    pub last_delete_atoms: u64,
    pub last_delete_bytes_approx: u64,
    pub total_atoms_deleted: u64,
    pub total_bytes_deleted_approx: u64,
    /// The newest delete step that rechecked candidates: how many it
    /// rechecked, how many it deleted and why it kept the rest.
    pub last_pass_candidates_revalidated: u64,
    pub last_pass_candidates_retained: crate::db_operations::GcAtomsRevalidationTally,
    /// Generation of the last lap that wrote its one info line.
    #[serde(skip)]
    pub(super) lap_logged_generation: u64,
}

/// Shared handle: the cadence writes, `lastdb status` reads.
#[derive(Debug, Clone, Default)]
pub struct AutomaticGcAtomsStatusCell(Arc<Mutex<AutomaticGcAtomsStatus>>);

impl AutomaticGcAtomsStatusCell {
    pub fn snapshot(&self) -> AutomaticGcAtomsStatus {
        self.lock().clone()
    }

    pub(super) fn update(&self, change: impl FnOnce(&mut AutomaticGcAtomsStatus)) {
        change(&mut self.lock());
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, AutomaticGcAtomsStatus> {
        // A poisoned lock only means a writer panicked mid-update. The fields
        // are plain counters, so the last written values stay usable.
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Clip an error to the status budget on a character boundary.
pub(super) fn clipped_error(error: &str) -> String {
    error.chars().take(MAX_ERROR_CHARS).collect()
}

/// Unix seconds of an RFC 3339 time, or zero when it does not parse.
pub(super) fn unix_secs_of(text: &str) -> u64 {
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .and_then(|time| u64::try_from(time.timestamp()).ok())
        .unwrap_or(0)
}

/// The serde name of a probe phase, for example `tip_versions`.
fn phase_name(phase: crate::db_operations::AutomaticGcAtomsProbePhase) -> Option<String> {
    serde_json::to_value(phase)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
}

impl AutomaticGcAtomsStatusCell {
    /// Record the cadence start (or, with a zero cap, that it is disabled).
    pub(crate) fn mark_started(&self, settings: &AutomaticGcAtomsSettings, now: u64) {
        self.update(|status| {
            status.running = settings.enabled();
            status.max_unreferenced_bytes = settings.max_unreferenced_bytes;
            status.step_interval_secs = settings.step_interval_secs;
            status.start_delay_secs = settings.start_delay_secs;
            status.grace_window_secs = settings.grace_window_secs;
            status.grace_bypass = settings.bypass_grace;
            status.allow_after_peer_apply = settings.allow_after_peer_apply;
            status.started_at_unix_secs = now;
            if !settings.enabled() {
                status.last_outcome = "skipped".to_string();
                status.last_skip_reason = Some(skip_reason::DISABLED.to_string());
            }
        });
    }

    /// Publish the peer-apply state a step just read. Returns `true` when the
    /// state differs from the last one published (the caller logs the change).
    pub(super) fn record_peer_state(&self, state: &str, allow: bool) -> bool {
        let mut changed = false;
        self.update(|status| {
            changed = status.peer_apply_state != state;
            status.peer_apply_state = state.to_string();
            status.allow_after_peer_apply = allow;
        });
        changed
    }

    /// A claimed step has begun.
    pub(super) fn mark_step_started(&self, now: u64) {
        self.update(|status| {
            status.step_in_progress = true;
            status.step_started_at_unix_secs = now;
        });
    }

    /// Keep the longest probe call.
    pub(super) fn record_probe_call(&self, elapsed: std::time::Duration) {
        let ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        self.update(|status| status.slowest_probe_call_ms = status.slowest_probe_call_ms.max(ms));
    }

    /// Keep the newest error text. The full error is in the log.
    pub(super) fn record_error(&self, error: &str) {
        self.update(|status| {
            status.last_error = Some(clipped_error(error));
            status.last_error_at_unix_secs = crate::clock::unix_secs();
        });
    }

    /// Where the probe stands. Read once per claimed step.
    pub(super) fn record_probe_progress(
        &self,
        checkpoint: &crate::db_operations::AutomaticGcAtomsProbeCheckpoint,
    ) {
        self.update(|status| {
            status.lap_generation = checkpoint.generation;
            status.lap_phase = phase_name(checkpoint.phase);
            status.lap_pages_completed = checkpoint.pages_completed;
            status.lap_started_at =
                (!checkpoint.started_at.is_empty()).then(|| checkpoint.started_at.clone());
        });
    }

    /// Keep the lap measurement. The first sight of each lap writes the one
    /// `info` line for it; later steps over the same finished lap stay quiet.
    pub(super) fn record_lap(
        &self,
        lap: &AutomaticGcAtomsProbeResult,
        settings: &AutomaticGcAtomsSettings,
    ) {
        let started = unix_secs_of(&lap.started_at);
        let completed = unix_secs_of(&lap.completed_at);
        let first_sight = {
            let mut status = self.lock();
            status.last_lap_generation = lap.generation;
            status.last_lap_completed_at_unix_secs = completed;
            status.last_lap_duration_secs = completed.saturating_sub(started);
            status.last_lap_atoms_scanned = lap.atoms_scanned;
            status.last_lap_unreferenced_atoms = lap.unreferenced_atoms;
            status.last_lap_unreferenced_bytes = lap.unreferenced_bytes_approx;
            let first = status.lap_logged_generation != lap.generation;
            status.lap_logged_generation = lap.generation;
            first
        };
        if first_sight {
            info!(
                target: "fold_db::gc_atoms",
                generation = lap.generation,
                started_at = %lap.started_at,
                completed_at = %lap.completed_at,
                duration_secs = completed.saturating_sub(started),
                atoms_scanned = lap.atoms_scanned,
                atoms_referenced = lap.atoms_referenced,
                unreferenced_atoms = lap.unreferenced_atoms,
                unreferenced_bytes = lap.unreferenced_bytes_approx,
                atoms_skipped_recent = lap.atoms_skipped_recent,
                threshold_bytes = settings.max_unreferenced_bytes,
                armed = automatic_gc_atoms_delete_armed(
                    lap.unreferenced_bytes_approx,
                    settings.max_unreferenced_bytes,
                    false
                ),
                "automatic gc-atoms lap complete"
            );
        }
    }

    /// When the cadence started (this process's boot), in unix seconds.
    pub(super) fn started_at_unix_secs(&self) -> u64 {
        self.lock().started_at_unix_secs
    }

    /// A completed lap was restarted before it could authorize a delete.
    pub(super) fn record_stale_lap_restart(&self, reason: &'static str) {
        self.update(|status| {
            status.stale_lap_restarts = status.stale_lap_restarts.saturating_add(1);
            status.last_stale_lap_restart_reason = Some(reason.to_string());
        });
    }

    /// The audit, the reaper or the delete pass made progress just now. The
    /// stamp is the wall clock at the call, not the start of the step: a step
    /// can run for many seconds, and the interval rule compares this stamp
    /// with the start of a later step.
    pub(super) fn record_progress(&self) {
        self.update(|status| status.last_progress_at_unix_secs = crate::clock::unix_secs());
    }

    /// True when the lap's reap or delete work progressed within `secs`.
    pub(super) fn progressed_within(&self, now: u64, secs: u64) -> bool {
        let at = self.lock().last_progress_at_unix_secs;
        at != 0 && now.saturating_sub(at) < secs
    }

    /// Keep the longest delete batch (lock hold) in milliseconds.
    pub(super) fn record_delete_batch(&self, millis: u64) {
        self.update(|status| {
            status.slowest_delete_batch_ms = status.slowest_delete_batch_ms.max(millis);
        });
    }

    /// True while the quiet time after boot has not passed.
    pub(super) fn in_start_delay(&self, now: u64, delay_secs: u64) -> bool {
        now < self.lock().started_at_unix_secs.saturating_add(delay_secs)
    }

    pub(super) fn record_audit(&self, progress: &super::reap::AuditProgress) {
        self.update(|status| {
            if progress.audited > 0 {
                status.last_progress_at_unix_secs = crate::clock::unix_secs();
            }
            status.last_audit_deferred = progress.deferred;
            status.audit_failed = status.audit_failed.saturating_add(progress.failed);
            if progress.deferred > 0 {
                status.audit_cut_short_steps = status.audit_cut_short_steps.saturating_add(1);
            }
        });
    }

    pub(super) fn record_reap(&self, now: u64, report: &AtomGcReapReport) {
        self.update(|status| {
            status.last_reap_at_unix_secs = now;
            status.last_reap_examined = report.candidates_examined;
            status.last_reap_deleted = report.atoms_deleted;
            status.last_reap_retained_before_grace = report.retained_before_grace;
            status.last_reap_retained_without_audit = report.retained_without_audit;
            status.last_reap_retained_audit_reachable = report.retained_audit_reachable;
            status.last_reap_retained_stale_audit = report.retained_stale_audit;
            status.last_reap_retained_error = report.retained_error;
            if report.atoms_deleted > 0 {
                status.last_progress_at_unix_secs = crate::clock::unix_secs();
            }
            status.note_delete(now, report.atoms_deleted, report.bytes_deleted_approx);
        });
    }
}

impl AutomaticGcAtomsStatus {
    /// Count a delete that removed at least one atom.
    pub(super) fn note_delete(&mut self, now: u64, atoms: u64, bytes: u64) {
        if atoms == 0 {
            return;
        }
        self.last_delete_at_unix_secs = now;
        self.last_delete_atoms = atoms;
        self.last_delete_bytes_approx = bytes;
        self.total_atoms_deleted = self.total_atoms_deleted.saturating_add(atoms);
        self.total_bytes_deleted_approx = self.total_bytes_deleted_approx.saturating_add(bytes);
    }
}

impl SyncCoordinator {
    /// The automatic `gc-atoms` cadence status for `lastdb status`.
    pub fn automatic_gc_atoms_status(&self) -> AutomaticGcAtomsStatus {
        AutomaticGcAtomsStatus {
            sync_engine_attached: self.is_enabled(),
            ..self.gc_atoms_status.snapshot()
        }
    }
}
