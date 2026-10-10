//! Support types and pure decision helpers for the sync cycle: scoped-download
//! inputs, the phase timer, mutation-log upload reports and coalescing.

use super::*;

/// Inputs for one scoped-download phase of [`SyncEngine::do_sync`].
///
/// Bundled so [`SyncEngine::run_scoped_downloads`] stays under the workspace
/// `clippy::too_many_arguments` deny threshold.
pub(super) struct ScopedDownloadPass<'a> {
    pub(super) targets: &'a [SyncTarget],
    pub(super) scoped_idxs: &'a [usize],
    pub(super) scoped_total: usize,
    pub(super) propagate_errors: bool,
    pub(super) downloaded: &'a mut u64,
    pub(super) proven_prefixes: &'a mut std::collections::HashSet<String>,
    pub(super) first_transfer_error: &'a mut Option<SyncError>,
}

/// Wall time per `do_sync` phase. Each [`Self::mark`] records the time since
/// the previous mark under a phase name; dropping the timer logs one line, so
/// an early return still reports the phases that ran.
pub(crate) struct SyncPhaseTimer {
    started: std::time::Instant,
    last: std::time::Instant,
    phases: Vec<(&'static str, u64)>,
}

impl SyncPhaseTimer {
    pub(crate) fn start() -> Self {
        let now = std::time::Instant::now();
        Self {
            started: now,
            last: now,
            phases: Vec::new(),
        }
    }

    pub(crate) fn mark(&mut self, phase: &'static str) {
        let now = std::time::Instant::now();
        let ms = now.duration_since(self.last).as_millis() as u64;
        self.last = now;
        self.phases.push((phase, ms));
    }

    /// `name=ms` pairs in phase order, for the cycle log line.
    pub(crate) fn summary(&self) -> String {
        self.phases
            .iter()
            .map(|(name, ms)| format!("{name}={ms}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// The phase with the largest wall time, if any phase ran.
    pub(crate) fn slowest(&self) -> Option<(&'static str, u64)> {
        self.phases.iter().copied().max_by_key(|(_, ms)| *ms)
    }
}

impl Drop for SyncPhaseTimer {
    fn drop(&mut self) {
        let (slowest_phase, slowest_ms) = self.slowest().unwrap_or(("none", 0));
        tracing::info!(
            target: "fold_db::sync::memory",
            total_ms = self.started.elapsed().as_millis() as u64,
            slowest_phase,
            slowest_ms,
            phases_ms = %self.summary(),
            "do_sync phase timings"
        );
    }
}

/// Aggregated outcome of one catch-up upload pass (zero or more bounded
/// cycles on one target).
#[derive(Debug, Clone, Default)]
pub(crate) struct MutationLogUploadPassReport {
    pub batches: usize,
    pub segments_uploaded: usize,
    pub records_considered: usize,
    pub bytes_uploaded: u64,
    pub records_quarantined: usize,
    pub published_frontier_after: u64,
    pub upload_backlog_after: u64,
    /// Peak PUT fan-out across published batches in this pass.
    pub put_concurrency: usize,
    pub last: MutationLogUploadReport,
}

/// A later upload batch failed after earlier batches in the same pass
/// already published. `pass` is the aggregate of those published batches.
#[derive(Debug, Clone, Default)]
pub(crate) struct MutationLogUploadPassFailure {
    pub pass: MutationLogUploadPassReport,
    pub error: String,
}

impl std::fmt::Display for MutationLogUploadPassFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.error)
    }
}

/// Continue looping bounded upload cycles inside one `do_sync` while the
/// publisher still has backlog and the catch-up time box has not elapsed.
///
/// `budget == 0` is one batch (legacy). A bounded read that did not reach
/// the end also needs another batch, even when the runtime frontier is empty.
pub(crate) fn should_continue_mutation_log_upload_batches(
    segments_uploaded: usize,
    upload_backlog_after: u64,
    wake_threshold_ns: u64,
    records_considered_is_lower_bound: bool,
    elapsed: std::time::Duration,
    budget: std::time::Duration,
) -> bool {
    segments_uploaded > 0
        && wake_threshold_ns > 0
        && (upload_backlog_after >= wake_threshold_ns || records_considered_is_lower_bound)
        && !budget.is_zero()
        && elapsed < budget
}

/// Skip peer apply while this node is draining unpublished local log, except
/// for the first attempt this process and once the min interval has elapsed.
///
/// `min_interval_ms == 0` never skips. A zero backlog (or a disabled wake
/// threshold) always runs peer apply.
pub(crate) fn should_skip_peer_apply_for_drain(
    upload_backlog_ns: u64,
    backlog_threshold_ns: u64,
    last_peer_apply_ms: u64,
    now_ms: u64,
    min_interval_ms: u64,
) -> bool {
    if min_interval_ms == 0 {
        return false;
    }
    if backlog_threshold_ns == 0 || upload_backlog_ns < backlog_threshold_ns {
        return false;
    }
    if last_peer_apply_ms == 0 {
        return false;
    }
    now_ms.saturating_sub(last_peer_apply_ms) < min_interval_ms
}

/// Stamp last-peer-apply after Ok and after non-Auth errors.
///
/// Auth must not stamp: `sync` refreshes and retries `do_sync`, and a stamp
/// would skip peer apply for the whole min interval while drain backlog
/// remains.
pub(crate) fn should_stamp_last_peer_apply_ms(peer_apply_err: Option<&SyncError>) -> bool {
    !matches!(peer_apply_err, Some(SyncError::Auth(_)))
}

/// Error that fails the peer-apply cycle after applied work is recorded.
///
/// Auth always fails the cycle, even when another target listed or applied
/// segments, so the drain skip clock is not stamped. Other errors fail the
/// cycle only when no target listed a segment.
pub(crate) fn peer_apply_cycle_terminal_error(
    segments_considered: usize,
    auth_err: Option<SyncError>,
    other_err: Option<SyncError>,
) -> Option<SyncError> {
    if let Some(e) = auth_err {
        return Some(e);
    }
    if segments_considered == 0 {
        return other_err;
    }
    None
}

/// Group mutation-log uploads so one write does not seal its own file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MutationLogCoalesceDecision {
    UploadNow,
    Defer {
        retry_after_ms: u64,
        hold_since_ms: u64,
    },
}

/// Decide whether this cycle seals the mutation log or waits.
///
/// Both windows at 0 upload now (tests and [`SyncConfig::default`]).
/// `last_append_ms == 0` means this process has not appended, so a restart
/// drain uploads now. Otherwise the hold starts at `now_ms`. Upload when
/// the quiet window has elapsed, or when the hold age reaches the cap.
/// A quiet-only stream that keeps appending defers. A max-only hold uploads
/// when the age is due.
pub(crate) fn decide_mutation_log_coalesce(
    now_ms: u64,
    last_append_ms: u64,
    hold_since_ms: u64,
    quiet_ms: u64,
    max_hold_ms: u64,
) -> MutationLogCoalesceDecision {
    if quiet_ms == 0 && max_hold_ms == 0 {
        return MutationLogCoalesceDecision::UploadNow;
    }
    if last_append_ms == 0 {
        return MutationLogCoalesceDecision::UploadNow;
    }
    let hold_since_ms = if hold_since_ms == 0 {
        now_ms
    } else {
        hold_since_ms
    };
    let since_append = now_ms.saturating_sub(last_append_ms);
    let hold_age = now_ms.saturating_sub(hold_since_ms);
    let quiet_due = quiet_ms > 0 && since_append >= quiet_ms;
    let max_due = max_hold_ms > 0 && hold_age >= max_hold_ms;
    if quiet_due || max_due {
        return MutationLogCoalesceDecision::UploadNow;
    }
    let until_quiet = if quiet_ms > 0 {
        quiet_ms.saturating_sub(since_append)
    } else {
        u64::MAX
    };
    let until_max = if max_hold_ms > 0 {
        max_hold_ms.saturating_sub(hold_age)
    } else {
        u64::MAX
    };
    MutationLogCoalesceDecision::Defer {
        retry_after_ms: until_quiet.min(until_max).max(1),
        hold_since_ms,
    }
}
