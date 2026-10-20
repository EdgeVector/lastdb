//! The TTL sweep after failed passes: wait, then try again.
//!
//! A pass in which no schema succeeded counts as failed. The first
//! `MAX_DRAIN_ATTEMPTS - 1` failures cost nothing extra: the next sampler tick
//! starts the next pass. From the third failure in a row the sweep waits before
//! the next pass, and the wait doubles with each further failure up to a cap.
//! It never stops for the life of the process: expiry that stopped until a
//! restart would let a retained series grow without bound while the node looks
//! healthy. A pass that succeeds ends the wait and resets the count.

use super::sampler_state::MAX_DRAIN_ATTEMPTS;
use super::*;

/// The wait after the third failed pass in a row.
const TTL_BACKOFF_BASE: Duration = Duration::from_secs(300);
/// The longest wait between two passes after repeated failure.
pub(super) const TTL_BACKOFF_CAP: Duration = Duration::from_secs(3600);

/// The wait that follows `failures` failed passes in a row. `None` for the
/// first failures, which wait for the next sampler tick only.
fn backoff_after(failures: u32) -> Option<Duration> {
    let doublings = failures.checked_sub(MAX_DRAIN_ATTEMPTS)?;
    Some(
        TTL_BACKOFF_BASE
            .saturating_mul(1u32 << doublings.min(10))
            .min(TTL_BACKOFF_CAP),
    )
}

impl SamplerRuntimeState {
    /// Claim the right to run one pass. `false` means a pass is running, or the
    /// sweep waits after repeated failure.
    pub(crate) fn begin_ttl_sweep(&self) -> bool {
        let mut ttl = self
            .ttl
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A completed empty pass only describes the last observed cutoff. A
        // later write can age past that cutoff, or an operator can add a new
        // policy, without restarting the daemon. Keep the single-flight guard,
        // but let the next sampler tick check the bounded policy and key paths.
        if ttl.in_flight || ttl.retry_at.is_some_and(|at| Instant::now() < at) {
            return false;
        }
        ttl.retry_at = None;
        ttl.next_attempt_unix_secs = None;
        ttl.settled = false;
        ttl.in_flight = true;
        true
    }

    /// Count a failed pass. From the third in a row, start a wait and say so
    /// once in the log. The status shows `failures` and `next_attempt_unix_secs`.
    pub(crate) fn record_ttl_sweep_failure(&self) {
        let mut ttl = self
            .ttl
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ttl.failures = ttl.failures.saturating_add(1);
        let Some(wait) = backoff_after(ttl.failures) else {
            return;
        };
        ttl.retry_at = Some(Instant::now() + wait);
        ttl.next_attempt_unix_secs = Some(unix_secs().saturating_add(wait.as_secs()));
        tracing::warn!(
            target: "lastdbd::ttl_sweep",
            failed_passes_in_a_row = ttl.failures,
            wait_secs = wait.as_secs(),
            cap_secs = TTL_BACKOFF_CAP.as_secs(),
            "ttl sweep waits before the next pass; the wait doubles with each failed pass up to the cap, and the sweep goes on after it"
        );
    }

    /// Count a pass in which a schema succeeded: the wait and the failure count
    /// end.
    pub(crate) fn mark_ttl_sweep_pass(&self, removed: usize, now: u64) {
        let mut ttl = self
            .ttl
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ttl.passes = ttl.passes.saturating_add(1);
        ttl.rows_reaped = ttl
            .rows_reaped
            .saturating_add(u64::try_from(removed).unwrap_or(u64::MAX));
        ttl.last_sweep_unix_secs = Some(now);
        ttl.failures = 0;
        ttl.retry_at = None;
        ttl.next_attempt_unix_secs = None;
        if removed == 0 {
            ttl.settled = true;
        }
    }
}
