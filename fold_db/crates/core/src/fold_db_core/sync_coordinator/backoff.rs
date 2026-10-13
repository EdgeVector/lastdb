use super::*;

/// Cap on the exponential backoff between sync cycles while the engine is in
/// [`SyncState::Offline`]. Ten minutes is a balance between responsiveness
/// (user opens the lid, expects sync within a reasonable time) and not
/// hammering Exemem / the device battery during a sustained outage.
pub(super) const MAX_OFFLINE_BACKOFF: tokio::time::Duration = tokio::time::Duration::from_secs(600);

/// Cap on the exponential backoff between sync cycles while authentication is
/// failing. Auth failures are usually operator-actionable (revoked key, missing
/// locked credential store), so probe much less often than transient network
/// outages after the first few failures.
pub(super) const MAX_AUTH_BACKOFF: tokio::time::Duration = tokio::time::Duration::from_secs(3600);

/// Cap on backoff after replay reaches a corrupt/undecryptable log entry.
///
/// Unlike transient network failures, a replay failure usually cannot make
/// progress until cloud state or local key material changes. Keep the cursor
/// pinned before the bad entry, but do not let write wakeups turn the same
/// corrupt object into a continuous retry loop.
pub(super) const MAX_REPLAY_BACKOFF: tokio::time::Duration = tokio::time::Duration::from_secs(3600);

/// Minimum spacing between forced syncs (`force_sync` / `stop`) WHILE a replay
/// blocker is armed. A UI/API "sync now" must not be able to hot-retrigger
/// replay of the same corrupt/undecryptable object faster than the failure
/// backoff would — that is how a poisoned prefix turned into a busy-loop before
/// PR #304's backoff, and force_sync bypasses that backoff entirely.
pub(super) const MIN_FORCED_SYNC_UNDER_BLOCKER: std::time::Duration =
    std::time::Duration::from_secs(30);

/// Seconds between local plane self-compaction probes.
///
/// A probe is a per-plane directory stat walk plus the per-plane rate limits
/// each trigger already enforces, so the cadence can be far shorter than the
/// probe intervals themselves without adding rewrites: a plane whose own
/// interval has not elapsed returns immediately. 60 s matches the tightest
/// useful reaction time on a busy local node while staying an order of
/// magnitude above the walk's cost.
///
/// `LASTDB_PLANE_COMPACTION_INTERVAL_SECS=0` disables the cadence and hands
/// reclaim back to `lastdb db compact --collection <plane> --execute`.
pub(super) const DEFAULT_PLANE_COMPACTION_INTERVAL_SECS: u64 = 60;

/// Read the local plane-compaction cadence once, at start.
pub(super) fn plane_compaction_interval_secs() -> u64 {
    env_flag::var_or(
        "LASTDB_PLANE_COMPACTION_INTERVAL_SECS",
        DEFAULT_PLANE_COMPACTION_INTERVAL_SECS,
    )
}

pub(super) fn capture_reexport_drain_delay(
    stats: &crate::sync::engine::CaptureTickStats,
    lap_progress: &mut bool,
) -> tokio::time::Duration {
    let made_progress =
        stats.puts_staged > 0 || stats.deletes_staged > 0 || stats.poison_dropped > 0;
    *lap_progress |= made_progress;
    if stats.scan_more {
        return tokio::time::Duration::from_secs(2);
    }
    let delay = if *lap_progress {
        tokio::time::Duration::from_secs(2)
    } else {
        // A complete lap that could not retire one marker must not retry the
        // same failed rows every two seconds.
        tokio::time::Duration::from_secs(30)
    };
    *lap_progress = false;
    delay
}

/// Compute the next inter-cycle sleep duration after a sync failure.
///
/// EVERY failure class now backs off exponentially — only a *successful* cycle
/// (handled by the caller) resets the delay to `base`. This matters because the
/// previous "reset to base unless Offline" rule let whole classes of failure
/// retry at the base interval forever:
/// - An S3/R2 `503` (or any `Storage`/`Crypto`/`SequenceGap`) only reached
///   `SyncState::Offline` for `Network` errors, so a sustained R2 outage
///   retried a full cycle every base interval indefinitely.
///
/// The per-class cap differs by how operator-actionable the failure is:
/// - `Auth` / `Banned`: `auth_max` (revoked/locked credential — probe rarely).
/// - `QuotaExceeded`: normal offline cap; a paid upgrade or quota increase
///   should be observed on the next ordinary retry window, not the auth cap.
/// - `CorruptEntry` / `KeyProofFailed`: [`MAX_REPLAY_BACKOFF`] (a poison object
///   or a wrong local key — cannot make progress until cloud state or key
///   material changes; do not hammer the same object).
/// - Everything else (network, storage, crypto, S3 5xx, sequence gap):
///   `offline_max`.
pub(super) fn next_backoff_on_failure(
    current: tokio::time::Duration,
    _base: tokio::time::Duration,
    offline_max: tokio::time::Duration,
    auth_max: tokio::time::Duration,
    error: &SyncError,
) -> tokio::time::Duration {
    let cap = match error {
        SyncError::Auth(_) | SyncError::Banned(_) => auth_max,
        SyncError::CorruptEntry { .. } | SyncError::KeyProofFailed { .. } => MAX_REPLAY_BACKOFF,
        _ => offline_max,
    };
    current.saturating_mul(2).min(cap)
}

/// Whether local-write wakeups should be suppressed for one cycle after this
/// failure. Now `true` for ALL failures: since every failure arms an
/// exponential backoff (see [`next_backoff_on_failure`]), a steady stream of
/// local writes must not collapse that backoff back to the write cadence. Left
/// unsuppressed, a revoked API key (Auth) or an R2 outage on a busy node would
/// retry at write rate and never reach the auth/offline cap — the historical
/// 401-loop cadence. The next successful cycle re-enables prompt wake-driven
/// flushing.
pub(super) fn failure_should_suppress_wakeups(_error: &SyncError) -> bool {
    true
}

pub(super) fn replay_blocker_forced_sync_suppressed(
    last_forced_under_blocker: &mut Option<std::time::Instant>,
    now: std::time::Instant,
    bypass_rate_limit: bool,
) -> bool {
    if bypass_rate_limit {
        return false;
    }

    match *last_forced_under_blocker {
        Some(prev) if now.duration_since(prev) < MIN_FORCED_SYNC_UNDER_BLOCKER => true,
        _ => {
            *last_forced_under_blocker = Some(now);
            false
        }
    }
}
