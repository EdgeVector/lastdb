//! Pure continuous-backup drain planning (no network).
//!
//! Contract: walk the **full** ordered candidate list, skip already-present
//! digests, collect up to `upload_target` missing digests. Never pre-truncate
//! to a fixed head window — that permanently stalls once the head is saturated.
//!
//! Also owns the **drain PUT backoff** decision (no error-string sniffing):
//! when a cycle attempted PUT work and completed none of it, the forever loop
//! backs off PUT fan-out on the same 30 s → 15 min curve as CAS, while the
//! enumerate / remaining-count half keeps running every interval.

use std::collections::HashSet;
use std::time::Duration;

/// First drain-PUT backoff after a fully-failed cycle (matches CAS start).
pub const DRAIN_PUT_BACKOFF_INITIAL: Duration = Duration::from_secs(30);
/// Cap for drain-PUT exponential backoff (matches CAS 15 min).
pub const DRAIN_PUT_BACKOFF_MAX: Duration = Duration::from_secs(900);

/// Plan the next digests to upload.
///
/// `candidates` is ordered `(sha256, bytes)`. `already_present` are digests
/// already confirmed in cloud (or known from prior cycles). Returns at most
/// `upload_target` missing digests from the full walk.
pub fn plan_backup_uploads(
    candidates: &[(String, u64)],
    already_present: &HashSet<String>,
    upload_target: usize,
) -> Vec<String> {
    let mut out = Vec::new();
    for (sha, _bytes) in candidates {
        if out.len() >= upload_target {
            break;
        }
        if already_present.contains(sha) {
            continue;
        }
        out.push(sha.clone());
    }
    out
}

/// True when this drain cycle **attempted PUT work and completed none of it**.
///
/// Deliberately keyed on counts, not on `429` / `QUOTA_EXCEEDED` strings, so
/// auth expiry, region outage, and bucket-policy poisoning get the same
/// throttle without a cloud error taxonomy. A cycle that only discovered
/// already-present chunks (`failed == 0`) is not a failure.
#[must_use]
pub fn drain_attempted_and_completed_none(selected: usize, uploaded: usize, failed: usize) -> bool {
    selected > 0 && uploaded == 0 && failed > 0
}

/// Next exponential drain-PUT backoff after a fully-failed cycle.
///
/// Curve: `0 → 30s → 60s → … → 900s` (same shape as CAS backoff).
#[must_use]
pub fn next_drain_put_backoff(current: Duration) -> Duration {
    if current.is_zero() {
        DRAIN_PUT_BACKOFF_INITIAL
    } else {
        current.saturating_mul(2).min(DRAIN_PUT_BACKOFF_MAX)
    }
}
