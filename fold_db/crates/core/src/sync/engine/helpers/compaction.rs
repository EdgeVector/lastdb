/// Inputs to the size/time-based compaction decision, pulled out so the policy
/// is a pure, exhaustively-testable function.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CompactionInputs {
    /// Serialized bytes of log entries uploaded since the last snapshot.
    pub(crate) log_bytes_since: u64,
    /// Byte size of the last snapshot (≈ DB size). `0` means "no snapshot taken
    /// yet this process" — the size ratio cannot be evaluated.
    pub(crate) snapshot_bytes: u64,
    /// Seconds since the last snapshot. `None` means "never snapshotted yet".
    pub(crate) elapsed_secs: Option<u64>,
    /// Entry count uploaded since the last snapshot (final backstop).
    pub(crate) entries_since: u64,
}

/// Decide whether to re-snapshot (compact) the personal log.
///
/// Policy (re-snapshot RARELY — see the `snapshot-compaction-cadence` card):
/// 1. **Min interval** — never snapshot more often than `min_interval_secs`
///    once a snapshot exists. This wins over everything else (a bulk import
///    can momentarily blow past the size ratio; the floor prevents a flurry).
///    It does NOT gate the very first snapshot (`elapsed.is_none()`).
/// 2. **Max interval** — if at least `max_interval_secs` have elapsed since the
///    last snapshot, compact (keeps a near-idle node's snapshot fresh).
/// 3. **Size ratio** — once a snapshot exists, compact when accumulated log
///    bytes reach `ratio * snapshot_bytes`. This is the normal trigger.
/// 4. **Entry-count backstop** — a last-resort ceiling so a pathological stream
///    can never grow the log unbounded between size-based snapshots.
///
/// When no snapshot has ever been taken (`snapshot_bytes == 0` /
/// `elapsed_secs.is_none()`), only the entry-count backstop can fire — that
/// gives a brand-new node its first snapshot once it has uploaded a meaningful
/// log tail, after which the size/time policy takes over.
pub(crate) fn should_compact(
    inputs: CompactionInputs,
    ratio: f64,
    max_interval_secs: u64,
    min_interval_secs: u64,
    entry_backstop: u64,
) -> bool {
    let CompactionInputs {
        log_bytes_since,
        snapshot_bytes,
        elapsed_secs,
        entries_since,
    } = inputs;

    // 1. Min-interval floor — applies only once a snapshot exists.
    if let Some(elapsed) = elapsed_secs {
        if min_interval_secs > 0 && elapsed < min_interval_secs {
            return false;
        }
        // 2. Max-interval ceiling.
        if max_interval_secs > 0 && elapsed >= max_interval_secs {
            return true;
        }
    }

    // 3. Size ratio — needs a prior snapshot to compare against.
    if ratio > 0.0 && snapshot_bytes > 0 {
        let target = (snapshot_bytes as f64) * ratio;
        if (log_bytes_since as f64) >= target {
            return true;
        }
    }

    // 4. Final entry-count backstop (also the only trigger for the first-ever
    //    snapshot, when there's nothing to size/time against).
    entry_backstop > 0 && entries_since >= entry_backstop
}

/// First retry delay after an automatic personal compaction fails.
pub(crate) const COMPACTION_FAILURE_BACKOFF_BASE_SECS: u64 = 30 * 60;

/// Longest retry delay after repeated automatic personal compaction failures.
pub(crate) const COMPACTION_FAILURE_BACKOFF_MAX_SECS: u64 = 24 * 60 * 60;

/// Retry delay after `consecutive_failures` failed automatic personal
/// compactions: 0 for none, then base, 2x base, 4x base, ... capped at max.
pub(crate) fn compaction_failure_backoff_secs(consecutive_failures: u32) -> u64 {
    if consecutive_failures == 0 {
        return 0;
    }
    let shift = (consecutive_failures - 1).min(16);
    COMPACTION_FAILURE_BACKOFF_BASE_SECS
        .saturating_mul(1u64 << shift)
        .min(COMPACTION_FAILURE_BACKOFF_MAX_SECS)
}

/// Retry gate for the automatic personal compaction paths in a sync cycle.
///
/// Why this exists (2026-09-29, primary node): the remote-log-count trigger
/// runs on every cycle whose `pending` queue is empty, and a failure recorded
/// nothing. One old over-ceiling row made every attempt fail after
/// `Snapshot::create` had materialized the whole store, so an idle node paid
/// +4 GiB footprint and +4 GB swap every ~20 min for zero result. A failure
/// now defers the next automatic attempt; success clears the gate. The state
/// is in-memory, so a restart retries at once (one attempt, then back off).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompactionFailureBackoff {
    pub(crate) consecutive_failures: u32,
    pub(crate) retry_at_secs: Option<u64>,
}

impl CompactionFailureBackoff {
    pub(crate) fn allows(&self, now_secs: u64) -> bool {
        self.retry_at_secs.is_none_or(|at| now_secs >= at)
    }

    /// Record one failure at `now_secs`; returns the delay before the next try.
    pub(crate) fn record_failure(&mut self, now_secs: u64) -> u64 {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let delay = compaction_failure_backoff_secs(self.consecutive_failures);
        self.retry_at_secs = Some(now_secs.saturating_add(delay));
        delay
    }

    pub(crate) fn record_success(&mut self) {
        *self = Self::default();
    }
}

pub(crate) fn target_log_compaction_seq(log_seqs: &[u64], threshold: u64) -> Option<u64> {
    let threshold = threshold.max(1);
    if (log_seqs.len() as u64) < threshold {
        return None;
    }
    log_seqs.iter().copied().max()
}

/// Seq set for the post-compaction personal `log_index.enc`.
///
/// Compact must never shrink the known-seq set relative to either:
/// - a cloud listing taken **after** compacted deletes (so concurrent peer
///   uploads that landed mid-compact are visible), or
/// - the existing index (which a concurrent peer may already have merged into
///   before this compact rewrites the file).
///
/// Both sources are filtered to `seq > last_seq` so entries the snapshot
/// supersedes are dropped. Pure helper so the race is unit-testable without S3.
pub(crate) fn personal_log_index_seqs_after_compact(
    last_seq: u64,
    listed_seqs: impl IntoIterator<Item = u64>,
    existing_index_seqs: impl IntoIterator<Item = u64>,
) -> Vec<u64> {
    let mut seqs: Vec<u64> = listed_seqs
        .into_iter()
        .chain(existing_index_seqs)
        .filter(|seq| *seq > last_seq)
        .collect();
    seqs.sort_unstable();
    seqs.dedup();
    seqs
}
