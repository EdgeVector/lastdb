use crate::sync::error::{SyncError, SyncResult};

/// Decide whether a bootstrap that found no snapshot must fail loudly instead
/// of silently starting fresh.
///
/// Bootstrap downloads a target's `latest.enc`; when it is absent the engine
/// would replay only the cloud log tail. For a **brand-new account** (no
/// snapshot AND an empty log prefix) that is correct — there is nothing to
/// restore, so it starts clean. But when the target's log prefix **already
/// holds history**, a missing snapshot means the prefix that a snapshot once
/// compacted is now gone (deleted / reset / a failed upload): replaying only
/// the tail restores a silently hollow store. That is the "missing snapshot"
/// migration trap — a device restoring an existing identity coming up
/// near-empty — which this guard closes by returning a structured
/// [`SyncError::MissingSnapshot`] naming the gap.
///
/// Returns `Err(SyncError::MissingSnapshot)` only when a snapshot is absent, the
/// target has log history, and the caller has NOT opted into a fresh start via
/// `accept_fresh`; otherwise `Ok(())` (snapshot present, brand-new account, or
/// an explicit fresh-start opt-in).
pub(crate) fn guard_missing_snapshot(
    target_label: &str,
    snapshot_present: bool,
    remote_log_entries: usize,
    accept_fresh: bool,
) -> SyncResult<()> {
    if snapshot_present || accept_fresh || remote_log_entries == 0 {
        return Ok(());
    }
    Err(SyncError::MissingSnapshot {
        target: target_label.to_string(),
        log_entries: remote_log_entries,
    })
}

/// How many **scoped** (org/share) targets a single steady-state cycle may
/// download after personal upload.
///
/// Re-enable thrash 2026-07-14: a node with ~21 distinct `org_hash` rows
/// (dogfood leaks labeled `org:edgevector`) downloaded every scoped target
/// *before* any personal upload; the first org alone pushed RSS 4→10 GiB.
/// When many scoped targets are registered, cap scoped pull to a small fixed
/// amount per cycle instead of disabling it — [`pick_scoped_round_robin`]
/// rotates through every registered target over successive cycles so
/// personal catch-up is still protected without stranding scoped downloads
/// forever (found live: a `scoped_total=319` node with scoped downloads
/// permanently disabled for 32 days,
/// `papercut-lastdb-scoped-downloads-hard-off-above-four-targets`).
///
/// - `0` scoped → download none
/// - `>0` scoped → download at most **1** per cycle, any count
///   (round-robin elsewhere keeps rotating through all registered targets)
pub(crate) fn scoped_downloads_per_cycle(scoped_target_count: usize) -> usize {
    if scoped_target_count == 0 {
        0
    } else {
        1
    }
}

/// Split target indices into personal (predicate true) vs scoped, preserving
/// original order within each group. `do_sync` **must** download personal
/// indices first, then at most [`scoped_downloads_per_cycle`] of the scoped
/// list after personal upload.
pub(crate) fn split_personal_and_scoped_indices<T, F>(
    targets: &[T],
    is_personal: F,
) -> (Vec<usize>, Vec<usize>)
where
    F: Fn(&T) -> bool,
{
    let mut personal = Vec::new();
    let mut scoped = Vec::new();
    for (i, t) in targets.iter().enumerate() {
        if is_personal(t) {
            personal.push(i);
        } else {
            scoped.push(i);
        }
    }
    (personal, scoped)
}

/// Round-robin pick of up to `max_scoped` entries from `scoped_indices`,
/// starting at `rr_start % len`. Empty when `max_scoped == 0` or no scoped.
pub(crate) fn pick_scoped_round_robin(
    scoped_indices: &[usize],
    max_scoped: usize,
    rr_start: usize,
) -> Vec<usize> {
    if scoped_indices.is_empty() || max_scoped == 0 {
        return Vec::new();
    }
    let n = scoped_indices.len();
    let start = rr_start % n;
    let take = max_scoped.min(n);
    (0..take)
        .map(|offset| scoped_indices[(start + offset) % n])
        .collect()
}
