//! Free helpers shared by the sync engine (key parsing, compaction policy, fetch pipeline).

use super::super::error::{SyncError, SyncResult};
use super::types::{
    CloudSyncBacklogSnapshot, CloudSyncFailureKey, CloudSyncFailureStats,
    CLOUD_SYNC_BACKLOG_THRESHOLDS, MAX_PRESIGN_BATCH,
};
use futures::stream::StreamExt;

/// Run a bounded-concurrency, order-preserving fetch → sequential-consume
/// pipeline over `items`.
///
/// This is the ordering/abort core shared by the bootstrap replay loop: the
/// per-item `fetch` futures (download + unseal — the network/crypto legs) run
/// up to `cap` at a time, but `buffered(cap)` yields their results in **input
/// order**, and `consume` is awaited strictly sequentially on each result in
/// that order. The first `Err` from either `fetch` or `consume` aborts the
/// whole pipeline and is returned — so a caller that advances a cursor only
/// after this returns `Ok` can never advance past an un-consumed item, even
/// though downloads complete out of order. Memory is bounded: at most `cap`
/// in-flight fetch results exist at once.
///
/// `cap` is clamped to `>= 1` (0 would make `buffered` deadlock-free but never
/// poll — treat it as fully serial).
pub(crate) async fn ordered_concurrent_fetch<I, T, Fetch, FetchFut, Consume, ConsumeFut, E>(
    items: I,
    cap: usize,
    fetch: Fetch,
    mut consume: Consume,
) -> Result<(), E>
where
    I: IntoIterator,
    Fetch: Fn(I::Item) -> FetchFut,
    FetchFut: std::future::Future<Output = Result<T, E>>,
    Consume: FnMut(T) -> ConsumeFut,
    ConsumeFut: std::future::Future<Output = Result<(), E>>,
{
    let cap = cap.max(1);
    let mut stream = futures::stream::iter(items.into_iter().map(fetch)).buffered(cap);
    while let Some(res) = stream.next().await {
        let item = res?;
        consume(item).await?;
    }
    Ok(())
}

pub(crate) fn parse_flat_log_key(key: &str) -> Option<u64> {
    parse_seq_key(relative_mutation_log_key(key).unwrap_or(key), "log/")
}

/// Strip an optional `{scope}/` prefix so list keys parse as `log/…`.
pub(crate) fn relative_mutation_log_key(listed: &str) -> Option<&str> {
    if listed.starts_with("log/") {
        return Some(listed);
    }
    listed.find("/log/").map(|i| &listed[i + 1..])
}

/// Parse a continuous-plane log object key into (optional writer, through_id).
///
/// Accepts schema-folder, writer-scoped, and legacy flat keys.
pub(crate) fn parse_mutation_log_object_key(key: &str) -> Option<(Option<String>, u64)> {
    if let Some(seq) = parse_flat_log_key(key) {
        return Some((None, seq));
    }
    let key = relative_mutation_log_key(key).unwrap_or(key);
    let rest = key.strip_prefix("log/")?;
    let (writer, rest) = rest.split_once('/')?;
    if writer.is_empty() || writer.contains("..") {
        return None;
    }
    let suffix = rest.strip_suffix(".enc")?;
    let path_parts = suffix.split('/').collect::<Vec<_>>();
    let seq = if path_parts.len() > 1 {
        let (filename, schema_parts) = path_parts.split_last()?;
        if schema_parts
            .iter()
            .any(|component| component.is_empty() || component.contains(".."))
        {
            return None;
        }
        let (utc_nanos, sequence) = filename.rsplit_once('_')?;
        let _: u64 = utc_nanos.parse().ok()?;
        sequence.parse::<u64>().ok()?
    } else {
        path_parts.first()?.parse::<u64>().ok()?
    };
    Some((Some(writer.to_string()), seq))
}

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

pub(crate) fn crossed_cloud_sync_backlog_threshold(
    pending_count: usize,
    max_pending: usize,
) -> Option<u8> {
    if pending_count == 0 || max_pending == 0 {
        return None;
    }
    let percent = pending_count.saturating_mul(100) / max_pending;
    CLOUD_SYNC_BACKLOG_THRESHOLDS
        .iter()
        .copied()
        .rev()
        .find(|threshold| percent >= *threshold as usize)
}

pub(crate) fn cloud_sync_failure_class(operation: &str, err: &SyncError) -> String {
    let kind = match err {
        SyncError::Network(_) => "network",
        SyncError::S3(_) => "s3",
        SyncError::Auth(_) => "auth",
        SyncError::Banned(_) => "auth_banned",
        SyncError::QuotaExceeded(_) => "quota_exceeded",
        SyncError::BackupSnapshotInProgress { .. }
        | SyncError::BackupSnapshotVerifyPending { .. } => "snapshot_in_progress",
        SyncError::Storage(_) => "storage",
        SyncError::DeviceLocked { .. } => "device_locked",
        SyncError::CorruptEntry { .. } | SyncError::PoisonEntry { .. } => "replay",
        // Distinct from "replay" (a bad cloud object) and from the "storage"
        // class this used to fall into: the entry is fine and the store is
        // fine, but this build cannot apply the entry, so the cursor is pinned
        // and every upload queues behind it. During the 2026-08-16 outage this
        // reported as the generic `download_storage`, indistinguishable from
        // ordinary disk trouble.
        SyncError::ReplayApplyFailed { .. } => "replay_apply_failed",
        SyncError::CorruptProofObject { .. } => "proof_object",
        SyncError::KeyProofFailed { .. } => "key_mismatch",
        SyncError::BackupBootstrapBlocked { .. } => "backup_bootstrap_blocked",
        SyncError::MissingSyncTarget { .. } => "missing_sync_target",
        SyncError::UnsupportedEnvelope { .. } => "unsupported_envelope",
        SyncError::UnsupportedBackupFormat { .. } => "unsupported_backup_format",
        SyncError::SequenceGap { .. } => "sequence_gap",
        SyncError::Crypto(_) | SyncError::WrongKey => "crypto",
        SyncError::Serialization(_) => "serialization",
        SyncError::Io(_) => "io",
        SyncError::SnapshotTooLarge { .. } => "snapshot_too_large",
        SyncError::MissingSnapshot { .. } => "missing_snapshot",
    };
    format!("{operation}_{kind}")
}

pub(crate) fn emit_cloud_sync_backlog_incident(
    key: &CloudSyncFailureKey,
    stats: &CloudSyncFailureStats,
    snapshot: CloudSyncBacklogSnapshot,
    sync_concurrency: usize,
) {
    tracing::error!(
        target: "fold_db::sync::backlog",
        sync_target = %key.sync_target,
        failure_class = %key.failure_class,
        failure_count = stats.count,
        pending_count = snapshot.pending_count,
        max_pending = snapshot.max_pending,
        pending_threshold_percent = snapshot.threshold_percent,
        oldest_pending_age_secs = snapshot.oldest_pending_age_secs,
        sync_concurrency = sync_concurrency,
        chunk_size = MAX_PRESIGN_BATCH,
        last_success_age_secs = snapshot.last_success_age_secs,
        error = %stats.last_error,
        "cloud sync backlog incident: repeated transfer failures crossed pending-depth threshold"
    );
}

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

/// Parse the sequence number from a point-in-time snapshot key
/// `snapshots/{seq}.enc`.
///
/// The storage Lambda strips the scope prefix on list responses, so keys come
/// back as `snapshots/{name}` (mirrors `parse_flat_log_key` for `log/`). The
/// `snapshots/latest.enc` pointer is intentionally **not** a `{seq}.enc` key
/// and returns `None` (`"latest"` does not parse as `u64`), so retention never
/// considers it for deletion.
pub(crate) fn parse_snapshot_key(key: &str) -> Option<u64> {
    parse_seq_key(key, "snapshots/")
}

pub(crate) fn parse_seq_key(key: &str, prefix: &str) -> Option<u64> {
    let key = key.strip_prefix(prefix)?;
    let seq_str = key.strip_suffix(".enc")?;
    seq_str.parse::<u64>().ok()
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

/// Default ceiling on the on-disk bytes of the planes a personal
/// `Snapshot::create` photograph reads.
///
/// That photograph is not streamed: it holds every row of every photographed
/// plane as base64 strings in one `Vec`, then `seal()` builds the JSON and the
/// ciphertext of the whole thing. Peak memory is a multiple of the store, not
/// of one namespace. On Tom's primary (2026-09-29, ~12.6 GiB of photographed
/// planes on disk) every attempt pushed the footprint from ~9.5 GiB to 14-17 GiB
/// against a 16 GiB memory-guard ceiling — and those attempts were cut short by
/// two unreadable `cas_blobs` rows; a full pass cannot fit at all. It retried
/// every ~22 min for days with no completion.
///
/// Above the cap the attempt is refused before any row is read. Personal data
/// stays protected by the chunked backup (`sync::backup`), which never holds
/// the store in one buffer. What stops is only the remote personal log prune.
pub(crate) const PERSONAL_PHOTOGRAPH_MAX_DISK_BYTES_DEFAULT: u64 = 1024 * 1024 * 1024;

/// The cap in force for this process, from
/// `LASTDB_PERSONAL_PHOTOGRAPH_MAX_DISK_BYTES`. `0` removes the cap (returns
/// `None`); unset or unparseable falls back to the default.
pub(crate) fn personal_photograph_max_disk_bytes() -> Option<u64> {
    parse_personal_photograph_max_disk_bytes(
        std::env::var("LASTDB_PERSONAL_PHOTOGRAPH_MAX_DISK_BYTES")
            .ok()
            .as_deref(),
    )
}

/// Pure half of [`personal_photograph_max_disk_bytes`], so tests do not mutate
/// a process-global env var that concurrent compaction tests would read.
pub(crate) fn parse_personal_photograph_max_disk_bytes(raw: Option<&str>) -> Option<u64> {
    match raw.and_then(|raw| raw.trim().parse::<u64>().ok()) {
        Some(0) => None,
        Some(max) => Some(max),
        None => Some(PERSONAL_PHOTOGRAPH_MAX_DISK_BYTES_DEFAULT),
    }
}

/// Refusal message when the photographed planes exceed the cap, else `None`.
///
/// `disk_bytes: None` means the backend cannot measure (in-memory stores,
/// tests); that keeps today's behaviour rather than guessing.
pub(crate) fn personal_photograph_size_refusal(
    disk_bytes: Option<u64>,
    max_bytes: Option<u64>,
) -> Option<String> {
    let (disk_bytes, max_bytes) = (disk_bytes?, max_bytes?);
    (disk_bytes > max_bytes).then(|| {
        format!(
            "personal photograph refused: the photographed planes hold {disk_bytes} bytes on \
             disk, over the {max_bytes}-byte cap for an in-memory photograph \
             (LASTDB_PERSONAL_PHOTOGRAPH_MAX_DISK_BYTES; 0 removes the cap). The chunked \
             backup is unaffected; only the remote personal log prune is skipped"
        )
    })
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

/// Given the bare snapshot names under `snapshots/` (as returned by
/// `list_snapshot_objects`, i.e. `snapshots/{name}`) and a retention count,
/// return the bare `{name}` of each point-in-time `{seq}.enc` snapshot that
/// should be **deleted**.
///
/// Retention policy:
/// - The `latest.enc` pointer is always kept (it never parses as a seq).
/// - The `retain` highest-seq `{seq}.enc` snapshots are kept.
/// - Every older `{seq}.enc` snapshot is returned for deletion.
///
/// `retain == 0` keeps ONLY `latest.enc` (every `{seq}.enc` is pruned). That is
/// safe and intended: `latest.enc` is always kept here, and restore reads the
/// snapshot together with its `last_seq` from `latest.enc` alone — the
/// timestamped copies are never needed to restore. Returns bare names
/// (e.g. `"42.enc"`) ready for `presign_snapshot_delete`.
pub(crate) fn snapshots_to_prune(keys: &[String], retain: usize) -> Vec<String> {
    // Collect (seq, bare_name) for every point-in-time snapshot.
    let mut snapshots: Vec<(u64, String)> = keys
        .iter()
        .filter_map(|key| {
            let seq = parse_snapshot_key(key)?;
            // Strip the `snapshots/` prefix to get the bare name the delete
            // endpoint expects (e.g. `42.enc`).
            let name = key.strip_prefix("snapshots/")?.to_string();
            Some((seq, name))
        })
        .collect();
    // Highest seq first; keep the first `retain`, prune the rest.
    snapshots.sort_unstable_by_key(|(seq, _)| std::cmp::Reverse(*seq));
    snapshots
        .into_iter()
        .skip(retain)
        .map(|(_, name)| name)
        .collect()
}

/// Parse the sequence number from a thumbnail pack key `thumbs/packs/{seq}.enc`.
///
/// Pack IDs share the `{last_seq}.enc` naming convention with point-in-time
/// snapshots (see `compact_target` / `backup_snapshot`). Non-seq names
/// (malformed, non-numeric) return `None` and are never selected for prune.
pub(crate) fn parse_thumb_pack_key(key: &str) -> Option<u64> {
    parse_seq_key(key, "thumbs/packs/")
}

/// Given bare or prefixed pack keys under `thumbs/packs/` and a retention
/// count, return each bare `{seq}.enc` pack id that should be **deleted**.
///
/// Retention policy (mirrors [`snapshots_to_prune`]):
/// - The `retain` highest-seq packs are kept.
/// - Every older `{seq}.enc` pack is returned for deletion.
///
/// Unlike snapshots, packs have **no** `latest.enc` dual-write — the current
/// pack *is* `{last_seq}.enc`. Callers that pass `snapshot_retention == 0`
/// should therefore pass `retain.max(1)` so the newest pack survives as the
/// restore source for packed thumbnails (loose objects are deleted after pack
/// upload). Returns bare ids (e.g. `"42.enc"`) for `presign_thumb_pack_delete`.
pub(crate) fn thumb_packs_to_prune(keys: &[String], retain: usize) -> Vec<String> {
    let mut packs: Vec<(u64, String)> = keys
        .iter()
        .filter_map(|key| {
            // Accept both full list keys (`thumbs/packs/42.enc`) and bare
            // ids already stripped by a caller.
            let seq = parse_thumb_pack_key(key).or_else(|| {
                let seq_str = key.strip_suffix(".enc")?;
                seq_str.parse::<u64>().ok()
            })?;
            let name = key
                .strip_prefix("thumbs/packs/")
                .unwrap_or(key.as_str())
                .to_string();
            // Only prune true `{seq}.enc` leaves; skip anything that lost
            // its numeric identity above.
            if !name.ends_with(".enc") {
                return None;
            }
            Some((seq, name))
        })
        .collect();
    packs.sort_unstable_by_key(|(seq, _)| std::cmp::Reverse(*seq));
    packs
        .into_iter()
        .skip(retain)
        .map(|(_, name)| name)
        .collect()
}

/// Extract the molecule uuid `M` from a per-key record key `mk:{M}:{key}`
/// (optionally `{org}:`- or `from:{sender}:`-prefixed). `M` is a 64-hex
/// molecule uuid and contains no `:`, so it is the segment between `mk:` and
/// the next `:`. Returns `None` if the key is not a per-key record key.
pub(crate) fn molecule_uuid_from_record_key(key: &str) -> Option<String> {
    let after = position_after_prefix(key, crate::atom::molecule_key_codec::MK_PREFIX)?;
    let rest = &key[after..];
    // `mk:{M}:{key}` — `M` ends at the first `:` (uuids never contain `:`).
    let (uuid, _) = rest.split_once(':')?;
    (!uuid.is_empty()).then(|| uuid.to_string())
}

pub(crate) fn hash_range_page_index_complete_key_for_record_key(
    key: &str,
    mol_uuid: &str,
) -> Option<String> {
    let record_prefix = crate::atom::molecule_key_codec::molecule_record_prefix(mol_uuid);
    let prefix_pos = key.find(&record_prefix)?;
    let suffix = &key[prefix_pos + record_prefix.len()..];
    if !suffix.contains('\u{0}') {
        return None;
    }
    crate::atom::molecule_key_codec::decode_hash_range_suffix(suffix)?;
    let storage_prefix = &key[..prefix_pos];
    Some(format!(
        "{}{}",
        storage_prefix,
        crate::atom::molecule_key_codec::hash_range_page_index_complete_key(mol_uuid)
    ))
}

/// Extract the molecule uuid `M` from a header key `mh:{M}` (optionally
/// prefixed). `M` is the whole remainder after `mh:`. Returns `None` if the key
/// is not a header key.
pub(crate) fn molecule_uuid_from_header_key(key: &str) -> Option<String> {
    let after = position_after_prefix(key, crate::atom::molecule_key_codec::MH_PREFIX)?;
    let uuid = &key[after..];
    (!uuid.is_empty()).then(|| uuid.to_string())
}

/// Whether `key` is a HashRange append-log order entry `mord:{M}:{seq}`
/// or `mord\0{M}:{seq}` (optionally `{org}:`-/`from:`-prefixed).
pub(crate) fn is_order_log_entry_key(key: &str) -> bool {
    crate::kind_partition::rest_of(key, "mord").is_some_and(|rest| !rest.is_empty())
}

/// Whether `key` is a HashRange append-log count record `moc:{M}` or
/// `moc\0{M}` (optionally `{org}:`-/`from:`-prefixed).
pub(crate) fn is_order_count_key(key: &str) -> bool {
    crate::kind_partition::rest_of(key, "moc").is_some_and(|rest| !rest.is_empty())
}

/// Byte index just past the first occurrence of `marker` in `key`, requiring
/// that `marker` is either at the start or immediately follows a `:` (so a
/// random substring `...mk:...` inside an arbitrary value can't be mistaken for
/// the record prefix — the only legitimate prefixes are `{org}:` and
/// `from:{sender}:`, both `:`-terminated).
pub(crate) fn position_after_prefix(key: &str, marker: &str) -> Option<usize> {
    if let Some(rest) = key.strip_prefix(marker) {
        return Some(key.len() - rest.len());
    }
    let needle = format!(":{marker}");
    let idx = key.find(&needle)?;
    Some(idx + needle.len())
}

/// Return the storage scope before a molecule key marker.
///
/// `build_storage_key` expects the prefix without its trailing delimiter:
/// `org:mk:{M}:k` -> `Some("org")`, while bare `mk:{M}:k` stays personal.
pub(crate) fn storage_scope_for_key_marker<'a>(key: &'a str, marker: &str) -> Option<&'a str> {
    if key.starts_with(marker) {
        return None;
    }
    let needle = format!(":{marker}");
    let marker_pos = key.find(&needle)?;
    (marker_pos > 0).then(|| &key[..marker_pos])
}
