use super::*;

pub(super) fn backup_storage_footprint_from_listing(
    listed: &[crate::sync::auth::S3ObjectInfo],
    keep: &std::collections::BTreeSet<String>,
) -> BackupStorageFootprint {
    let pairs: Vec<(&str, u64)> = listed
        .iter()
        .filter_map(|object| {
            // Any remainder under backup/chunks/ (v1 semantics, unchanged).
            // v2 keys are backup storage but not v1 chunk bytes; they are
            // never part of the v1 keep-set footprint.
            let sha = super::super::backup_keys::v1_chunk_remainder(&object.key)?;
            Some((sha, object.size))
        })
        .collect();
    compute_backup_storage_footprint(pairs, keep)
}

/// Max successful sealed-chunk puts per continuous drain cycle.
///
/// Two regimes, because one number cannot serve both:
///
/// - **steady state** — a cut has already landed, so the home is restorable and
///   the uploader's job is to keep up with the delta. A small budget keeps
///   backup from competing with foreground work. Unchanged: default 4, ceiling
///   16.
/// - **catch-up** — no restore base has landed, or the newest committed restore
///   base is stale. The steady-state budget is not politeness here, it is the
///   reason the backup never catches up. On the 2026-07-31 primary the budget
///   was already pinned at its 16 ceiling and the uploader interval lowered to
///   20s, and the measured ETA was still **185 hours** against a node whose
///   uptime was under 4 hours: 16 puts per ~106s cycle, 3.34 GB remaining,
///   ~5 KB/s. A home with no fresh backup should spend bandwidth now to become
///   restorable, not ration it.
///
/// The ceiling is a budget on *how much work one cycle may take*, not on
/// resource usage — in-flight PUTs are bounded separately by
/// [`backup_upload_concurrency`], and cycle wall time by
/// [`backup_catchup_cycle_budget`].
pub(super) fn backup_upload_target_per_cycle(catching_up: bool) -> usize {
    if catching_up {
        return env_flag::var_or("LASTDB_BACKUP_CATCHUP_TARGET_PER_CYCLE", 256).clamp(1, 4096);
    }
    env_flag::var_or("LASTDB_BACKUP_UPLOAD_TARGET_PER_CYCLE", 4).clamp(1, 16)
}

/// Wall-clock budget for one catch-up drain cycle.
///
/// A put budget alone bounds cycle *work*, not cycle *time*: on a slow link 256
/// puts at concurrency 2 is most of an hour, during which no publish attempt
/// runs and no progress sample is recorded. Past this deadline the cycle stops
/// starting new PUTs and returns what it has; the held cut (#1048) means the
/// next cycle resumes against the same candidate list, so stopping early costs
/// nothing but a loop iteration.
///
/// Only consulted in catch-up mode — the steady-state budget of 4 cannot run
/// long enough to need it.
pub(super) fn backup_catchup_cycle_budget() -> Duration {
    let secs = env_flag::var_or("LASTDB_BACKUP_CATCHUP_CYCLE_SECS", 300u64).clamp(5, 3600);
    Duration::from_secs(secs)
}

/// Resolve the restore-base age that re-enters backup catch-up mode.
///
/// Default: 6 hours. `LASTDB_BACKUP_CATCHUP_STALENESS_SECS` is clamped to
/// 5 minutes..=7 days. Invalid values use the default.
pub(super) fn backup_catchup_staleness_secs_from_env() -> u64 {
    parse_backup_catchup_staleness_secs(
        std::env::var("LASTDB_BACKUP_CATCHUP_STALENESS_SECS")
            .ok()
            .as_deref(),
    )
}

pub(super) fn parse_backup_catchup_staleness_secs(raw: Option<&str>) -> u64 {
    raw.and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_BACKUP_CATCHUP_STALENESS_SECS)
        .clamp(
            MIN_BACKUP_CATCHUP_STALENESS_SECS,
            MAX_BACKUP_CATCHUP_STALENESS_SECS,
        )
}

/// Unknown age is deliberately catch-up: it covers both a never-published home
/// and an old durability marker that proves a commit but predates timestamp
/// stamping. Neither case proves the restore base is fresh.
pub(super) fn backup_is_catching_up(
    last_publish_age_secs: Option<u64>,
    staleness_secs: u64,
) -> bool {
    last_publish_age_secs.is_none_or(|age| age > staleness_secs)
}

/// Catch-up when the restore base is missing/stale **or** the remaining
/// shortfall is large enough that a cycle already re-lists the object store.
///
/// `backlog_chunks` is [`SyncEngine::PRESENCE_RESEED_SHORTFALL`]: the same
/// number that triggers `reseed_backup_presence_on_shortfall`. If we pay for
/// a full listing every cycle, we must also use the catch-up PUT budget
/// (256 vs 4) and concurrency floor (8 vs 2). A local last-publish copied
/// from another cloud identity (prod CoW → DEV) is not a restore base for
/// this destination.
pub(super) fn backup_is_catching_up_for_drain(
    last_publish_age_secs: Option<u64>,
    staleness_secs: u64,
    remaining_chunks: usize,
    backlog_chunks: usize,
) -> bool {
    backup_is_catching_up(last_publish_age_secs, staleness_secs)
        || remaining_chunks >= backlog_chunks
}

/// Minimum time between full `backup/chunks/` presence listings.
///
/// Default 120s. `LASTDB_BACKUP_PRESENCE_RESEED_MIN_SECS` is clamped to
/// 0..=1 hour; 0 restores the previous every-call listing. A progressing
/// catch-up drain already records PUT success into `backup_known_present`,
/// so listing the prefix again every cycle (and twice when the operator
/// snapshot and continuous publisher both reseed) is not how remaining
/// drops.
pub(super) const DEFAULT_BACKUP_PRESENCE_RESEED_MIN_SECS: u64 = 120;
pub(super) const MAX_BACKUP_PRESENCE_RESEED_MIN_SECS: u64 = 60 * 60;

pub(super) fn backup_presence_reseed_min_interval() -> Duration {
    Duration::from_secs(parse_backup_presence_reseed_min_secs(
        std::env::var("LASTDB_BACKUP_PRESENCE_RESEED_MIN_SECS")
            .ok()
            .as_deref(),
    ))
}

pub(super) fn parse_backup_presence_reseed_min_secs(raw: Option<&str>) -> u64 {
    raw.and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_BACKUP_PRESENCE_RESEED_MIN_SECS)
        .min(MAX_BACKUP_PRESENCE_RESEED_MIN_SECS)
}

/// True when a presence listing should run: never listed in this process, or
/// the last successful listing is at least `min_interval` old.
///
/// A zero interval is always due, including immediately after a listing, so
/// tests and operators can restore the old every-call behavior.
pub(super) fn presence_reseed_is_due(
    last: Option<Instant>,
    now: Instant,
    min_interval: Duration,
) -> bool {
    match last {
        None => true,
        Some(listed_at) => now.saturating_duration_since(listed_at) >= min_interval,
    }
}

/// In-flight PUT concurrency for one backup drain cycle.
///
/// Default follows the adaptive upload-policy concurrency (same knob the
/// mutation-log uploader uses). Override with `LASTDB_BACKUP_UPLOAD_CONCURRENCY`.
/// Floor is 2 so a multi-Mbps link is never stuck at a single serial stream
/// (the 2026-07-30 ~137 KB/s ceiling).
///
/// In **catch-up** mode the floor is raised, because the adaptive policy is
/// tuned for the opposite situation. `upload_policy` drops concurrency to
/// `MIN_CONCURRENCY` whenever `interactive_busy` is set — which on a machine
/// running an agent fleet is essentially always — so the 2026-07-31 primary was
/// uploading its *first ever* backup at concurrency 2. Yielding to foreground
/// work is right for a home that is already durable; for one that has never
/// been restorable it is how a backup stays unfinished indefinitely. An
/// explicit `LASTDB_BACKUP_UPLOAD_CONCURRENCY` still wins in both modes.
/// A process-local owner override sits between env and adaptive policy:
/// `valid environment override > runtime owner override > adaptive/catch-up`.
pub(super) fn backup_upload_concurrency(policy_concurrency: usize, catching_up: bool) -> usize {
    backup_upload_concurrency_with_runtime(policy_concurrency, catching_up, None)
}

pub(super) fn backup_upload_concurrency_env() -> Option<usize> {
    env_flag::var_parsed("LASTDB_BACKUP_UPLOAD_CONCURRENCY").map(|value: usize| value.clamp(1, 32))
}

pub(super) fn backup_upload_concurrency_status(
    runtime_override: Option<usize>,
) -> BackupUploadConcurrencyStatus {
    if let Some(explicit) = backup_upload_concurrency_env() {
        return BackupUploadConcurrencyStatus {
            effective_override: Some(explicit),
            effective_source: "environment",
            runtime_override,
        };
    }
    BackupUploadConcurrencyStatus {
        effective_override: runtime_override,
        effective_source: if runtime_override.is_some() {
            "runtime"
        } else {
            "adaptive"
        },
        runtime_override,
    }
}

pub(super) fn backup_upload_concurrency_with_runtime(
    policy_concurrency: usize,
    catching_up: bool,
    runtime_override: Option<usize>,
) -> usize {
    if let Some(explicit) = backup_upload_concurrency_env() {
        return explicit;
    }
    if let Some(explicit) = runtime_override {
        return explicit;
    }
    let floor = if catching_up {
        env_flag::var_or("LASTDB_BACKUP_CATCHUP_CONCURRENCY", 8).clamp(1, 32)
    } else {
        2
    };
    policy_concurrency.max(floor).clamp(1, 32)
}

/// Outcome of one parallel upload cycle over a pre-selected work list.
#[derive(Debug, Default)]
pub(super) struct ParallelUploadCycleResult {
    pub(super) uploaded: usize,
    pub(super) already_present: usize,
    pub(super) failed: usize,
    pub(super) bytes_uploaded: u64,
    /// Candidates not started because the cycle's wall-clock budget expired.
    /// Neither progress nor failure — the held cut retries them next cycle.
    pub(super) deadline_skipped: usize,
    /// Candidates whose local sealed file is gone. Shas collected so the caller
    /// can stop selecting them for the rest of this cut.
    pub(super) source_missing: Vec<String>,
    pub(super) first_error: Option<SyncError>,
    pub(super) quota_exceeded: bool,
}

/// Fan-out PUT scheduler used by [`SyncEngine::drain_backup_candidates`].
///
/// Extracted so tests can drive the real `buffer_unordered` path with a
/// controllable uploader (in-flight count, per-chunk failure isolation).
pub(super) async fn parallel_backup_upload_cycle<C, F, Fut>(
    work: Vec<C>,
    concurrency: usize,
    upload_one: F,
) -> ParallelUploadCycleResult
where
    C: Send + 'static,
    F: Fn(C) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = (String, u64, SyncResult<UploadOneOutcome>)> + Send,
{
    let concurrency = concurrency.max(1);
    let mut result = ParallelUploadCycleResult::default();
    let mut put_stream =
        stream::iter(work.into_iter().map(upload_one)).buffer_unordered(concurrency);

    while let Some((sha, bytes, outcome)) = put_stream.next().await {
        match outcome {
            Ok(UploadOneOutcome::AlreadyPresent) => result.already_present += 1,
            Ok(UploadOneOutcome::DeadlineReached) => result.deadline_skipped += 1,
            Ok(UploadOneOutcome::SourceMissing) => result.source_missing.push(sha),
            Ok(UploadOneOutcome::Uploaded) => {
                result.uploaded += 1;
                result.bytes_uploaded = result.bytes_uploaded.saturating_add(bytes);
            }
            Err(e) => {
                result.failed += 1;
                if matches!(&e, SyncError::QuotaExceeded(_)) {
                    result.quota_exceeded = true;
                }
                // One WARN per cycle, not one per chunk. When the failure is
                // systemic rather than per-chunk — a full cloud quota rejecting
                // every PUT with the same 429 — the per-chunk form emitted the
                // whole ~1.5 KB error body 16 times per 5s cycle. Measured on
                // the primary 2026-08-05: ~8,000 lines per 25 minutes, which
                // rotated the node's logs every ~25 minutes and left barely two
                // hours of history. The flood is worst exactly when a stall is
                // being diagnosed, and it costs the diagnosis its evidence.
                //
                // The first failure keeps its full detail; the rest are debug
                // and are counted into one summary line below.
                if result.first_error.is_none() {
                    tracing::warn!(
                        target: "fold_db::sync::backup",
                        chunk_sha256 = %sha,
                        error = %redact_sync_error_text(&e.to_string()),
                        "backup chunk upload failed; continuing the drain"
                    );
                    result.first_error = Some(e);
                } else {
                    tracing::debug!(
                        target: "fold_db::sync::backup",
                        chunk_sha256 = %sha,
                        error = %redact_sync_error_text(&e.to_string()),
                        "backup chunk upload failed; continuing the drain"
                    );
                }
            }
        }
    }
    if result.failed > 1 {
        tracing::warn!(
            target: "fold_db::sync::backup",
            failed = result.failed,
            uploaded = result.uploaded,
            "backup drain cycle lost multiple chunks; first failure logged above, rest at debug"
        );
    }
    result
}

pub(super) fn backup_uploader_interval() -> Duration {
    let secs = env_flag::var_or(
        "LASTDB_BACKUP_UPLOADER_INTERVAL_SECS",
        DEFAULT_BACKUP_UPLOADER_INTERVAL_SECS,
    );
    Duration::from_secs(secs.max(1))
}

pub(super) fn snapshot_log_publish_interval() -> Duration {
    let secs = env_flag::var_or(
        "LASTDB_SNAPSHOT_LOG_PUBLISH_INTERVAL_SECS",
        DEFAULT_SNAPSHOT_LOG_PUBLISH_INTERVAL_SECS,
    );
    Duration::from_secs(secs.max(1))
}

pub(super) fn is_backup_chunk_sha(sha: &str) -> bool {
    sha.len() == 64 && sha.bytes().all(|b| b.is_ascii_hexdigit())
}

pub(crate) fn read_backup_presence_cache(path: &std::path::Path) -> HashSet<String> {
    let Ok(bytes) = std::fs::read(path) else {
        return HashSet::new();
    };
    let values: Vec<String> = serde_json::from_slice(&bytes)
        .or_else(|_| {
            serde_json::from_slice::<HashSet<String>>(&bytes).map(|set| set.into_iter().collect())
        })
        .unwrap_or_default();
    values
        .into_iter()
        .filter(|sha| is_backup_chunk_sha(sha))
        .collect()
}

pub(crate) fn write_backup_presence_cache(
    path: &std::path::Path,
    shas: &HashSet<String>,
) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut values: Vec<_> = shas
        .iter()
        .filter(|sha| is_backup_chunk_sha(sha))
        .cloned()
        .collect();
    values.sort();
    let bytes = serde_json::to_vec(&values).map_err(std::io::Error::other)?;
    std::fs::write(path, bytes)
}
