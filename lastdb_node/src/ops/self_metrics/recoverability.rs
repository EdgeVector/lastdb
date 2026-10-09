use super::*;

pub(super) fn durability_as_backup_health(
    durability: &DurabilityHealth,
) -> fold_db::backup_durability::BackupDurabilityHealth {
    fold_db::backup_durability::BackupDurabilityHealth {
        backup_manifest_counter: wire_u64(&durability.backup_manifest_counter),
        last_backup_commit_ts: wire_u64(&durability.last_backup_commit_ts),
        age_secs: wire_u64(&durability.backup_age_secs),
        max_age_secs: wire_u64(&durability.max_age_secs)
            .unwrap_or(fold_db::backup_durability::DEFAULT_MAX_BACKUP_AGE_SECS),
        degraded: durability.degraded,
        reasons: durability.reasons.clone(),
    }
}

pub(super) fn mutation_log_plane_recording(sync: &SyncHealth) -> bool {
    sync.enabled
        && sync.recording_local_changes == Some(true)
        && sync.mutation_log_active == Some(true)
}

pub(super) fn mutation_log_plane_known_off(sync: &SyncHealth) -> bool {
    !sync.enabled
        || sync.recording_local_changes == Some(false)
        || sync.mutation_log_active == Some(false)
}

/// Crash recoverability headline. Two clocks, never collapsed:
/// recording log plane → RPO is confirmed log lag; otherwise photo age.
pub(super) fn recoverability_status_line(
    now_secs: u64,
    sync: &SyncHealth,
    durability: &DurabilityHealth,
) -> String {
    let photo = durability_as_backup_health(durability);
    let photo_age = photo
        .age_secs
        .map_or_else(|| "unknown".to_string(), format_age_secs);
    let photo_id = photo
        .backup_manifest_counter
        .map_or_else(|| "none".to_string(), |c| format!("#{c}"));

    if !mutation_log_plane_recording(sync) {
        // A missed probe leaves recording/active as None. That is unavailable,
        // not "not recording". The latter is a durability-outage sentence.
        if !sync_snapshot_complete(sync) && !mutation_log_plane_known_off(sync) {
            return format!(
                "Recoverability: unavailable (probe missed) — last committed snapshot {photo_id} is {photo_age} old (photo age is the durability signal)"
            );
        }
        let why = if sync.enabled {
            "log plane not recording"
        } else {
            "sync off"
        };
        return format!(
            "Recoverability: {why} — last committed snapshot {photo_id} is {photo_age} old (photo age is the durability signal)"
        );
    }

    let lag = wire_u64(&sync.mutation_log_lag);
    let published_through = wire_u64(&sync.mutation_log_published_through)
        .or_else(|| wire_u64(&sync.mutation_log_frontier_f));
    let confirmed = cloud_confirmed_phrase(sync, published_through);
    let measured_rpo = crash_loss_window_secs(now_secs, sync);
    let holes = quarantine_hole_clause(sync);
    let miss = if sync_snapshot_complete(sync) {
        ""
    } else {
        " (probe missed)"
    };
    match measured_rpo {
        Some(0) => format!("Recoverability: RPO ~0s ({confirmed}) · recording{holes}{miss}"),
        Some(secs) => format!(
            "Recoverability: RPO ~{} ({confirmed}) · recording{holes}{miss}",
            format_age_secs(secs)
        ),
        None => {
            let lag_txt = lag.map_or_else(|| "unknown".to_string(), |n| n.to_string());
            format!("Recoverability: log_lag={lag_txt} seq ({confirmed}) · recording{holes}{miss}")
        }
    }
}

/// The clause that stops an RPO claiming more than it can.
///
/// An RPO is a statement about a **gap-free prefix**: everything up to the
/// confirmed point is off-box. Quarantine breaks that. An unsealable
/// `MutationIntent` is skipped and its durable row deleted, so the outgoing
/// stream has a hole that `mutation_log_lag` and `upload_backlog` cannot show —
/// the row they would have counted is gone. Measured on the primary
/// 2026-08-22: 569 dropped records across 293 distinct missing atoms, next to
/// `log_lag=0` and `RPO ~15s`.
///
/// So the count belongs on the same line as the claim it qualifies. Reading
/// `quarantined=569` twelve lines further down, in a field an operator has to
/// know to look for, is not the same thing.
pub(super) fn quarantine_hole_clause(sync: &SyncHealth) -> String {
    match sync.mutation_log_records_quarantined.filter(|n| *n > 0) {
        Some(1) => {
            " · 1 record quarantined — the log stream has a gap this RPO does not cover".to_string()
        }
        Some(n) => {
            format!(" · {n} records quarantined — the log stream has gaps this RPO does not cover")
        }
        None => String::new(),
    }
}

/// The bar above which F is read as a unix-ns clock rather than a sequence
/// number. 2020s unix-ns is ~1.6e18, so anything at or above 1e15 is a clock.
///
/// One constant, because two readers ask the same question of the same field:
/// [`rpo_secs_from_log_lag`] converts a lag in those units, and
/// [`confirmed_frontier_unix_secs`] converts the frontier itself.
pub(super) const FRONTIER_F_UNIX_NS_BAR: u64 = 1_000_000_000_000_000;

/// Convert mutation-log lag to seconds when F is a unix-ns (or similar) clock.
/// Small integer F is a sequence number — do not pretend those units are seconds.
pub(super) fn rpo_secs_from_log_lag(lag: Option<u64>, frontier_f: Option<u64>) -> Option<u64> {
    let lag = lag?;
    if lag == 0 {
        return Some(0);
    }
    let frontier = frontier_f.unwrap_or(0);
    // 2020s unix-ns is ~1.6e18. Treat large F or multi-million lag as ns.
    if frontier >= FRONTIER_F_UNIX_NS_BAR || lag >= 1_000_000 {
        return Some(lag.saturating_add(500_000_000) / 1_000_000_000);
    }
    None
}

/// The unix second the confirmed frontier F stands at, when F is a clock.
///
/// `None` for a sequence-number F: a small integer has no wall-clock meaning
/// and must be printed as the raw F instead of guessed at.
pub(super) fn confirmed_frontier_unix_secs(frontier_f: Option<u64>) -> Option<u64> {
    frontier_f
        .filter(|f| *f >= FRONTIER_F_UNIX_NS_BAR)
        .map(|f| f / 1_000_000_000)
}

/// The crash loss window: how old the data at the recovery point is.
///
/// An RPO answers "if the process dies now, how much time's worth of writes is
/// gone". That is `now - F`, where F is the cloud-confirmed frontier: every
/// record at or before F is off-box, everything after it is not.
///
/// It is NOT `now - published_at`, which is what
/// `mutation_log_recovery_point_age_secs` carries. That field is the age of the
/// last publish EVENT, and a publish confirms whatever was sealed before it
/// started — so F is already behind when the publish runs, and the event age is
/// always the smaller number. Measured read-only on the live primary
/// 2026-09-06, both figures from one `lastdb status`:
///
/// ```text
/// at 10:16:02Z  rpo_secs=1027 (17m07s)  F=09:51:12Z  now - F = 24m50s
/// at 10:30:06Z  rpo_secs=278  (4m38s)   F=10:16:09Z  now - F = 13m57s
/// ```
///
/// A factor of three at 10:30, understated in the direction that hurts: an
/// operator reading the headline believes less data is at risk than is.
///
/// `lag == 0` is the one case that short-circuits. Lag is
/// `last_durable_frontier - published_frontier`, so zero means nothing sealed
/// locally is still unpublished and there is no unprotected data — an idle node
/// whose F stopped advancing hours ago is at RPO 0, not RPO 4h.
///
/// `None` where the window cannot be measured: a sequence-number F has no
/// wall-clock meaning, and a `now` that reads before F is a skewed clock, not a
/// zero window. Both fall back rather than inventing a number.
pub(super) fn rpo_secs_from_frontier(
    now_secs: u64,
    lag: Option<u64>,
    frontier_f: Option<u64>,
) -> Option<u64> {
    if lag == Some(0) {
        return Some(0);
    }
    let f_secs = confirmed_frontier_unix_secs(frontier_f)?;
    now_secs.checked_sub(f_secs)
}

/// The one crash-loss-window computation both status lines print.
///
/// The headline and the `Sync:` line used to read different fields for the same
/// quantity, which is how one line came to carry two clocks. They now share
/// this, so they cannot disagree.
///
/// Order of preference: the frontier-derived window, then the publish-event age
/// (the pre-existing signal, still right-shaped for a sequence-number F), then
/// the lag conversion. Falling back never invents a number the inputs do not
/// support.
pub(super) fn crash_loss_window_secs(now_secs: u64, sync: &SyncHealth) -> Option<u64> {
    let lag = wire_u64(&sync.mutation_log_lag);
    let published_through = wire_u64(&sync.mutation_log_published_through)
        .or_else(|| wire_u64(&sync.mutation_log_frontier_f));
    rpo_secs_from_frontier(now_secs, lag, published_through)
        .or_else(|| wire_u64(&sync.mutation_log_recovery_point_age_secs))
        .or_else(|| rpo_secs_from_log_lag(lag, published_through))
}

/// The watermark clause of the recoverability line.
///
/// "Cloud confirmed through T" is a claim about DATA: every record up to T is
/// off-box. Only the published frontier F carries that. `last_success_ts` is a
/// different clock — the wall time the last sync CYCLE succeeded — and a cycle
/// confirms whatever was sealed before it started, which is always older than
/// the cycle itself. Reading the cycle clock as the data watermark reports the
/// gap between them as durable when it is not.
///
/// Measured on the live primary 2026-09-06T10:16:02Z, one line, both halves:
///
/// ```text
/// Recoverability: RPO ~17m (cloud confirmed through 2026-09-06T10:13Z) · recording
/// ```
///
/// `last_success_ts` was 10:13Z (3 minutes old) while F stood at
/// 1788688272453930000 ns = 09:51Z (25 minutes old). The parenthetical told an
/// operator the loss window was 3 minutes; the number beside it said 17; the
/// watermark said 25. So the phrase now comes from F, and the cycle clock is
/// only mentioned where F is unknown — under its own name, never as a
/// confirmation.
pub(super) fn cloud_confirmed_phrase(sync: &SyncHealth, frontier_f: Option<u64>) -> String {
    if let Some(secs) = confirmed_frontier_unix_secs(frontier_f) {
        return format!("cloud confirmed through {}", format_unix_utc(secs));
    }
    if let Some(f) = frontier_f.filter(|f| *f > 0) {
        return format!("published F={f}");
    }
    if let Some(ts) = wire_u64(&sync.last_success_ts).filter(|ts| *ts >= 1_000_000_000) {
        return format!(
            "cloud-confirmed frontier unknown, last cycle ok {}",
            format_unix_utc(ts)
        );
    }
    "cloud-confirmed frontier unknown".to_string()
}

pub(super) fn durability_status_line(
    durability: &DurabilityHealth,
    sync_enabled: bool,
    log_plane_recording: bool,
) -> String {
    let health = durability_as_backup_health(durability);
    if log_plane_recording {
        fold_db::backup_durability::snapshot_line(&health)
    } else {
        fold_db::backup_durability::status_line(&health, sync_enabled)
    }
}

pub(super) fn backup_progress_status_line(backup: &BackupProgressHealth) -> Option<String> {
    // Not gated on `show_progress` alone: the failing / never-completed states
    // carry no chunk counts but are exactly what an operator must be told about.
    //
    // A live failure streak is its own reason to report, independent of the
    // chunk bar. `show_progress` is `!complete && chunks_total > 0`, so once
    // every sealed chunk is in cloud BOTH of the other two terms are false and
    // this gate suppressed the line unconditionally — including
    // `format_backup_progress_line`'s FAILING branch, which exists precisely
    // for the complete-but-not-committing case and says so in its own comment.
    // That made the streak fix (#1167) invisible through `lastdb status` in the
    // one state it was written for. Measured on the primary 2026-08-04: chunks
    // 13567/13567, 32 consecutive rejected CAS flips over 7h, no committed
    // manifest for 2d13h — and not one `Backup:` line.
    let consecutive_failures = wire_u64(&backup.consecutive_failures).unwrap_or(0);
    let worth_reporting =
        backup.show_progress || (backup.enabled && (!backup.complete || consecutive_failures > 0));
    if !worth_reporting {
        return None;
    }
    // Reconstruct fold_db snapshot shape for shared formatter.
    let snap = fold_db::backup_progress::BackupProgressSnapshot {
        enabled: backup.enabled,
        complete: backup.complete,
        percent: backup.percent,
        chunks_total: wire_u64(&backup.chunks_total).unwrap_or(0),
        chunks_present: wire_u64(&backup.chunks_present).unwrap_or(0),
        chunks_remaining: wire_u64(&backup.chunks_remaining).unwrap_or(0),
        bytes_remaining: wire_u64(&backup.bytes_remaining),
        elapsed_secs: wire_u64(&backup.elapsed_secs),
        eta_secs: wire_u64(&backup.eta_secs),
        ewma_upload_bps: backup.ewma_upload_bps,
        ewma_link_bps: backup.ewma_link_bps,
        ewma_overhead_secs: backup.ewma_overhead_secs,
        last_cycle_uploaded: wire_u64(&backup.last_cycle_uploaded).unwrap_or(0),
        last_cycle_already_present: wire_u64(&backup.last_cycle_already_present).unwrap_or(0),
        last_cycle_bytes_uploaded: wire_u64(&backup.last_cycle_bytes_uploaded).unwrap_or(0),
        last_cycle_failed: wire_u64(&backup.last_cycle_failed).unwrap_or(0),
        chunks_source_missing: wire_u64(&backup.chunks_source_missing).unwrap_or(0),
        chunks_unbackable_manifest: wire_u64(&backup.chunks_unbackable_manifest).unwrap_or(0),
        cas_counter: wire_u64(&backup.cas_counter),
        phase: backup.phase.clone(),
        show_progress: backup.show_progress,
        last_success_unix: wire_u64(&backup.last_success_unix),
        last_success_age_secs: wire_u64(&backup.last_success_age_secs),
        consecutive_failures: consecutive_failures as u32,
        last_error: backup.last_error.clone(),
        chunks_gained: wire_u64(&backup.chunks_gained).unwrap_or(0),
        chunks_erased: wire_u64(&backup.chunks_erased).unwrap_or(0),
        net_progressing: backup.net_progressing,
        recent_gained: wire_u64(&backup.recent_gained).unwrap_or(0),
        recent_erased: wire_u64(&backup.recent_erased).unwrap_or(0),
        net_progress_window_cycles: wire_u64(&backup.net_progress_window_cycles).unwrap_or(0),
        cycles_since_net_gain: wire_u64(&backup.cycles_since_net_gain).unwrap_or(0),
        target_generation: wire_u64(&backup.target_generation),
        last_publish_unix: wire_u64(&backup.last_publish_unix),
        last_publish_age_secs: wire_u64(&backup.last_publish_age_secs),
    };
    fold_db::backup_progress::format_backup_progress_line(&snap)
}
