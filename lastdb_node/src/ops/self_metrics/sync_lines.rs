use super::*;

/// Compact request-ops offender lines for `lastdb status`.
///
/// The status command prints only six lines. Keep the parser-stable app rows
/// here so four offenders remain visible without the table headings from the
/// full `lastdb ops` view.
pub fn request_ops_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    let snap = &snapshot.request_ops;
    if snap.top_by_total_ms.is_empty() && snap.idle_wait.is_empty() {
        return crate::request_telemetry::snapshot_lines(snap);
    }
    let mut lines = vec![format!(
        "Request ops: {} lifetime samples | recent ring {}/{}",
        crate::request_telemetry::human_count(snap.sample_count),
        snap.recent.len().min(snap.ring_capacity),
        snap.ring_capacity
    )];
    lines.extend(
        crate::request_telemetry::app_verb_lines(snap)
            .into_iter()
            .take(5),
    );
    lines.extend(crate::request_telemetry::persist_lane_lines(snap));
    lines
}

/// App/client by verb latency rollup lines for `lastdb ops --by-app`.
pub fn request_ops_app_verb_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    crate::request_telemetry::app_verb_lines(&snapshot.request_ops)
}

/// Operator-facing line for adaptive upload throttle reason + source.
///
/// Always emits so `lastdb status` / `lastdb ops` name the brake even when it
/// is idle (`throttle_source=none`). JSON already carries the full
/// `upload_policy` snapshot; this is the text plane that used to omit it.
pub(super) fn upload_policy_status_line(sync: &SyncHealth) -> String {
    let Some(policy) = sync.upload_policy.as_ref() else {
        return "Upload policy: (unavailable)".to_string();
    };
    let mode = policy
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let budget = policy
        .get("budget_bytes")
        .and_then(Value::as_u64)
        .map_or_else(|| "unknown".to_string(), |n| n.to_string());
    let concurrency = policy
        .get("concurrency")
        .and_then(Value::as_u64)
        .map_or_else(|| "unknown".to_string(), |n| n.to_string());
    // Fine-grained reason is optional in JSON when idle; treat missing as none.
    let reason = policy
        .get("throttle_reason")
        .and_then(Value::as_str)
        .unwrap_or("none");
    // Coarse source is always serialized (default `none`).
    let source = policy
        .get("throttle_source")
        .and_then(Value::as_str)
        .unwrap_or("none");
    format!(
        "Upload policy: mode={mode} budget_bytes={budget} concurrency={concurrency} \
         throttle_reason={reason} throttle_source={source}"
    )
}

/// Text for the Sync line's last_success field.
///
/// A missed probe has no new timestamp. Render last-known time with a probe-miss
/// clause, or `unavailable`. `never` is reserved for a complete snapshot whose
/// durability cursor is actually empty.
pub(super) fn last_success_status_text(sync: &SyncHealth) -> String {
    match (
        wire_u64(&sync.last_success_ts),
        sync_snapshot_complete(sync),
    ) {
        // Human, not a raw epoch. Until 2026-09-06 the healthy path printed
        // the integer and only the probe-missed path below formatted it, so
        // `lastdb status` rendered the same instant two ways depending on
        // whether a probe had missed. Four `cloud-sync-health-fix` fires filed
        // that as a defect (`papercut-lastdb-status-last-success-prints-unix-
        // epoch-20260904`) and each one had to run `date -u -r` by hand.
        (Some(ts), true) => format_unix_utc(ts),
        (Some(ts), false) => {
            let age = unix_secs().saturating_sub(ts);
            format!(
                "{} (probe missed, {} stale)",
                format_unix_utc(ts),
                format_age_secs(age)
            )
        }
        (None, true) => "never".to_string(),
        (None, false) => "unavailable".to_string(),
    }
}

pub(super) fn sync_status_line(now_secs: u64, sync: &SyncHealth) -> String {
    // lint:fn-size-ok moved verbatim from self_metrics.rs; splitting this function is separate work.
    if !sync.enabled {
        return "Sync: disabled".to_string();
    }
    let staging_count = wire_u64(&sync.durable_outbox_count)
        .or_else(|| wire_u64(&sync.pending_count))
        .map_or_else(|| "unknown".to_string(), |n| n.to_string());
    let staging_max = wire_u64(&sync.durable_outbox_max).map_or_else(
        || "unlimited".to_string(),
        |n| {
            if n == 0 {
                "unlimited".to_string()
            } else {
                n.to_string()
            }
        },
    );
    let upload_queue_count =
        wire_u64(&sync.upload_queue_count).map_or_else(|| "unknown".to_string(), |n| n.to_string());
    let upload_queue_max = wire_u64(&sync.upload_queue_max).map_or_else(
        || "unlimited".to_string(),
        |n| {
            if n == 0 {
                "unlimited".to_string()
            } else {
                n.to_string()
            }
        },
    );
    let mut line = format!(
        "Sync: state={} staging={}/{} upload_queue={}/{} local_writable={} degraded={} last_success={}",
        sync.state.as_deref().unwrap_or("unknown"),
        staging_count,
        staging_max,
        upload_queue_count,
        upload_queue_max,
        sync.local_writable
            .map_or_else(|| "unknown".to_string(), |b| b.to_string()),
        sync.sync_degraded
            .map_or_else(|| "unknown".to_string(), |b| b.to_string()),
        last_success_status_text(sync)
    );
    if let Some(recording) = sync.recording_local_changes {
        line.push_str(&format!(" recording={recording}"));
    }
    // Continuous mutation-log plane: log lag + F are the primary health story
    // (not sealed-chunk remaining %). Surface when the plane is active.
    if sync.mutation_log_active == Some(true) {
        let lag = wire_u64(&sync.mutation_log_lag)
            .map_or_else(|| "unknown".to_string(), |n| n.to_string());
        let f = wire_u64(&sync.mutation_log_frontier_f)
            .map_or_else(|| "unknown".to_string(), |n| n.to_string());
        let published_through = wire_u64(&sync.mutation_log_published_through)
            .map_or_else(|| "unknown".to_string(), |n| n.to_string());
        // `rpo_secs` keeps its name and gains the correct meaning: the same
        // crash loss window the Recoverability headline prints. The routine
        // `cloud-sync-health-fix` parses this token and reports it as RPO, so
        // renaming it would have moved the wrong number to a new name and left
        // the reader on the old one.
        let rpo_secs = crash_loss_window_secs(now_secs, sync)
            .map_or_else(|| "unknown".to_string(), |n| n.to_string());
        // The publish-event age is a real signal — it says the uploader is
        // alive — and it is what the `mutation_log_lag` degrade trigger reads.
        // It keeps a home here, under a name that says which clock it is.
        let publish_age_secs = wire_u64(&sync.mutation_log_recovery_point_age_secs)
            .map_or_else(|| "unknown".to_string(), |n| n.to_string());
        line.push_str(&format!(
            " log_lag={lag} F={f} published_through={published_through} rpo_secs={rpo_secs} publish_age_secs={publish_age_secs}"
        ));
        if let Some(registered) = sync.mutation_log_capture_registered {
            line.push_str(&format!(" capture_registered={registered}"));
        }
        if let Some(writer) = sync
            .mutation_log_writer_id
            .as_deref()
            .filter(|w| !w.is_empty())
        {
            line.push_str(&format!(" writer_id={writer}"));
        }
        // Peer apply is the download side of this plane. Rendered whenever the
        // plane is active, including at zero: "0 applied" on a node whose peers
        // are publishing is the signal, and hiding it behind `> 0` would make
        // the broken case indistinguishable from a single-writer node.
        if let Some(applied) = sync.mutation_log_peer_segments_applied {
            let records = sync.mutation_log_peer_records_applied.unwrap_or(0);
            line.push_str(&format!(
                " peer_applied={{segments={applied} records={records}}}"
            ));
        }
        if let Some(q) = sync.mutation_log_records_quarantined.filter(|n| *n > 0) {
            line.push_str(&format!(" quarantined={q}"));
            if let Some(reason) = sync
                .mutation_log_last_quarantine_reason
                .as_deref()
                .filter(|r| !r.is_empty())
            {
                line.push_str(&format!(" quarantine_reason={reason}"));
            }
        }
    }
    if let Some(expired) = sync.sync_off_grace_expired.filter(|b| *b) {
        let _ = expired;
        line.push_str(" sync_off_grace=expired");
        if let Some(strategy) = sync.reenable_strategy.as_deref() {
            line.push_str(&format!(" reenable={strategy}"));
        }
    } else if let Some(strategy) = sync.reenable_strategy.as_deref() {
        line.push_str(&format!(" reenable={strategy}"));
    }
    if let Some(dl) = &sync.last_download {
        let bytes = dl
            .get("bytes_downloaded")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let replayed = dl
            .get("entries_replayed")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let deferred = dl
            .get("entries_health_deferred")
            .or_else(|| dl.get("entries_deferred"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let raw_deferred = dl
            .get("entries_deferred")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(deferred);
        line.push_str(&format!(
            " download_last={{replayed={replayed} bytes={} deferred={deferred}}}",
            format_bytes(bytes)
        ));
        if raw_deferred > deferred {
            line.push_str(&format!(" self_echo_deferred={raw_deferred}"));
        }
    }
    // Liveness before the error text: a failure streak is the signal that
    // survives an empty outbox, and it is what distinguishes a live breakage
    // from the stale `last_error` string that once masked a 36 h backup stall.
    if let Some(reasons) = sync.degraded_reasons.as_ref().filter(|r| !r.is_empty()) {
        line.push_str(&format!(" degraded_reasons={}", reasons.join(",")));
    }
    if let Some(failures) = wire_u64(&sync.consecutive_sync_failures).filter(|n| *n > 0) {
        line.push_str(&format!(" consecutive_failures={failures}"));
        if let Some(since) = wire_u64(&sync.failing_since) {
            line.push_str(&format!(
                " failing_for={}",
                format_age_secs(unix_secs().saturating_sub(since))
            ));
        }
    }
    if let Some(err) = &sync.last_error {
        line.push_str(&format!(" last_error={err}"));
        // An undated error reads as current. Age it so an hours-stale
        // transient failure cannot pose as the live explanation of health.
        if let Some(at) = wire_u64(&sync.last_error_at) {
            line.push_str(&format!(
                " last_error_age={}",
                format_age_secs(unix_secs().saturating_sub(at))
            ));
        }
    }
    line
}

pub(super) fn capture_status_line(sync: &SyncHealth) -> Option<String> {
    let capture = sync.capture.as_ref()?.as_object()?;
    let marker_presence = match sync.capture_reexport_pending_known_nonempty {
        None => "unknown",
        Some(true) => "present",
        Some(false) => "empty_after_full_scan",
    };
    let marker_estimate = wire_u64(&sync.capture_reexport_pending_count_estimate)
        .map_or_else(|| "unavailable".to_string(), |count| count.to_string());
    let number = |key: &str| {
        capture
            .get(key)
            .and_then(Value::as_u64)
            .map_or_else(|| "unavailable".to_string(), |value| value.to_string())
    };
    let text = |key: &str| {
        capture
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or("none")
            .to_string()
    };
    let automatic = capture
        .get("automatic_compactions")
        .and_then(Value::as_object)
        .map(|planes| {
            planes
                .iter()
                .map(|(plane, state)| {
                    let trigger = state
                        .get("last_trigger")
                        .and_then(Value::as_str)
                        .unwrap_or("never");
                    let at = state
                        .get("last_compacted_at_unix_s")
                        .and_then(Value::as_u64)
                        .map_or_else(|| "never".to_string(), |value| value.to_string());
                    let max_bytes = state
                        .get("configured_max_bytes")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    let overhang = state
                        .get("overhang_bytes")
                        .and_then(Value::as_u64)
                        .map_or_else(|| "none".to_string(), |value| value.to_string());
                    let bps = state
                        .get("overhang_bps")
                        .and_then(Value::as_u64)
                        .map_or_else(|| "none".to_string(), |value| value.to_string());
                    let above = state
                        .get("above_trigger")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    let dead = state
                        .get("dead_bytes")
                        .and_then(Value::as_u64)
                        .map_or_else(|| "none".to_string(), |value| value.to_string());
                    let reclaim = state
                        .get("reclaimable_estimate_bytes")
                        .and_then(Value::as_u64)
                        .map_or_else(|| "none".to_string(), |value| value.to_string());
                    format!(
                        "{plane}:{trigger}@{at}/overhang={overhang}/dead={dead}/reclaim={reclaim}/bps={bps}/above={above}/max={max_bytes}"
                    )
                })
                .collect::<Vec<_>>()
                .join(",")
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "none".to_string());
    Some(format!(
        "Capture: pin_log_live_keys={} pin_log_disk_bytes={} \
         pin_log_last_compact_trigger={} capture_envelope_version={} \
         capture_physical_fallback_records={} capture_physical_catalog_records={} \
         reexport_disk_bytes={} reexport_pending={marker_presence} \
         reexport_pending_estimate={marker_estimate} automatic_compactions={automatic}",
        number("pin_log_live_keys"),
        number("pin_log_disk_bytes"),
        text("pin_log_last_compact_trigger"),
        text("capture_envelope_version"),
        number("capture_physical_fallback_records"),
        number("capture_physical_catalog_records"),
        number("reexport_disk_bytes"),
    ))
}

/// One human-readable line for cloud backup billable honesty.
///
/// Always educates when reclaimable > 0 (orphans waiting for auto-GC or a
/// manual `backup-gc --execute`); still useful at zero reclaimable so billed
/// never looks like an unexplained multiple of local data_dir size.
pub(super) fn backup_storage_status_line(s: &BackupStorageHealth) -> String {
    let reclaimable = gauge_or_zero(&s.reclaimable_bytes);
    let mut line = format!(
        "Backup storage: referenced {} · billed {} · reclaimable {}",
        format_bytes(gauge_or_zero(&s.referenced_bytes)),
        format_bytes(gauge_or_zero(&s.billed_bytes)),
        format_bytes(reclaimable),
    );
    if reclaimable > 0 {
        line.push_str(" (orphans; auto-GC after tip or `lastdb cloud backup-gc --execute`)");
    }
    line
}

/// At most one in-flight list_objects for status footprint fill.
pub(super) static BACKUP_STORAGE_REFRESHING: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Fire-and-forget: list + keep-set → cache on the sync engine.
///
/// Status never awaits this. Failures are silent (no keep-set / cloud down);
/// GC paths still populate the cache on their own successful list.
pub(super) fn schedule_backup_storage_refresh(host: &Host) {
    let Some(engine) = host.db.sync_engine() else {
        return;
    };
    if engine.backup_storage_footprint_snapshot().is_some() {
        return;
    }
    if BACKUP_STORAGE_REFRESHING
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }
    let cache_path = crate::host::backup_manifest_cache_path(&host.home);
    // lint:spawn-bare-ok process-lifetime status backup-storage cache fill —
    // must not block /api/status; engine cache is best-effort.
    tokio::spawn(async move {
        let clear = || BACKUP_STORAGE_REFRESHING.store(false, Ordering::SeqCst);
        let Ok(bytes) = tokio::fs::read(&cache_path).await else {
            clear();
            return;
        };
        let Ok(manifest) =
            serde_json::from_slice::<fold_db::storage::laststore::BackupManifest>(&bytes)
        else {
            clear();
            return;
        };
        let _ = engine
            .refresh_backup_storage_footprint(std::slice::from_ref(&manifest))
            .await;
        clear();
    });
}

// lint:file-size-ok moved verbatim from self_metrics.rs; cohesive unit, split further in a later pass
