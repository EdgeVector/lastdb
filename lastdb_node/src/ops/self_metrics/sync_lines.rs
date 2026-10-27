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

fn count_text(value: Option<u64>) -> String {
    value.map_or_else(|| "unknown".to_string(), |n| n.to_string())
}

/// A zero limit means unlimited.
fn limit_text(value: Option<u64>) -> String {
    match value {
        None | Some(0) => "unlimited".to_string(),
        Some(n) => n.to_string(),
    }
}

pub(super) fn sync_status_line(now_secs: u64, sync: &SyncHealth) -> String {
    if !sync.enabled {
        return "Sync: disabled".to_string();
    }
    let staging_count = wire_u64(&sync.durable_outbox_count)
        .or_else(|| wire_u64(&sync.pending_count))
        .map_or_else(|| "unknown".to_string(), |n| n.to_string());
    let staging_max = limit_text(wire_u64(&sync.durable_outbox_max));
    let upload_queue_count = count_text(wire_u64(&sync.upload_queue_count));
    let upload_queue_max = limit_text(wire_u64(&sync.upload_queue_max));
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
    append_mutation_log_fields(&mut line, now_secs, sync);
    append_reenable_fields(&mut line, sync);
    append_last_download(&mut line, sync);
    append_failure_fields(&mut line, sync);
    line
}

/// Continuous mutation-log plane: log lag + F are the primary health story
/// (not sealed-chunk remaining %). Surface when the plane is active.
fn append_mutation_log_fields(line: &mut String, now_secs: u64, sync: &SyncHealth) {
    if sync.mutation_log_active != Some(true) {
        return;
    }
    let lag = count_text(wire_u64(&sync.mutation_log_lag));
    let f = count_text(wire_u64(&sync.mutation_log_frontier_f));
    let published_through = count_text(wire_u64(&sync.mutation_log_published_through));
    // `rpo_secs` keeps its name and gains the correct meaning: the same
    // crash loss window the Recoverability headline prints. The routine
    // `cloud-sync-health-fix` parses this token and reports it as RPO, so
    // renaming it would have moved the wrong number to a new name and left
    // the reader on the old one.
    let rpo_secs = count_text(crash_loss_window_secs(now_secs, sync));
    // The publish-event age is a real signal — it says the uploader is
    // alive — and it is what the `mutation_log_lag` degrade trigger reads.
    // It keeps a home here, under a name that says which clock it is.
    let publish_age_secs = count_text(wire_u64(&sync.mutation_log_recovery_point_age_secs));
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

fn append_reenable_fields(line: &mut String, sync: &SyncHealth) {
    if sync.sync_off_grace_expired == Some(true) {
        line.push_str(" sync_off_grace=expired");
    }
    if let Some(strategy) = sync.reenable_strategy.as_deref() {
        line.push_str(&format!(" reenable={strategy}"));
    }
}

fn append_last_download(line: &mut String, sync: &SyncHealth) {
    let Some(dl) = &sync.last_download else {
        return;
    };
    let field = |key: &str| dl.get(key).and_then(serde_json::Value::as_u64);
    let bytes = field("bytes_downloaded").unwrap_or(0);
    let replayed = field("entries_replayed").unwrap_or(0);
    let deferred = field("entries_health_deferred")
        .or_else(|| field("entries_deferred"))
        .unwrap_or(0);
    let raw_deferred = field("entries_deferred").unwrap_or(deferred);
    line.push_str(&format!(
        " download_last={{replayed={replayed} bytes={} deferred={deferred}}}",
        format_bytes(bytes)
    ));
    if raw_deferred > deferred {
        line.push_str(&format!(" self_echo_deferred={raw_deferred}"));
    }
}

/// Liveness before the error text: a failure streak is the signal that
/// survives an empty outbox, and it is what distinguishes a live breakage
/// from the stale `last_error` string that once masked a 36 h backup stall.
fn append_failure_fields(line: &mut String, sync: &SyncHealth) {
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
}
