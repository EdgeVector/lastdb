use super::*;

/// `lastdb status` line for `lastdb db verify-refs`. The `atom_counters`
/// object keeps its name in `/api/status`.
pub fn verify_refs_status_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    let Some(report) = snapshot.atom_counters.as_ref() else {
        return vec!["Verify refs: unsupported by daemon (no verifier report)".to_string()];
    };
    vec![verify_refs_line(report)]
}

/// The `Verify refs` line: the last complete run of `lastdb db verify-refs`, and
/// a run in progress. The verifier keeps its result in memory, so a restart
/// clears the line.
fn verify_refs_line(report: &fold_db::db_operations::AtomCounterReport) -> String {
    let Some(summary) = report.verify_refs.as_ref() else {
        return "Verify refs: unsupported by daemon (no verifier summary)".to_string();
    };
    let mut line = match summary.last.as_ref() {
        None => "Verify refs: not run since the daemon started (lastdb db verify-refs)".to_string(),
        Some(last) => {
            let m = &last.mismatches;
            format!(
                "Verify refs: last run {} scope={} complete pages={} faults={} (queue_rows={} blob_counters={})",
                last.finished_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                last.scope.as_deref().unwrap_or("personal"),
                last.pages,
                last.mismatches_total,
                m.queue_entry_without_directory,
                m.blob.total(),
            )
        }
    };
    if let Some(running) = summary.running.as_ref() {
        line.push_str(&format!(
            "; run in progress phase={:?} pages={}",
            running.phase, running.pages
        ));
    }
    line
}

/// `lastdb status` line for the blob counters and the dwell queue.
///
/// The totals are process-lifetime. `queue_depth` and `queue_buckets` are the
/// size of the dwell queue, counted by its bucket directory with bounded reads;
/// a `+` says the count stopped at its cap. `next_due_in` is the earliest time
/// at which the oldest bucket can hold a due entry. A blob counter fault
/// (underflow, dangling removal) keeps the blob and raises `suspect`; the recent
/// faults name the blobs by reference. A pass that failed shows its error and
/// the time to the next try.
pub fn blob_counter_status_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    let Some(report) = snapshot.blob_counters.as_ref() else {
        return vec!["Blob counters: unsupported by daemon (no blob counter report)".to_string()];
    };
    let now_ms = fold_db::clock::unix_millis();
    let until = |at: Option<u64>| {
        at.map_or_else(
            || "none".to_string(),
            |at| format!("{}s", at.saturating_sub(now_ms) / 1000),
        )
    };
    let since = |at: Option<u64>| {
        at.map_or_else(
            || "none".to_string(),
            |at| format!("{}s", now_ms.saturating_sub(at) / 1000),
        )
    };
    let mut head = format!(
        "Blob counters: created={} uploads_registered={} zero_reached={} dwell_cancelled={} blobs_deleted={} stale_queue_entries={} underflows={} dangling_removals={} late_references={} unreadable_counters={}; queue_depth={}{} queue_buckets={} next_due_in={} last_pass_ago={} dwell={}s passes={}; totals are process-lifetime (reset on daemon restart)",
        report.counters_created,
        report.uploads_registered,
        report.zero_reached,
        report.dwell_cancelled,
        report.blobs_deleted,
        report.stale_queue_entries,
        report.underflows,
        report.dangling_removals,
        report.late_references,
        report.unreadable_counters,
        report.queue_depth,
        if report.queue_depth_capped { "+" } else { "" },
        report
            .queue_buckets
            .map_or_else(|| "unknown".to_string(), |buckets| buckets.to_string()),
        until(report.next_due_unix_ms),
        since(report.last_pass_unix_ms),
        report.dwell_secs,
        report.dwell_passes,
    );
    if report.net_sweeps > 0 {
        head.push_str(&format!(
            " net_sweeps={} buckets_repaired={}",
            report.net_sweeps, report.buckets_repaired
        ));
    }
    if report.pass_failures > 0 {
        head.push_str(&format!(
            " pass_failures={} last_error={:?} retry_in={}s",
            report.pass_failures,
            report.last_error.as_deref().unwrap_or("none"),
            report.retry_in_secs.unwrap_or(0),
        ));
    }
    if !report.recent_faults.is_empty() {
        head.push_str(&format!(
            " recent_faults=[{}]",
            report.recent_faults.join(", ")
        ));
    }
    vec![head]
}
