use super::*;

/// `lastdb status` line for the atom counter planner.
///
/// The totals are process-lifetime. A counter fault (underflow, counter
/// absent, dangling removal) keeps the body and raises `suspect`; the recent
/// faults name the atoms. Always emits one line, so a node that has written
/// nothing and a daemon that predates the field read differently.
pub fn atom_counter_status_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    let Some(report) = snapshot.atom_counters.as_ref() else {
        return vec!["Atom counters: unsupported by daemon (no planner report)".to_string()];
    };
    let mut head = format!(
        "Atom counters: bodies_created={} bodies_deleted={} underflows={} counters_absent={} dangling_removals={}; process-lifetime totals (reset on daemon restart)",
        report.bodies_created,
        report.bodies_deleted,
        report.underflows,
        report.counters_absent,
        report.dangling_removals,
    );
    if !report.recent_faults.is_empty() {
        head.push_str(&format!(
            " recent_faults=[{}]",
            report.recent_faults.join(", ")
        ));
    }
    vec![head]
}

/// `lastdb status` line for the blob counters and the dwell queue.
///
/// The totals are process-lifetime. `queue_depth` is what the last dwell pass
/// read from the first page of the queue; a `+` says the page was full. A blob
/// counter fault (underflow, dangling removal) keeps the blob and raises
/// `suspect`; the recent faults name the blobs by reference.
pub fn blob_counter_status_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    let Some(report) = snapshot.blob_counters.as_ref() else {
        return vec!["Blob counters: unsupported by daemon (no blob counter report)".to_string()];
    };
    let now_ms = fold_db::clock::unix_millis();
    let in_secs = |at: Option<u64>| {
        at.map_or_else(
            || "none".to_string(),
            |at| format!("{}s", (at as i64 - now_ms as i64) / 1000),
        )
    };
    let mut head = format!(
        "Blob counters: created={} uploads_registered={} zero_reached={} dwell_cancelled={} blobs_deleted={} stale_queue_entries={} underflows={} dangling_removals={}; queue_depth={}{} next_due_in={} last_pass_ago={} dwell={}s passes={}; totals are process-lifetime (reset on daemon restart)",
        report.counters_created,
        report.uploads_registered,
        report.zero_reached,
        report.dwell_cancelled,
        report.blobs_deleted,
        report.stale_queue_entries,
        report.underflows,
        report.dangling_removals,
        report.queue_depth,
        if report.queue_depth_capped { "+" } else { "" },
        in_secs(report.next_due_unix_ms),
        in_secs(report.last_pass_unix_ms).trim_start_matches('-'),
        report.dwell_secs,
        report.dwell_passes,
    );
    if !report.recent_faults.is_empty() {
        head.push_str(&format!(
            " recent_faults=[{}]",
            report.recent_faults.join(", ")
        ));
    }
    vec![head]
}
