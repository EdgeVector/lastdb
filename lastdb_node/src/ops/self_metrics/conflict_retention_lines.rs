use super::*;

/// `lastdb status` line for the retention of kept conflict records.
///
/// The janitor releases a kept conflict record at the end of its retention
/// window. The line shows the window, the janitor's last run, the queue depth
/// and the number of queue buckets when the status was sampled, the slot drains
/// that wait, the failed wakes of the janitor, and what the janitor and the
/// purge released. Always emits one
/// line, so a daemon that predates the field reads differently from one with
/// an empty queue.
pub fn conflict_retention_status_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    let Some(report) = snapshot.conflict_retention.as_ref() else {
        return vec!["Conflict retention: unsupported by daemon (no retention report)".to_string()];
    };
    let janitor = if report.janitor_interval_ms == 0 {
        "janitor off".to_string()
    } else {
        format!("janitor every {} ms", report.janitor_interval_ms)
    };
    let last = if report.last_run_unix_ms == 0 {
        "no run yet".to_string()
    } else {
        let at = chrono::DateTime::from_timestamp_millis(
            i64::try_from(report.last_run_unix_ms).unwrap_or(0),
        )
        .map_or_else(
            || report.last_run_unix_ms.to_string(),
            |at| at.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        );
        format!(
            "last run {at} released {} (runs={})",
            report.last_run_released, report.runs
        )
    };
    let depth = match report.queue_depth {
        None => "unknown".to_string(),
        Some(depth) if report.queue_depth_capped => format!("{depth}+"),
        Some(depth) => depth.to_string(),
    };
    let buckets = report
        .queue_buckets
        .map_or_else(String::new, |buckets| format!(" in {buckets} buckets"));
    let mut line = format!(
        "Conflict retention: window {} s; {janitor}; {last}; queue depth {depth}{buckets}; released by janitor {}, by purge {}",
        report.window_secs, report.released_by_janitor, report.released_by_purge,
    );
    if report.pending_drains > 0 {
        line.push_str(&format!("; slot drains pending {}", report.pending_drains));
    }
    if report.consecutive_failures > 0 {
        line.push_str(&format!(
            "; janitor failed {} wakes in a row",
            report.consecutive_failures
        ));
        if report.next_retry_unix_ms > 0 {
            let at = chrono::DateTime::from_timestamp_millis(
                i64::try_from(report.next_retry_unix_ms).unwrap_or(0),
            )
            .map_or_else(
                || report.next_retry_unix_ms.to_string(),
                |at| at.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            );
            line.push_str(&format!(", next try {at}"));
        }
    }
    if report.purge_capped_slots > 0 {
        line.push_str(&format!(
            "; slots whose records a purge left to the window {}",
            report.purge_capped_slots
        ));
    }
    if report.undecodable_rows > 0 {
        line.push_str(&format!("; undecodable rows {}", report.undecodable_rows));
    }
    if let Some(error) = &report.last_error {
        line.push_str(&format!("; last error: {error}"));
    }
    vec![line]
}
