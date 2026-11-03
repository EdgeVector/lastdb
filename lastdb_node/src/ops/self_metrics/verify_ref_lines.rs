use super::*;

/// `lastdb status` line for `lastdb db verify-refs`: the last complete run, and
/// a run in progress. The verifier keeps its result in memory, so a restart
/// clears the line.
pub fn verify_refs_status_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    let summary = &snapshot.verify_refs;
    let mut line = match summary.last.as_ref() {
        None => "Verify refs: not run since the daemon started (lastdb db verify-refs)".to_string(),
        Some(last) => {
            let m = &last.mismatches;
            format!(
                "Verify refs: last run {} scope={} complete pages={} faults={} (queue_rows={})",
                last.finished_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                last.scope.as_deref().unwrap_or("personal"),
                last.pages,
                last.mismatches_total,
                m.queue_entry_without_directory,
            )
        }
    };
    if let Some(running) = summary.running.as_ref() {
        line.push_str(&format!(
            "; run in progress phase={:?} pages={}",
            running.phase, running.pages
        ));
    }
    vec![line]
}
