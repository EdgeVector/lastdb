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
    lines
}

/// App/client by verb latency rollup lines for `lastdb ops --by-app`.
pub fn request_ops_app_verb_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    crate::request_telemetry::app_verb_lines(&snapshot.request_ops)
}
