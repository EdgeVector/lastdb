//! Small helpers that the `lastdb db` verbs share.

/// Format one per-pass progress line for a resumable, multi-pass admin verb.
///
/// `keys_walked` is cumulative on purpose: it is the resume position, and the
/// label says "so far". Every other counter is THIS pass's own delta. A running
/// total printed under a per-pass label rises forever, which reads as a plan
/// that keeps growing instead of draining. The cumulative figures are not lost:
/// every one of these verbs prints the full `totals` block after the loop.
pub(crate) fn pass_progress_line(pass_no: u64, keys_walked: u64, deltas: &[(&str, u64)]) -> String {
    let mut line = format!("  … pass {pass_no}: {keys_walked} keys walked so far");
    if !deltas.is_empty() {
        let body = deltas
            .iter()
            .map(|(label, n)| format!("+{n} {label}"))
            .collect::<Vec<_>>()
            .join(" / ");
        line.push_str(&format!(", {body} this pass"));
    }
    line
}

pub(crate) fn report_u64(report: &serde_json::Value, key: &str) -> u64 {
    report
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
}

pub(crate) fn report_bool(report: &serde_json::Value, key: &str) -> bool {
    report
        .get(key)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}
