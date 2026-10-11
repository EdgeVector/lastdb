use super::*;

const CALLS_TABLE_HEADERS: [&str; 10] = [
    "#",
    "CLIENT",
    "KIND",
    "SCHEMA / ROUTE",
    "CALLS",
    "TOTAL",
    "AVG",
    "MAX",
    "ERR",
    "ERROR DETAIL",
];
const CALLS_TABLE_RIGHT: [bool; 10] = [
    true, false, false, false, true, true, true, true, true, false,
];

/// The calls/time/error table shared by the "top by total time" and
/// "top by call count" sections.
fn calls_table(aggregates: &[OpAggregate], labels: &SchemaLabels, now: u64) -> Vec<String> {
    let rows: Vec<Vec<String>> = aggregates
        .iter()
        .take(10)
        .enumerate()
        .map(|(i, a)| {
            vec![
                (i + 1).to_string(),
                truncate_cell(&a.client, OPS_CLIENT_CELL_WIDTH),
                a.kind.as_str().to_string(),
                identity_cell(a.schema.as_deref(), a.route.as_deref(), labels),
                human_count(a.count),
                human_duration_ms(a.sum_ms),
                human_duration_ms(a.avg_ms()),
                human_duration_ms(a.max_ms),
                human_count(a.error_count),
                error_cell(&a.error_detail(now)),
            ]
        })
        .collect();
    aligned_table(&CALLS_TABLE_HEADERS, &rows, &CALLS_TABLE_RIGHT)
}

fn body_volume_table(aggregates: &[OpAggregate], labels: &SchemaLabels) -> Vec<String> {
    let rows: Vec<Vec<String>> = aggregates
        .iter()
        .take(10)
        .enumerate()
        .map(|(i, a)| {
            vec![
                (i + 1).to_string(),
                truncate_cell(&a.client, OPS_CLIENT_CELL_WIDTH),
                a.kind.as_str().to_string(),
                identity_cell(a.schema.as_deref(), a.route.as_deref(), labels),
                human_bytes(a.sum_body_bytes),
                human_bytes(a.max_body_bytes),
                human_bytes(a.avg_body_bytes()),
                human_count(a.count),
                human_duration_ms(a.sum_ms),
            ]
        })
        .collect();
    aligned_table(
        &[
            "#",
            "CLIENT",
            "KIND",
            "SCHEMA / ROUTE",
            "TOTAL",
            "MAX",
            "AVG/CALL",
            "CALLS",
            "TIME",
        ],
        &rows,
        &[true, false, false, false, true, true, true, true, true],
    )
}

fn idle_wait_table(aggregates: &[OpAggregate]) -> Vec<String> {
    let rows: Vec<Vec<String>> = aggregates
        .iter()
        .take(10)
        .enumerate()
        .map(|(i, a)| {
            vec![
                (i + 1).to_string(),
                truncate_cell(&a.client, OPS_CLIENT_CELL_WIDTH),
                a.kind.as_str().to_string(),
                human_count(a.count),
                human_duration_ms(a.sum_ms),
                human_duration_ms(a.avg_ms()),
                human_duration_ms(a.max_ms),
            ]
        })
        .collect();
    aligned_table(
        &["#", "CLIENT", "KIND", "CALLS", "WAIT", "AVG", "MAX"],
        &rows,
        &[true, false, false, true, true, true, true],
    )
}

/// Human-readable durable rollup with optional schema catalog labels.
pub fn rollup_lines_with_schema_labels(
    snap: &RequestOpsRollupSnapshot,
    labels: &SchemaLabels,
) -> Vec<String> {
    let mut lines = vec![format!(
        "Request ops rollup: {} rows | window {}",
        human_count(snap.row_count as u64),
        human_duration_ms(snap.until_ms.saturating_sub(snap.since_ms))
    )];
    lines.push(format!("  Unix ms: {} -> {}", snap.since_ms, snap.until_ms));

    if snap.is_empty() {
        lines.push("  (no durable rollups in that window yet)".to_string());
        return lines;
    }

    // Persisted rollup rows carry only `error_count` today, so `error_detail`
    // renders nothing for them. Wiring it here anyway means the breakdown
    // appears the moment the sampler starts persisting it, with no second
    // edit to the renderer.
    let now = unix_millis();
    lines.push(String::new());
    lines.push("Top by total time — durable window; idle long-poll wait excluded".to_string());
    lines.push(
        "  Service time excludes socket queue wait. See the queue_wait phase below.".to_string(),
    );
    lines.extend(calls_table(&snap.top_by_total_ms, labels, now));

    if !snap.top_by_body_bytes.is_empty() {
        lines.push(String::new());
        lines.push(
            "Request body volume — durable window (caller input, not disk growth)".to_string(),
        );
        lines.extend(body_volume_table(&snap.top_by_body_bytes, labels));
    }

    if !snap.idle_wait.is_empty() {
        lines.push(String::new());
        lines.push("Idle long-poll wait — client-requested sleep, not node work".to_string());
        lines.extend(idle_wait_table(&snap.idle_wait));
    }

    if !snap.top_by_count.is_empty() {
        lines.push(String::new());
        lines.push("Top by call count — durable window".to_string());
        lines.extend(calls_table(&snap.top_by_count, labels, now));
    }

    // Merged interval phase SUMS from the persisted rollup rows — the same
    // table shape as the live view, so an operator pivots between `lastdb
    // ops` and `--since` without relearning the vocabulary. Absent until a
    // row in the window carries phase fields (rows written before phase
    // persistence read back as an absent phase set).
    lines.extend(phase_table_lines(&snap.top_by_total_ms, labels));

    lines
}

pub(super) fn sanitize_label(raw: &str, max_len: usize, empty: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return empty.to_string();
    }
    // Keep printable ASCII-ish labels; drop control chars.
    let cleaned: String = trimmed
        .chars()
        .map(|c| if c.is_control() || c == '\0' { '_' } else { c })
        .take(max_len)
        .collect();
    if cleaned.is_empty() {
        empty.to_string()
    } else {
        cleaned
    }
}
