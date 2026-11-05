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
pub(super) fn calls_table(
    aggregates: &[OpAggregate],
    labels: &SchemaLabels,
    now: u64,
) -> Vec<String> {
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

pub(super) fn body_volume_table(aggregates: &[OpAggregate], labels: &SchemaLabels) -> Vec<String> {
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

pub(super) fn idle_wait_table(aggregates: &[OpAggregate]) -> Vec<String> {
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
