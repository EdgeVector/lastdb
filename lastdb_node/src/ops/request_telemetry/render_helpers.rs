use super::*;

/// Coarse age for an error timestamp. Deliberately coarse: the question it
/// answers is "is this still happening or is it hours stale".
pub(crate) fn human_age(now_ms: u64, then_ms: u64) -> String {
    let secs = now_ms.saturating_sub(then_ms) / 1000;
    if secs < 60 {
        format!("{secs}s ago")
    } else if secs < 3600 {
        format!("{}m ago", secs / 60)
    } else if secs < 86_400 {
        format!("{}h ago", secs / 3600)
    } else {
        format!("{}d ago", secs / 86_400)
    }
}

/// Compact byte size for ops tables. Integers under 1 KiB stay exact so a
/// 12 B mutation is not rounded into noise; larger sizes use one decimal.
pub(super) fn human_bytes(n: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * 1024;
    if n < KIB {
        format!("{n}B")
    } else if n < MIB {
        #[allow(clippy::cast_precision_loss)]
        let kib = n as f64 / KIB as f64;
        format!("{kib:.1}KiB")
    } else {
        #[allow(clippy::cast_precision_loss)]
        let mib = n as f64 / MIB as f64;
        format!("{mib:.1}MiB")
    }
}

/// Readable schema names keyed by the runtime schema `name` that request
/// telemetry records. The CLI fills this from the cheap schema-catalog list.
pub type SchemaLabels = HashMap<String, String>;

pub(super) const OPS_SCHEMA_CELL_WIDTH: usize = 32;
pub(super) const OPS_CLIENT_CELL_WIDTH: usize = 24;
pub(super) const OPS_DETAIL_WIDTH: usize = 112;

pub(crate) fn human_count(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

pub(super) fn human_duration_ms(ms: u64) -> String {
    if ms < 1_000 {
        return format!("{ms}ms");
    }
    if ms < 60_000 {
        #[allow(clippy::cast_precision_loss)]
        return format!("{:.1}s", ms as f64 / 1_000.0);
    }
    if ms < 3_600_000 {
        return format!("{}m{:02}s", ms / 60_000, (ms / 1_000) % 60);
    }
    format!("{}h{:02}m", ms / 3_600_000, (ms / 60_000) % 60)
}

pub(crate) fn human_duration_us(us: u64) -> String {
    if us < 1_000 {
        return format!("{us}us");
    }
    if us < 1_000_000 {
        #[allow(clippy::cast_precision_loss)]
        return format!("{:.1}ms", us as f64 / 1_000.0);
    }
    human_duration_ms(us / 1_000)
}

pub(super) fn truncate_cell(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    if max <= 3 {
        return ".".repeat(max);
    }
    let mut out: String = value.chars().take(max - 3).collect();
    out.push_str("...");
    out
}

pub(super) fn looks_like_schema_hash(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

pub(super) fn short_schema_id(value: &str) -> String {
    if looks_like_schema_hash(value) {
        return value.chars().take(8).collect();
    }
    truncate_cell(value, OPS_SCHEMA_CELL_WIDTH)
}

pub(crate) fn schema_cell(schema: Option<&str>, labels: &SchemaLabels) -> String {
    let Some(schema) = schema.filter(|s| !s.is_empty()) else {
        return "-".to_string();
    };
    if let Some(label) = labels.get(schema).filter(|label| !label.trim().is_empty()) {
        let id = short_schema_id(schema);
        if label == schema {
            return id;
        }
        let suffix = format!(" [{id}]");
        let label_width = OPS_SCHEMA_CELL_WIDTH.saturating_sub(suffix.len());
        return format!("{}{}", truncate_cell(label, label_width), suffix);
    }
    short_schema_id(schema)
}

/// What identifies one ranked row: its schema, or — when it has none — the
/// route the node matched.
///
/// `schema_cell` alone renders `-` for every schema-less row, which is how a
/// `<client> / other / -` row came to be the top entry in a table an operator
/// reads to name a load. A route label is self-evidently not a schema (it is
/// the `DataRoute` variant name), so the two never read as the same thing in
/// one column.
pub(crate) fn identity_cell(
    schema: Option<&str>,
    route: Option<&str>,
    labels: &SchemaLabels,
) -> String {
    if schema.is_some_and(|s| !s.is_empty()) {
        return schema_cell(schema, labels);
    }
    match route.filter(|r| !r.is_empty()) {
        Some(route) => truncate_cell(route, OPS_SCHEMA_CELL_WIDTH),
        None => "-".to_string(),
    }
}

pub(super) fn error_cell(detail: &str) -> String {
    detail
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string()
}

pub(crate) fn aligned_table(headers: &[&str], rows: &[Vec<String>], right: &[bool]) -> Vec<String> {
    debug_assert_eq!(headers.len(), right.len());
    debug_assert!(rows.iter().all(|row| row.len() == headers.len()));
    let mut widths: Vec<usize> = headers.iter().map(|header| header.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }

    let render_row = |cells: &[String]| {
        let rendered: Vec<String> = cells
            .iter()
            .enumerate()
            .map(|(i, cell)| {
                if right[i] {
                    format!("{cell:>width$}", width = widths[i])
                } else {
                    format!("{cell:<width$}", width = widths[i])
                }
            })
            .collect();
        format!("  {}", rendered.join("  "))
    };

    let header_cells: Vec<String> = headers.iter().map(|s| (*s).to_string()).collect();
    let separator_cells: Vec<String> = widths.iter().map(|width| "-".repeat(*width)).collect();
    let mut lines = Vec::with_capacity(rows.len() + 2);
    lines.push(render_row(&header_cells));
    lines.push(format!("  {}", separator_cells.join("  ")));
    lines.extend(rows.iter().map(|row| render_row(row)));
    lines
}

pub(super) fn wrap_detail(prefix: &str, tokens: impl IntoIterator<Item = String>) -> Vec<String> {
    let continuation = " ".repeat(prefix.chars().count());
    let mut lines = Vec::new();
    let mut line = prefix.to_string();
    for token in tokens {
        let separator = usize::from(line.chars().count() > prefix.chars().count());
        if line.chars().count() + separator + token.chars().count() > OPS_DETAIL_WIDTH
            && line.chars().count() > prefix.chars().count()
        {
            lines.push(line);
            line = continuation.clone();
        }
        if line.chars().count() > prefix.chars().count() {
            line.push(' ');
        }
        line.push_str(&token);
    }
    if line.chars().count() > prefix.chars().count() {
        lines.push(line);
    }
    lines
}

pub(super) fn phase_tokens(phases: PhaseTimings) -> Vec<String> {
    phases
        .named_us()
        .into_iter()
        .filter(|(_, us)| *us > 0)
        .map(|(name, us)| format!("{name}={}", human_duration_us(us)))
        .collect()
}
