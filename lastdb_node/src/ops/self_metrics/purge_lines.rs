use super::*;

/// Per-schema purge counters for `lastdb status` / `lastdb ops`.
///
/// Always emits at least one line. This lets an operator distinguish an empty
/// process-lifetime ledger from an omitted display section.
pub fn purge_stats_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    if snapshot.purge_stats.is_empty() {
        return vec!["Purge stats: (none since process start)".to_string()];
    }
    let mut names: Vec<&String> = snapshot.purge_stats.keys().collect();
    names.sort();
    let mut lines = Vec::with_capacity(names.len() + 1);
    lines.push(format!(
        "Purge stats: {} schema(s), process totals; delta the totals.",
        names.len()
    ));
    for name in names {
        let s = &snapshot.purge_stats[name];
        lines.push(format!(
            "  {name}: purges={} records_purged={} purge_target_slots={} last_purge_finished_unix_ms={}",
            s.purges,
            s.records_purged,
            s.purge_target_slots,
            s.last_purge_finished_unix_ms
        ));
    }
    lines
}

/// Aligned purge ledger for the default `lastdb ops` human view.
///
/// `lastdb status` keeps the compact key-value form above. The ops command can
/// spend more lines on a table and can add catalog labels for runtime IDs.
pub fn purge_stats_table_lines(
    snapshot: &StatusSnapshot,
    labels: &crate::request_telemetry::SchemaLabels,
) -> Vec<String> {
    use crate::request_telemetry::{aligned_table, human_age, human_count, schema_cell};

    if snapshot.purge_stats.is_empty() {
        return vec!["Purge stats: none since process start.".to_string()];
    }

    let mut names: Vec<&String> = snapshot.purge_stats.keys().collect();
    names.sort();
    let rows: Vec<Vec<String>> = names
        .iter()
        .map(|name| {
            let stats = &snapshot.purge_stats[*name];
            vec![
                schema_cell(Some(name), labels),
                human_count(stats.purges),
                human_count(stats.records_purged),
                human_count(stats.purge_target_slots),
                human_age(
                    fold_db::clock::unix_millis(),
                    stats.last_purge_finished_unix_ms,
                ),
            ]
        })
        .collect();
    let mut lines = vec![format!(
        "Purge stats: {} schema(s) — process lifetime totals",
        names.len()
    )];
    lines.extend(aligned_table(
        &["SCHEMA", "PURGES", "RECORDS", "TARGET SLOTS", "LAST"],
        &rows,
        &[false, true, true, true, true],
    ));
    lines
}

/// Render configured retention series and the counters that prove their cost.
pub fn local_retention_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    let health = &snapshot.local_retention;
    let last = health
        .last_sweep_unix_secs
        .map_or_else(|| "never".to_string(), |value| value.to_string());
    let mut lines = vec![format!(
        "Local retention: enabled={} policies={} passes={} rows_reaped={} last_sweep_unix_secs={} failures={} in_flight={} settled={}",
        health.enabled,
        health.policies.len(),
        health.passes,
        health.rows_reaped,
        last,
        health.failures,
        health.in_flight,
        health.settled,
    )];
    lines.push(format!(
        "  last_pass: finished_backlog={} keys_left={} cut={} schemas_failed={} missing_dropped={} schema_failures_total={}",
        health.last_pass.finished_backlog,
        health.last_pass.keys_left,
        health.last_pass.cut,
        health.last_pass.schemas_failed,
        health.last_pass.missing_dropped,
        health.schema_failures,
    ));
    if let Some(next) = health.next_attempt_unix_secs {
        lines.push(format!(
            "  sweep_waits: failed_passes_in_a_row={} next_attempt_unix_secs={next} (the wait doubles with each failed pass, up to {} s; a pass that succeeds ends it)",
            health.failures,
            super::ttl_backoff::TTL_BACKOFF_CAP.as_secs(),
        ));
    }
    if let Some(error) = health.policy_error.as_deref() {
        lines.push(format!("  policy_read_error={error}"));
    }
    let mut names: Vec<&String> = health.policies.keys().collect();
    names.sort();
    for name in names {
        let policy = &health.policies[name];
        let purge = snapshot.purge_stats.get(name).copied().unwrap_or_default();
        let partitions = if policy.hash_partitions.is_empty() {
            "-".to_string()
        } else {
            policy.hash_partitions.join(",")
        };
        lines.push(format!(
            "  {name}: ttl_seconds={} source={} hash_partitions={} records_reaped={} last_purge_finished_unix_ms={}",
            policy.ttl_seconds,
            policy.source.as_str(),
            partitions,
            purge.records_purged,
            purge.last_purge_finished_unix_ms,
        ));
    }
    lines
}
