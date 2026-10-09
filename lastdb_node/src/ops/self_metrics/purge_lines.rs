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
        "Purge stats: {} schema(s), process totals. exclusive_hold_us is purge critical-section time; delta the totals.",
        names.len()
    ));
    for name in names {
        let s = &snapshot.purge_stats[name];
        lines.push(format!(
            "  {name}: purges={} records_purged={} exclusive_hold_us={} schema_barrier_acquisitions={} purge_target_slots={} purge_candidate_atoms={} purge_reverse_edge_reads={} last_purge_finished_unix_ms={}",
            s.purges,
            s.records_purged,
            s.exclusive_hold_us,
            s.schema_barrier_acquisitions,
            s.purge_target_slots,
            s.purge_candidate_atoms,
            s.purge_reverse_edge_reads,
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
    use crate::request_telemetry::{
        aligned_table, human_age, human_count, human_duration_us, schema_cell,
    };

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
                human_count(stats.schema_barrier_acquisitions),
                human_count(stats.purge_target_slots),
                human_count(stats.purge_candidate_atoms),
                human_count(stats.purge_reverse_edge_reads),
                human_duration_us(stats.exclusive_hold_us),
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
        &[
            "SCHEMA",
            "PURGES",
            "RECORDS",
            "BARRIERS",
            "TARGET SLOTS",
            "CANDIDATES",
            "EDGE READS",
            "CRITICAL SECTION",
            "LAST",
        ],
        &rows,
        &[false, true, true, true, true, true, true, true, true],
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
            "  {name}: ttl_seconds={} hash_partitions={} records_reaped={} exclusive_hold_us={} exclusive_hold_semantics=purge_critical_section last_purge_finished_unix_ms={}",
            policy.ttl_seconds,
            partitions,
            purge.records_purged,
            purge.exclusive_hold_us,
            purge.last_purge_finished_unix_ms,
        ));
    }
    lines
}
