//! Helpers that merge durable request-ops rollup rows into one snapshot.
use super::*;

use lastdb_node::request_telemetry::{OpAggregate, PhaseTimings};

/// Read one rollup row into a fresh aggregate plus its merge key.
/// Returns `None` for a row that lacks `fields`, `client`, or a known `kind`.
pub(super) fn aggregate_from_row(row: &serde_json::Value) -> Option<(String, OpAggregate)> {
    let fields = row.get("fields")?;
    let client = field_str(fields, "client")?;
    let kind = field_str(fields, "kind")
        .as_deref()
        .and_then(op_kind_from_str)?;
    let schema = field_str(fields, "schema").filter(|s| !s.is_empty());
    // Absent on rows written before route identity shipped, and on every
    // row whose schema already identifies it. Absent reads as "no route
    // attribution", so those rows merge and render exactly as before.
    let route = field_str(fields, "route").filter(|r| !r.is_empty());
    // Must agree with `OpAggregate::key` and `request_ops_rollup_key` on
    // row identity: a merge that collapses two routes the writer kept
    // apart hands the operator back the single unidentifiable row this
    // field exists to split.
    let key = format!(
        "{}\0{}\0{}\0{}",
        client,
        kind.as_str(),
        schema.as_deref().unwrap_or(""),
        route.as_deref().unwrap_or("")
    );
    let phase_sums = PhaseTimings::from_rollup_fields(fields);
    let agg = OpAggregate {
        client,
        kind,
        schema,
        route,
        count: field_u64(fields, "count").unwrap_or(0),
        sum_ms: field_u64(fields, "sum_ms").unwrap_or(0),
        max_ms: field_u64(fields, "max_ms").unwrap_or(0),
        last_ts_ms: field_u64(fields, "last_ts_ms").unwrap_or(0),
        error_count: field_u64(fields, "error_count").unwrap_or(0),
        // Absent on rows written before write-volume attribution shipped.
        sum_body_bytes: field_u64(fields, "sum_body_bytes").unwrap_or(0),
        max_body_bytes: field_u64(fields, "max_body_bytes").unwrap_or(0),
        // Durable rollup rows carry `error_count` without a status
        // breakdown; `error_detail` renders nothing for an empty one,
        // so `--since` output is unchanged.
        error_statuses: std::collections::BTreeMap::new(),
        error_statuses_overflow: 0,
        last_error_status: None,
        last_error_ts_ms: None,
        phase_sums,
        phase_count: field_u64(fields, "phase_count").unwrap_or(0),
        // Absent on rows written before the residual shipped. Reading 0 makes
        // `unphased_us` render nothing for those intervals, which is right —
        // the wall clock for their phased population was never recorded, and
        // a residual derived from `sum_ms` would be wrong, not approximate.
        phased_sum_ms: field_u64(fields, "phased_sum_ms").unwrap_or(0),
        // Absent on rows written before the field key work counters shipped;
        // both read 0 there. The record model also keeps both at 0, and the
        // text output no longer renders them.
        sum_field_keys_persisted: field_u64(fields, "sum_field_keys_persisted").unwrap_or(0),
        sum_field_key_store_commits: field_u64(fields, "sum_field_key_store_commits").unwrap_or(0),
        // Same additive migration: rows written before the resident commit
        // counters shipped read 0 here, which renders no ratio rather than a
        // wrong one.
        sum_resident_commits: field_u64(fields, "sum_resident_commits").unwrap_or(0),
        sum_resident_operations: field_u64(fields, "sum_resident_operations").unwrap_or(0),
    };
    Some((key, agg))
}

/// Fold `other` (one more row for the same key) into `agg`.
pub(super) fn merge_aggregate(agg: &mut OpAggregate, other: &OpAggregate) {
    agg.count = agg.count.saturating_add(other.count);
    agg.sum_ms = agg.sum_ms.saturating_add(other.sum_ms);
    agg.max_ms = agg.max_ms.max(other.max_ms);
    agg.last_ts_ms = agg.last_ts_ms.max(other.last_ts_ms);
    agg.error_count = agg.error_count.saturating_add(other.error_count);
    agg.sum_body_bytes = agg.sum_body_bytes.saturating_add(other.sum_body_bytes);
    agg.max_body_bytes = agg.max_body_bytes.max(other.max_body_bytes);
    // Interval SUMS merge by addition — same invariant the
    // sampler's delta writer relies on.
    agg.phase_sums.accumulate(other.phase_sums);
    agg.phase_count = agg.phase_count.saturating_add(other.phase_count);
    agg.phased_sum_ms = agg.phased_sum_ms.saturating_add(other.phased_sum_ms);
    agg.sum_field_keys_persisted = agg
        .sum_field_keys_persisted
        .saturating_add(other.sum_field_keys_persisted);
    agg.sum_field_key_store_commits = agg
        .sum_field_key_store_commits
        .saturating_add(other.sum_field_key_store_commits);
    agg.sum_resident_commits = agg
        .sum_resident_commits
        .saturating_add(other.sum_resident_commits);
    agg.sum_resident_operations = agg
        .sum_resident_operations
        .saturating_add(other.sum_resident_operations);
}

/// Rank merged aggregates into the four top-32 tables of a snapshot.
pub(super) fn rank_aggregates(
    aggregates: HashMap<String, OpAggregate>,
    row_count: usize,
    since_ms: u64,
    until_ms: u64,
) -> lastdb_node::request_telemetry::RequestOpsRollupSnapshot {
    // Same segregation the live snapshot applies: idle long-poll wait must not
    // outrank real work in a "top by total time" table an operator reads as
    // "biggest consumer".
    let (mut idle_wait, mut top_by_total_ms): (Vec<_>, Vec<_>) = aggregates
        .values()
        .cloned()
        .partition(|a| a.kind.is_idle_wait());
    top_by_total_ms.sort_by(|a, b| {
        b.sum_ms
            .cmp(&a.sum_ms)
            .then_with(|| b.max_ms.cmp(&a.max_ms))
            .then_with(|| b.count.cmp(&a.count))
    });
    top_by_total_ms.truncate(32);
    idle_wait.sort_by(|a, b| b.sum_ms.cmp(&a.sum_ms).then_with(|| b.count.cmp(&a.count)));
    idle_wait.truncate(32);

    let mut top_by_body_bytes: Vec<_> = aggregates
        .values()
        .filter(|a| !a.kind.is_idle_wait())
        .cloned()
        .collect();
    top_by_body_bytes.sort_by(|a, b| {
        b.sum_body_bytes
            .cmp(&a.sum_body_bytes)
            .then_with(|| b.max_body_bytes.cmp(&a.max_body_bytes))
            .then_with(|| b.count.cmp(&a.count))
    });
    top_by_body_bytes.truncate(32);
    if top_by_body_bytes
        .first()
        .is_none_or(|a| a.sum_body_bytes == 0)
    {
        top_by_body_bytes.clear();
    }

    let mut top_by_count: Vec<_> = aggregates.into_values().collect();
    top_by_count.sort_by(|a, b| {
        b.count
            .cmp(&a.count)
            .then_with(|| b.sum_ms.cmp(&a.sum_ms))
            .then_with(|| b.max_ms.cmp(&a.max_ms))
    });
    top_by_count.truncate(32);

    lastdb_node::request_telemetry::RequestOpsRollupSnapshot {
        top_by_total_ms,
        top_by_count,
        top_by_body_bytes,
        idle_wait,
        row_count,
        since_ms,
        until_ms,
    }
}
