//! Helpers that merge durable request-ops rollup rows into one snapshot.
use super::*;

use lastdb_node::request_telemetry::{
    required_rollup_str, required_rollup_u64, OpAggregate, PhaseTimings,
};

/// Read one rollup record into a fresh aggregate plus its merge key.
///
/// Every field is required. A record without `fields`, or with a missing or
/// wrongly typed field, is an error that names the field and the record's
/// `sample_id`. `schema` and `route` store "" for "none" and read back as
/// `None`.
pub(super) fn aggregate_from_row(row: &serde_json::Value) -> Result<(String, OpAggregate), String> {
    let record_key = || {
        row.get("key")
            .map_or_else(|| "<no key>".to_string(), ToString::to_string)
    };
    let fields = row.get("fields").ok_or_else(|| {
        format!(
            "request-ops rollup record {}: no `fields` object",
            record_key()
        )
    })?;
    let sample_id = fields
        .get("sample_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            format!(
                "request-ops rollup record {}: field `sample_id` is missing or not a string",
                record_key()
            )
        })?;
    let client = required_rollup_str(fields, "client", sample_id)?.to_string();
    let kind_name = required_rollup_str(fields, "kind", sample_id)?;
    let kind = op_kind_from_str(kind_name).ok_or_else(|| {
        format!(
            "request-ops rollup record {sample_id}: field `kind` has unknown value {kind_name:?}"
        )
    })?;
    let schema = identity_or_none(required_rollup_str(fields, "schema", sample_id)?);
    // "" on every record whose schema already identifies it.
    let route = identity_or_none(required_rollup_str(fields, "route", sample_id)?);
    // Must agree with `OpAggregate::key` and `request_ops_rollup_key` on
    // record identity: a merge that collapses two routes the writer kept
    // apart hands the operator back the single unidentifiable row this
    // field exists to split.
    let key = format!(
        "{}\0{}\0{}\0{}",
        client,
        kind.as_str(),
        schema.as_deref().unwrap_or(""),
        route.as_deref().unwrap_or("")
    );
    let number = |name: &str| required_rollup_u64(fields, name, sample_id);
    let agg = OpAggregate {
        client,
        kind,
        schema,
        route,
        count: number("count")?,
        sum_ms: number("sum_ms")?,
        max_ms: number("max_ms")?,
        last_ts_ms: number("last_ts_ms")?,
        error_count: number("error_count")?,
        sum_body_bytes: number("sum_body_bytes")?,
        max_body_bytes: number("max_body_bytes")?,
        // Durable rollup records carry `error_count` without a status
        // breakdown; `error_detail` renders nothing for an empty one.
        error_statuses: std::collections::BTreeMap::new(),
        error_statuses_overflow: 0,
        last_error_status: None,
        last_error_ts_ms: None,
        phase_sums: PhaseTimings::from_rollup_fields(fields, sample_id)?,
        phase_count: number("phase_count")?,
        phased_sum_ms: number("phased_sum_ms")?,
        sum_resident_commits: number("sum_resident_commits")?,
        sum_resident_operations: number("sum_resident_operations")?,
    };
    Ok((key, agg))
}

/// `schema` and `route` store "" for "none".
fn identity_or_none(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
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
