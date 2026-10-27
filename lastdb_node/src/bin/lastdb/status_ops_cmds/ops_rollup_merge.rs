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
    let phase_sums = phase_sums_from_fields(fields);
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
        // Absent on rollup rows written before read-cost attribution shipped.
        sum_cold_shard_loads: field_u64(fields, "sum_cold_shard_loads").unwrap_or(0),
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
        // Absent on rows written before the molecule work counters shipped.
        // Both read 0 there, which `molecule_work_detail` renders as nothing —
        // the honest answer, since the ratio's denominator was never recorded.
        sum_molecules_persisted: field_u64(fields, "sum_molecules_persisted").unwrap_or(0),
        sum_molecule_store_commits: field_u64(fields, "sum_molecule_store_commits").unwrap_or(0),
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
    agg.sum_cold_shard_loads = agg
        .sum_cold_shard_loads
        .saturating_add(other.sum_cold_shard_loads);
    agg.sum_body_bytes = agg.sum_body_bytes.saturating_add(other.sum_body_bytes);
    agg.max_body_bytes = agg.max_body_bytes.max(other.max_body_bytes);
    // Interval SUMS merge by addition — same invariant the
    // sampler's delta writer relies on.
    agg.phase_sums.accumulate(other.phase_sums);
    agg.phase_count = agg.phase_count.saturating_add(other.phase_count);
    agg.phased_sum_ms = agg.phased_sum_ms.saturating_add(other.phased_sum_ms);
    agg.sum_molecules_persisted = agg
        .sum_molecules_persisted
        .saturating_add(other.sum_molecules_persisted);
    agg.sum_molecule_store_commits = agg
        .sum_molecule_store_commits
        .saturating_add(other.sum_molecule_store_commits);
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

// Rows written before phase persistence shipped lack these fields
// entirely; `unwrap_or(0)` reads them as an absent phase set — the
// "no phases reported" state, not a measurement of zero. The struct
// literal is deliberate: adding another phase to the model breaks
// this build until the merge learns to read it.
fn phase_sums_from_fields(fields: &serde_json::Value) -> PhaseTimings {
    PhaseTimings {
        queue_wait_us: field_u64(fields, "sum_queue_wait_us").unwrap_or(0),
        admission_wait_us: field_u64(fields, "sum_admission_wait_us").unwrap_or(0),
        parse_us: field_u64(fields, "sum_parse_us").unwrap_or(0),
        schema_resolve_us: field_u64(fields, "sum_schema_resolve_us").unwrap_or(0),
        validate_us: field_u64(fields, "sum_validate_us").unwrap_or(0),
        lock_wait_us: field_u64(fields, "sum_lock_wait_us").unwrap_or(0),
        purge_barrier_us: field_u64(fields, "sum_purge_barrier_us").unwrap_or(0),
        purge_plan_us: field_u64(fields, "sum_purge_plan_us").unwrap_or(0),
        purge_commit_us: field_u64(fields, "sum_purge_commit_us").unwrap_or(0),
        purge_materialize_us: field_u64(fields, "sum_purge_materialize_us").unwrap_or(0),
        purge_trace_us: field_u64(fields, "sum_purge_trace_us").unwrap_or(0),
        purge_retention_guard_us: field_u64(fields, "sum_purge_retention_guard_us").unwrap_or(0),
        purge_delete_us: field_u64(fields, "sum_purge_delete_us").unwrap_or(0),
        purge_finalize_us: field_u64(fields, "sum_purge_finalize_us").unwrap_or(0),
        molecule_gate_us: field_u64(fields, "sum_molecule_gate_us").unwrap_or(0),
        cas_precondition_us: field_u64(fields, "sum_cas_precondition_us").unwrap_or(0),
        count_us: field_u64(fields, "sum_count_us").unwrap_or(0),
        hydrate_us: field_u64(fields, "sum_hydrate_us").unwrap_or(0),
        hydrate_atoms_us: field_u64(fields, "sum_hydrate_atoms_us").unwrap_or(0),
        hydrate_format_us: field_u64(fields, "sum_hydrate_format_us").unwrap_or(0),
        hydrate_sort_us: field_u64(fields, "sum_hydrate_sort_us").unwrap_or(0),
        hydrate_filter_us: field_u64(fields, "sum_hydrate_filter_us").unwrap_or(0),
        annotate_us: field_u64(fields, "sum_annotate_us").unwrap_or(0),
        apply_us: field_u64(fields, "sum_apply_us").unwrap_or(0),
        // Rows written before the apply residual was carved carry the
        // whole bucket in `sum_apply_us` and nothing in these fields, so
        // `apply` appears to drop at the upgrade boundary rather than
        // the work having moved. Same additive-migration shape as the
        // persist and hydrate splits.
        apply_memory_us: field_u64(fields, "sum_apply_memory_us").unwrap_or(0),
        schema_load_us: field_u64(fields, "sum_schema_load_us").unwrap_or(0),
        idempotency_check_us: field_u64(fields, "sum_idempotency_check_us").unwrap_or(0),
        grouping_us: field_u64(fields, "sum_grouping_us").unwrap_or(0),
        restore_molecules_us: field_u64(fields, "sum_restore_molecules_us").unwrap_or(0),
        protein_sibling_fold_us: field_u64(fields, "sum_protein_sibling_fold_us").unwrap_or(0),
        spawn_indexing_us: field_u64(fields, "sum_spawn_indexing_us").unwrap_or(0),
        sync_uuids_us: field_u64(fields, "sum_sync_uuids_us").unwrap_or(0),
        schema_reload_us: field_u64(fields, "sum_schema_reload_us").unwrap_or(0),
        persist_us: field_u64(fields, "sum_persist_us").unwrap_or(0),
        // Rows written before the durable-store bucket was split carry
        // the whole bucket in `sum_persist_us` and nothing in these
        // three, so `persist` appears to drop at the upgrade boundary
        // while the sub-steps appear from nothing. That is the split
        // showing up in history, not a regression cured — the same
        // reading the change-feed split needs on `sum_change_record_us`.
        persist_molecules_us: field_u64(fields, "sum_persist_molecules_us").unwrap_or(0),
        persist_schema_us: field_u64(fields, "sum_persist_schema_us").unwrap_or(0),
        persist_idempotency_us: field_u64(fields, "sum_persist_idempotency_us").unwrap_or(0),
        flush_us: field_u64(fields, "sum_flush_us").unwrap_or(0),
        index_wait_us: field_u64(fields, "sum_index_wait_us").unwrap_or(0),
        change_record_us: field_u64(fields, "sum_change_record_us").unwrap_or(0),
        change_record_lock_wait_us: field_u64(fields, "sum_change_record_lock_wait_us")
            .unwrap_or(0),
        change_record_write_us: field_u64(fields, "sum_change_record_write_us").unwrap_or(0),
        response_envelope_us: field_u64(fields, "sum_response_envelope_us").unwrap_or(0),
        status_backup_us: field_u64(fields, "sum_status_backup_us").unwrap_or(0),
        status_durability_us: field_u64(fields, "sum_status_durability_us").unwrap_or(0),
        status_data_dir_us: field_u64(fields, "sum_status_data_dir_us").unwrap_or(0),
        status_request_ops_us: field_u64(fields, "sum_status_request_ops_us").unwrap_or(0),
    }
}
