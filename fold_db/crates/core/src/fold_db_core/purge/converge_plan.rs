//! Target planning for a delete converge; see `converge.rs`.

use super::converge_candidates::{
    build_candidate_removals, load_candidate_tips, protect_retained_history, trace_candidate_tips,
};
use super::guarded_steps::removed_tip_keys_of;
use super::*;

/// Load the schema metadata for a delete converge and validate every key.
/// Returns the prefixed schema and the mutation keys in input order.
pub(super) fn load_converge_schema(
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    mutations: &[crate::schema::types::Mutation],
    storage_prefix: Option<&str>,
) -> Result<(crate::schema::types::Schema, Vec<KeyValue>), SchemaError> {
    let mut schema = schema_manager
        .get_schema_metadata(schema_name)?
        .ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "Schema '{schema_name}' not found for delete converge"
            ))
        })?;
    crate::fold_db_core::mutation_manager::helpers::apply_storage_prefix_to_schema(
        &mut schema,
        storage_prefix,
    );

    let keys: Vec<KeyValue> = mutations
        .iter()
        .map(|mutation| mutation.key_value.clone())
        .collect();
    for key in &keys {
        validate_purge_key_shape(&schema, schema_name, key)?;
    }
    Ok((schema, keys))
}

/// Flush the Delete barriers for the planned targets and return the tip keys
/// whose current tip a barrier blocks (the tips that converge may remove).
/// The memory Delete registered its pending barrier before this lane ran.
/// The durable lock covers barrier flush and source removal. A newer Put can
/// publish in memory while its durable batch waits for this lock.
pub(super) async fn flush_converge_barriers(
    db_ops: &Arc<DbOperations>,
    planned_barriers: &[crate::atom::delete_barrier::DeleteBarrier],
    storage_prefix: Option<&str>,
) -> Result<HashSet<String>, SchemaError> {
    let mut removable_tip_keys = HashSet::new();
    let mut barrier_items = Vec::new();
    let targets = db_ops
        .atoms()
        .delete_converge_targets(planned_barriers, storage_prefix)
        .await?;
    for (mut barrier, tip) in targets {
        if let Some(tip) = &tip {
            barrier.displaced_atom_uuid = Some(tip.entry.atom_uuid.clone());
            if barrier.blocks_tip(&tip.entry) {
                removable_tip_keys.insert(barrier.mk_key.clone());
            }
        }
        let barrier_key =
            crate::atom::delete_barrier::delete_barrier_key(barrier.mk_key.as_bytes());
        let value = serde_json::to_value(&barrier).map_err(|error| {
            SchemaError::InvalidData(format!("serialize Delete barrier: {error}"))
        })?;
        barrier_items.push((barrier_key, value));
    }
    delete_barrier_flush::flush_delete_barriers(db_ops, barrier_items).await?;
    Ok(removable_tip_keys)
}

/// Archived tv: chain keys and atom-ref edge keys traced for removable tips.
pub(super) struct ConvergeTrace {
    /// Indexes of `keys` that have a current atom on a removable tip.
    pub(super) traced: HashSet<usize>,
    pub(super) tip_version_keys: Vec<String>,
    pub(super) atom_ref_edge_keys: Vec<String>,
    pub(super) atom_ref_v2_edge_keys: Vec<String>,
}

/// Per-step timings of the destructive stage, for the final log line.
#[derive(Default)]
pub(super) struct ConvergeTimings {
    pub(super) refresh: std::time::Duration,
    pub(super) trace: std::time::Duration,
    pub(super) remove: std::time::Duration,
    pub(super) measure: std::time::Duration,
    pub(super) ledger: std::time::Duration,
    pub(super) batch: std::time::Duration,
    pub(super) schema: std::time::Duration,
    pub(super) flush: std::time::Duration,
}

/// Everything the destructive stage needs after the targets are resolved.
pub(super) struct ConvergePlan {
    pub(super) schema: crate::schema::types::Schema,
    /// Deduplicated keys that have a current atom on a removable tip.
    pub(super) keys: Vec<KeyValue>,
    pub(super) target_slots: usize,
    pub(super) ledger_descriptor: String,
    pub(super) owned_for_batch: Vec<OwnedMoleculeKeyRemoval>,
    pub(super) removed_tip_keys: Vec<String>,
    pub(super) tip_debits: Vec<(String, u64)>,
    pub(super) tip_version_keys: Vec<String>,
    pub(super) atom_ref_edge_keys: Vec<String>,
    pub(super) atom_ref_v2_edge_keys: Vec<String>,
    pub(super) timings: ConvergeTimings,
}

/// Resolve the converge targets under the tip locks. `None` means nothing is
/// left to remove; the caller then clears the barriers and returns empty.
pub(super) async fn prepare_converge_plan(
    db_ops: &Arc<DbOperations>,
    schema: crate::schema::types::Schema,
    all_keys: &[KeyValue],
    removable_tip_keys: &HashSet<String>,
) -> Result<Option<ConvergePlan>, SchemaError> {
    // Same dedup rationale as `purge_records_bulk`: a batch is a SET of
    // records, and a repeated key must not trip a stale count.
    let mut seen: HashSet<String> = HashSet::new();
    let deduped: Vec<KeyValue> = all_keys
        .iter()
        .filter(|k| seen.insert(describe_key(k)))
        .cloned()
        .collect();
    let keys: &[KeyValue] = &deduped;

    // Read only the finite exact readable candidates under their existing tip locks.
    let step_started = std::time::Instant::now();
    let (candidate_tips, retained_tips) =
        load_candidate_tips(db_ops, &schema, keys, removable_tip_keys).await?;
    let refresh_elapsed = step_started.elapsed();

    let step_started = std::time::Instant::now();
    let mut removable_trace = trace_candidate_tips(db_ops, &candidate_tips).await?;
    let retained_trace = trace_candidate_tips(db_ops, &retained_tips).await?;
    protect_retained_history(&mut removable_trace, retained_trace);
    let ConvergeTrace {
        traced,
        tip_version_keys,
        atom_ref_edge_keys,
        atom_ref_v2_edge_keys,
    } = removable_trace;
    let trace_elapsed = step_started.elapsed();

    let keys_present: Vec<KeyValue> = keys
        .iter()
        .enumerate()
        .filter(|(idx, _)| traced.contains(idx))
        .map(|(_, k)| k.clone())
        .collect();
    if keys_present.is_empty() {
        return Ok(None);
    }
    let keys: &[KeyValue] = &keys_present;
    let target_slots = candidate_tips.len();

    // Same shape the inline path fingerprints: one key by name, a batch by
    // size. Computed on `keys_present`, so the row describes what is actually
    // being removed rather than what was asked for.
    let ledger_descriptor = if keys.len() == 1 {
        describe_key(&keys[0])
    } else {
        format!("<bulk:{} keys>", keys.len())
    };

    let owned_for_batch = build_candidate_removals(db_ops, candidate_tips).await?;
    let removed_tip_keys = removed_tip_keys_of(&owned_for_batch);
    let tip_debits = db_ops
        .atoms()
        .keep_small_measure_tip_keys(&removed_tip_keys)
        .await;
    let measure_elapsed = step_started.elapsed();
    Ok(Some(ConvergePlan {
        schema,
        target_slots,
        keys: keys_present,
        ledger_descriptor,
        owned_for_batch,
        removed_tip_keys,
        tip_debits,
        tip_version_keys,
        atom_ref_edge_keys,
        atom_ref_v2_edge_keys,
        timings: ConvergeTimings {
            refresh: refresh_elapsed,
            trace: trace_elapsed,
            measure: measure_elapsed,
            ..ConvergeTimings::default()
        },
    }))
}

/// Load the schema, lock and flush the Delete barriers, and resolve the plan.
/// The plan is `None` when no tip can be removed. The returned guards hold the
/// exact tip locks; the caller releases them after source removal.
pub(super) async fn lock_and_plan_converge(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    mutations: &[crate::schema::types::Mutation],
    storage_prefix: Option<&str>,
) -> Result<
    (
        Vec<crate::atom::delete_barrier::DeleteBarrier>,
        Vec<tokio::sync::OwnedMutexGuard<()>>,
        Option<ConvergePlan>,
    ),
    SchemaError,
> {
    let (schema, keys) =
        load_converge_schema(schema_manager, schema_name, mutations, storage_prefix)?;
    let planned_barriers = plan_normal_delete_barriers(db_ops, &schema, mutations)?;
    let tip_keys: Vec<String> = planned_barriers
        .iter()
        .map(|barrier| barrier.mk_key.clone())
        .collect();
    let tip_guards = db_ops.atoms().lock_tip_commits(&tip_keys).await;
    let removable_tip_keys =
        flush_converge_barriers(db_ops, &planned_barriers, storage_prefix).await?;
    let plan = if removable_tip_keys.is_empty() {
        None
    } else {
        prepare_converge_plan(db_ops, schema, &keys, &removable_tip_keys).await?
    };
    Ok((planned_barriers, tip_guards, plan))
}
