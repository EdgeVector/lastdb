//! Delete and purge: point reads, then one planner batch.
//!
//! The caller owns the native write gate through a request scope. This code
//! reads the tips of its keys, plans every counter change in one place (the
//! planner in `db_operations::atom_store::counter`), and stages one batch: the
//! tip deletes, the counter changes, the bodies of atoms whose count reaches
//! zero, the headers, the delete barriers, and the ledger row. A purge writes no
//! barrier. A delete writes its barrier. Nothing here reads a whole schema, and
//! no lock beyond the gate is needed.

use super::targets::{plan_targets, LoadedTargets, TargetPlan};
use super::*;
use crate::atom::delete_barrier::DeleteBarrier;
use crate::schema::types::Mutation;

/// What one erase needs besides the keys.
#[derive(Clone, Copy)]
pub(in crate::fold_db_core) struct EraseOptions<'a> {
    pub(in crate::fold_db_core) storage_prefix: Option<&'a str>,
    pub(in crate::fold_db_core) verb: HardEraseVerb,
    pub(in crate::fold_db_core) caller: &'static str,
}

/// Erase the keys of `mutations` from one schema in the current native scope.
///
/// A key that has no tip is skipped. The caller checks presence first when the
/// verb must refuse a missing key. A purge spares a key that has a tip written
/// at or after the cutoff of its mutation. A delete spares the tips that its
/// barrier does not cover.
pub(in crate::fold_db_core) async fn erase_keys(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    mutations: &[Mutation],
    options: EraseOptions<'_>,
) -> Result<BulkPurgeReport, SchemaError> {
    if mutations.is_empty() {
        return Ok(BulkPurgeReport::empty());
    }
    if schema_manager.get_schema_state_cached(schema_name)? == crate::schema::SchemaState::Blocked {
        return Err(SchemaError::Blocked(format!(
            "Schema '{schema_name}' is blocked and cannot be erased"
        )));
    }
    let keys: Vec<KeyValue> = mutations
        .iter()
        .map(|mutation| mutation.key_value.clone())
        .collect();
    let target_plan = plan_targets(
        db_ops,
        schema_manager,
        schema_name,
        &keys,
        options.storage_prefix,
    )?;
    // The barriers of a delete are planned from metadata. The exact tip locks
    // come before the first read, so a memory Delete that registered its
    // barrier orders against this write.
    let planned = if options.verb.is_delete() {
        plan_normal_delete_barriers(db_ops, &target_plan.schema, mutations)?
    } else {
        Vec::new()
    };
    let guards = lock_barrier_tips(db_ops, &planned).await;
    let result = erase_planned(
        db_ops,
        schema_manager,
        schema_name,
        (target_plan, planned.clone()),
        mutations,
        options,
    )
    .await;
    db_ops.atoms().clear_pending_delete_barriers(&planned);
    drop(guards);
    result
}

async fn erase_planned(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    (target_plan, planned): (TargetPlan, Vec<DeleteBarrier>),
    mutations: &[Mutation],
    options: EraseOptions<'_>,
) -> Result<BulkPurgeReport, SchemaError> {
    let alternate_schema = target_plan.schema.clone_for_read_shared();
    let alternate_keys: Vec<_> = mutations
        .iter()
        .map(|mutation| mutation.key_value.clone())
        .collect();
    let alternate_before = if alternate_schema.alternate_keys.is_empty() {
        Vec::new()
    } else {
        db_ops
            .atoms()
            .alternate_fields(&alternate_schema, &alternate_keys, options.storage_prefix)
            .await?
    };
    let loaded = target_plan.read(db_ops, options.storage_prefix).await?;
    let plan = if options.verb.is_delete() {
        plan_delete(db_ops, &loaded, &planned).await?
    } else {
        plan_purge(&loaded, mutations)
    };
    let result =
        super::erase_apply::apply_plan(db_ops, schema_manager, schema_name, loaded, plan, options)
            .await?;
    db_ops
        .atoms()
        .stage_alternate_changes(
            &alternate_schema,
            &alternate_keys,
            &alternate_before,
            options.storage_prefix,
        )
        .await?;
    Ok(result)
}

/// The tips to remove and the delete barriers to write.
pub(super) struct ErasePlan {
    pub(super) remove: Vec<usize>,
    /// Indexes of the distinct keys that a purge left alone, because a tip of
    /// the key is newer than the cutoff of its mutation.
    pub(super) spared: HashSet<usize>,
    /// Barrier rows of the delete verbs, with the displaced atom of each.
    pub(super) barrier_rows: Vec<(String, serde_json::Value)>,
}

/// A purge removes every tip, except the tips of a key that was rewritten at or
/// after the cutoff of its mutation.
fn plan_purge(loaded: &LoadedTargets, mutations: &[Mutation]) -> ErasePlan {
    let cutoffs = key_cutoffs(&loaded.keys, mutations);
    let spared: HashSet<usize> = loaded
        .targets
        .iter()
        .filter(|target| {
            cutoffs
                .get(&target.key_index)
                .is_some_and(|cutoff| target.record.entry.written_at >= *cutoff)
        })
        .map(|target| target.key_index)
        .collect();
    ErasePlan {
        remove: (0..loaded.targets.len())
            .filter(|&index| !spared.contains(&loaded.targets[index].key_index))
            .collect(),
        spared,
        barrier_rows: Vec::new(),
    }
}

/// The cutoff of each distinct key, in unix nanoseconds, when the mutation has one.
fn key_cutoffs(keys: &[KeyValue], mutations: &[Mutation]) -> HashMap<usize, u64> {
    let mut by_key: HashMap<KeyValue, u64> = HashMap::new();
    for mutation in mutations {
        if let Some(cutoff) = mutation.erase_tips_before_nanos {
            by_key.insert(mutation.key_value.clone(), cutoff);
        }
    }
    keys.iter()
        .enumerate()
        .filter_map(|(index, key)| by_key.get(key).map(|cutoff| (index, *cutoff)))
        .collect()
}

/// A delete removes the tips that its barrier covers, and it writes the
/// barrier of every candidate key, so a later replayed Put cannot revive it.
async fn plan_delete(
    db_ops: &Arc<DbOperations>,
    loaded: &LoadedTargets,
    planned: &[DeleteBarrier],
) -> Result<ErasePlan, SchemaError> {
    let scope = loaded
        .targets
        .first()
        .and_then(|target| target.candidate.storage_prefix.as_deref());
    let converge = db_ops
        .atoms()
        .delete_converge_targets(planned, scope)
        .await?;
    let mut barrier_rows = Vec::with_capacity(converge.len());
    let mut blocked: HashSet<String> = HashSet::new();
    for (barrier, tip) in converge {
        if let Some(tip) = &tip {
            if barrier.blocks_tip(&tip.entry) {
                blocked.insert(barrier.mk_key.clone());
            }
        }
        let key = crate::atom::delete_barrier::delete_barrier_key(barrier.mk_key.as_bytes());
        let value = serde_json::to_value(&barrier).map_err(|error| {
            SchemaError::InvalidData(format!("serialize Delete barrier: {error}"))
        })?;
        barrier_rows.push((key, value));
    }
    Ok(ErasePlan {
        remove: (0..loaded.targets.len())
            .filter(|&index| blocked.contains(&loaded.targets[index].candidate.mk_key))
            .collect(),
        // A surviving field keeps the logical key visible. Its retention age
        // and search record must survive even when other fields were erased.
        spared: loaded
            .targets
            .iter()
            .filter(|target| !blocked.contains(&target.candidate.mk_key))
            .map(|target| target.key_index)
            .collect(),
        barrier_rows,
    })
}

/// Hold the exact tip locks that the delete publication uses. They order this
/// write against a memory Delete that registered its barrier before the lane
/// ran. A purge takes none.
async fn lock_barrier_tips(
    db_ops: &Arc<DbOperations>,
    planned: &[DeleteBarrier],
) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
    if planned.is_empty() {
        return Vec::new();
    }
    let keys: Vec<String> = planned
        .iter()
        .map(|barrier| barrier.mk_key.clone())
        .collect();
    db_ops.atoms().lock_tip_commits(&keys).await
}
