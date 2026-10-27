//! Delete and purge: point reads, then one planner batch.
//!
//! The caller owns the native write gate through a request scope. This code
//! reads the tips of its keys, plans every counter change in one place (the
//! planner in `db_operations::atom_store::counter`), and stages one batch: the
//! tip deletes, the counter changes, the bodies of atoms whose count reaches
//! zero, the headers, the delete barriers, and the ledger row. A purge writes no
//! barrier. A delete writes its barrier. Nothing here reads a whole schema, and
//! no lock beyond the gate is needed.

use super::removals::build_candidate_removals;
use super::targets::{plan_targets, LoadedTargets, Target, TargetPlan};
use super::*;
use crate::atom::delete_barrier::DeleteBarrier;
use crate::db_operations::atom_store::counter::{TipCommit, TipDebit};
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
    let loaded = target_plan.read(db_ops, options.storage_prefix).await?;
    let plan = if options.verb.is_delete() {
        plan_delete(db_ops, &loaded, &planned).await?
    } else {
        plan_purge(&loaded, mutations)
    };
    apply_plan(db_ops, schema_manager, schema_name, loaded, plan, options).await
}

/// The tips to remove and the delete barriers to write.
struct ErasePlan {
    remove: Vec<usize>,
    /// Indexes of the distinct keys that a purge left alone, because a tip of
    /// the key is newer than the cutoff of its mutation.
    spared: HashSet<usize>,
    /// Barrier rows of the delete verbs, with the displaced atom of each.
    barrier_rows: Vec<(String, serde_json::Value)>,
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

async fn apply_plan(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    loaded: LoadedTargets,
    plan: ErasePlan,
    options: EraseOptions<'_>,
) -> Result<BulkPurgeReport, SchemaError> {
    let LoadedTargets {
        schema,
        keys,
        targets,
    } = loaded;
    let tips_read = targets.len();
    let ErasePlan {
        remove,
        spared,
        barrier_rows,
    } = plan;
    let spared_keys: Vec<KeyValue> = spared.iter().map(|&index| keys[index].clone()).collect();
    let mut keep = vec![false; targets.len()];
    for index in remove {
        keep[index] = true;
    }
    let removed: Vec<Target> = targets
        .into_iter()
        .zip(keep)
        .filter_map(|(target, removed)| removed.then_some(target))
        .collect();
    let erased_keys: HashSet<usize> = removed.iter().map(|target| target.key_index).collect();
    tracing::debug!(
        schema = %schema_name,
        verb = options.verb.ledger_verb(),
        keys = keys.len(),
        tips_read = tips_read,
        tips_removed = removed.len(),
        barrier_rows = barrier_rows.len(),
        spared_keys = spared_keys.len(),
        "erase plan"
    );
    if removed.is_empty() && barrier_rows.is_empty() {
        return Ok(BulkPurgeReport {
            spared_keys,
            ..BulkPurgeReport::empty()
        });
    }
    let target_slots = removed.len();
    let removed_tip_keys: Vec<String> = removed
        .iter()
        .map(|target| target.candidate.mk_key.clone())
        .collect();
    let removals = build_candidate_removals(db_ops, removed).await?;
    let refs: Vec<_> = removals
        .iter()
        .map(|(uuid, data, slots, prefix)| {
            (uuid.as_str(), data, slots.as_slice(), prefix.as_deref())
        })
        .collect();
    let started = std::time::Instant::now();
    let commit = db_ops
        .atoms()
        .remove_molecules_keys_with_rows(&refs, barrier_rows)
        .await?;
    let batch_elapsed = started.elapsed();
    let tip_debits = removed_tip_debits(&removed_tip_keys, &commit);
    let report = BulkPurgeReport {
        records_purged: erased_keys.len(),
        atom_rows_deleted: commit.deleted.len(),
        target_slots: u64::try_from(target_slots).unwrap_or(u64::MAX),
        spared_keys,
    };
    // A schema refresh may persist meter state. Stage the final erased
    // aggregate after that refresh, so its header cannot overwrite our debit.
    store_and_reload_schema(db_ops, schema_manager, schema_name, schema).await?;
    if report.records_purged > 0 {
        record_erase(
            db_ops,
            schema_name,
            &keys,
            &erased_keys,
            &commit,
            &tip_debits,
            options,
        )
        .await?;
    }
    tracing::info!(
        schema = %schema_name,
        records = report.records_purged,
        atoms_deleted = report.atom_rows_deleted,
        batch_ms = u64::try_from(batch_elapsed.as_millis()).unwrap_or(u64::MAX),
        verb = options.verb.ledger_verb(),
        "erase staged"
    );
    Ok(report)
}

/// The audit row and the meter debit of one erase. Both join the batch.
async fn record_erase(
    db_ops: &Arc<DbOperations>,
    schema_name: &str,
    keys: &[KeyValue],
    erased_keys: &HashSet<usize>,
    commit: &TipCommit,
    tip_debits: &[TipDebit],
    options: EraseOptions<'_>,
) -> Result<(), SchemaError> {
    let descriptor = if erased_keys.len() == 1 {
        let index = *erased_keys.iter().next().expect("one erased key");
        describe_key(&keys[index])
    } else {
        format!("<bulk:{} keys>", erased_keys.len())
    };
    let live_head_debits = live_head_debits(db_ops, commit);
    let mut entry = crate::db_operations::AtomDeleteLedgerEntry::erasure(
        options.verb.ledger_verb(),
        options.caller,
        schema_name,
        &descriptor,
    );
    entry.atoms_deleted = commit.deleted.len() as u64;
    entry.records_converged = erased_keys.len() as u64;
    entry.tips_removed = erased_keys.len() as u64;
    entry.file_pointer_atoms_purged = commit
        .deleted
        .iter()
        .filter(|atom| !atom.blobs.is_empty())
        .count() as u64;
    let atoms = db_ops.atoms();
    atoms
        .stage_erase_meters(schema_name, &live_head_debits, tip_debits)
        .await?;
    atoms.record_atom_delete_audit(None, entry).await?;
    Ok(())
}

/// The meter debit of each deleted atom, from the size its counter held. An
/// atom of unknown size makes the meters incomplete, and a later repair
/// measures them.
fn live_head_debits(
    db_ops: &Arc<DbOperations>,
    commit: &TipCommit,
) -> Vec<(Option<String>, String, u64)> {
    let meters = db_ops.atoms().keep_small();
    let mut debits = Vec::new();
    for atom in &commit.deleted {
        match atom.size {
            Some(size) => debits.push((
                atom.source_schema.clone(),
                atom.uuid.clone(),
                size.atom_bytes(),
            )),
            None => meters.mark_incomplete("erase_deleted_atom_size_unknown"),
        }
    }
    debits
}

/// The meter debit of each removed tip: its stored row length and its atom's
/// size, both from the commit that removed it.
fn removed_tip_debits(removed_tip_keys: &[String], commit: &TipCommit) -> Vec<TipDebit> {
    removed_tip_keys
        .iter()
        .filter_map(|key| {
            let old = commit.previous.get(key)?;
            let source = commit.atom_sizes.get(&old.atom_uuid).map(|size| {
                crate::db_operations::MoleculeTipCounterSource {
                    atom_uuid: old.atom_uuid.clone(),
                    atom_value_bytes: size.atom_bytes(),
                    blob_reference_bytes: size.blob_bytes(),
                }
            });
            Some((key.clone(), old.row_bytes, source))
        })
        .collect()
}

/// Store the schema when it changed, then refresh the schema cache with the
/// metadata-only schema, so the next write reads its molecules from disk.
async fn store_and_reload_schema(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    mut schema: crate::schema::types::Schema,
) -> Result<(), SchemaError> {
    crate::fold_db_core::mutation_manager::helpers::clear_storage_prefix_on_schema(&mut schema);
    schema.sync_molecule_uuids();
    let unchanged = matches!(
        db_ops.schemas().get_schema(schema_name).await,
        Ok(Some(ref old)) if *old == schema
    );
    if !unchanged {
        db_ops.store_schema(schema_name, &schema).await?;
    }
    schema_manager.load_schema_internal(schema).await
}
