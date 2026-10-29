use super::erase::{EraseOptions, ErasePlan};
use super::removals::build_candidate_removals;
use super::targets::{LoadedTargets, Target};
use super::*;
use crate::db_operations::atom_store::counter::{TipCommit, TipDebit};

pub(super) async fn apply_plan(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    loaded: LoadedTargets,
    plan: ErasePlan,
    options: EraseOptions<'_>,
) -> Result<BulkPurgeReport, SchemaError> {
    let (history, mut erased_keys) =
        super::atom_history::release_for_erase(db_ops, &loaded, &plan.spared, options).await?;
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
    erased_keys.extend(removed.iter().map(|target| target.key_index));
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
    if removed.is_empty() && barrier_rows.is_empty() && erased_keys.is_empty() {
        return Ok(BulkPurgeReport {
            spared_keys,
            atom_rows_deleted: history.deleted.len(),
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
    let mut commit = db_ops
        .atoms()
        .remove_molecules_keys_with_rows(&refs, barrier_rows, options.verb.is_delete())
        .await?;
    commit.absorb(history);
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
