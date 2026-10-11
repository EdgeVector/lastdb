//! Moved verbatim out of the parent module; see the parent for context.

use super::*;

use super::guarded_prepare::*;
use super::guarded_steps::*;

/// The single destructive implementation shared by logical API-key purge and
/// exact storage-slot purge. Everything above this boundary is target planning;
/// everything below it is reachability-guarded erasure.
#[allow(clippy::too_many_arguments)]
pub(super) async fn execute_guarded_purge(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    mut schema: crate::schema::types::Schema,
    storage_slots: &[PlannedStorageSlot],
    candidate_atoms: HashSet<String>,
    history_keys: Vec<String>,
    tip_version_keys: Vec<String>,
    atom_ref_edge_keys: Vec<String>,
    atom_ref_v2_edge_keys: Vec<String>,
    history_referenced_by_retained: HashSet<String>,
    records_purged: usize,
    ledger_descriptor: &str,
    verb: HardEraseVerb,
    acct: &mut PurgeCommitAccounting,
    evict_resident: bool,
    reachability: PurgeReachability,
    precomputed_retained: Option<HashSet<String>>,
) -> Result<BulkPurgeReport, SchemaError> {
    let target_slot_ids: HashSet<StorageSlotIdentity> = storage_slots
        .iter()
        .map(PlannedStorageSlot::identity)
        .collect();
    // Resolve resident aliases before the first destructive write. Encoding
    // failure must refuse the purge while every durable row is still intact.
    let resident_api_keys = resident_api_keys_for_storage_slots(db_ops.as_ref(), &target_slot_ids)?;
    let inputs = GuardedRemovalInputs {
        candidate_atoms: &candidate_atoms,
        history_keys: &history_keys,
        tip_version_keys: &tip_version_keys,
        atom_ref_edge_keys: &atom_ref_edge_keys,
        atom_ref_v2_edge_keys: &atom_ref_v2_edge_keys,
        reachability,
    };
    let GuardedErasure {
        report,
        atoms_to_delete,
        keys_to_delete,
        tip_debits,
        live_head_debits,
        file_pointer_atoms_purged,
    } = prepare_guarded_erasure(
        db_ops,
        &mut schema,
        schema_name,
        storage_slots,
        &target_slot_ids,
        &inputs,
        &history_referenced_by_retained,
        records_purged,
        precomputed_retained,
        acct,
    )
    .await?;

    let ledger_entry = build_erasure_ledger_entry(
        verb,
        schema_name,
        ledger_descriptor,
        &report,
        keys_to_delete.len(),
        file_pointer_atoms_purged,
    );
    let ledger_handle = db_ops
        .atoms()
        .begin_delete_ledger_row(None, ledger_entry)
        .await?;
    // Resolve the debit once. `store_schema` below rebinds shared molecules,
    // so a second resolution at commit could name a different schema.
    let atoms = db_ops.atoms();
    let meter_plan =
        atoms.plan_keep_small_hard_erase(schema_name, &live_head_debits, &tip_debits)?;
    db_ops
        .atoms()
        .prepare_keep_small_hard_erase_plan(ledger_handle.key(), &meter_plan)
        .await?;

    batch_delete_keys(db_ops, keys_to_delete, acct).await?;
    if evict_resident {
        evict_resident_state(db_ops, storage_slots, &resident_api_keys, &atoms_to_delete);
    }
    finalize_schema_after_purge(db_ops, schema_manager, schema_name, schema, acct).await?;
    // The destructive flush precedes the durable meter debit. A crash in
    // this short gap leaves meter trust incomplete rather than a false debit.
    db_ops
        .atoms()
        .commit_keep_small_hard_erase_plan(ledger_handle.key(), &meter_plan)
        .await?;
    db_ops
        .atoms()
        .commit_delete_ledger_row(ledger_handle, |entry| {
            entry.embedding_rows_deleted = report.embedding_rows_deleted as u64;
        })
        .await;

    log_hard_removed(schema_name, &report);

    Ok(report)
}
