//! Moved verbatim out of the parent module; see the parent for context.

use super::*;

use super::guarded_atoms::AtomErasureAccounting;
use super::guarded_prepare::*;
use super::guarded_steps::*;

/// The single destructive implementation shared by logical API-key purge and
/// exact storage-slot purge. Everything above this boundary is target planning;
/// everything below it is reachability-guarded erasure.
pub(super) async fn execute_guarded_purge(
    scope: PurgeScope<'_>,
    mut schema: crate::schema::types::Schema,
    storage_slots: &[PlannedStorageSlot],
    purge: GuardedPurge,
    ledger_descriptor: &str,
    verb: HardEraseVerb,
    acct: &mut PurgeCommitAccounting,
) -> Result<BulkPurgeReport, SchemaError> {
    let PurgeScope {
        db_ops,
        schema_manager,
        schema_name,
        storage_prefix,
    } = scope;
    let caller = purge.mode.caller;
    let inputs = GuardedRemovalInputs {
        candidate_atoms: &purge.candidate_atoms,
        history_keys: &purge.history_keys,
        atom_ref_v2_edge_keys: &purge.atom_ref_v2_edge_keys,
        history_referenced_by_retained: &purge.history_referenced_by_retained,
        reachability: purge.mode.reachability,
    };
    let GuardedErasure {
        report,
        keys_to_delete,
        tip_debits,
        atom_accounting:
            AtomErasureAccounting {
                live_head_debits,
                file_pointer_atoms_purged,
                schema_markers,
            },
    } = prepare_guarded_erasure(
        scope,
        &mut schema,
        storage_slots,
        &inputs,
        purge.records_purged,
        acct,
    )
    .await?;

    let ledger_entry = build_erasure_ledger_entry(
        (verb, caller),
        schema_name,
        ledger_descriptor,
        &report,
        keys_to_delete.len().saturating_add(schema_markers.len()),
        file_pointer_atoms_purged,
    );
    let ledger_handle = db_ops
        .atoms()
        .begin_delete_ledger_row(storage_prefix, ledger_entry)
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
    db_ops
        .atoms()
        .remove_atom_schema_markers(&schema_markers, storage_prefix)
        .await?;
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
