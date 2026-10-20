//! Target resolution and molecule removal of the guarded purge; see `guarded.rs`.

use super::guarded_atoms::{account_atom_erasures, push_atom_storage_keys, AtomErasureAccounting};
use super::guarded_steps::*;
use super::*;

/// Inputs of the molecule-removal stage that the caller already owns.
pub(super) struct GuardedRemovalInputs<'a> {
    pub(super) candidate_atoms: &'a HashSet<String>,
    pub(super) history_keys: &'a [String],
    pub(super) atom_ref_v2_edge_keys: &'a [String],
    pub(super) history_referenced_by_retained: &'a HashSet<String>,
    pub(super) reachability: PurgeReachability,
}

/// Result of the molecule-removal stage.
pub(super) struct GuardedRemoval {
    pub(super) atoms_to_delete: Vec<String>,
    pub(super) tip_debits: Vec<(String, u64)>,
}

/// Remove the purged slots from the schema and persist each field's removal
/// by deleting exactly the purged slots' rows.
///
/// This used to delete the whole molecule and rewrite it. That made the cost
/// of purging ONE record O(cardinality of every field molecule it touched):
/// measured on the primary 2026-08-09, a single-record purge of a 24-field
/// schema holding ~19.2k keys per field molecule rewrote ~347.7k `mk:` rows
/// and grew the `tips` plane ~442 MiB, with the store flat whenever no purge
/// was running. Purge is the only primitive that can shrink a plane, so the
/// reclaim path was the growth path and an operator answering disk pressure
/// by purging harder made it worse. See
/// `AtomStore::remove_molecule_keys` for what it does and does not touch.
///
/// The per-field path still paid one durable delete + one header put per
/// field (x24 on BoardCards) under the exclusive purge barrier. Batch them
/// into a single store round-trip pair via `remove_molecules_keys_batch`.
pub(super) async fn remove_slots_and_persist(
    db_ops: &Arc<DbOperations>,
    schema: &mut crate::schema::types::Schema,
    schema_name: &str,
    storage_slots: &[PlannedStorageSlot],
    inputs: &GuardedRemovalInputs<'_>,
    acct: &mut PurgeCommitAccounting,
) -> Result<GuardedRemoval, SchemaError> {
    let removed_by_field =
        remove_slots_from_schema(schema, schema_name, storage_slots, "Purge plan")?;
    let atoms_to_delete: Vec<String> =
        if inputs.reachability == PurgeReachability::GuardedComplement {
            unreferenced_candidate_atoms(
                db_ops,
                schema,
                inputs.candidate_atoms,
                inputs.history_referenced_by_retained,
                storage_slots,
                acct,
            )?
        } else {
            Vec::new()
        };

    let remove_keys_start = std::time::Instant::now();
    let owned_for_batch = build_owned_molecule_removals(schema, &removed_by_field);
    let removed_tip_keys = removed_tip_keys_of(&owned_for_batch);
    let tip_debits = db_ops
        .atoms()
        .keep_small_measure_tip_keys(&removed_tip_keys)
        .await;
    persist_molecule_removals(db_ops, &owned_for_batch, inputs).await?;
    acct.record(
        crate::request_phases::RequestPhase::PurgeDelete,
        remove_keys_start.elapsed(),
    );
    Ok(GuardedRemoval {
        atoms_to_delete,
        tip_debits,
    })
}

/// Everything the erasure stage needs once the targets are resolved and the
/// molecule rows are gone.
pub(super) struct GuardedErasure {
    pub(super) report: BulkPurgeReport,
    pub(super) keys_to_delete: Vec<Vec<u8>>,
    pub(super) tip_debits: Vec<(String, u64)>,
    pub(super) atom_accounting: AtomErasureAccounting,
}

/// Collect every storage key the final batch delete removes.
pub(super) async fn collect_keys_to_delete(
    db_ops: &Arc<DbOperations>,
    storage_prefix: Option<&str>,
    inputs: &GuardedRemovalInputs<'_>,
    atoms_to_delete: &[String],
    acct: &mut PurgeCommitAccounting,
) -> Result<Vec<Vec<u8>>, SchemaError> {
    let mut keys_to_delete: Vec<Vec<u8>> =
        if inputs.reachability == PurgeReachability::GuardedComplement {
            inputs
                .history_keys
                .iter()
                .map(|key| key.as_bytes().to_vec())
                .collect()
        } else {
            Vec::new()
        };
    let key_set_start = std::time::Instant::now();
    push_atom_storage_keys(db_ops, storage_prefix, atoms_to_delete, &mut keys_to_delete).await?;
    acct.record(
        crate::request_phases::RequestPhase::PurgeDelete,
        key_set_start.elapsed(),
    );
    Ok(keys_to_delete)
}

/// Resolve the targets and remove the molecule rows. Nothing after this point
/// deletes by reachability: the atoms and keys it returns are final.
pub(super) async fn prepare_guarded_erasure(
    scope: PurgeScope<'_>,
    schema: &mut crate::schema::types::Schema,
    storage_slots: &[PlannedStorageSlot],
    inputs: &GuardedRemovalInputs<'_>,
    records_purged: usize,
    acct: &mut PurgeCommitAccounting,
) -> Result<GuardedErasure, SchemaError> {
    let PurgeScope {
        db_ops,
        schema_name,
        storage_prefix,
        ..
    } = scope;
    let live_head_atoms = collect_live_head_atoms(schema, storage_slots);
    let GuardedRemoval {
        mut atoms_to_delete,
        tip_debits,
    } = remove_slots_and_persist(db_ops, schema, schema_name, storage_slots, inputs, acct).await?;

    // Both modes authorize body removal through the same current reverse
    // references after the target tip edges are gone. The complement's
    // live/history checks can retain more atoms, but cannot authorize deletion
    // of an atom another schema still references.
    let complement_candidates = (inputs.reachability == PurgeReachability::GuardedComplement)
        .then(|| std::mem::take(&mut atoms_to_delete).into_iter().collect());
    let candidates = complement_candidates
        .as_ref()
        .unwrap_or(inputs.candidate_atoms);
    let reverse_edge_reads = collect_unreferenced_by_reverse_edges(
        db_ops,
        schema,
        candidates,
        storage_slots,
        &mut atoms_to_delete,
    )
    .await?;
    let atom_accounting = account_atom_erasures(
        db_ops,
        &atoms_to_delete,
        &live_head_atoms,
        storage_prefix,
        acct,
    )
    .await?;
    let keys_to_delete =
        collect_keys_to_delete(db_ops, storage_prefix, inputs, &atoms_to_delete, acct).await?;
    let report = BulkPurgeReport {
        records_purged,
        history_rows_deleted: inputs.history_keys.len(),
        tip_versions_pruned: 0,
        atom_rows_deleted: atoms_to_delete.len(),
        embedding_rows_deleted: records_purged,
        target_slots: u64::try_from(storage_slots.len()).unwrap_or(u64::MAX),
        candidate_atoms: u64::try_from(inputs.candidate_atoms.len()).unwrap_or(u64::MAX),
        reverse_edge_reads,
    };
    Ok(GuardedErasure {
        report,
        keys_to_delete,
        tip_debits,
        atom_accounting,
    })
}

/// Log the completed guarded purge.
pub(super) fn log_hard_removed(schema_name: &str, report: &BulkPurgeReport) {
    tracing::info!(
        schema = %schema_name,
        records_purged = report.records_purged,
        history_rows_deleted = report.history_rows_deleted,
        tip_versions_pruned = report.tip_versions_pruned,
        atom_rows_deleted = report.atom_rows_deleted,
        embedding_rows_deleted = report.embedding_rows_deleted,
        "purge core: hard-removed target batch"
    );
}
