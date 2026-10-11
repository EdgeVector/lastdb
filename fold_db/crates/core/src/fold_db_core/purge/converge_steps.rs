//! Write and report steps of a delete converge; see `converge.rs`.

use super::converge_plan::*;
use super::*;

/// Store the schema when it changed, then refresh the schema cache.
/// Returns whether the schema store was skipped because nothing changed.
/// Schema metadata can take molecule locks, so callers release every exact tip
/// lock before this runs.
pub(super) async fn store_and_reload_converged_schema(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    mut schema: crate::schema::types::Schema,
) -> Result<bool, SchemaError> {
    crate::fold_db_core::mutation_manager::helpers::clear_storage_prefix_on_schema(&mut schema);
    schema.sync_molecule_uuids();
    let schema_unchanged = matches!(
        db_ops.schemas().get_schema(schema_name).await,
        Ok(Some(ref old)) if *old == schema
    );
    if !schema_unchanged {
        db_ops.store_schema(schema_name, &schema).await?;
        db_ops.flush().await.map_err(|error| {
            SchemaError::InvalidData(format!("flush schema after delete converge: {error}"))
        })?;
    }
    schema_manager.load_schema_internal(schema).await?;
    Ok(schema_unchanged)
}

/// Tip-version chain keys (with their backrefs) and trailing edge keys that
/// the molecule-key removal deletes together with the tips.
pub(super) async fn converge_extra_and_trailing_keys(
    db_ops: &Arc<DbOperations>,
    plan: &ConvergePlan,
) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>), SchemaError> {
    let tip_version_backref_keys = db_ops
        .atoms()
        .tip_version_backref_delete_keys_for_tv_keys(&plan.tip_version_keys)
        .await?;
    let mut extra_keys: Vec<Vec<u8>> = plan
        .tip_version_keys
        .iter()
        .map(|key| key.as_bytes().to_vec())
        .collect();
    extra_keys.extend(tip_version_backref_keys);
    let trailing_keys: Vec<Vec<u8>> = plan
        .atom_ref_edge_keys
        .iter()
        .chain(&plan.atom_ref_v2_edge_keys)
        .map(|key| key.as_bytes().to_vec())
        .collect();
    Ok((extra_keys, trailing_keys))
}

/// Write-ahead audit row, before anything is destroyed. Converge reclaims
/// no atom body, so `atoms_deleted` stays 0 and the verb is
/// `delete-converge` rather than `delete` — see the `delete_ledger` module
/// docs for why a row that deletes nothing still has to exist. Without it
/// the ledger answers "no deletes in this window" for every window on a
/// node whose live delete path is this function, which is the shape that
/// let a dangling-tip investigation read a dead instrument as evidence.
pub(super) fn build_converge_ledger_entry(
    schema_name: &str,
    plan: &ConvergePlan,
    extra_key_count: usize,
    trailing_key_count: usize,
) -> crate::db_operations::AtomDeleteLedgerEntry {
    let mut ledger_entry = crate::db_operations::AtomDeleteLedgerEntry::delete_converge(
        "mutation-pipeline",
        schema_name,
        &plan.ledger_descriptor,
    );
    ledger_entry.tips_removed = plan.removed_tip_keys.len() as u64;
    ledger_entry.records_converged = plan.keys.len() as u64;
    ledger_entry.tip_versions_pruned = plan.tip_version_keys.len() as u64;
    ledger_entry.storage_keys_deleted =
        (plan.removed_tip_keys.len() + extra_key_count + trailing_key_count) as u64;
    ledger_entry
}

/// Log the converge timings and build the report.
pub(super) fn finish_converge_report(
    schema_name: &str,
    records_converged: usize,
    tip_versions_pruned: usize,
    target_slots: usize,
    schema_unchanged: bool,
    t: &ConvergeTimings,
) -> BulkPurgeReport {
    tracing::info!(
        schema = %schema_name,
        records_converged,
        tip_versions_pruned,
        refresh_ms = t.refresh.as_millis() as u64,
        trace_ms = t.trace.as_millis() as u64,
        remove_ms = t.remove.as_millis() as u64,
        measure_ms = t.measure.as_millis() as u64,
        ledger_ms = t.ledger.as_millis() as u64,
        batch_ms = t.batch.as_millis() as u64,
        schema_ms = t.schema.as_millis() as u64,
        schema_store_skipped = schema_unchanged,
        flush_ms = t.flush.as_millis() as u64,
        "delete converge: tip absence persisted"
    );

    BulkPurgeReport {
        records_purged: records_converged,
        history_rows_deleted: 0,
        tip_versions_pruned,
        atom_rows_deleted: 0,
        embedding_rows_deleted: 0,
        target_slots: u64::try_from(target_slots).unwrap_or(u64::MAX),
        candidate_atoms: 0,
        reverse_edge_reads: 0,
    }
}

/// Clear the pending Delete barriers and release the exact tip locks.
pub(super) fn release_converge_locks(
    db_ops: &Arc<DbOperations>,
    planned_barriers: &[crate::atom::delete_barrier::DeleteBarrier],
    tip_guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
) {
    db_ops
        .atoms()
        .clear_pending_delete_barriers(planned_barriers);
    drop(tip_guards);
}
