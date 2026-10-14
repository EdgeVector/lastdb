//! Moved verbatim out of the parent module; see the parent for context.

use super::*;

use super::converge_plan::*;
use super::converge_steps::*;

/// Converge the tips of a live `Delete` to disk.
///
/// Every exit leaves the pending Delete barriers cleared. A failed converge
/// that kept them would hide its keys from readers until the next restart.
pub(in crate::fold_db_core) async fn converge_delete_tips(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    mutations: &[crate::schema::types::Mutation],
    storage_prefix: Option<&str>,
) -> Result<BulkPurgeReport, SchemaError> {
    if mutations.is_empty() {
        return Ok(BulkPurgeReport::empty());
    }

    let (planned_barriers, tip_guards, plan) = lock_and_plan_converge(
        db_ops,
        schema_manager,
        schema_name,
        mutations,
        storage_prefix,
    )
    .await?;
    let Some(plan) = plan else {
        release_converge_locks(db_ops, &planned_barriers, tip_guards);
        return Ok(BulkPurgeReport::empty());
    };
    let caller = mutations
        .iter()
        .find_map(|mutation| mutation.erase_caller)
        .unwrap_or("mutation-pipeline");
    let converged = converge_planned(
        db_ops,
        schema_manager,
        schema_name,
        caller,
        plan,
        (&planned_barriers, tip_guards),
    )
    .await;
    if converged.is_err() {
        // Idempotent: a converge that failed after its own release cleared
        // the barriers already.
        db_ops
            .atoms()
            .clear_pending_delete_barriers(&planned_barriers);
    }
    converged
}

/// The destructive stage of a converge, after the plan exists. `locks` holds
/// the planned barriers and the exact tip guards. A guard drops on any early
/// exit. The caller clears the barriers when this returns an error.
async fn converge_planned(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    caller: &str,
    mut plan: ConvergePlan,
    locks: (
        &[crate::atom::delete_barrier::DeleteBarrier],
        Vec<tokio::sync::OwnedMutexGuard<()>>,
    ),
) -> Result<BulkPurgeReport, SchemaError> {
    let (planned_barriers, tip_guards) = locks;
    let (extra_keys, trailing_keys) = converge_extra_and_trailing_keys(db_ops, &plan).await?;
    let batch_refs: Vec<_> = plan
        .owned_for_batch
        .iter()
        .map(|(uuid, data, slots, prefix)| {
            (uuid.as_str(), data, slots.as_slice(), prefix.as_deref())
        })
        .collect();

    let step_started = std::time::Instant::now();
    let ledger_entry = build_converge_ledger_entry(
        caller,
        schema_name,
        &plan,
        extra_keys.len(),
        trailing_keys.len(),
    );
    let ledger_started = std::time::Instant::now();
    let ledger_handle = db_ops
        .atoms()
        .begin_delete_ledger_row(None, ledger_entry)
        .await?;
    // Resolve the debit once. `store_schema` below rebinds shared molecules,
    // so a second resolution at commit could name a different schema.
    let meter_plan =
        db_ops
            .atoms()
            .plan_keep_small_hard_erase(schema_name, &[], &plan.tip_debits)?;
    db_ops
        .atoms()
        .prepare_keep_small_hard_erase_plan(ledger_handle.key(), &meter_plan)
        .await?;
    plan.timings.ledger = ledger_started.elapsed();

    let batch_started = std::time::Instant::now();
    db_ops
        .atoms()
        .remove_molecules_keys_with_extra_and_trailing_batch(&batch_refs, extra_keys, trailing_keys)
        .await?;
    plan.timings.batch = batch_started.elapsed();
    plan.timings.remove = step_started.elapsed();

    let step_started = std::time::Instant::now();
    let flushed = db_ops.flush().await.map_err(|error| {
        SchemaError::InvalidData(format!("Flush failed after delete converge: {error}"))
    });
    plan.timings.flush = step_started.elapsed();
    flushed?;
    release_converge_locks(db_ops, planned_barriers, tip_guards);

    // Release every exact tip lock after source removal reaches disk, then
    // refresh the schema cache.
    let step_started = std::time::Instant::now();
    let schema_unchanged =
        store_and_reload_converged_schema(db_ops, schema_manager, schema_name, plan.schema).await?;
    plan.timings.schema = step_started.elapsed();
    // Flush the deletion first. A debit that survives a crash must never
    // describe rows whose deletion did not reach durable storage.
    db_ops
        .atoms()
        .commit_keep_small_hard_erase_plan(ledger_handle.key(), &meter_plan)
        .await?;
    // Confirm only after both the delete and its small meter debit are durable.
    db_ops
        .atoms()
        .commit_delete_ledger_row(ledger_handle, |_| {})
        .await;

    Ok(finish_converge_report(
        schema_name,
        plan.keys.len(),
        plan.tip_version_keys.len(),
        plan.target_slots,
        schema_unchanged,
        &plan.timings,
    ))
}
