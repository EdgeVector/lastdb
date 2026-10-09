//! Aggregate finalize and repair handlers.

use super::*;

/// Execute `POST /api/aggregate/finalize` after a caller verifies a complete
/// member-partition repair. The core checks the exact guard association and
/// token under the target lock before it marks the summary valid.
///
/// # Errors
/// [`HostError`] `400` when the association or token is invalid, `404` when a
/// schema is unknown, and `500` on an internal write failure.
pub async fn execute_aggregate_finalize<H: HostNode>(
    host: &H,
    mut finalize: AggregateFinalize,
    ctx: &AccessContext,
) -> Result<Value, HostError> {
    if !ctx.is_owner {
        return Err(HostError::new(404, "Not Found"));
    }
    let resolve_started = std::time::Instant::now();
    let resolved = resolve_aggregate_schema_triplet(
        host,
        &finalize.source_schema_name,
        &finalize.target_schema_name,
        &finalize.member_schema_name,
    );
    request_phases::add_phase(RequestPhase::SchemaResolve, resolve_started.elapsed());
    let (source, target, member) = resolved?;
    finalize.source_schema_name = source;
    finalize.target_schema_name = target;
    finalize.member_schema_name = member;

    let admission_started = std::time::Instant::now();
    let permit = host.acquire_op_permit(Lane::Interactive).await;
    request_phases::add_phase(RequestPhase::AdmissionWait, admission_started.elapsed());
    let _permit = permit?;

    let receipt = host
        .fold_db()
        .mutation_manager()
        .finalize_aggregate_summary_with_access_receipt(finalize, ctx)
        .await
        .map_err(|error| {
            let mapped = HostError::from(error);
            if mapped.status == 500 {
                HostError::internal(format!("Aggregate finalize failed: {}", mapped.message))
            } else {
                mapped
            }
        })?;
    let receipt = require_durable_aggregate_receipt(receipt, "Aggregate finalize")?;

    Ok(serde_json::json!({
        "success": true,
        "mutation_ids": receipt.mutation_ids,
        "revision": receipt.revision,
        "operations": operations_json(&receipt.operations),
        "durability": receipt.durability.as_str(),
        "resident_stages_us": stages_json(&receipt.stages),
        "touched_group_ids": receipt.touched_group_ids
    }))
}

/// Execute `POST /api/aggregate/repair` after a caller reconciles the source
/// and bounded member partitions. The core recomputes the summary, leaves it
/// invalid, and returns the only token that a later finalize can accept.
///
/// # Errors
/// [`HostError`] `400` when the association or prior token is invalid, `404`
/// when a schema is unknown, and `500` on a read or write failure.
pub async fn execute_aggregate_repair<H: HostNode>(
    host: &H,
    mut repair: AggregateRepair,
    ctx: &AccessContext,
) -> Result<Value, HostError> {
    if !ctx.is_owner {
        return Err(HostError::new(404, "Not Found"));
    }
    let resolve_started = std::time::Instant::now();
    let resolved = resolve_aggregate_schema_triplet(
        host,
        &repair.source_schema_name,
        &repair.target_schema_name,
        &repair.member_schema_name,
    );
    request_phases::add_phase(RequestPhase::SchemaResolve, resolve_started.elapsed());
    let (source, target, member) = resolved?;
    repair.source_schema_name = source;
    repair.target_schema_name = target;
    repair.member_schema_name = member;

    let admission_started = std::time::Instant::now();
    let permit = host.acquire_op_permit(Lane::Bulk).await;
    request_phases::add_phase(RequestPhase::AdmissionWait, admission_started.elapsed());
    let _permit = permit?;

    let repaired = host
        .fold_db()
        .mutation_manager()
        .repair_aggregate_summary_with_access_receipt(repair, ctx)
        .await
        .map_err(|error| {
            let mapped = HostError::from(error);
            if mapped.status == 500 {
                HostError::internal(format!("Aggregate repair failed: {}", mapped.message))
            } else {
                mapped
            }
        })?;
    let receipt = require_durable_aggregate_receipt(repaired.receipt, "Aggregate repair")?;

    Ok(serde_json::json!({
        "receipt": {
            "mutation_ids": receipt.mutation_ids,
            "revision": receipt.revision,
            "operations": operations_json(&receipt.operations),
            "durability": receipt.durability.as_str(),
            "resident_stages_us": stages_json(&receipt.stages),
            "touched_group_ids": receipt.touched_group_ids
        },
        "repair_token": repaired.repair_token
    }))
}
