//! Aggregate finalize and repair handlers.

use fold_db::access::AccessContext;
use fold_db::request_phases::{self, RequestPhase};
use fold_db::schema::types::{AggregateFinalize, AggregateRepair};
use serde_json::Value;

use crate::host_node::HostNode;
use crate::qos::Lane;
use crate::HostError;

use super::fields::{require_durable_aggregate_receipt, resolve_aggregate_schema_triplet};
use super::{operations_json, stages_json};

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
    let (_permit, source, target, member) = prepare_aggregate_write(
        host,
        ctx,
        Lane::Interactive,
        &finalize.source_schema_name,
        &finalize.target_schema_name,
        &finalize.member_schema_name,
    )
    .await?;
    finalize.source_schema_name = source;
    finalize.target_schema_name = target;
    finalize.member_schema_name = member;

    let receipt = host
        .fold_db()
        .mutation_manager()
        .finalize_aggregate_summary_with_access_receipt(finalize, ctx)
        .await
        .map_err(|error| prefix_aggregate_failure(error, "Aggregate finalize failed"))?;
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
    let (_permit, source, target, member) = prepare_aggregate_write(
        host,
        ctx,
        Lane::Bulk,
        &repair.source_schema_name,
        &repair.target_schema_name,
        &repair.member_schema_name,
    )
    .await?;
    repair.source_schema_name = source;
    repair.target_schema_name = target;
    repair.member_schema_name = member;

    let repaired = host
        .fold_db()
        .mutation_manager()
        .repair_aggregate_summary_with_access_receipt(repair, ctx)
        .await
        .map_err(|error| prefix_aggregate_failure(error, "Aggregate repair failed"))?;
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

/// Refuse a non-owner, resolve the three schema names, then take `lane`.
/// The caller keeps the permit until the core call ends.
async fn prepare_aggregate_write<H: HostNode>(
    host: &H,
    ctx: &AccessContext,
    lane: Lane,
    source_name: &str,
    target_name: &str,
    member_name: &str,
) -> Result<(H::OpPermit, String, String, String), HostError> {
    if !ctx.is_owner {
        return Err(HostError::new(404, "Not Found"));
    }
    let resolve_started = std::time::Instant::now();
    let resolved = resolve_aggregate_schema_triplet(host, source_name, target_name, member_name);
    request_phases::add_phase(RequestPhase::SchemaResolve, resolve_started.elapsed());
    let (source, target, member) = resolved?;
    let admission_started = std::time::Instant::now();
    let permit = host.acquire_op_permit(lane).await;
    request_phases::add_phase(RequestPhase::AdmissionWait, admission_started.elapsed());
    Ok((permit?, source, target, member))
}

/// Keep a typed status. A true 500 keeps the historical prefix.
fn prefix_aggregate_failure(error: impl Into<HostError>, failed: &str) -> HostError {
    let mapped = error.into();
    if mapped.status == 500 {
        HostError::internal(format!("{failed}: {}", mapped.message))
    } else {
        mapped
    }
}
