//! Single-mutation steps using the existing host interface.

use fold_db::request_phases::{self, RequestPhase};
use fold_db::schema::types::FileWrite;
use serde_json::Value;

use crate::host_node::HostNode;
use crate::HostError;

use super::fields::{fields_payload_bytes, resolve_required_schema_name, validate_mutation_fields};
use super::{
    commit_mutation, mutation_response, wait_for_requested_convergence, MutationComponents,
};

/// Execute `POST /api/mutation`. Schema
/// resolution precedes field validation and the write.
///
/// # Errors
/// [`HostError`] `404` when the schema is unknown, `400` for invalid request
/// rules or fields, and the core's typed status for a rejected write.
pub async fn execute_mutation<H: HostNode>(
    host: &H,
    components: MutationComponents,
) -> Result<Value, HostError> {
    let canonical = resolve_mutation_schema(host, &components.schema)?;
    let validate_started = std::time::Instant::now();
    validate_mutation_fields(
        host,
        &components.schema,
        &canonical,
        &components.record_fields,
    )
    .await?;
    request_phases::add_phase(RequestPhase::Validate, validate_started.elapsed());
    let payload_bytes = fields_payload_bytes(&components.record_fields)
        + components.file_write.as_ref().map_or(0, FileWrite::row_len);

    let convergence = components.convergence;
    let receipt = commit_mutation(host, components, canonical, payload_bytes).await?;
    let mutation_id = receipt
        .mutation_ids
        .last()
        .cloned()
        .ok_or_else(|| HostError::internal("Mutation returned no IDs"))?;
    let background_tasks_drained = wait_for_requested_convergence(host, convergence).await;
    Ok(mutation_response(
        &mutation_id,
        background_tasks_drained,
        &receipt,
        payload_bytes,
    ))
}

/// Resolve the schema name, with one phase timer.
fn resolve_mutation_schema<H: HostNode>(host: &H, schema: &str) -> Result<String, HostError> {
    let resolve_started = std::time::Instant::now();
    let resolved = resolve_required_schema_name(host, schema);
    request_phases::add_phase(RequestPhase::SchemaResolve, resolve_started.elapsed());
    resolved
}
