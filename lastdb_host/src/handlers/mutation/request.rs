//! Single-mutation steps using the existing host interface.

use fold_db::record_value::InlineWrite;
use fold_db::schema::types::FileWrite;
use serde_json::Value;

use crate::host_node::HostNode;
use crate::HostError;

use super::fields::{fields_payload_bytes, resolve_required_schema_name, validate_mutation_fields};
use super::{commit_mutation, mutation_response, MutationComponents};

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
    let canonical = resolve_required_schema_name(host, &components.schema)?;
    validate_mutation_fields(
        host,
        &components.schema,
        &canonical,
        &components.record_fields,
    )
    .await?;
    let payload_bytes = fields_payload_bytes(&components.record_fields)
        + components.file_write.as_ref().map_or(0, FileWrite::row_len)
        + match &components.inline_write {
            Some(InlineWrite::Put(file)) => file.data.len(),
            Some(InlineWrite::Clear) | None => 0,
        };

    let convergence = components.convergence;
    let receipt = commit_mutation(host, components, canonical).await?;
    let mutation_id = receipt
        .mutation_ids
        .last()
        .cloned()
        .ok_or_else(|| HostError::internal("Mutation returned no IDs"))?;
    let background_tasks_drained = convergence.waits();
    Ok(mutation_response(
        &mutation_id,
        background_tasks_drained,
        &receipt,
        payload_bytes,
    ))
}
