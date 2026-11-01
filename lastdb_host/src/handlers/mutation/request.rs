//! Single-mutation steps using the existing host interface.

use fold_db::schema::types::KeyValuePageOrder;
use std::collections::HashSet;

use fold_db::access::AccessContext;
use fold_db::request_phases::{self, RequestPhase};
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::{KeyValue, Query};
use serde_json::Value;

use crate::host_node::HostNode;
use crate::HostError;

use super::fields::{fields_payload_bytes, resolve_required_schema_name, validate_mutation_fields};
use super::plan::MutationPlan;
use super::{
    commit_mutation, empty_delete_response, mutation_response, wait_for_requested_convergence,
    MutationComponents,
};

/// Execute `POST /api/mutation` under the caller's access context. Schema
/// resolution precedes pure request rules, field validation, and the write.
///
/// # Errors
/// [`HostError`] `404` when the schema is unknown, `400` for invalid request
/// rules or fields, and the core's typed status for a rejected write.
pub async fn execute_mutation<H: HostNode>(
    host: &H,
    components: MutationComponents,
    ctx: &AccessContext,
) -> Result<Value, HostError> {
    let canonical = resolve_mutation_schema(host, &components.schema)?;
    let plan = MutationPlan::new(components, canonical)?;
    let validate_started = std::time::Instant::now();
    validate_mutation_fields(
        host,
        &plan.components.schema,
        &plan.canonical,
        &plan.components.fields_and_values,
    )
    .await?;
    request_phases::add_phase(RequestPhase::Validate, validate_started.elapsed());
    let payload_bytes = fields_payload_bytes(&plan.components.fields_and_values);

    let key_values = match &plan.components.key_range_prefix {
        Some(prefix) => {
            resolve_delete_prefix_keys(
                host,
                plan.canonical.clone(),
                plan.components
                    .key_value
                    .hash
                    .clone()
                    .expect("validated hash"),
                prefix.clone(),
                ctx,
            )
            .await?
        }
        None => vec![plan.components.key_value.clone()],
    };
    if key_values.is_empty() {
        return Ok(empty_delete_response());
    }

    let convergence = plan.components.convergence;
    let receipt = commit_mutation(host, plan, key_values, ctx, payload_bytes).await?;
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

async fn resolve_delete_prefix_keys<H: HostNode>(
    host: &H,
    schema: String,
    hash: String,
    prefix: String,
    ctx: &AccessContext,
) -> Result<Vec<KeyValue>, HostError> {
    let query = Query::new_with_filter(
        schema,
        Vec::new(),
        Some(HashRangeFilter::HashRangePrefix { hash, prefix }),
    );
    let rows = host
        .fold_db()
        .query_executor()
        .query_with_access(query, ctx)
        .await
        .map_err(HostError::from)?;
    let mut keys = HashSet::new();
    for fields in rows.values() {
        keys.extend(fields.keys().cloned());
    }
    let mut keys: Vec<_> = keys.into_iter().collect();
    keys.sort_by(KeyValue::cmp_page_order);
    Ok(keys)
}
