//! Single-mutation steps using the existing host interface.

use std::collections::HashSet;

use fold_db::access::AccessContext;
use fold_db::request_phases::{self, RequestPhase};
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::{AggregateSet, KeyValue, Query};
use serde_json::Value;

use crate::host_node::HostNode;
use crate::HostError;

use super::fields::{
    fields_payload_bytes, resolve_aggregate_set_schema_names, resolve_required_schema_name,
    source_mutation_id, validate_mutation_fields,
};
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
    mut components: MutationComponents,
    ctx: &AccessContext,
) -> Result<Value, HostError> {
    let (canonical, aggregate_set) =
        resolve_mutation_schema(host, &components.schema, components.aggregate_set.take())?;
    components.aggregate_set = aggregate_set;
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
    let (receipt, aggregate_write) =
        commit_mutation(host, plan, key_values, ctx, payload_bytes).await?;
    let mutation_id = if aggregate_write {
        source_mutation_id(&receipt)?
    } else {
        receipt
            .mutation_ids
            .last()
            .cloned()
            .ok_or_else(|| HostError::internal("Mutation returned no IDs"))?
    };
    let background_tasks_drained = wait_for_requested_convergence(host, convergence).await;
    Ok(mutation_response(
        &mutation_id,
        background_tasks_drained,
        &receipt,
        payload_bytes,
    ))
}

/// Resolve the source and aggregate schema names together, with one phase timer.
fn resolve_mutation_schema<H: HostNode>(
    host: &H,
    schema: &str,
    aggregate_set: Option<AggregateSet>,
) -> Result<(String, Option<AggregateSet>), HostError> {
    let resolve_started = std::time::Instant::now();
    let resolved = (|| {
        let canonical = resolve_required_schema_name(host, schema)?;
        let aggregate_set = aggregate_set
            .map(|aggregate| resolve_aggregate_set_schema_names(host, aggregate))
            .transpose()?;
        Ok::<_, HostError>((canonical, aggregate_set))
    })();
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
