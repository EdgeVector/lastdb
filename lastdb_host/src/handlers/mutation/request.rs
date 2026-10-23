//! Single-mutation control flow, independent of the host implementation.

use serde_json::Value;

use crate::HostError;

use super::fields::{fields_payload_bytes, source_mutation_id};
use super::plan::MutationPlan;
use super::services::MutationServices;
use super::{empty_delete_response, mutation_response, MutationComponents};

pub(super) async fn execute<S: MutationServices>(
    services: &S,
    mut components: MutationComponents,
) -> Result<Value, HostError> {
    let (canonical, aggregate_set) =
        services.resolve_schema_names(&components.schema, components.aggregate_set.take())?;
    components.aggregate_set = aggregate_set;
    let plan = MutationPlan::new(components, canonical)?;
    services.validate_fields(&plan).await?;
    let payload_bytes = fields_payload_bytes(&plan.components.fields_and_values);

    let key_values = match &plan.components.key_range_prefix {
        Some(prefix) => {
            services
                .resolve_prefix_keys(
                    &plan.canonical,
                    plan.components
                        .key_value
                        .hash
                        .clone()
                        .expect("validated hash"),
                    prefix.clone(),
                )
                .await?
        }
        None => vec![plan.components.key_value.clone()],
    };
    if key_values.is_empty() {
        return Ok(empty_delete_response());
    }

    let (receipt, aggregate_write) = services.commit(&plan, key_values, payload_bytes).await?;
    let mutation_id = if aggregate_write {
        source_mutation_id(&receipt)?
    } else {
        receipt
            .mutation_ids
            .last()
            .cloned()
            .ok_or_else(|| HostError::internal("Mutation returned no IDs"))?
    };
    let background_tasks_drained = services
        .wait_for_convergence(plan.components.convergence)
        .await;
    Ok(mutation_response(
        &mutation_id,
        background_tasks_drained,
        &receipt,
        payload_bytes,
    ))
}
