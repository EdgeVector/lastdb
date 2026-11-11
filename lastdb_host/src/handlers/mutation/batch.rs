//! Batch request steps, mutation construction, commit, and response rendering.
//! Pure batch rules live in `rules`; host work stays explicit in the request.

use fold_db::fold_db_core::mutation_manager::ResidentCommitReceipt;
use fold_db::schema::types::Mutation;
use serde_json::Value;

use crate::host_node::HostNode;
use crate::HostError;

use super::fields::{resolve_required_schema_name, validate_mutation_fields};
use super::{commit_core_json, extend_json, wait_for_requested_convergence, MutationComponents};

mod rules;

use self::rules::batch_convergence;

/// Execute `POST /api/mutations/batch`: resolve each schema, build all
/// mutations with the node's public key, and write them in one batch.
///
/// # Errors
/// [`HostError`] `400` for invalid batch rules or fields, `404` when any schema
/// is unknown, and the core's typed status for a rejected write.
pub async fn execute_mutations_batch<H: HostNode>(
    host: &H,
    components: Vec<MutationComponents>,
) -> Result<Value, HostError> {
    let convergence = batch_convergence(&components)?;
    let mut mutations = Vec::with_capacity(components.len());
    for components in components {
        let canonical = resolve_required_schema_name(host, &components.schema)?;
        validate_mutation_fields(
            host,
            &components.schema,
            &canonical,
            &components.record_fields,
        )
        .await?;
        mutations.push(components.into_mutation(canonical, host.public_key(), host.ids()));
    }

    let count = mutations.len();
    let receipt = write_batch(host, mutations).await?;
    let background_tasks_drained = wait_for_requested_convergence(host, convergence).await;

    Ok(batch_response(&receipt, count, background_tasks_drained))
}

fn batch_response(
    receipt: &ResidentCommitReceipt,
    count: usize,
    background_tasks_drained: bool,
) -> Value {
    // One committed revision lets the caller read back every batch record
    // without a post-write verification round trip.
    extend_json(
        commit_core_json(receipt),
        serde_json::json!({
            "mutation_ids": &receipt.mutation_ids,
            "count": count,
            "background_tasks_drained": background_tasks_drained,
            "convergence_pending": !background_tasks_drained,
        }),
    )
}

/// Write the batch, mapping a core failure to a [`HostError`].
async fn write_batch<H: HostNode>(
    host: &H,
    mutations: Vec<Mutation>,
) -> Result<ResidentCommitReceipt, HostError> {
    host.fold_db()
        .mutation_manager()
        .write_mutations_receipt(mutations)
        .await
        .map_err(|e| super::prefix_core_failure(e, "Mutation batch failed"))
}
