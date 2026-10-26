//! Batch request steps, mutation construction, commit, and response rendering.
//! Pure batch rules live in `rules`; host work stays explicit in the request.

use fold_db::access::AccessContext;
use fold_db::fold_db_core::mutation_manager::ResidentCommitReceipt;
use fold_db::request_phases::{self, RequestPhase};
use fold_db::schema::types::Mutation;
use serde_json::Value;

use crate::host_node::HostNode;
use crate::qos::Lane;
use crate::HostError;

use super::fields::{fields_payload_bytes, resolve_required_schema_name, validate_mutation_fields};
use super::plan::reject_illegal_must_exist;
use super::{
    configure_mutation, operations_json, stages_json, wait_for_requested_convergence,
    MutationComponents,
};

mod rules;

use self::rules::{batch_convergence, validate_batch_options};

/// Execute `POST /api/mutations/batch`: resolve each schema, build all
/// mutations with the node's public key, and write them in one access-checked
/// batch.
///
/// # Errors
/// [`HostError`] `400` for invalid batch rules or fields, `404` when any schema
/// is unknown, and the core's typed status for a rejected write.
pub async fn execute_mutations_batch<H: HostNode>(
    host: &H,
    components: Vec<MutationComponents>,
    ctx: &AccessContext,
) -> Result<Value, HostError> {
    let convergence = batch_convergence(&components)?;
    let mut mutations = Vec::with_capacity(components.len());
    let mut payload_bytes = 0usize;
    for components in components {
        validate_batch_options(&components)?;

        // Sum per-item phases without changing which rejection wins first.
        let resolve_started = std::time::Instant::now();
        let resolved = resolve_required_schema_name(host, &components.schema);
        request_phases::add_phase(RequestPhase::SchemaResolve, resolve_started.elapsed());
        let canonical = resolved?;
        reject_illegal_must_exist(components.mutation_type, components.must_exist)?;

        let validate_started = std::time::Instant::now();
        validate_mutation_fields(
            host,
            &components.schema,
            &canonical,
            &components.fields_and_values,
        )
        .await?;
        request_phases::add_phase(RequestPhase::Validate, validate_started.elapsed());

        payload_bytes =
            payload_bytes.saturating_add(fields_payload_bytes(&components.fields_and_values));
        mutations.push(configure_mutation(
            Mutation::new(
                canonical,
                components.fields_and_values,
                components.key_value,
                host.public_key(),
                components.mutation_type,
            ),
            components.source_file_name,
            components.durability,
            components.expected,
            components.must_exist,
            components.aggregate_set,
        ));
    }

    let count = mutations.len();
    let receipt = write_batch(host, mutations, ctx, payload_bytes).await?;
    let background_tasks_drained = wait_for_requested_convergence(host, convergence).await;

    Ok(batch_response(&receipt, count, background_tasks_drained))
}

fn batch_response(
    receipt: &ResidentCommitReceipt,
    count: usize,
    background_tasks_drained: bool,
) -> Value {
    serde_json::json!({
        "mutation_ids": &receipt.mutation_ids,
        "count": count,
        "background_tasks_drained": background_tasks_drained,
        "convergence_pending": !background_tasks_drained,
        // One committed revision lets the caller read back every batch record
        // without a post-write verification round trip.
        "revision": receipt.revision,
        "operations": operations_json(&receipt.operations),
        "durability": receipt.durability.as_str(),
        "resident_stages_us": stages_json(&receipt.stages),
        "touched_group_ids": &receipt.touched_group_ids
    })
}

/// Admit the whole batch on the lane its combined payload size implies and
/// write it, mapping a core failure to a [`HostError`].
async fn write_batch<H: HostNode>(
    host: &H,
    mutations: Vec<Mutation>,
    ctx: &AccessContext,
    payload_bytes: usize,
) -> Result<ResidentCommitReceipt, HostError> {
    // Admit the whole batch on the lane its combined payload size implies, held
    // across the batch write. As in the single route, the admission wait is
    // its own phase so governor queueing stops masquerading as write time.
    let admission_started = std::time::Instant::now();
    let _permit = host
        .acquire_op_permit(Lane::for_write_bytes(payload_bytes))
        .await?;
    request_phases::add_phase(RequestPhase::AdmissionWait, admission_started.elapsed());
    host.fold_db()
        .mutation_manager()
        .write_mutations_with_access_receipt(mutations, ctx)
        .await
        .map_err(|e| {
            let mapped = HostError::from(e);
            if mapped.status == 500 {
                HostError::internal(format!("Mutation batch failed: {}", mapped.message))
            } else {
                mapped
            }
        })
}
