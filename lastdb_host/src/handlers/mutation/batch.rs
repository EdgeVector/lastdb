//! Batch request steps, mutation construction, commit, and response rendering.
//! Pure batch rules live in `rules`; host work stays explicit in the request.

use super::*;

mod rules;

use self::rules::{batch_convergence, validate_batch_options};

/// Per-class operation counts as the route renders them.
pub(in super::super) fn operations_json(operations: &ResidentCommitOperations) -> Value {
    serde_json::json!({
        "created": operations.created,
        "updated": operations.updated,
        "deleted": operations.deleted,
        "no_op": operations.no_op,
        "total": operations.total(),
    })
}

/// Resident-commit stage clocks, in microseconds.
///
/// These ride the response rather than `status.request_ops` phases: the phase
/// set is a partition of the request's wall clock and these spans overlap
/// buckets that already exist, so publishing them there would corrupt every
/// request's `unattributed` remainder. See
/// `fold_db::fold_db_core::mutation_manager::receipt`.
pub(in super::super) fn stages_json(stages: &ResidentCommitStages) -> Value {
    serde_json::json!({
        "prepare": stages.prepare_us,
        "gate_wait": stages.gate_wait_us,
        "publish": stages.publish_us,
        "ack": stages.ack_us,
    })
}

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
    let mut requires_durable_cloud_capture = false;
    let mut mutations = Vec::with_capacity(components.len());
    let mut payload_bytes = 0usize;
    for components in components {
        validate_batch_options(&components)?;

        // Sum per-item phases without changing which rejection wins first.
        let resolve_started = std::time::Instant::now();
        let resolved = resolve_schema_name(host, &components.schema)?;
        request_phases::add_phase(RequestPhase::SchemaResolve, resolve_started.elapsed());
        let Some(canonical) = resolved else {
            return Err(HostError::new(
                404,
                format!("Schema not found: {}", components.schema),
            ));
        };
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

        requires_durable_cloud_capture |=
            is_durable_delete(components.mutation_type, components.durability);
        payload_bytes =
            payload_bytes.saturating_add(fields_payload_bytes(&components.fields_and_values));
        mutations.push(build_batch_mutation(host, components, canonical));
    }

    let count = mutations.len();
    let receipt = write_batch(
        host,
        mutations,
        ctx,
        payload_bytes,
        requires_durable_cloud_capture,
    )
    .await?;
    let background_tasks_drained = wait_for_requested_convergence(host, convergence).await;

    Ok(batch_response(
        &receipt,
        count,
        background_tasks_drained,
        requires_durable_cloud_capture,
    ))
}

fn batch_response(
    receipt: &ResidentCommitReceipt,
    count: usize,
    background_tasks_drained: bool,
    requires_durable_cloud_capture: bool,
) -> Value {
    let mut response = serde_json::json!({
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
    });
    if requires_durable_cloud_capture {
        attach_batch_cloud_capture(&mut response, &receipt.mutation_ids, receipt.cloud.as_ref());
    }
    response
}

pub(in super::super) async fn wait_for_requested_convergence<H: HostNode>(
    host: &H,
    convergence: MutationConvergence,
) -> bool {
    if !convergence.waits() {
        return false;
    }

    let index_wait_started = std::time::Instant::now();
    let background_tasks_drained = host
        .wait_for_background_tasks(MUTATION_BACKGROUND_TASK_TIMEOUT)
        .await;
    request_phases::add_phase(RequestPhase::IndexWait, index_wait_started.elapsed());
    background_tasks_drained
}

/// Build one validated mutation with the host's identity and the caller's options.
fn build_batch_mutation<H: HostNode>(
    host: &H,
    components: MutationComponents,
    canonical: String,
) -> Mutation {
    let mut mutation = Mutation::new(
        canonical,
        components.fields_and_values,
        components.key_value,
        host.public_key(),
        components.mutation_type,
    );
    mutation.source_file_name = components.source_file_name;
    if components.durability.waits_for_persist() {
        mutation.synchronous = Some(true);
    }
    let mutation = match components.expected {
        Some(expectation) => mutation.with_expected(expectation),
        None => mutation,
    };
    match components.must_exist {
        Some(flag) => mutation.with_must_exist(flag),
        None => mutation,
    }
}

/// Admit the whole batch on the lane its combined payload size implies and
/// write it, mapping a core failure to a [`HostError`].
async fn write_batch<H: HostNode>(
    host: &H,
    mutations: Vec<Mutation>,
    ctx: &AccessContext,
    payload_bytes: usize,
    requires_durable_cloud_capture: bool,
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
        .write_mutations_with_access_receipt_cloud(
            mutations,
            ctx,
            if requires_durable_cloud_capture {
                CloudCapturePolicy::Durable
            } else {
                CloudCapturePolicy::Async
            },
        )
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
