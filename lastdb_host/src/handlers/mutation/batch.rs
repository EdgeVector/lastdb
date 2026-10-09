//! Batch mutation handler, response renderers and convergence wait.

use super::*;

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
/// [`HostError`] `400` for an empty batch, `404` when any schema is unknown,
/// `500` on a write failure.
pub async fn execute_mutations_batch<H: HostNode>(
    host: &H,
    components: Vec<MutationComponents>,
    ctx: &AccessContext,
) -> Result<Value, HostError> {
    if components.is_empty() {
        return Err(HostError::new(
            400,
            "Mutation batch must contain at least one mutation",
        ));
    }

    let wait_for_convergence = components
        .iter()
        .any(|component| component.convergence.waits());
    let mut requires_durable_cloud_capture = false;
    let mut mutations = Vec::with_capacity(components.len());
    let mut payload_bytes = 0usize;
    for components in components {
        let item = prepare_batch_item(host, components).await?;
        requires_durable_cloud_capture |= item.durable_delete;
        payload_bytes = payload_bytes.saturating_add(item.payload_bytes);
        mutations.push(item.mutation);
    }

    let count = mutations.len();
    // Resident receipts omit erasure UUIDs. Keep the original request IDs for
    // the cloud-capture receipt so a mixed durable-delete batch names every
    // envelope, independent of which operation appears first.
    let cloud_mutation_ids = if requires_durable_cloud_capture {
        mutations
            .iter()
            .map(|mutation| mutation.uuid.clone())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let receipt = write_batch(
        host,
        mutations,
        ctx,
        payload_bytes,
        requires_durable_cloud_capture,
    )
    .await?;
    let convergence = if wait_for_convergence {
        MutationConvergence::Sync
    } else {
        MutationConvergence::Async
    };
    let background_tasks_drained = wait_for_requested_convergence(host, convergence).await;

    let mut response = serde_json::json!({
        "mutation_ids": &receipt.mutation_ids,
        "count": count,
        "background_tasks_drained": background_tasks_drained,
        "convergence_pending": !background_tasks_drained,
        // One committed revision for every row in the batch — the primary
        // record and each exact projection — so the caller can read back
        // without a post-write verification round trip.
        "revision": receipt.revision,
        "operations": operations_json(&receipt.operations),
        "durability": receipt.durability.as_str(),
        "resident_stages_us": stages_json(&receipt.stages),
        "touched_group_ids": &receipt.touched_group_ids
    });
    if requires_durable_cloud_capture {
        attach_batch_cloud_capture(&mut response, &cloud_mutation_ids, receipt.cloud.as_ref());
    }
    Ok(response)
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

/// One validated batch item, ready to write.
struct PreparedBatchItem {
    mutation: Mutation,
    payload_bytes: usize,
    durable_delete: bool,
}

/// Validate one batch item (batch-unsupported fields, schema, field values)
/// and build its signed [`Mutation`].
async fn prepare_batch_item<H: HostNode>(
    host: &H,
    components: MutationComponents,
) -> Result<PreparedBatchItem, HostError> {
    let MutationComponents {
        schema,
        fields_and_values,
        key_value,
        mutation_type,
        expected,
        convergence: _,
        durability,
        cloud_publication,
        must_exist,
        key_range_prefix,
        aggregate_set,
    } = components;

    if key_range_prefix.is_some() {
        return Err(HostError::new(
            400,
            "key_range_prefix is only supported by /api/mutation",
        ));
    }

    if cloud_publication.is_some() {
        return Err(HostError::new(
            400,
            "cloud_publication is only supported by /api/mutation",
        ));
    }
    if aggregate_set.is_some() {
        return Err(HostError::new(
            400,
            "aggregate_set requires the single mutation route".to_string(),
        ));
    }
    let durable_delete = is_durable_delete(mutation_type, durability);

    // Per-item accumulation: `add_phase` sums into the request's totals,
    // so a batch reports the SUM of its items' resolve/validate time
    // rather than a point sample of whichever item ran last.
    let resolve_started = std::time::Instant::now();
    let resolved = resolve_schema_name(host, &schema)?;
    request_phases::add_phase(RequestPhase::SchemaResolve, resolve_started.elapsed());
    let Some(canonical) = resolved else {
        return Err(HostError::new(404, format!("Schema not found: {schema}")));
    };
    reject_illegal_must_exist(mutation_type, must_exist)?;
    let validate_started = std::time::Instant::now();
    validate_mutation_fields(host, &schema, &canonical, &fields_and_values).await?;
    request_phases::add_phase(RequestPhase::Validate, validate_started.elapsed());

    let payload_bytes = fields_payload_bytes(&fields_and_values);
    let mut mutation = Mutation::new(
        canonical,
        fields_and_values,
        key_value,
        host.public_key(),
        mutation_type,
    );
    if durability.waits_for_persist() {
        mutation.synchronous = Some(true);
    }
    let mutation = match expected {
        Some(expectation) => mutation.with_expected(expectation),
        None => mutation,
    };
    let mutation = match must_exist {
        Some(flag) => mutation.with_must_exist(flag),
        None => mutation,
    };
    Ok(PreparedBatchItem {
        mutation,
        payload_bytes,
        durable_delete,
    })
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
