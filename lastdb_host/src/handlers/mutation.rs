//! Mutation, aggregate and batch-mutation handlers.

use super::*;

mod fields;
pub use fields::*;
mod cloud;
pub(in crate::handlers) use cloud::*;
mod aggregate;
pub use aggregate::*;
mod batch;
pub use batch::*;

// ---------------------------------------------------------------------------
// Mutation (POST /api/mutation)
// ---------------------------------------------------------------------------

/// Bundled, already-parsed components of an `Operation::Mutation`. The caller
/// (each socket executor) parses the wire body into this shape; the shared
/// handler owns schema resolution + the write.
pub struct MutationComponents {
    pub schema: String,
    pub fields_and_values: HashMap<String, Value>,
    pub key_value: KeyValue,
    pub mutation_type: MutationType,
    pub expected: Option<fold_db::schema::types::cas::CasExpectation>,
    pub convergence: MutationConvergence,
    pub durability: MutationDurability,
    pub cloud_publication: Option<MutationCloudPublication>,
    pub must_exist: Option<bool>,
    /// Delete all live rows under `key_value.hash` with this range prefix.
    pub key_range_prefix: Option<String>,
    /// Concrete replacement for this source row's aggregate member. The
    /// single-mutation path uses the atomic aggregate commit primitive.
    pub aggregate_set: Option<AggregateSet>,
}

/// Maximum off-box publication wait after a durable Delete commits locally.
///
/// The Mini route budget adds this interval to its ordinary local-work budget,
/// so a slow but in-budget local commit cannot consume the publication wait.
pub const MUTATION_CLOUD_PUBLICATION_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(30);

/// Execute `POST /api/mutation`: resolve the schema, build the [`Mutation`]
/// signed by the node's public key, and write it under the given access context
/// (the I2 namespace write-guard runs inside `write_mutations_with_access`).
/// Returns the `{ mutation_id, success }` payload. Single copy of both hosts'
/// mutation route.
///
/// # Errors
/// [`HostError`] `404` when the schema is unknown, `500` on a write failure.
pub async fn execute_mutation<H: HostNode>(
    host: &H,
    components: MutationComponents,
    ctx: &AccessContext,
) -> Result<Value, HostError> {
    let MutationComponents {
        schema,
        fields_and_values,
        key_value,
        mutation_type,
        expected,
        convergence,
        durability,
        cloud_publication,
        must_exist,
        key_range_prefix,
        aggregate_set,
    } = components;

    // Phase timers are always-on: each is one pair of monotonic clock reads
    // (sub-microsecond) around work that is µs-to-ms scale, reported through
    // the task-local accumulator (`fold_db::request_phases`) so no signature
    // here changes. Outside an instrumented socket request the adds no-op.
    let resolve_started = std::time::Instant::now();
    let resolved = (|| {
        let canonical = resolve_required_schema_name(host, &schema)?;
        let aggregate_set = aggregate_set
            .map(|aggregate| resolve_aggregate_set_schema_names(host, aggregate))
            .transpose()?;
        Ok::<_, HostError>((canonical, aggregate_set))
    })();
    request_phases::add_phase(RequestPhase::SchemaResolve, resolve_started.elapsed());
    let (canonical, aggregate_set) = resolved?;
    reject_illegal_must_exist(mutation_type, must_exist)?;
    let cloud_policy =
        cloud_capture_policy(mutation_type, durability, cloud_publication, must_exist)?;
    if let Some(prefix) = key_range_prefix.as_deref() {
        validate_key_range_prefix(
            prefix,
            mutation_type,
            &key_value,
            cloud_publication.is_some(),
            aggregate_set.is_some(),
        )?;
    }
    let validate_started = std::time::Instant::now();
    validate_mutation_fields(host, &schema, &canonical, &fields_and_values).await?;
    request_phases::add_phase(RequestPhase::Validate, validate_started.elapsed());

    // Classify + admit BEFORE the write so a large blob write queues on the bulk
    // lane instead of thrashing the shared flush log / blocking pool ahead of
    // interactive writes. Held across the whole write.
    let payload_bytes = fields_payload_bytes(&fields_and_values);

    let key_values = match key_range_prefix {
        Some(prefix) => {
            resolve_delete_prefix_keys(
                host,
                canonical.clone(),
                key_value.hash.clone().expect("validated hash"),
                prefix,
                ctx,
            )
            .await?
        }
        None => vec![key_value],
    };
    if key_values.is_empty() {
        return Ok(empty_delete_response());
    }
    let template = MutationTemplate {
        canonical: &canonical,
        fields_and_values: &fields_and_values,
        mutation_type,
        durability,
        expected: &expected,
        must_exist,
        aggregate_set: &aggregate_set,
    };
    let mutations = build_mutations(host, key_values, &template);

    let (receipt, aggregate_write) =
        write_mutation_receipt(host, mutations, ctx, cloud_policy, payload_bytes).await?;
    let mutation_id = if aggregate_write {
        source_mutation_id(&receipt)?
    } else {
        receipt
            .mutation_ids
            .last()
            .cloned()
            .ok_or_else(|| HostError::internal("Mutation returned no IDs"))?
    };
    // Default convergence is Async: return without waiting on the global
    // pending-task tracker. The "index_wait" phase only runs for Sync, and
    // times this barrier — not off-thread index/embedding work (that is
    // spawned and never blocks the request even under Sync).
    let background_tasks_drained = wait_for_requested_convergence(host, convergence).await;

    let mut response = serde_json::json!({
        "mutation_id": mutation_id,
        "success": true,
        "background_tasks_drained": background_tasks_drained,
        "convergence_pending": !background_tasks_drained,
        "revision": receipt.revision,
        "operations": operations_json(&receipt.operations),
        "durability": receipt.durability.as_str(),
        "resident_stages_us": stages_json(&receipt.stages),
        "touched_group_ids": receipt.touched_group_ids,
        // The fixed-size logical accounting of the value this write just
        // attributed: the same payload-bytes measure QoS lane admission
        // already computed, returned inline so a caller (or the attribution
        // proof) has the write's size before it does a second read.
        "size": payload_bytes
    });
    attach_cloud_receipt(&mut response, &mutation_id, receipt.cloud.as_ref());
    Ok(response)
}

/// Reject a `key_range_prefix` delete that combines unsupported fields.
fn validate_key_range_prefix(
    prefix: &str,
    mutation_type: MutationType,
    key_value: &KeyValue,
    has_cloud_publication: bool,
    has_aggregate_set: bool,
) -> Result<(), HostError> {
    if !matches!(mutation_type, MutationType::Delete) {
        return Err(HostError::new(
            400,
            "key_range_prefix is only valid on delete",
        ));
    }
    if prefix.is_empty() {
        return Err(HostError::new(400, "key_range_prefix must not be empty"));
    }
    if key_value.hash.as_deref().is_none_or(str::is_empty) {
        return Err(HostError::new(
            400,
            "key_range_prefix requires key_value.hash",
        ));
    }
    if key_value.range.is_some() {
        return Err(HostError::new(
            400,
            "key_range_prefix cannot be combined with key_value.range",
        ));
    }
    if has_cloud_publication {
        return Err(HostError::new(
            400,
            "key_range_prefix does not support cloud_publication",
        ));
    }
    if has_aggregate_set {
        return Err(HostError::new(
            400,
            "key_range_prefix does not support aggregate_set",
        ));
    }
    Ok(())
}

/// Response for a prefix delete that matched no rows.
fn empty_delete_response() -> Value {
    let operations = ResidentCommitOperations::default();
    let stages = ResidentCommitStages::default();
    serde_json::json!({
        "mutation_id": "",
        "success": true,
        "background_tasks_drained": true,
        "convergence_pending": false,
        "revision": Value::Null,
        "operations": operations_json(&operations),
        "durability": ResidentDurability::Queued.as_str(),
        "resident_stages_us": stages_json(&stages),
        "touched_group_ids": [],
        "size": 0
    })
}

/// The request fields shared by every mutation a single request builds.
struct MutationTemplate<'a> {
    canonical: &'a str,
    fields_and_values: &'a HashMap<String, Value>,
    mutation_type: MutationType,
    durability: MutationDurability,
    expected: &'a Option<fold_db::schema::types::cas::CasExpectation>,
    must_exist: Option<bool>,
    aggregate_set: &'a Option<AggregateSet>,
}

/// Build one signed [`Mutation`] per key from the shared template.
fn build_mutations<H: HostNode>(
    host: &H,
    key_values: Vec<KeyValue>,
    tpl: &MutationTemplate<'_>,
) -> Vec<Mutation> {
    key_values
        .into_iter()
        .map(|key_value| {
            let mut mutation = Mutation::new(
                tpl.canonical.to_string(),
                tpl.fields_and_values.clone(),
                key_value,
                host.public_key(),
                tpl.mutation_type,
            );
            if tpl.durability.waits_for_persist() {
                mutation.synchronous = Some(true);
            }
            let mutation = match tpl.expected.clone() {
                Some(expectation) => mutation.with_expected(expectation),
                None => mutation,
            };
            let mutation = match tpl.must_exist {
                Some(flag) => mutation.with_must_exist(flag),
                None => mutation,
            };
            match tpl.aggregate_set.clone() {
                Some(aggregate) => mutation.with_aggregate_set(aggregate),
                None => mutation,
            }
        })
        .collect::<Vec<_>>()
}

/// Admit and write the mutations, mapping a core failure to a [`HostError`].
/// Returns the receipt and whether it came from the aggregate commit path.
async fn write_mutation_receipt<H: HostNode>(
    host: &H,
    mutations: Vec<Mutation>,
    ctx: &AccessContext,
    cloud_policy: CloudCapturePolicy,
    payload_bytes: usize,
) -> Result<(ResidentCommitReceipt, bool), HostError> {
    // The QoS admission wait was previously invisible — charged as handler
    // work in `duration_ms` with nothing to distinguish "queued behind the
    // write governor" from "the write was slow". Time it as its own bucket.
    let admission_started = std::time::Instant::now();
    let permit = host
        .acquire_op_permit(Lane::for_write_bytes(payload_bytes))
        .await;
    request_phases::add_phase(RequestPhase::AdmissionWait, admission_started.elapsed());
    let _permit = permit?;
    let manager = host.fold_db().mutation_manager();
    let aggregate_write = mutations.len() == 1 && mutations[0].aggregate_set.is_some();
    let receipt = if aggregate_write {
        manager
            .write_aggregate_set_with_access_receipt_cloud(
                mutations
                    .into_iter()
                    .next()
                    .expect("one aggregate mutation"),
                ctx,
                cloud_policy,
            )
            .await
    } else {
        manager
            .write_mutations_with_access_receipt_cloud(mutations, ctx, cloud_policy)
            .await
    }
    .map_err(|e| {
        // Prefer a typed mapping when available; keep the historical
        // "Mutation execution failed:" prefix only for true 500s so
        // owner diagnostics stay readable.
        let mapped = HostError::from(e);
        if mapped.status == 500 {
            HostError::internal(format!("Mutation execution failed: {}", mapped.message))
        } else {
            mapped
        }
    })?;

    let receipt = if aggregate_write {
        require_durable_aggregate_receipt(receipt, "Aggregate set")?
    } else {
        receipt
    };
    Ok((receipt, aggregate_write))
}
