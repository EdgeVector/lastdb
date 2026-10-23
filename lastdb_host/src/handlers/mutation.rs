//! Mutation, aggregate and batch-mutation handlers.
//!
//! `request` shows the single-request steps, and `plan` holds pure request
//! rules. The write helpers here use the existing host interface directly.

use std::future::Future;
use std::pin::Pin;

use fold_db::fold_db_core::mutation_manager::MutationManager;

use self::plan::MutationPlan;
use super::*;

mod plan;
mod request;
pub use request::execute_mutation;

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
    pub source_file_name: Option<String>,
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

/// Build the `{ mutation_id, success, … }` payload of a committed mutation.
fn mutation_response(
    mutation_id: &str,
    background_tasks_drained: bool,
    receipt: &ResidentCommitReceipt,
    payload_bytes: usize,
) -> Value {
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
    attach_cloud_receipt(&mut response, mutation_id, receipt.cloud.as_ref());
    response
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

/// Build one mutation identity per selected key. UUID allocation stays
/// outside the pure request policy.
fn build_mutations<H: HostNode>(
    host: &H,
    key_values: Vec<KeyValue>,
    plan: &MutationPlan,
) -> Vec<Mutation> {
    let components = &plan.components;
    key_values
        .into_iter()
        .map(|key_value| {
            let mut mutation = Mutation::new(
                plan.canonical.clone(),
                components.fields_and_values.clone(),
                key_value,
                host.public_key(),
                components.mutation_type,
            );
            mutation
                .source_file_name
                .clone_from(&components.source_file_name);
            if components.durability.waits_for_persist() {
                mutation.synchronous = Some(true);
            }
            let mutation = match components.expected.clone() {
                Some(expectation) => mutation.with_expected(expectation),
                None => mutation,
            };
            let mutation = match components.must_exist {
                Some(flag) => mutation.with_must_exist(flag),
                None => mutation,
            };
            match components.aggregate_set.clone() {
                Some(aggregate) => mutation.with_aggregate_set(aggregate),
                None => mutation,
            }
        })
        .collect()
}

/// Build, admit, and commit the selected mutations under the caller's access
/// context. Return the receipt and whether it came from the aggregate path.
async fn commit_mutation<H: HostNode>(
    host: &H,
    plan: &MutationPlan,
    key_values: Vec<KeyValue>,
    ctx: &AccessContext,
    payload_bytes: usize,
) -> Result<(ResidentCommitReceipt, bool), HostError> {
    let mutations = build_mutations(host, key_values, plan);
    let admission_started = std::time::Instant::now();
    let permit = host
        .acquire_op_permit(Lane::for_write_bytes(payload_bytes))
        .await;
    request_phases::add_phase(RequestPhase::AdmissionWait, admission_started.elapsed());
    let _permit = permit?;
    let manager = host.fold_db().mutation_manager();
    let aggregate_write = mutations.len() == 1 && mutations[0].aggregate_set.is_some();
    let receipt = start_core_commit(manager, mutations, ctx, plan.cloud_policy, aggregate_write)
        .await
        .map_err(|e| {
            // Preserve typed status codes and the historical prefix for true 500s.
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

type CoreCommitFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ResidentCommitReceipt, SchemaError>> + Send + 'a>>;

/// Construct the large write future in a separate frame, as the socket route
/// dispatcher does. That frame returns before the request polls the write,
/// so debug builds do not keep its construction stack through the whole commit.
#[inline(never)]
fn start_core_commit<'a>(
    manager: &'a MutationManager,
    mutations: Vec<Mutation>,
    ctx: &'a AccessContext,
    cloud_policy: CloudCapturePolicy,
    aggregate_write: bool,
) -> CoreCommitFuture<'a> {
    if aggregate_write {
        Box::pin(
            manager.write_aggregate_set_with_access_receipt_cloud(
                mutations
                    .into_iter()
                    .next()
                    .expect("one aggregate mutation"),
                ctx,
                cloud_policy,
            ),
        )
    } else {
        Box::pin(manager.write_mutations_with_access_receipt_cloud(mutations, ctx, cloud_policy))
    }
}
