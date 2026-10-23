//! Mutation, aggregate and batch-mutation handlers.
//!
//! The single-request path has three parts: `request` coordinates the steps,
//! `plan` holds pure request rules, and `services` connects those steps to the
//! host. The coordinator receives only its operation-specific services.

use super::*;

mod plan;
mod request;
mod services;

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
    let services = services::HostMutationServices { host, ctx };
    request::execute(&services, components).await
}

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
